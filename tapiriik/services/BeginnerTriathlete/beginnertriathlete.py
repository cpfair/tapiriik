from tapiriik.settings import WEB_ROOT, BT_APIKEY
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.tcx import TCXIO
from tapiriik.services.gpx import GPXIO
from tapiriik.services.fit import FITIO
from tapiriik.services.sessioncache import SessionCache
from urllib.parse import urlparse
import pytz

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import dateutil.parser
import requests
import logging
import os

logger = logging.getLogger(__name__)

class _DeviceFileTypes:
    FIT = ".fit"
    TCX = ".tcx"
    GPX = ".gpx"

#
# BeginnerTriathlete upload/download synchronization support
#
# - API key is specified in /local_settings.py
# - Authorization exchanges username and password for a token, which is included on all authenticated requests.
# - DownloadActivityList makes requests via the WebAPI
# - Detailed device data, if present, is acquired in DownloadActivity. Otherwise does nothing.
# - Uploads use the FIT helper and the device upload endpoint
class BeginnerTriathleteService(ServiceBase):

    # TODO: BT has a kcal expenditure calculation, it just isn't being reported. Supply that for a future update..
    # TODO: Implement laps on manual entries
    # TODO: BT supports activities other than the standard swim/bike/run, but a different interface is regrettably used

    ID = "beginnertriathlete"
    DisplayName = "BeginnerTriathlete"
    DisplayAbbreviation = "BT"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    ReceivesStationaryActivities = True
    SupportsHR = True
    SupportsActivityDeletion = True

    # Don't need to cache user settings for long, it is a quick lookup But if a user changes their timezone
    # or privacy settings, let's catch it *relatively* quick. Five minutes seems good.
    _sessionCache = SessionCache("beginnertriathlete", lifetime=timedelta(minutes=5), freshen_on_get=False)

    # Private fields
    _urlRoot = "https://beginnertriathlete.com/WebAPI/api/"
    #_urlRoot = "http://192.168.1.13:53367/api/"
    _loginUrlRoot = _urlRoot + "login/"
    _sbrEventsUrlRoot = _urlRoot + "sbreventsummary/"
    _sbrEventDeleteUrlRoot = _urlRoot + "deletesbrevent/"
    _deviceUploadUrl = _urlRoot + "deviceupload/"
    _accountSettingsUrl = _urlRoot + "GeneralSettings/"
    _accountProfileUrl = _urlRoot + "profilesettings/"
    _accountInformationUrl = _urlRoot + "accountinformation/"
    _viewEntryUrl = "https://beginnertriathlete.com/discussion/training/view-event.asp?id="
    _dateFormat = "{d.month}/{d.day}/{d.year}"
    _serverDefaultTimezone = "US/Central"
    _workoutTypeMappings = {
        "3": ActivityType.Swimming,
        "1": ActivityType.Cycling,
        "2": ActivityType.Running
    }
    _mimeTypeMappings = {
        "application/gpx+xml": _DeviceFileTypes.GPX,
        "application/vnd.garmin.tcx+xml": _DeviceFileTypes.TCX,
        "application/vnd.ant.fit": _DeviceFileTypes.FIT
    }
    _fileExtensionMappings = {
        ".gpx": _DeviceFileTypes.GPX,
        ".tcx": _DeviceFileTypes.TCX,
        ".fit": _DeviceFileTypes.FIT
    }
    SupportedActivities = [
        ActivityType.Running,
        ActivityType.Cycling,
        ActivityType.MountainBiking,
        ActivityType.Walking,
        ActivityType.Swimming]

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    # Exchange username & password for a UserToken and store it in ExtendedAuthorization if the user has elected to
    # remember login details.
    def Authorize(self, username, password):
        session = self._prepare_request()
        requestParameters = {"username": username,  "password": password}
        user_resp = session.get(self._loginUrlRoot, params=requestParameters)

        if user_resp.status_code != 200:
            raise APIException("Login error")

        response = user_resp.json()

        if response["LoginResponseCode"] == 3:
            from tapiriik.auth.credential_storage import CredentialStore
            member_id = int(response["MemberId"])
            token = response["UserToken"]
            return member_id, {}, {"UserToken": CredentialStore.Encrypt(token)}

        if response["LoginResponseCode"] == 0:
            raise APIException("Invalid API key")

        # Incorrect username or password
        if response["LoginResponseCode"] == -3 or response["LoginResponseCode"] == -2 or response["LoginResponseCode"] == -1:
            raise APIException(
                "Invalid login - Bad username or password",
                block=True,
                user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        # Account is inactive or locked out - Rarely would happen
        if response["LoginResponseCode"] == 1 or response["LoginResponseCode"] == 2:
            raise APIException(
                "Invalid login - Account is inactive",
                block=True,
                user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

        # Something extra unusual has happened
        raise APIException(
            "Invalid login - Unknown error",
            block=True,
            user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

    # Get an activity summary over a date range
    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        activities = []
        if exhaustive:
            listEnd = datetime.now().date() + timedelta(days=1.5)
            firstEntry = self._getFirstTrainingEntryForMember(self._getUserToken(serviceRecord))
            listStart = dateutil.parser.parse(firstEntry).date()
        else:
            listEnd = datetime.now() + timedelta(days=1.5)
            listStart = listEnd - timedelta(days=60)

        # Set headers necessary for a successful API request
        session = self._prepare_request(self._getUserToken(serviceRecord))
        settings = self._getUserSettings(serviceRecord)

        # Iterate through the date range 60 days at a time. Dates are inclusive for all events on that date,
        # and do not contain timestamps. 5/1/20xx through 5/2/20xx would include all events 5/1 => 5/2 11:59:59PM
        while listStart < listEnd:
            pageDate = listStart + timedelta(days=59)
            if pageDate > listEnd:
                pageDate = listEnd

            print("Requesting %s to %s" % (listStart, pageDate))

            # Request their actual logged data. Not their workout plan. Start and end date are inclusive and
            # the end date includes everything up until midnight, that day
            # Member ID can be sent as zero because we are retrieving our token's data, not someone else's. We
            # could store & supply the user's member id, but it would gain nothing
            requestParameters = {
                "startDate": self._dateFormat.format(d=listStart),
                "endDate": self._dateFormat.format(d=pageDate),
                "planned": "false",
                "memberid": "0"}
            workouts_resp = session.get(self._sbrEventsUrlRoot, params=requestParameters)

            if workouts_resp.status_code != 200:
                if workouts_resp.status_code == 401:
                    # After login, the API does not differentiate between an unauthorized token and an invalid API key
                    raise APIException(
                        "Invalid login or API key",
                        block=True,
                        user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

                # Some other kind of error has occurred. It could be a server error.
                raise APIException("Workout listing error")

            workouts = workouts_resp.json()
            for workout in workouts:
                activity = self._populate_sbr_activity(workout, settings)
                activities.append(activity)

            listStart = listStart + timedelta(days=60)

        return activities, []

    # Populate an activity with the information from BT's sbreventsummary endpoint. Contains basic data like
    # event type, duration, pace, HR, date and time. At the moment, manually entered laps are not reported.
    # Detailed activity data, laps, and GPS may be present in a .fit, .tcx, or .gpx file if the record came
    # from a device upload

    def _populate_sbr_activity(self, api_sbr_activity, usersettings):
        # Example JSON feed (unimportant fields have been removed)
        # [{
        #    "EventId": 63128401,                   #  Internal ID
        #    "EventType": 3,                        #  Swim (3), bike (1), or run (2)
        #    "EventDate": "4/22/2016",
        #    "EventTime": "7:44 AM",                #  User's time, time zone not specified
        #    "Planned": false,                      #  Training plan or actual data
        #    "TotalMinutes": 34.97,
        #    "TotalKilometers": 1.55448,
        #    "AverageHeartRate": 125,
        #    "MinimumHeartRate": 100,
        #    "MaximumHeartRate": 150,
        #    "MemberId": 999999,
        #    "MemberUsername": "Smith",
        #    "HasDeviceUpload": true,
        #    "DeviceUploadFile": "http://beginnertriathlete.com/discussion/storage/workouts/555555/abcd-123.fit",
        #    "RouteName": "",                       #  Might contain a description of the event
        #    "Comments": "",                        #  Same as above. Not overly often used.
        # }, ... ]

        activity = UploadedActivity()
        workout_id = api_sbr_activity["EventId"]
        eventType = api_sbr_activity["EventType"]
        eventDate = api_sbr_activity["EventDate"]
        eventTime = api_sbr_activity["EventTime"]
        totalMinutes = api_sbr_activity["TotalMinutes"]
        totalKms = api_sbr_activity["TotalKilometers"]
        averageHr = api_sbr_activity["AverageHeartRate"]
        minimumHr = api_sbr_activity["MinimumHeartRate"]
        maximumHr = api_sbr_activity["MaximumHeartRate"]
        deviceUploadFile = api_sbr_activity["DeviceUploadFile"]

        # Basic SBR data does not include GPS or sensor data. If this event originated from a device upload,
        # DownloadActivity will find it.
        activity.Stationary = True

        # Same as above- The data might be there, but it's not supplied in the basic activity feed.
        activity.GPS = False

        activity.Private = usersettings["Privacy"]
        activity.Type = self._workoutTypeMappings[str(eventType)]

        # Get the user's timezone from their profile. (Activity.TZ should be mentioned in the object hierarchy docs?)
        # Question: I believe if DownloadActivity finds device data, it will overwrite this. Which is OK with me.
        # The device data will most likely be more accurate.
        try:
            activity.TZ = pytz.timezone(usersettings["TimeZone"])
        except pytz.exceptions.UnknownTimeZoneError:
            activity.TZ = pytz.timezone(self._serverDefaultTimezone)

        # activity.StartTime and EndTime aren't mentioned in the object hierarchy docs, but I see them
        # set in all the other providers.
        activity.StartTime = dateutil.parser.parse(
            eventDate + " " + eventTime,
            dayfirst=False).replace(tzinfo=activity.TZ)
        activity.EndTime = activity.StartTime + timedelta(minutes=totalMinutes)

        # We can calculate some metrics from the supplied data. Would love to see some non-source code documentation
        # on each statistic and what it expects as input.
        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers,
                                                    value=totalKms)
        activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute,
                                              avg=float(averageHr),
                                              min=float(minimumHr),
                                              max=float(maximumHr))
        activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds,
                                                      value=float(totalMinutes * 60))
        activity.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Seconds,
                                                     value=float(totalMinutes * 60))
        # While BT does support laps, the current API doesn't report on them - a limitation that may need to be
        # corrected in a future update. For now, treat manual entries as a single lap. As more and more people upload
        # workouts using devices anyway, this probably matters much less than it once did.
        lap = Lap(stats=activity.Stats, startTime=activity.StartTime, endTime=activity.EndTime)
        activity.Laps = [lap]

        # Not 100% positive how this is utilized, but it is common for all providers. Detects duplicate downloads?
        activity.CalculateUID()

        # If a device file is attached, we'll get more details about this event in DownloadActivity
        activity.ServiceData = {
            "ID": int(workout_id),
            "DeviceUploadFile": deviceUploadFile
        }

        return activity

    def DownloadActivity(self, serviceRecord, activity):
        deviceUploadFile = activity.ServiceData.get("DeviceUploadFile")

        # No additional data about this event is available.
        if not deviceUploadFile:
            return activity

        logger.info("Downloading device file %s" % deviceUploadFile)
        session = self._prepare_request(self._getUserToken(serviceRecord))
        res = session.get(deviceUploadFile)

        if res.status_code == 200:
            try:
                contentType = self._mimeTypeMappings[res.headers["content-type"]]
                if not contentType:
                    remoteUrl = urlparse(deviceUploadFile).path
                    extension = os.path.splitext(remoteUrl)[1]
                    contentType = self._fileExtensionMappings[extension]

                if contentType:
                    if contentType == _DeviceFileTypes.FIT:
                        # Oh no! Not supported! So close ....
                        # FITIO.Parse(res.content, activity)
                        return activity
                    if contentType == _DeviceFileTypes.TCX:
                        TCXIO.Parse(res.content, activity)
                    if contentType == _DeviceFileTypes.GPX:
                        GPXIO.Parse(res.content, activity)
            except ValueError as e:
                raise APIExcludeActivity("Parse error " + deviceUploadFile + " " + str(e),
                                         user_exception=UserException(UserExceptionType.Corrupt),
                                         permanent=True)

        return activity

    def UploadActivity(self, serviceRecord, activity):
        # Upload the workout as a .FIT file
        session = self._prepare_request(self._getUserToken(serviceRecord))
        uploaddata = FITIO.Dump(activity)
        files = {"deviceFile": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".fit", uploaddata)}
        response = session.post(self._deviceUploadUrl, files=files)

        if response.status_code != 200:
            raise APIException(
                "Error uploading workout",
                block=False)

        responseJson = response.json()

        if not responseJson['Success']:
            raise APIException(
                "Error uploading workout - " + response.Message,
                block=False)

        # The upload didn't return a PK for some reason. The queue might be stuck or some other internal error
        # but that doesn't necessarily warrant a reupload.
        eventId = responseJson["EventId"]
        if eventId == 0:
            return None
        return eventId

    def RevokeAuthorization(self, serviceRecord):
        # nothing to do here...
        pass

    def UserUploadedActivityURL(self, uploadId):
        return self._viewEntryUrl + str(uploadId)

    def DeleteActivity(self, serviceRecord, uploadId):
        session = self._prepare_request(self._getUserToken(serviceRecord))
        requestParameters = {"id": uploadId}
        response = session.post(self._sbrEventDeleteUrlRoot, params=requestParameters)

        self._handleHttpErrorCodes(response)

        responseJson = response.json()

        if not responseJson:
            raise APIException(
                "Error deleting workout - " + uploadId,
                block=False)

    def DeleteCachedData(self, serviceRecord):
        # nothing to do here...
        pass

    # Sets the API key header necessary for all requests, and optionally the authentication token too.
    def _prepare_request(self, userToken=None):
        session = requests.Session()
        session.headers.update(self._set_request_api_headers())

        # If the serviceRecord was included, try to include the UserToken, authenticating the request
        # The service record will contain ExtendedAuthorization data if the user chose to remember login details.
        if userToken:
            session.headers.update(self._set_request_authentication_header(userToken))
        return session

    # The APIKey header is required for all requests. A key can be obtained by emailing support@beginnertriathlete.com.
    def _set_request_api_headers(self):
        return {"APIKey": BT_APIKEY}

    # Upon successful authentication by Authorize, the ExtendedAuthorization dict will have a UserToken
    def _set_request_authentication_header(self, userToken):
        return {"UserToken": userToken}

    def _getFirstTrainingEntryForMember(self, userToken):
        session = self._prepare_request(userToken)
        response = session.get(self._accountInformationUrl)
        self._handleHttpErrorCodes(response)

        try:
            responseJson = response.json()
            return responseJson['FirstTrainingLog']
        except ValueError as e:
            raise APIException("Parse error reading profile JSON " + str(e))

    def _getUserSettings(self, serviceRecord, skip_cache=False):
        cached = self._sessionCache.Get(serviceRecord.ExternalID)
        if cached and not skip_cache:
            return cached
        if serviceRecord:
            timeZone = self._getTimeZone(self._getUserToken(serviceRecord))
            privacy = self._getPrivacy(self._getUserToken(serviceRecord))
            cached = {
                "TimeZone": timeZone,
                "Privacy": privacy
            }
            self._sessionCache.Set(serviceRecord.ExternalID, cached)
        return cached

    def _getUserToken(self, serviceRecord):
        userToken = None
        if serviceRecord:
            from tapiriik.auth.credential_storage import CredentialStore
            userToken = CredentialStore.Decrypt(serviceRecord.ExtendedAuthorization["UserToken"])
        return userToken

    def _getTimeZone(self, token):
        session = self._prepare_request(token)
        response = session.get(self._accountSettingsUrl)
        self._handleHttpErrorCodes(response)

        try:
            # BT does not record whether the user observes DST and I am not even attempting to guess.
            responseJson = response.json()
            timezone = responseJson["UtcOffset"]
            if timezone == 0:
                timezoneStr = "Etc/GMT"
            elif timezone > 0:
                timezoneStr = "Etc/GMT+" + str(timezone)
            elif timezone < 0:
                timezoneStr = "Etc/GMT" + str(timezone)
            return timezoneStr
        except ValueError as e:
            raise APIException("Parse error reading profile JSON " + str(e))

    def _getPrivacy(self, token):
        session = self._prepare_request(token)
        response = session.get(self._accountProfileUrl)
        self._handleHttpErrorCodes(response)

        try:
            # public           - Everyone. Public
            # publicrestricted - Registered members. Public
            # friends          - BT friends only. Private
            # private          - Private
            responseJson = response.json()
            privacy = responseJson["TrainingPrivacy"]
            return not (privacy == "public" or privacy == "publicrestricted")
        except ValueError as e:
            raise APIException("Parse error reading privacy JSON " + str(e))

    def _handleHttpErrorCodes(self, response):
        if response.status_code != 200:
            if response.status_code == 401:
                # After login, the API does not differentiate between an unauthorized token and an invalid API key
                raise APIException(
                    "Invalid login or API key",
                    block=True,
                    user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            # Server error?
            raise APIException(
                "HTTP error " + str(response.status_code),
                block=True)
