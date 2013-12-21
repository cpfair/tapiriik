from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.pwx import PWXIO
from lxml import etree

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import dateutil.parser
import requests
import logging
import re

logger = logging.getLogger(__name__)

class TrainingPeaksService(ServiceBase):
    ID = "trainingpeaks"
    DisplayName = "TrainingPeaks"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    ReceivesStationaryActivities = False

    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = True

    # Not-so-coincidentally, similar to PWX.
    _workoutTypeMappings = {
        "Bike": ActivityType.Cycling,
        "Run": ActivityType.Running,
        "Walk": ActivityType.Walking,
        "Swim": ActivityType.Swimming,
        "MTB": ActivityType.MountainBiking,
        "XC-Ski": ActivityType.CrossCountrySkiing,
        "Rowing": ActivityType.Rowing,
        "X-Train": ActivityType.Other,
        "Strength": ActivityType.Other,
        "Race": ActivityType.Other,
        "Custom": ActivityType.Other,
        "Other": ActivityType.Other,
    }
    SupportedActivities = list(_workoutTypeMappings.values())

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": self.ID})

    def _authData(self, serviceRecord):
        from tapiriik.auth.credential_storage import CredentialStore
        password = CredentialStore.Decrypt(serviceRecord.ExtendedAuthorization["Password"])
        username = CredentialStore.Decrypt(serviceRecord.ExtendedAuthorization["Username"])
        return {"username": username, "password": password}

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        resp = requests.post("https://www.trainingpeaks.com/tpwebservices/service.asmx/AuthenticateAccount", data={"username":email, "password": password})
        if resp.status_code != 200:
            raise APIException("Invalid login")
        sess_guid = etree.XML(resp.content).text
        cookies = {"mySession_Production": sess_guid}
        resp = requests.get("https://www.trainingpeaks.com/m/Shared/PersonInfo.js", cookies=cookies)
        accountIsPremium = re.search("currentAthlete\.IsBasicUser\s*=\s*(true|false);", resp.text).group(1) == "false"
        personId = re.search("currentAthlete\.PersonId\s*=\s*(\d+);", resp.text).group(1)
        # Yes, I have it on good authority that this is checked further on on the remote end.
        if not accountIsPremium:
            raise APIException("Account not premium", block=True, user_exception=UserException(UserExceptionType.AccountUnpaid, intervention_required=True, extra=personId))
        return (personId, {}, {"Username": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def RevokeAuthorization(self, serviceRecord):
        pass  # No auth tokens to revoke...

    def DeleteCachedData(self, serviceRecord):
        pass  # No cached data...

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        ns = {
            "tpw": "http://www.trainingpeaks.com/TPWebServices/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"
            }
        activities = []
        exclusions = []

        reqData = self._authData(svcRecord)

        limitDateFormat = "%d %B %Y"

        if exhaustive:
            listEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = datetime(day=1, month=1, year=1980) # The beginning of time
        else:
            listEnd = datetime.now() + timedelta(days=1.5) # Who knows which TZ it's in
            listStart = listEnd - timedelta(days=20) # Doesn't really matter

        lastActivityDay = None
        discoveredWorkoutIds = []
        while True:
            reqData.update({"startDate": listStart.strftime(limitDateFormat), "endDate": listEnd.strftime(limitDateFormat)})
            print("Requesting %s to %s" % (listStart, listEnd))
            resp = requests.post("https://www.trainingpeaks.com/tpwebservices/service.asmx/GetWorkoutsForAthlete", data=reqData)
            xresp = etree.XML(resp.content)
            for xworkout in xresp:
                activity = UploadedActivity()

                workoutId = xworkout.find("tpw:WorkoutId", namespaces=ns).text

                workoutDayEl = xworkout.find("tpw:WorkoutDay", namespaces=ns)
                startTimeEl = xworkout.find("tpw:StartTime", namespaces=ns)

                workoutDay = dateutil.parser.parse(workoutDayEl.text)
                startTime = dateutil.parser.parse(startTimeEl.text) if startTimeEl is not None and startTimeEl.text else None

                if lastActivityDay is None or workoutDay.replace(tzinfo=None) > lastActivityDay:
                    lastActivityDay = workoutDay.replace(tzinfo=None)

                if startTime is None:
                    continue # Planned but not executed yet.
                activity.StartTime = startTime

                endTimeEl = xworkout.find("tpw:TimeTotalInSeconds", namespaces=ns)
                if not endTimeEl.text:
                    exclusions.append(APIExcludeActivity("Activity has no duration", activityId=workoutId))
                    continue

                activity.EndTime = activity.StartTime + timedelta(seconds=float(endTimeEl.text))

                distEl = xworkout.find("tpw:DistanceInMeters", namespaces=ns)
                if distEl.text:
                    activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=float(distEl.text))
                # PWX is damn near comprehensive, no need to fill in any of the other statisitcs here, really

                if workoutId in discoveredWorkoutIds:
                    continue # There's the possibility of query overlap, if there are multiple activities on a single day that fall across the query return limit
                discoveredWorkoutIds.append(workoutId)

                workoutTypeEl = xworkout.find("tpw:WorkoutTypeDescription", namespaces=ns)
                if workoutTypeEl.text:
                    if workoutTypeEl.text == "Day Off":
                        continue # TrainingPeaks has some weird activity types...
                    if workoutTypeEl.text not in self._workoutTypeMappings:
                        exclusions.append(APIExcludeActivity("Activity type %s unknown" % workoutTypeEl.text, activityId=workoutId))
                        continue
                    activity.Type = self._workoutTypeMappings[workoutTypeEl.text]

                activity.ServiceData = {"WorkoutID": workoutId}
                activity.CalculateUID()
                activities.append(activity)

            if not exhaustive:
                break

            # Since TP only lets us query by date range, to get full activity history we need to query successively smaller ranges
            if len(xresp):
                if listStart == lastActivityDay:
                    break # This wouldn't work if you had more than #MaxQueryReturn activities on that day - but that number is probably 50+
                listStart = lastActivityDay
            else:
                break # We're done

        return activities, exclusions

    def DownloadActivity(self, svcRecord, activity):
        params = self._authData(svcRecord)
        params.update({"workoutIds": activity.ServiceData["WorkoutID"], "personId": svcRecord.ExternalID})
        resp = requests.get("https://www.trainingpeaks.com/tpwebservices/service.asmx/GetExtendedWorkoutsForAccessibleAthlete", params=params)
        activity = PWXIO.Parse(resp.content, activity)
        return activity

    def UploadActivity(self, svcRecord, activity):
        pwxdata = PWXIO.Dump(activity)
        params = self._authData(svcRecord)
        resp = requests.post("https://www.trainingpeaks.com/TPWebServices/EasyFileUpload.ashx", params=params, data=pwxdata)
        if resp.text != "OK":
            raise APIException("Unable to upload activity response " + resp.text + " status " + str(resp.status_code))
