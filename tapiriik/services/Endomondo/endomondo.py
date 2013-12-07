from tapiriik.settings import WEB_ROOT
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType
from tapiriik.services.sessioncache import SessionCache
from tapiriik.services.fit import FITIO

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
import requests
import pytz
import re
import zlib
import os
import logging
import pickle

logger = logging.getLogger(__name__)


class EndomondoService(ServiceBase):
    ID = "endomondo"
    DisplayName = "Endomondo"
    AuthenticationType = ServiceAuthenticationType.UsernamePassword
    RequiresExtendedAuthorizationDetails = True
    UserProfileURL = "http://www.endomondo.com/profile/{0}"
    UserActivityURL = "http://www.endomondo.com/workouts/{1}/{0}"

    _sessionCache = SessionCache(lifetime=timedelta(minutes=30), freshen_on_get=True)

    _activityMappings = {
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,  # the order of these matters since it picks the first match for uploads
        1:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        9:  ActivityType.Rowing,  # canoeing
        18: ActivityType.Walking,
        14: ActivityType.Walking,  # fitness walking
        16: ActivityType.Hiking,
        17: ActivityType.Hiking,  # orienteering
        20: ActivityType.Swimming,
        40: ActivityType.Swimming,  # scuba diving
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    _reverseActivityMappings = {  # so that ambiguous events get mapped back to reasonable types
        0:  ActivityType.Running,
        2:  ActivityType.Cycling,
        3:  ActivityType.MountainBiking,
        4:  ActivityType.Skating,
        6:  ActivityType.CrossCountrySkiing,
        7:  ActivityType.DownhillSkiing,
        8:  ActivityType.Snowboarding,
        11: ActivityType.Rowing,
        18: ActivityType.Walking,
        16: ActivityType.Hiking,
        20: ActivityType.Swimming,
        22: ActivityType.Other,
        92: ActivityType.Wheelchair
    }

    SupportedActivities = list(_activityMappings.values())
    SupportsHR = True
    SupportsCalories = False  # not inside the activity? p.sure it calculates this after the fact anyways

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("auth_simple", kwargs={"service": "endomondo"})

    def _parseKVP(self, data):
        out = {}
        for line in data.split("\n"):
            if line == "OK":
                continue
            match = re.match("(?P<key>[^=]+)=(?P<val>.+)$", line)
            if match is None:
                continue
            out[match.group("key")] = match.group("val")
        return out

    def _get_web_cookies(self, record=None, email=None, password=None):
        from tapiriik.auth.credential_storage import CredentialStore
        if record:
            cached = self._sessionCache.Get(record.ExternalID)
            if cached:
                return cached
            password = CredentialStore.Decrypt(record.ExtendedAuthorization["Password"])
            email = CredentialStore.Decrypt(record.ExtendedAuthorization["Email"])
        params = {"email": email, "password": password}
        resp = requests.post("https://www.endomondo.com/access?wicket:interface=:1:pageContainer:lowerSection:lowerMain:lowerMainContent:signInPanel:signInFormPanel:signInForm::IFormSubmitListener::", data=params, allow_redirects=False)
        if resp.status_code >= 500 and resp.status_code<600:
            raise APIException("Remote API failure")
        if resp.status_code != 302:  # yep
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        if record:
            self._sessionCache.Set(record.ExternalID, resp.cookies)
        return resp.cookies

    def Authorize(self, email, password):
        from tapiriik.auth.credential_storage import CredentialStore
        params = {"email": email, "password": password, "v": "2.4", "action": "pair", "deviceId": "TAP-SYNC-" + email.lower(), "country": "N/A"}  # note to future self: deviceId can't change intra-account otherwise we'll get different tokens back

        resp = requests.get("https://api.mobile.endomondo.com/mobile/auth", params=params)
        if resp.text.strip() == "USER_UNKNOWN" or resp.text.strip() == "USER_EXISTS_PASSWORD_WRONG":
            raise APIException("Invalid login", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        data = self._parseKVP(resp.text)

        return (data["userId"], {"AuthToken": data["authToken"], "SecureToken": data["secureToken"]}, {"Email": CredentialStore.Encrypt(email), "Password": CredentialStore.Encrypt(password)})

    def RevokeAuthorization(self, serviceRecord):
        #  you can't revoke the tokens endomondo distributes :\
        pass

    def _downloadRawTrackRecord(self, serviceRecord, trackId):
        params = {"authToken": serviceRecord.Authorization["AuthToken"], "trackId": trackId}
        response = requests.get("http://api.mobile.endomondo.com/mobile/readTrack", params=params)
        return response.text

    def _populateActivityFromTrackData(self, activity, recordText, minimumWaypoints=False):
        lap = Lap()
        activity.Laps = [lap]
        ###       1ST RECORD      ###
        # userID;
        # timestamp - create date?;
        # type? W=1st
        # User name;
        # activity name;
        # activity type;
        # another timestamp - start time of event?;
        # duration.00;
        # distance (km);
        # kcal;
        #;
        # max alt;
        # min alt;
        # max HR;
        # avg HR;

        ###     TRACK RECORDS     ###
        # timestamp;
        # type (2=start, 3=end, 0=pause, 1=resume);
        # latitude;
        # longitude;
        #;
        #;
        # alt;
        # hr;
        wptsWithLocation = False
        wptsWithNonZeroAltitude = False
        rows = recordText.split("\n")
        for row in rows:
            if row == "OK" or len(row) == 0:
                continue
            split = row.split(";")
            if split[2] == "W":
                # init record
                lap.Stats.TimerTime = ActivityStatistic(ActivityStatisticUnit.Time, value=timedelta(seconds=float(split[7])) if split[7] != "" else None)
                lap.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=float(split[8]) if split[8] != "" else None)
                lap.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=float(split[14]) if split[14] != "" else None, max=float(split[13]) if split[13] != "" else None)
                lap.Stats.Elevation = ActivityStatistic(ActivityStatisticUnit.Meters, min=float(split[12]) if split[12] != "" else None, max=float(split[11]) if split[11] != "" else None)
                lap.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=float(split[12]) if split[12] != "" else None)
                activity.Stats.update(lap.Stats)
                lap.Stats = activity.Stats
                activity.Name = split[4]
            else:
                wp = Waypoint()
                if split[1] == "2":
                    wp.Type = WaypointType.Start
                elif split[1] == "3":
                    wp.Type = WaypointType.End
                elif split[1] == "0":
                    wp.Type = WaypointType.Pause
                elif split[1] == "1":
                    wp.Type = WaypointType.Resume
                else:
                    wp.Type == WaypointType.Regular

                if split[0] == "":
                    continue  # no timestamp, for whatever reason
                wp.Timestamp = pytz.utc.localize(datetime.strptime(split[0], "%Y-%m-%d %H:%M:%S UTC"))  # it's like this as opposed to %z so I know when they change things (it'll break)
                if split[2] != "":
                    wp.Location = Location(float(split[2]), float(split[3]), None)
                    if wp.Location.Longitude > 180 or wp.Location.Latitude > 90 or wp.Location.Longitude < -180 or wp.Location.Latitude < -90:
                        raise APIExcludeActivity("Out of range lat/lng")
                    if wp.Location.Latitude is not None and wp.Location.Latitude is not None:
                        wptsWithLocation = True
                    if split[6] != "":
                        wp.Location.Altitude = float(split[6])  # why this is missing: who knows?
                        if wp.Location.Altitude != 0:
                            wptsWithNonZeroAltitude = True

                if split[7] != "":
                    wp.HR = float(split[7])
                lap.Waypoints.append(wp)
                if wptsWithLocation and minimumWaypoints:
                    break
        lap.Waypoints = sorted(activity.Waypoints, key=lambda v: v.Timestamp)
        if wptsWithLocation:
            activity.EnsureTZ(recalculate=True)
            if not wptsWithNonZeroAltitude:  # do this here so, should the activity run near sea level, altitude data won't be spotty
                for x in lap.Waypoints:  # clear waypoints of altitude data if all of them were logged at 0m (invalid)
                    if x.Location is not None:
                        x.Location.Altitude = None
        else:
            lap.Waypoints = []  # practically speaking

    def DownloadActivityList(self, serviceRecord, exhaustive=False):

        activities = []
        exclusions = []
        earliestDate = None
        earliestFirstPageDate = None
        paged = False

        while True:
            before = "" if earliestDate is None else earliestDate.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            params = {"authToken": serviceRecord.Authorization["AuthToken"], "maxResults": 45, "before": before}
            logger.debug("Req with " + str(params))
            response = requests.get("http://api.mobile.endomondo.com/mobile/api/workout/list", params=params)

            if response.status_code != 200:
                if response.status_code == 401 or response.status_code == 403:
                    raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
                raise APIException("Unable to retrieve activity list " + str(response))
            data = response.json()

            if "error" in data and data["error"]["type"] == "AUTH_FAILED":
                raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))

            track_ids = []
            this_page_activities = []
            for act in data["data"]:
                startTime = pytz.utc.localize(datetime.strptime(act["start_time"], "%Y-%m-%d %H:%M:%S UTC"))
                if earliestDate is None or startTime < earliestDate:  # probably redundant, I would assume it works out the TZes...
                    earliestDate = startTime
                logger.debug("activity pre")
                if "tracking" in act and act["tracking"]:
                    logger.warning("\t tracking")
                    exclusions.append(APIExcludeActivity("In progress", activityId=act["id"], permanent=False))
                    continue  # come back once they've completed the activity
                track_ids.append(act["id"])
                activity = UploadedActivity()
                activity.StartTime = startTime
                activity.EndTime = activity.StartTime + timedelta(0, round(act["duration_sec"]))
                logger.debug("\tActivity s/t " + str(activity.StartTime))

                activity.Stationary = not act["has_points"]

                if int(act["sport"]) in self._activityMappings:
                    activity.Type = self._activityMappings[int(act["sport"])]
                activity.ServiceData = {"ActivityID": act["id"]}

                this_page_activities.append(activity)
            cached_track_tzs = cachedb.endomondo_activity_cache.find({"TrackID":{"$in": track_ids}})
            cached_track_tzs = dict([(x["TrackID"], x) for x in cached_track_tzs])
            logger.debug("Have" + str(len(cached_track_tzs.keys())) + "/" + str(len(track_ids)) + " cached TZ records")

            for activity in this_page_activities:
                # attn service makers: why #(*%$ can't you all agree to use naive local time. So much simpler.
                cachedTrackData = None
                track_id = activity.ServiceData["ActivityID"]

                if track_id not in cached_track_tzs:
                    logger.debug("\t Resolving TZ for %s" % activity.StartTime)
                    cachedTrackData = self._downloadRawTrackRecord(serviceRecord, track_id)
                    try:
                        self._populateActivityFromTrackData(activity, cachedTrackData, minimumWaypoints=True)
                    except APIExcludeActivity as e:
                        e.ExternalActivityID = track_id
                        logger.info("Encountered APIExcludeActivity %s" % str(e))
                        exclusions.append(e)
                        continue

                    if not activity.TZ and not activity.Stationary:
                        logger.info("Couldn't determine TZ")
                        exclusions.append(APIExcludeActivity("Couldn't determine TZ", activityId=track_id))
                        continue
                    cachedTrackRecord = {"Owner": serviceRecord.ExternalID, "TrackID": track_id, "TZ": pickle.dumps(activity.TZ), "StartTime": activity.StartTime}
                    cachedb.endomondo_activity_cache.insert(cachedTrackRecord)
                elif not activity.Stationary:
                    activity.TZ = pickle.loads(cached_track_tzs[track_id]["TZ"])
                    activity.AdjustTZ()  # Everything returned is in UTC

                activity.Laps = []
                if int(act["sport"]) in self._activityMappings:
                    activity.Type = self._activityMappings[int(act["sport"])]

                activity.ServiceData = {"ActivityID": act["id"], "ActivityData": cachedTrackData}
                activity.CalculateUID()
                activities.append(activity)

            if not paged:
                earliestFirstPageDate = earliestDate
            if not exhaustive or ("more" in data and data["more"] is False):
                break
            else:
                paged = True
        return activities, exclusions

    def DownloadActivity(self, serviceRecord, activity):
        trackData = activity.ServiceData["ActivityData"]

        if not trackData:
            # If this is a new activity, we will already have the track data, otherwise download it.
            trackData = self._downloadRawTrackRecord(serviceRecord, activity.ServiceData["ActivityID"])

        self._populateActivityFromTrackData(activity, trackData)

        cookies = self._get_web_cookies(record=serviceRecord)
        summary_page = requests.get("http://www.endomondo.com/workouts/%d" % activity.ServiceData["ActivityID"], cookies=cookies)

        def _findStat(name):
            nonlocal summary_page
            result = re.findall('<li class="' + name + '">.+?<span class="value">([^<]+)</span>', summary_page.text, re.DOTALL)
            return result[0] if len(result) else None
        def _mapStat(name, statKey, type):
            nonlocal activity
            _unitMap = {
                "mi": ActivityStatisticUnit.Miles,
                "km": ActivityStatisticUnit.Kilometers,
                "kcal": ActivityStatisticUnit.Kilocalories,
                "ft": ActivityStatisticUnit.Feet,
                "m": ActivityStatisticUnit.Meters,
                "rpm": ActivityStatisticUnit.RevolutionsPerMinute,
                "avg-hr": ActivityStatisticUnit.BeatsPerMinute,
                "max-hr": ActivityStatisticUnit.BeatsPerMinute,
            }
            statValue = _findStat(name)
            if statValue:
                statUnit = statValue.split(" ")[1] if " " in statValue else None
                unit = _unitMap[statUnit] if statUnit else _unitMap[name]
                statValue = statValue.split(" ")[0]
                valData = {type: float(statValue)}
                activity.Stats.__dict__[statKey].update(ActivityStatistic(unit, **valData))

        _mapStat("max-hr","HR","max")
        _mapStat("avg-hr","HR","avg")
        _mapStat("calories","Kilocalories","value")
        _mapStat("elevation-asc","Elevation","gain")
        _mapStat("elevation-desc","Elevation","loss")
        _mapStat("cadence","Cadence","avg") # I would presume?
        _mapStat("distance","Distance","value") # I would presume?

        notes = re.findall('<div class="notes editable".+?<p>(.+?)</p>', summary_page.text)
        if len(notes):
            activity.Notes = notes[0]

        return activity

    def UploadActivity(self, serviceRecord, activity):

        cookies = self._get_web_cookies(record=serviceRecord)
        # Wicket sucks sucks sucks sucks sucks sucks.
        # Step 0
        #   http://www.endomondo.com/?wicket:bookmarkablePage=:com.endomondo.web.page.workout.CreateWorkoutPage2
        #   Get URL of file upload
        #       <a href="#" id="id13a" onclick="var wcall=wicketAjaxGet('?wicket:interface=:8:pageContainer:lowerSection:lowerMain:lowerMainContent:importFileLink::IBehaviorListener:0:',function() { }.bind(this),function() { }.bind(this), function() {return Wicket.$('id13a') != null;}.bind(this));return !wcall;">...                    <div class="fileImport"></div>
        upload_select = requests.get("http://www.endomondo.com/?wicket:bookmarkablePage=:com.endomondo.web.page.workout.CreateWorkoutPage2", cookies=cookies)
        upload_lightbox_url = re.findall('<a.+?onclick="var wcall=wicketAjaxGet\(\'(.+?)\'', upload_select.text)[3]
        logger.debug("Will request upload lightbox from %s" % upload_lightbox_url)
        # Step 1
        #   http://www.endomondo.com/upload-form-url
        #   Get IFrame src
        upload_iframe = requests.get("http://www.endomondo.com/" + upload_lightbox_url, cookies=cookies)
        upload_iframe_src = re.findall('src="(.+?)"', upload_iframe.text)[0]
        logger.debug("Will request upload form from %s" % upload_iframe_src)
        # Step 2
        #   http://www.endomondo.com/iframe-url
        #   Follow redirect to upload page
        #   Get form ID
        #   Get form target from <a class="next" name="uploadSumbit" id="id18d" value="Next" onclick="document.getElementById('fileUploadWaitIcon').style.display='block';var wcall=wicketSubmitFormById('id18c', '?wicket:interface=:13:importPanel:wizardStepPanel:uploadForm:uploadSumbit::IActivePageBehaviorListener:0:-1&amp;wicket:ignoreIfNotActive=true', 'uploadSumbit' ,function() { }.bind(this),function() { }.bind(this), function() {return Wicket.$$(this)&amp;&amp;Wicket.$$('id18c')}.bind(this));;; return false;">Next</a>
        upload_form_rd = requests.get("http://www.endomondo.com/" + upload_iframe_src, cookies=cookies, allow_redirects=False)
        assert(upload_form_rd.status_code == 302) # Need to manually follow the redirect to keep the cookies available
        upload_form = requests.get(upload_form_rd.headers["location"], cookies=cookies)
        upload_form_id = re.findall('<form.+?id="([^"]+)"', upload_form.text)[0]
        upload_form_target = re.findall("wicketSubmitFormById\('[^']+', '([^']+)'", upload_form.text)[0]
        logger.debug("Will POST upload form ID %s to %s" % (upload_form_id, upload_form_target))
        # Step 3
        #   http://www.endomondo.com/upload-target
        #   POST
        #       formID_hf_0
        #       file as `uploadFile`
        #       uploadSubmit=1
        #   Get ID from form
        #   Get confirm target <a class="next" name="reviewSumbit" id="id191" value="Save" onclick="document.getElementById('fileSaveWaitIcon').style.display='block';var wcall=wicketSubmitFormById('id190', '?wicket:interface=:13:importPanel:wizardStepPanel:reviewForm:reviewSumbit::IActivePageBehaviorListener:0:-1&amp;wicket:ignoreIfNotActive=true', 'reviewSumbit' ,function() { }.bind(this),function() { }.bind(this), function() {return Wicket.$$(this)&amp;&amp;Wicket.$$('id190')}.bind(this));;; return false;">Save</a>
        activity.EnsureTZ()
        fit_file = FITIO.Dump(activity)
        files = {"uploadFile": ("tap-sync-" + str(os.getpid()) + "-" + activity.UID + ".fit", fit_file)}
        data = {"uploadSumbit":1, upload_form_id + "_hf_0":""}
        upload_result = requests.post("http://www.endomondo.com/" + upload_form_target, data=data, files=files, cookies=cookies)
        confirm_form_id = re.findall('<form.+?id="([^"]+)"', upload_result.text)[0]
        confirm_form_target = re.findall("wicketSubmitFormById\('[^']+', '([^']+)'", upload_result.text)[0]
        logger.debug("Will POST confirm form ID %s to %s" % (confirm_form_id, confirm_form_target))
        # Step 4
        #   http://www.endomondo.com/confirm-target
        #   POST
        #       formID_hf_0
        #       workoutRow:0:mark=on
        #       workoutRow:0:sport=X
        #       reviewSumbit=1
        sportId = [k for k, v in self._reverseActivityMappings.items() if v == activity.Type]
        if len(sportId) == 0:
            raise ValueError("Endomondo service does not support activity type " + activity.Type)
        else:
            sportId = sportId[0]

        data = {confirm_form_id + "_hf_0":"", "workoutRow:0:mark":"on", "workoutRow:0:sport":sportId, "reviewSumbit":1}
        confirm_result = requests.post("http://www.endomondo.com" + confirm_form_target, data=data, cookies=cookies)
        assert(confirm_result.status_code == 200)
        # Step 5
        #   http://api.mobile.endomondo.com/mobile/api/workout/list
        #   GET
        #       authToken=xyz
        #       maxResults=1
        #       before=utcTS+1
        #   Get activity ID
        before = (activity.StartTime + timedelta(seconds=90)).astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        params = {"authToken": serviceRecord.Authorization["AuthToken"], "maxResults": 1, "before": before}
        id_result = requests.get("http://api.mobile.endomondo.com/mobile/api/workout/list", params=params)
        act_id = id_result.json()["data"][0]["id"]
        logger.debug("Retrieved activity ID %s" % act_id)

        # Step 6
        #   http://www.endomondo.com/workouts/xyz
        #   Get edit URL <a class="enabled button edit" href="#" id="id171" onclick="var wcall=wicketAjaxGet('../?wicket:interface=:10:pageContainer:lowerSection:lowerMain:lowerMainContent:workout:details:actions:ownActions:editButton::IBehaviorListener:0:1',function() { }.bind(this),function() { }.bind(this), function() {return Wicket.$('id171') != null;}.bind(this));return !wcall;">Edit</a>
        summary_page = requests.get("http://www.endomondo.com/workouts/%s" % act_id, cookies=cookies)
        edit_url = re.findall('<a.+class="enabled button edit".+?onclick="var wcall=wicketAjaxGet\(\'../(.+?)\'', summary_page.text)[0]
        logger.debug("Will request edit form from %s" % edit_url)
        # Step 7
        #   http://www.endomondo.com/edit-url
        #   Get form ID
        #   Get form target from <a class="halfbutton" href="#" style="float:left;" name="saveButton" id="id1d5" value="Save" onclick="var wcall=wicketSubmitFormById('id1d4', '../?wicket:interface=:14:pageContainer:lightboxContainer:lightboxContent:panel:detailsContainer:workoutForm:saveButton::IActivePageBehaviorListener:0:1&amp;wicket:ignoreIfNotActive=true', 'saveButton' ,function() { }.bind(this),function() { }.bind(this), function() {return Wicket.$$(this)&amp;&amp;Wicket.$$('id1d4')}.bind(this));;; return false;">Save</a>
        edit_page = requests.get("http://www.endomondo.com/" + edit_url, cookies=cookies)
        edit_form_id = re.findall('<form.+?id="([^"]+)"', edit_page.text)[0]
        edit_form_target = re.findall("wicketSubmitFormById\('[^']+', '([^']+)'", edit_page.text)[0]
        logger.debug("Will POST edit form ID %s to %s" % (edit_form_id, edit_form_target))
        # Step 8
        #   http://www.endomondo.com/edit-finish-url
        #   POST
        #       id34e_hf_0
        #       sport: X
        #       name: name123
        #       startTime:YYYY-MM-DD HH:MM
        #       distance:1.00 km
        #       duration:0h:10m:00s
        #       metersAscent:
        #       metersDescent:
        #       averageHeartRate:30
        #       maximumHeartRate:100
        #       validityToggle:on ("include in statistics")
        #       calorieRecomputeToggle:on
        #       notes:asdasdasd
        #       saveButton:1
        duration = (activity.EndTime - activity.StartTime)
        duration_formatted = "%dh:%dm:%ds" % (duration.seconds/3600, duration.seconds%3600/60, duration.seconds%(60))
        data = {
            edit_form_id + "_hf_0":"",
            "saveButton":"1",
            "validityToggle": "on",
            "calorieRecomputeToggle": "on",
            "startTime": activity.StartTime.strftime("%Y-%m-%d %H:%M"),
            "distance":  "%s km" % activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value,
            "sport": sportId,
            "duration": duration_formatted,
            "name": activity.Name,
        }
        if activity.Stats.Elevation.Gain is not None:
            data["metersAscent"] = int(round(activity.Stats.Elevation.Gain))
        if activity.Stats.Elevation.Gain is not None:
            data["metersDescent"] = int(round(activity.Stats.Elevation.Loss))
        if activity.Stats.HR.Average is not None:
            data["averageHeartRate"] = int(round(activity.Stats.HR.Average))
        if activity.Stats.HR.Max is not None:
            data["maximumHeartRate"] = int(round(activity.Stats.HR.Max))
        edit_result = requests.post("http://www.endomondo.com/" + edit_form_target, data=data, cookies=cookies)
        assert edit_result.status_code == 200 and "feedbackPanelERROR" not in edit_result.text

    def DeleteCachedData(self, serviceRecord):
        cachedb.endomondo_activity_cache.remove({"Owner": serviceRecord.ExternalID})
