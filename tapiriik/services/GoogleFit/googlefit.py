from tapiriik.settings import WEB_ROOT, GOOGLEFIT_CLIENT_ID, GOOGLEFIT_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.oauth2 import OAuth2Client
from tapiriik.services.interchange import UploadedActivity, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException
from tapiriik.database import cachedb
from django.core.urlresolvers import reverse
import logging
import requests
import json
from datetime import timedelta, datetime
import pytz
import calendar

from .activitytypes import googlefit_to_atype, atype_to_googlefit

logger = logging.getLogger(__name__)

# Full scope needed so that we can read files that user adds by hand
_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.activity.write",
    "https://www.googleapis.com/auth/fitness.body.read",  # for HR data
    "https://www.googleapis.com/auth/fitness.body.write",
    "https://www.googleapis.com/auth/fitness.location.read",
    "https://www.googleapis.com/auth/fitness.location.write"]

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://accounts.google.com/o/oauth2/token"
GOOGLE_REVOKE_URL = "https://accounts.google.com/o/oauth2/revoke"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

API_BASE_URL = "https://www.googleapis.com/fitness/v1/users/me/"

POST_HEADER = {"Content-type": "application/json"}


def _floatField(name):
    # Description for field when creating source.
    return {"name": name, "format": "floatPoint"}


def _intField(name):
    # Description for field when creating source.
    return {"name": name, "format": "integer"}

# See https://developers.google.com/fit/rest/v1/data-types
# We have to include the description when creating sources, for reasons.
SUPPORTED_DATATYPES = {
    "com.google.activity.summary": [_intField("activity"), _intField("duration"), _intField("num_segments")],
    # "com.google.activity.sample" : [...],
    # "com.google.activity.segment": [...], # TODO: would be nice to support this as Lap?
    "com.google.location.sample": [_floatField("latitude"), _floatField("longitude"), _floatField("accuracy"), _floatField("altitude")],
    "com.google.heart_rate.bpm": [_floatField("bpm")],
    "com.google.calories.expended": [_floatField("calories")],  # I presume calories.consumed actually means "ingested", otherwise it's the same thing??
    "com.google.cycling.pedaling.cadence": [_floatField("rpm")],
    "com.google.step_count.cadence": [_floatField("rpm")],
    "com.google.distance.delta": [_floatField("distance")],
    "com.google.power.sample": [_floatField("watts")],
    "com.google.speed": [_floatField("speed")],
    # "com.google.step_count.delta": [...], # TODO: would be nice to support this?
}

APP_NAME = "com.tapiriik.sync"


def _fpVal(f):
    return {"fpVal": f}


def _intVal(i):
    return {"intVal": int(i)}


def _sourceAppName(source):
    name = None
    app = source.get("application")
    if app:
        name = app.get("name") or app.get("packageName")
    return name


class GoogleFitService(ServiceBase):
    ID = "googlefit"
    DisplayName = "Google Fit"
    DisplayAbbreviation = "GF"
    AuthenticationType = ServiceAuthenticationType.OAuth
    Configurable = True
    ReceivesStationaryActivities = True
    AuthenticationNoFrame = True
    SupportsHR = SupportsCalories = SupportsCadence = SupportsPower = True
    SupportsTemp = False  # could created a custom data type, but not supported by default..
    SupportedActivities = atype_to_googlefit.keys()
    GlobalRateLimits = [(timedelta(days=1), 86400)]

    _oaClient = OAuth2Client(GOOGLEFIT_CLIENT_ID, GOOGLEFIT_CLIENT_SECRET, GOOGLE_TOKEN_URL, tokenTimeoutMin=55)

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("oauth_redirect", kwargs={"service": self.ID})
        pass

    def GenerateUserAuthorizationURL(self, session, level=None):
        return_url = WEB_ROOT + reverse("oauth_return", kwargs={"service": self.ID})
        params = {"redirect_uri": return_url, "response_type": "code", "access_type": "offline", "client_id": GOOGLEFIT_CLIENT_ID, "scope": " ".join(_OAUTH_SCOPES)}
        return requests.Request(url=GOOGLE_AUTH_URL, params=params).prepare().url

    def RetrieveAuthorizationToken(self, req, level):
        def fetchUid(tokenData):
            access_token = tokenData["access_token"]
            uid_res = self._oaClient.get(None, GOOGLE_USERINFO_URL, access_token=access_token)
            return uid_res.json()["id"]

        return self._oaClient.retrieveAuthorizationToken(self, req, WEB_ROOT + reverse("oauth_return", kwargs={"service": self.ID}), fetchUid)

    def RevokeAuthorization(self, serviceRec):
        self._oaClient.revokeAuthorization(serviceRec, GOOGLE_REVOKE_URL)

    def DeleteCachedData(self, serviceRecord):
        cachedb.googlefit_source_cache.remove({"ExternalID": serviceRecord.ExternalID})

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        session = self._oaClient.session(serviceRecord)
        session_list_url = API_BASE_URL + "sessions"
        activities = []
        excluded = []

        sources = self._getDataSources(serviceRecord, session, forceRefresh=True)
        if not sources:
            # No sources of interest, don't bother listing sessions (save an API call)
            return activities, excluded

        page_token = None
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token
            session_list = session.get(session_list_url, params=params).json()
            slist = session_list.get("session")
            if not slist:
                break

            for s in slist:
                act = UploadedActivity()
                if "application" not in s or s["activityType"] not in googlefit_to_atype:
                    # Unknown/unsupported activity type or no app data
                    continue
                act.StartTime = pytz.utc.localize(datetime.utcfromtimestamp(float(s["startTimeMillis"])/1000))
                act.EndTime = pytz.utc.localize(datetime.utcfromtimestamp(float(s["endTimeMillis"])/1000))
                act.Type = googlefit_to_atype[s["activityType"]]
                # NOTE: this is not really right, but google fit doesn't support timezones.  at least all timestamps are UTC.
                act.TZ = pytz.UTC
                act.Notes = s.get("description") or None
                act.Name = s.get("name") or None
                act.ServiceData = {"Id": s.get("id")}
                appdata = s["application"]
                act.ServiceData["ApplicationPackage"] = appdata.get("packageName")
                act.ServiceData["ApplicationVersion"] = appdata.get("version")
                act.ServiceData["ApplicationName"] = appdata.get("name")
                act.CalculateUID()
                #logger.debug("google fit activity UID %s" % act.UID)
                activities.append(act)

            page_token = session_list.get("nextPageToken")
            if not exhaustive or not page_token:
                break

        return activities, excluded

    def _getDataSources(self, serviceRecord, session, forceRefresh=False):
        datasource_url = API_BASE_URL + "dataSources"
        sources = None
        if not forceRefresh:
            sources = cachedb.googlefit_source_cache.find_one({"ExternalID": serviceRecord.ExternalID})

        if sources is None:
            sources = session.get(datasource_url, params={"dataTypeName": list(SUPPORTED_DATATYPES.keys())}).json()
            if "dataSource" not in sources:
                sources = {"dataSource": []}
            #logger.debug("got %d sources from google fit." % len(sources["dataSource"]))
            cachedb.googlefit_source_cache.update({"ExternalID": serviceRecord.ExternalID}, sources)

        return sources["dataSource"]

    def _toUTCNano(self, ts):
        return calendar.timegm(ts.utctimetuple()) * int(1e9)

    def _toUTCMilli(self, ts):
        return calendar.timegm(ts.utctimetuple()) * int(1e3)

    def DownloadActivity(self, serviceRecord, activity):
        session = self._oaClient.session(serviceRecord)
        # If it came from DownloadActivityList it will have this..
        assert "ApplicationPackage" in activity.ServiceData
        dataset_url = API_BASE_URL + "dataSources/%s/datasets/%d-%d"

        # Pad the end time to make sure we get all the data
        start_nano = self._toUTCNano(activity.StartTime)
        end_nano = self._toUTCNano(activity.EndTime + timedelta(seconds=1))

        # Grab the streams from the same app as this session:
        sources = self._getDataSources(serviceRecord, session)
        sources = [x for x in sources if _sourceAppName(x) == activity.ServiceData["ApplicationPackage"]]
        # Sort the sources do that derived data comes first, raw data comes
        # second.  If we have both for the same waypoint then we want to
        # overwrite the derived with the raw.
        sources.sort(key=lambda x: x["dataStreamId"])

        # Combine the data for each point from each stream.
        waypoints = {}
        hasLoc = False
        for source in sources:
            streamid = source["dataStreamId"]
            # sourcedatatype = source["dataType"]["name"]
            #logger.debug("fetch data for stream %s" % streamid)
            source_url = dataset_url % (streamid, start_nano, end_nano)

            data = session.get(source_url)

            try:
                data = data.json()
            except:
                raise APIException("JSON parse error")

            points = data.get("point")
            if not points:
                continue

            for point in points:
                pointType = point["dataTypeName"]
                startms = int(float(point["startTimeNanos"]) / 1000000)
                wp = waypoints.get(startms)
                if wp is None:
                    wp = waypoints[startms] = Waypoint(pytz.utc.localize(datetime.utcfromtimestamp(startms/1000.0)))

                values = point["value"]
                if pointType == "com.google.location.sample":
                    wp.Location = Location()
                    wp.Location.Latitude = values[0]["fpVal"]
                    wp.Location.Longitude = values[1]["fpVal"]
                    # values[2] is accuracy
                    wp.Location.Altitude = values[3]["fpVal"]
                    hasLoc = True
                elif pointType == "com.google.heart_rate.bpm":
                    wp.HR = values[0]["fpVal"]
                elif pointType == "com.google.calories.expended":
                    wp.Calories = values[0]["fpVal"]
                elif pointType == "com.google.cycling.pedaling.cadence" or pointType == "com.google.cycling.wheel_revolution.rpm":
                    wp.Cadence = values[0]["fpVal"]
                elif pointType == "com.google.step_count.cadence":
                    wp.RunCadence = values[0]["fpVal"]
                elif pointType == "com.google.speed":
                    wp.Speed = values[0]["fpVal"]
                elif pointType == "com.google.distance.delta":
                    wp.Distance = values[0]["fpVal"]
                elif pointType == "com.google.power.sample":
                    wp.Power = values[0]["fpVal"]
                # elif pointType == "com.google.step_count.delta":
                #   # steps = values[0]["intVal"]
                #   wp.RunCadence = ...
                else:
                    logger.info("Unexpected point data type %s.." % pointType)

        # Sort all points by time
        wpkeys = list(waypoints.keys())
        wpkeys.sort()
        lap = Lap(startTime=activity.StartTime, endTime=activity.EndTime)  # no laps in google fit.. just make one.
        activity.Laps = [lap]
        lap.Waypoints = [waypoints[x] for x in wpkeys]
        if len(lap.Waypoints):
            # A bit approximate..
            lap.Waypoints[0].Type = WaypointType.Start
            lap.Waypoints[-1].Type = WaypointType.End

        activity.GPS = hasLoc
        activity.Stationary = activity.CountTotalWaypoints() <= 1
        return activity

    def _ensureSourcesExist(self, serviceRecord, session, sources):
        datasource_url = API_BASE_URL + "dataSources"

        tap_sources = [x for x in sources if _sourceAppName(x) == APP_NAME]
        #logger.debug("%d tapiriik sources already at google fit" % len(tap_sources))
        added = False

        for tname in SUPPORTED_DATATYPES:
            if [x for x in tap_sources if x["dataType"]["name"] == tname]:
                continue
            description = {
                "name": "tapiriik-%s" % tname.replace(".", "-"),
                "application": {"name": APP_NAME, "detailsUrl": WEB_ROOT},
                # Why do I have to tell Google what fields their data types have?
                "dataType": {"name": tname, "field": SUPPORTED_DATATYPES[tname]},
                "type": "raw",
            }
            response = session.post(datasource_url, data=json.dumps(description), headers=POST_HEADER)
            logger.debug("create %s source at google fit: response %d." % (tname, response.status_code))

            if response.status_code != 200:
                raise APIException("Error %d creating google fit source: %s" % (response.status_code, response.text))
            newdesc = response.json()
            sources.append(newdesc)
            added = True
        if added:
            cachedb.googlefit_source_cache.update({"ExternalID": serviceRecord.ExternalID}, {"dataSource": sources})
        return sources

    def UploadActivity(self, serviceRecord, activity):
        session = self._oaClient.session(serviceRecord)
        session_url = API_BASE_URL + "sessions/%s"
        sources = self._getDataSources(serviceRecord, session)
        sources = self._ensureSourcesExist(serviceRecord, session, sources)

        # Create a session representing this activity
        startms = self._toUTCMilli(activity.StartTime)
        endms = self._toUTCMilli(activity.EndTime)
        modms = self._toUTCMilli(datetime.now())  # hmm.. Is this ok?
        sess_data = {
            "id": str(startms),
            "name": activity.Name or activity.Type,
            "description": activity.Notes or "",
            "startTimeMillis": startms,
            "endTimeMillis": endms,
            "modifiedTimeMillis": modms,
            "application": {"name": APP_NAME},
            "activityType": atype_to_googlefit[activity.Type]
        }
        response = session.put(session_url % str(startms), data=json.dumps(sess_data), headers=POST_HEADER)
        logger.debug("create session %s: %d" % (session_url % str(startms), response.status_code))
        if response.status_code != 200:
            raise APIException("Error %d creating google fit session: %s" % (response.status_code, response.text))
        try:
            # TODO: check this matches what we put in.
            response.json()
        except:
            raise APIException("Response to creating google fit session not json: %s" % (response.text,))

        # Split the activity into data streams, as we have to upload each one individually
        locs = []
        hr = []
        cals = []
        cadence = []
        runcad = []
        speed = []
        dist = []
        power = []

        for lap in activity.Laps:
            for wp in lap.Waypoints:
                wp_nanos = self._toUTCNano(wp.Timestamp)
                if wp.Location is not None:
                    # Just put in 1m accuracy here since they wanted something.. what else to do?
                    locs.append((wp_nanos, [_fpVal(wp.Location.Latitude), _fpVal(wp.Location.Longitude), _fpVal(1), _fpVal(wp.Location.Altitude)]))
                if wp.HR is not None:
                    hr.append((wp_nanos, [_fpVal(wp.HR)]))
                if wp.Calories is not None:
                    cals.append((wp_nanos, [_fpVal(wp.Calories)]))
                if wp.Cadence is not None:
                    cadence.append((wp_nanos, [_fpVal(wp.Cadence)]))
                if wp.RunCadence is not None:
                    runcad.append((wp_nanos, [_fpVal(wp.RunCadence)]))
                if wp.Speed is not None:
                    speed.append((wp_nanos, [_fpVal(wp.RunCadence)]))
                if wp.Distance is not None:
                    dist.append((wp_nanos, [_fpVal(wp.Distance)]))
                if wp.Power is not None:
                    power.append((wp_nanos, [_fpVal(wp.Power)]))

        # Each point type is a separate stream, so we have to split them out
        # and upload them separately.
        dataset_url = API_BASE_URL + "dataSources/%s/datasets/%d-%d"
        data_types = [
            (locs, "com.google.location.sample"),
            (hr, "com.google.heart_rate.bpm"),
            (cals, "com.google.calories.expended"),
            (cadence, "com.google.cycling.pedaling.cadence"),
            (runcad, "com.google.step_count.cadence"),
            (speed, "com.google.speed"),
            (dist, "com.google.distance.delta"),
            (power, "com.google.power.sample"), ]

        # Upload each stream that we have data for
        for points, tname in data_types:
            if not points:
                continue

            s = [x for x in sources if _sourceAppName(x) == APP_NAME and x["dataType"]["name"] == tname]
            if not s or "dataStreamId" not in s[0]:
                raise APIException("Data source for %s not created correctly!" % tname)
            streamId = s[0]["dataStreamId"]

            min_ns = points[0][0]
            max_ns = points[-1][0]

            logger.debug("Process %d points for %s to %s" % (len(points), tname, dataset_url % (streamId, min_ns, max_ns)))

            def make_point(x):
                return {"dataTypeName": tname, "startTimeNanos": x[0], "endTimeNanos": x[0], "value": x[1]}
            point_list = [make_point(x) for x in points]

            put_data = {"dataSourceId": streamId, "minStartTimeNs": min_ns, "maxEndTimeNs": max_ns, "point": point_list}
            response = session.patch(dataset_url % (streamId, min_ns, max_ns), data=json.dumps(put_data), headers=POST_HEADER)

            if response.status_code != 200:
                raise APIException("Error %d adding points to google fit stream: %s" % (response.status_code, response.text))
            try:
                # TODO: check this matches what we put in.
                response.json()
            except:
                raise APIException("Response to adding google fit points not json: %s" % (response.text,))

        # Add a summary point (a bit ugly.. mostly duplicated code from loop above)
        s = [x for x in sources if _sourceAppName(x) == APP_NAME and x["dataType"]["name"] == "com.google.activity.summary"]
        if not s or "dataStreamId" not in s[0]:
            raise APIException("Data source for summary not created correctly!")
        streamId = s[0]["dataStreamId"]
        startns = startms * 1e6
        endns = endms * 1e6
        point_list = [{"dataTypeName": "com.google.activity.summary",
                       "startTimeNanos": startns, "endTimeNanos": endns,
                       "value": [_intVal(atype_to_googlefit[activity.Type]), _intVal(startms - endms), _intVal(1)]}]
        put_data = {"dataSourceId": streamId, "minStartTimeNs": startns, "maxEndTimeNs": endns, "point": point_list}
        logger.debug("Add summary point for %s" % (dataset_url % (streamId, startns, endns)))
        response = session.patch(dataset_url % (streamId, startns, endns), data=json.dumps(put_data), headers=POST_HEADER)
        if response.status_code != 200:
            raise APIException("Error %d adding points to google fit stream: %s" % (response.status_code, response.text))
        try:
            response.json()
        except:
            raise APIException("Response to adding google fit points not json: %s" % (response.text,))

        return str(startms)
