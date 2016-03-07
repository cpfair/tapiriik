from tapiriik.settings import WEB_ROOT, HEXOSKIN_CLIENT_SECRET, HEXOSKIN_CLIENT_ID, HEXOSKIN_CLIENT_RESOURCE_ID
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from tapiriik.services.tcx import TCXIO

from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests
import logging
import pytz
import time

logger = logging.getLogger(__name__)

class HexoskinService(ServiceBase):
    """Define the base service object"""
    ID = "hexoskin"
    DisplayName = "Hexoskin"
    DisplayAbbreviation = "Hx"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "https://api.hexoskin.com/api/account/"
    UserActivityURL = "https://api.hexoskin.com/api/range/"
    AuthenticationNoFrame = True  # They don't prevent the iframe, it just looks really ugly.
    LastUpload = None

    SupportsHR = SupportsCadence = SupportsTemp = True

    SupportsActivityDeletion = False

    # For mapping common->Hexoskin; no ambiguity in Hexoskin activity type
    _activityTypeMappings = {
        ActivityType.Cycling: "/api/activitytype/1/",
        ActivityType.MountainBiking: "/api/activitytype/1/",
        ActivityType.Hiking: "/api/activitytype/5/",
        ActivityType.Running: "/api/activitytype/6/",
        ActivityType.Walking: "/api/activitytype/15/",
        ActivityType.Snowboarding: "/api/activitytype/13/",
        ActivityType.Skating: "/api/activitytype/11/",
        ActivityType.CrossCountrySkiing: "/api/activitytype/3/",
        ActivityType.DownhillSkiing: "/api/activitytype/4/",
        ActivityType.Swimming: "/api/activitytype/14/",
        ActivityType.Gym: "/api/activitytype/28/",
        ActivityType.Rowing: "/api/activitytype/9/",
        ActivityType.Elliptical: "/api/activitytype/997/",
        ActivityType.Other:"/api/activitytype/997/"
    }


    # For mapping Hexoskin->common
    def _reverseActivityTypeMappings(self, key):
        _reverseActivityTypeMappingsKeys = {
            "/api/activitytype/1/": ActivityType.Cycling,
            "/api/activitytype/3/": ActivityType.CrossCountrySkiing,
            "/api/activitytype/4/": ActivityType.DownhillSkiing,
            "/api/activitytype/5/": ActivityType.Hiking,
            "/api/activitytype/6/": ActivityType.Running,
            "/api/activitytype/7/": ActivityType.MountainBiking,
            "/api/activitytype/9/": ActivityType.Rowing,
            "/api/activitytype/10/": ActivityType.Running,
            "/api/activitytype/11/": ActivityType.Skating,
            "/api/activitytype/13/": ActivityType.Snowboarding,
            "/api/activitytype/14/": ActivityType.Swimming,
            "/api/activitytype/15/": ActivityType.Walking,
            "/api/activitytype/24/": ActivityType.Running,
            "/api/activitytype/28/": ActivityType.Gym,
        }
        if key in _reverseActivityTypeMappingsKeys.keys():
            return _reverseActivityTypeMappingsKeys[key]
        else:
            return ActivityType.Other

    SupportedActivities = list(_activityTypeMappings.keys())


    def UserUploadedActivityURL(self, uploadId):
        return "https://api.hexoskin.com/api/range/%d/" % uploadId


    def WebInit(self):
        """
        prepare the oauth process request. Done separately because it needs to be
        initialized on page display
        """
        from uuid import uuid4
        params = {'scope':'readwrite',
                  'client_id':HEXOSKIN_CLIENT_ID,
                  'response_type':'code',
                  'state': str(uuid4()),
                  'redirect_uri':WEB_ROOT + reverse("oauth_return", kwargs={"service": "hexoskin"})}
        self.UserAuthorizationURL = "https://api.hexoskin.com/api/connect/oauth2/auth/?" + urlencode(params)


    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "Bearer " + serviceRecord.Authorization["OAuthToken"]}


    def RetrieveAuthorizationToken(self, req, level):
        """In OAuth flow, retrieve the Authorization Token"""
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code,"client_id":HEXOSKIN_CLIENT_ID, "client_secret": HEXOSKIN_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "hexoskin"})}
        path = "https://api.hexoskin.com/api/connect/oauth2/token/"
        response = requests.post(path, params=params, auth=(HEXOSKIN_CLIENT_ID,HEXOSKIN_CLIENT_SECRET))

        if response.status_code != 200:
            raise APIException("Invalid code")

        data = response.json()
        authorizationData = {"OAuthToken": data["access_token"]}
        id_resp = requests.get("https://api.hexoskin.com/api/account/", headers=self._apiHeaders(ServiceRecord({"Authorization": authorizationData})))
        return (id_resp.json()['objects'][0]['id'], authorizationData)


    def RevokeAuthorization(self, serviceRecord):
        """Delete authorization token"""
        path = "https://api.hexoskin.com/api/oauth2token/%s/" % HEXOSKIN_CLIENT_RESOURCE_ID
        headers = self._apiHeaders(serviceRecord)
        result = requests.delete(path, headers=headers)
        if result.status_code is not 204:
            APIException("Revoking token was unsuccessful")


    def _is_ride_valid(self, ride):
        # Sync only top-level activities, and exclude rest, rest test and sleep
        valid = (
            ride['rank'] is 0
            and ride['context']['activitytype'] is not None
            and not any(y in ride['context']['activitytype'] for y in ['/8/', '/12/', '/106/'])
        )
        return valid

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        """
        Get list of user's activities in Hexoskin and return it to tapiriik database
        """
        logger.debug('Hexoskin - starting to download activity list for user %s' % serviceRecord.ExternalID)
        activities = []
        exclusions = []
        if exhaustive:
            listEnd = (datetime.now() + timedelta(days=1.5) - datetime(1970,1,1)).total_seconds()*256
            listStart = (datetime(day=21, month=8, year=1985) - datetime(1970,1,1)).total_seconds()*256  # The distant past
            resp = requests.get("https://api.hexoskin.com/api/range/?user=%s&limit=100&rank=0&start__range=%s,%s" % (serviceRecord.ExternalID, int(listStart), int(listEnd)), headers=self._apiHeaders(serviceRecord))
        else:
            listEnd = (datetime.now() + timedelta(days=1.5) - datetime(1970,1,1)).total_seconds()*256
            listStart = (datetime.now() - timedelta(days=30) - datetime(1970,1,1)).total_seconds()*256
            resp = requests.get("https://api.hexoskin.com/api/range/?user=%s&limit=30&rank=0&start__range=%s,%s" % (serviceRecord.ExternalID, int(listStart), int(listEnd)), headers=self._apiHeaders(serviceRecord))
        if resp.status_code == 401:
            raise APIException("No authorization to retrieve activity list", block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        try:
            reqdata = resp.json()['objects']
        except ValueError:
            logger.debug("Failed parsing hexoskin list response %s - %s" % (resp.status_code, resp.text))
            raise APIException("Failed parsing hexoskin list response %s - %s" % (resp.status_code, resp.text))
        for ride in reqdata:
            try:
                if not (ride['status'] == 'complete'):
                    pass  # exclude that range for now, without excluding it in the future
                elif self._is_ride_valid(ride):
                    activity = UploadedActivity()
                    activity.StartTime = pytz.utc.localize(datetime.fromtimestamp(ride['start']/256.0))
                    activity.EndTime = pytz.utc.localize(datetime.fromtimestamp(ride['end']/256.0))
                    activity.ServiceData = {"ActivityID": ride["id"]}
                    activity.Type = self._reverseActivityTypeMappings(ride['context']['activitytype'])
                    for metric in ride['metrics']:
                        # TODO check for IDs instead of titles
                        if metric['resource_uri'] == '/api/metric/17/':  # Cadence
                            activity.Stats.RunCadence.update(ActivityStatistic(ActivityStatisticUnit.StepsPerMinute, value=metric['value']))
                        if metric['resource_uri'] == '/api/metric/44/':  # Heart rate Average
                            activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, avg=metric["value"]))
                        if metric['resource_uri'] == '/api/metric/46/':  # Heart rate Max
                            activity.Stats.HR.update(ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, max=metric["value"]))
                        if metric['resource_uri'] == '/api/metric/71/':  # Step count
                            activity.Stats.Strides.update(ActivityStatistic(ActivityStatisticUnit.Strides, value=metric["value"]))
                        if metric['resource_uri'] == '/api/metric/149/':  # Energy kcal
                            activity.Stats.Energy.update(ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=metric['value']))
                        if metric['resource_uri'] == '/api/metric/501/':  # Speed Max
                            activity.Stats.Speed.update(ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, max=metric["value"]))
                        if metric['resource_uri'] == '/api/metric/502/':  # Speed Avg
                            activity.Stats.Speed.update(ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=metric["value"]))
                        if metric['resource_uri'] == '/api/metric/2038/':  # Distance
                            activity.Stats.Distance.update(ActivityStatistic(ActivityStatisticUnit.Meters, value=metric['value']))

                    activity.Name = ride["name"]
                    activity.Stationary = False
                    ride_track = requests.get("https://api.hexoskin.com/api/track/?range=%s" % ride['id'], headers=self._apiHeaders(serviceRecord))
                    time.sleep(0.2)
                    activity.GPS = (True if ride_track.json()['objects'] else False)

                    activity.CalculateUID()
                    activities.append(activity)
                else:
                    exclusions.append(APIExcludeActivity("Unsupported activity type %s" % ride['context']['activitytype'], activity_id=ride["id"], user_exception=UserException(UserExceptionType.Other)))
            except TypeError as e:
                logger.debug("Failed parsing ranges url, response: %s\n%s" % (resp.url, resp.content))
                raise e
        logger.debug('Hexoskin - %s activities found, %s excluded. Activities' % ([x.ServiceData['ActivityID'] for x in activities], [x.ExternalActivityID for x in exclusions]))
        return activities, exclusions


    def DownloadActivity(self, serviceRecord, activity):
        """Extract activity from Hexoskin"""
        activityID = activity.ServiceData["ActivityID"]
        activityPreTCXParseName = activity.Name  # Keep activity name in memory because TCX overrides activity name with somehting that's more a note than a name
        logger.debug('Hexoskin - Extracting activity %s' % activityID)
        headers = self._apiHeaders(serviceRecord)
        headers.update({"Accept":"application/vnd.garmin.tcx+xml"})
        range_tcx = requests.get("https://api.hexoskin.com/api/range/%s/" % (str(activityID)), headers=headers)
        TCXIO.Parse(range_tcx.content, activity)
        activity.Notes = 'Hexoskin - %s' % activity.Name
        activity.Name = activityPreTCXParseName
        return activity


    def UploadActivity(self, serviceRecord, activity):
        """Import data into Hexoskin using TCX format"""
        tcx_data = TCXIO.Dump(activity)
        headers = self._apiHeaders(serviceRecord)
        headers.update({"Content-Type":"application/vnd.garmin.tcx+xml"})

        range_tcx = requests.post("https://api.hexoskin.com/api/import/", data=tcx_data.encode('utf-8'), headers=headers)
        import_id = str(range_tcx.json()['resource_uri'])
        # TODO In line below, fix the way the resource_uri is constructed when the /importfile/ fix is deployed
        uploaded = False
        process_start_time = datetime.now()
        while uploaded is False:
            time.sleep(1)
            range_upload_status = requests.get("https://api.hexoskin.com/" + import_id, headers=headers).json()
            if (datetime.now() - process_start_time).total_seconds() < 20:
                if range_upload_status['results'] is not None and 'error' in range_upload_status['results']:
                    err = range_upload_status['results']['error']
                    logger.debug('Hexoskin - Import range failed, see error')
                    raise APIException('Error uploading to Hexoskin: %s' % err)
                elif range_upload_status['progress'] == 1 and range_upload_status['results']:
                    for entry in range_upload_status['results']:
                        if 'resource_uri' in entry.keys() and "range" in entry['resource_uri'] and entry['rank'] == 0:
                            upload_id = entry['id']
                            logger.debug('Hexoskin - Imported range %s' % upload_id)
                            uploaded = True
                            break
            else:
                raise APIException('Timeout uploading activity for import id %s' % import_id)  # when the '0' bug is fixed, change this
        return upload_id


    def DeleteCachedData(self, serviceRecord):
        """No cached data"""
        pass


    def DeleteActivity(self, serviceRecord, uploadId):
        """We would rather have users delete data from their dashboard instead of an automatic tool"""
        pass
