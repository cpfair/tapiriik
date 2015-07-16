import logging
from datetime import timedelta
from collections import defaultdict

import dateutil
from django.core.urlresolvers import reverse
from smashrun import Smashrun as SmashrunClient

from tapiriik.settings import WEB_ROOT, SMASHRUN_CLIENT_ID, SMASHRUN_CLIENT_SECRET
from tapiriik.services.service_base import ServiceBase, ServiceAuthenticationType
from tapiriik.services.interchange import (UploadedActivity, ActivityType, ActivityStatistic,
                                           ActivityStatisticUnit, Waypoint, WaypointType,
                                           Location, Lap, LapIntensity)
from tapiriik.services.api import APIException, APIExcludeActivity, UserException, UserExceptionType

logger = logging.getLogger(__name__)


class SmashrunService(ServiceBase):
    ID = "smashrun"
    DisplayName = "Smashrun"
    DisplayAbbreviation = "SR"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # unfortunately, the smashrun dialog doesnt fit in the iframe...
    SupportedActivities = [ActivityType.Running]
    SupportsHR = SupportsCalories = SupportsCadence = SupportsTemp = True
    SupportsActivityDeletion = False

    _reverseActivityMappings = {
        ActivityType.Running: "running",
    }
    _activityMappings = {
        "running": ActivityType.Running,
    }

    _intensityMappings = {
        LapIntensity.Active: 'work',
        LapIntensity.Rest: 'recovery',
        LapIntensity.Warmup: 'warmup',
        LapIntensity.Cooldown: 'cooldown',
    }

    def _getClient(self, serviceRec=None):
        redirect_uri = None
        if not serviceRec:
            # reverse() doesn't work in the worker and we only really need the redirect
            # uri in the ui...
            redirect_uri = WEB_ROOT + reverse('oauth_return', kwargs={'service': 'smashrun'})
        client = SmashrunClient(client_id=SMASHRUN_CLIENT_ID,
                                client_secret=SMASHRUN_CLIENT_SECRET,
                                redirect_uri=redirect_uri)
        if serviceRec:
            client.refresh_token(refresh_token=serviceRec.Authorization['refresh_token'])
        return client

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": "smashrun"})

    def GenerateUserAuthorizationURL(self, level=None):
        client = self._getClient()
        url, state = client.get_auth_url()
        return url

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        client = self._getClient()
        token = client.fetch_token(code=code)
        uid = client.get_userinfo()['id']
        return (uid, token)

    def RevokeAuthorization(self, serviceRecord):
        pass  # TODO: smashrun doesn't seem to support this yet

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        activities = []
        exclusions = []

        client = self._getClient(serviceRec=serviceRecord)

        for i, act in enumerate(client.get_activities()):
            # bail out early after some arbitrary number if not exhaustive
            if exhaustive and i > 20:
                break
            activity = UploadedActivity()
            activity.StartTime = dateutil.parser.parse(act['startDateTimeLocal'])
            activity.EndTime = activity.StartTime + timedelta(seconds=act['duration'])
            _type = self._activityMappings.get(act['activityType'])
            if not _type:
                exclusions.append(APIExcludeActivity("Unsupported activity type %s" % act['activityType'],
                                                     activity_id=act["activityId"],
                                                     user_exception=UserException(UserExceptionType.Other)))
            activity.ServiceData = {"ActivityID": act['activityId']}
            activity.Type = _type
            activity.Notes = act['notes']
            activity.GPS = bool(act.get('startLatitude'))
            activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Kilometers, value=act['distance'])
            activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=act['calories'])
            if 'heartRateMin' in act:
                activity.Stats.HR = ActivityStatistic(ActivityStatisticUnit.BeatsPerMinute, min=act['heartRateMin'],
                                                      max=act['heartRateMax'], avg=act['heartRateAverage'])
            activity.Stats.MovingTime = ActivityStatistic(ActivityStatisticUnit.Seconds, value=act['duration'])

            if 'temperature' in act:
                activity.Stats.Temperature = ActivityStatistic(ActivityStatisticUnit.DegreesCelcius,
                                                               avg=act['temperature'])
            activity.CalculateUID()
            activities.append(activity)

        return activities, exclusions

    # TODO: handle pauses
    def DownloadActivity(self, serviceRecord, activity):
        client = self._getClient(serviceRec=serviceRecord)
        act = client.get_activity(activity.ServiceData['ActivityID'])
        recordingKeys = act.get('recordingKeys')
        if act['source'] == 'manual' or not recordingKeys:
            # it's a manually entered run, can't get much info
            activity.Stationary = True
            activity.Laps = [Lap(startTime=activity.StartTime, endTime=activity.EndTime, stats=activity.Stats)]
            return activity

        # FIXME: technically it could still be stationary if there are no long/lat values...
        activity.Stationary = False

        if not act['laps']:
            # no laps, just make one big lap
            activity.Laps = [Lap(startTime=activity.StartTime, endTime=activity.EndTime, stats=activity.Stats)]

        prevEndTime = activity.StartTime
        for lapRecord in act['laps']:
            lap = Lap(startTime=prevEndTime,
                      endTime=prevEndTime + timedelta(seconds=lapRecord['endDuration']))
            activity.Laps.append(lap)

        for value in zip(*act['recordingValues']):
            record = dict(zip(recordingKeys, value))
            ts = activity.StartTime + timedelta(seconds=record['clock'])
            if 'latitude' in record:
                alt = record.get('elevation')
                lat = record['latitude']
                lon = record['longitude']
                # Smashrun seems to replace missing measurements with -1
                if lat == -1:
                    lat = None
                if lon == -1:
                    lon = None
                location = Location(lat=lat, lon=lon, alt=alt)
            hr = record.get('heartRate')
            runCadence = record.get('cadence')
            temp = record.get('temperature')
            distance = record.get('distance') * 1000
            wp = Waypoint(timestamp=ts, location=location, hr=hr,
                          runCadence=runCadence, temp=temp,
                          distance=distance)
            # put the waypoint inside the lap it corresponds to
            for lap in activity.Laps:
                if lap.StartTime <= wp.Timestamp <= lap.EndTime:
                    lap.Waypoints.append(wp)

        return activity

    def _resolveDuration(self, obj):
        if obj.Stats.TimerTime.Value is not None:
            return obj.Stats.TimerTime.asUnits(ActivityStatisticUnit.Seconds).Value
        if obj.Stats.MovingTime.Value is not None:
            return obj.Stats.MovingTime.asUnits(ActivityStatisticUnit.Seconds).Value
        return (obj.EndTime - obj.StartTime).total_seconds()

    def UploadActivity(self, serviceRecord, activity):
        client = self._getClient(serviceRecord)
        data = {}
        data['startDateTimeLocal'] = activity.StartTime.isoformat()
        data['distance'] = activity.Stats.Distance.asUnits(ActivityStatisticUnit.Kilometers).Value
        data['duration'] = self._resolveDuration(activity)
        data['activityType'] = self._reverseActivityMappings.get(activity.Type)

        def setIfNotNone(d, k, *vs, f=lambda x: x):
            for v in vs:
                if v is not None:
                    d[k] = f(v)
                    return

        setIfNotNone(data, 'notes', activity.Notes, activity.Name)
        setIfNotNone(data, 'cadenceAverage', activity.Stats.RunCadence.Average, f=int)
        setIfNotNone(data, 'cadenceMin', activity.Stats.RunCadence.Min, f=int)
        setIfNotNone(data, 'cadenceMax', activity.Stats.RunCadence.Max, f=int)
        setIfNotNone(data, 'heartRateAverage', activity.Stats.HR.Average, f=int)
        setIfNotNone(data, 'heartRateMin', activity.Stats.HR.Min, f=int)
        setIfNotNone(data, 'heartRateMax', activity.Stats.HR.Max, f=int)
        setIfNotNone(data, 'temperatureAverage', activity.Stats.Temperature.Average)

        if not activity.Laps[0].Waypoints:
            # no info, no need to go further
            client.create_activity(data)
            return

        data['laps'] = []
        recordings = defaultdict(list)

        def getattr_nested(obj, attr):
            attrs = attr.split('.')
            while attrs:
                r = getattr(obj, attrs.pop(0), None)
                obj = r
            return r

        def hasStat(activity, stat):
            for lap in activity.Laps:
                for wp in lap.Waypoints:
                    if getattr_nested(wp, stat) is not None:
                        return True
            return False

        hasDistance = hasStat(activity, 'Distance')
        hasTimestamp = hasStat(activity, 'Timestamp')
        hasLatitude = hasStat(activity, 'Location.Latitude')
        hasLongitude = hasStat(activity, 'Location.Longitude')
        hasAltitude = hasStat(activity, 'Location.Altitude')
        hasHeartRate = hasStat(activity, 'HR')
        hasCadence = hasStat(activity, 'RunCadence')
        hasTemp = hasStat(activity, 'Temp')

        for lap in activity.Laps:
            lapinfo = {'lapType': self._intensityMappings.get(lap.Intensity, 'general'),
                       'endDuration': (lap.EndTime - activity.StartTime).total_seconds(),
                       'endDistance': lap.Waypoints[-1].Distance / 1000}
            data['laps'].append(lapinfo)
            for wp in lap.Waypoints:
                if hasDistance:
                    recordings['distance'].append(wp.Distance / 1000)
                if hasTimestamp:
                    clock = (wp.Timestamp - activity.StartTime).total_seconds()
                    recordings['clock'].append(int(clock))
                if hasLatitude:
                    recordings['latitude'].append(wp.Location.Latitude)
                if hasLongitude:
                    recordings['longitude'].append(wp.Location.Longitude)
                if hasAltitude:
                    recordings['elevation'].append(wp.Location.Altitude)
                if hasHeartRate:
                    recordings['heartRate'].append(wp.HR)
                if hasCadence:
                    recordings['cadence'].append(wp.RunCadence)
                if hasTemp:
                    recordings['temperature'].append(wp.Temp)

        data['recordingKeys'] = sorted(recordings.keys())
        data['recordingValues'] = [recordings[k] for k in data['recordingKeys']]
        assert len(set(len(v) for v in data['recordingValues'])) == 1
        client.create_activity(data)

    def DeleteCachedData(self, serviceRecord):
        pass

    def DeleteActivity(self, serviceRecord, uploadId):
        pass  # TODO: smashrun doesn't support this yet
