# Synchronization module for decathloncoach.com
# (c) 2018 Charles Anssens, charles.anssens@decathlon.com
from tapiriik.settings import WEB_ROOT, DECATHLONCOACH_CLIENT_SECRET, DECATHLONCOACH_CLIENT_ID, DECATHLONCOACH_API_KEY
from tapiriik.services.service_base import ServiceAuthenticationType, ServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.database import cachedb
from tapiriik.services.interchange import UploadedActivity, ActivityType, ActivityStatistic, ActivityStatisticUnit, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity
from lxml import etree
import xml.etree.ElementTree as xml
from django.core.urlresolvers import reverse
from datetime import datetime, timedelta
from urllib.parse import urlencode
import calendar
import requests
import os
import logging
import pytz
import re
import time
import json
from dateutil.parser import parse


logger = logging.getLogger(__name__)

class DecathlonCoachService(ServiceBase):
    ID = "decathloncoach"
    DisplayName = "DecathlonCoach"
    DisplayAbbreviation = "DC"
    AuthenticationType = ServiceAuthenticationType.OAuth
    UserProfileURL = "https://www.decathloncoach.com/fr-fr/portal/?{0}"
    UserActivityURL = "http://www.decathloncoach.com/fr-fr/portal/activities/{1}"
    accountOauth = "https://account.geonaute.com/oauth"
    AuthenticationNoFrame = True  # They don't prevent the iframe, it just looks really ugly.
    PartialSyncRequiresTrigger = False
    LastUpload = None
    
    ApiEndpoint = "https://api.decathlon.net/linkdata"
    OauthEndpoint = "https://account.geonaute.com"
    
    SupportsHR = SupportsCadence = SupportsTemp = SupportsPower = False

    SupportsActivityDeletion = False

    # For mapping common->DecathlonCoach sport id
    _activityTypeMappings = {
        ActivityType.Cycling: "381",
        ActivityType.MountainBiking: "388",
        ActivityType.Hiking: "153",
        ActivityType.Running: "121",
        ActivityType.Walking: "113",
        ActivityType.Snowboarding: "185",
        ActivityType.Skating: "20",
        ActivityType.CrossCountrySkiing: "183",
        ActivityType.DownhillSkiing: "176",
        ActivityType.Swimming: "274",
        ActivityType.Gym: "91",
        ActivityType.Rowing: "263",
        ActivityType.Elliptical: "397",
        ActivityType.RollerSkiing: "367",
        ActivityType.StrengthTraining: "98",
        ActivityType.Climbing: "153",
        ActivityType.Other: "121"
    }

    # For mapping DecathlonCoach sport id->common
    _reverseActivityTypeMappings = {
        "381": ActivityType.Cycling,
        "385": ActivityType.Cycling,
        "401": ActivityType.Cycling,#Home Trainer"
        "388": ActivityType.MountainBiking,
        "121": ActivityType.Running,
        "126": ActivityType.Running,#trail
        "153": ActivityType.Hiking,
        "113": ActivityType.Walking,
        "114": ActivityType.Walking,#nordic walking
        "320": ActivityType.Walking,
        "176": ActivityType.DownhillSkiing,
        "177": ActivityType.CrossCountrySkiing,#Nordic skiing
        "183": ActivityType.CrossCountrySkiing,#Nordic skiing alternatif
        "184": ActivityType.CrossCountrySkiing,#Nordic skiing skating
        "185": ActivityType.Snowboarding,
        "274": ActivityType.Swimming,
        "91": ActivityType.Gym,
        "263": ActivityType.Rowing,
        "98": ActivityType.StrengthTraining,
        "161" : ActivityType.Climbing,
        "397" : ActivityType.Elliptical,
        "367" : ActivityType.RollerSkiing,
        "99" : ActivityType.Other,
        "168": ActivityType.Walking,
        "402": ActivityType.Walking,
        "109":ActivityType.Gym,#pilates
        "174": ActivityType.DownhillSkiing,
        "264" : ActivityType.Other, #bodyboard
        "296" : ActivityType.Other, #Surf
        "301" : ActivityType.Other, #sailling
        "173": ActivityType.Walking, #ski racket
        "110": ActivityType.Cycling,#bike room
        "395": ActivityType.Running,
        "79" : ActivityType.Other, #dansing
        "265" : ActivityType.Other,#Canoë kayak
        "77" : ActivityType.Other,#Triathlon
        "200" : ActivityType.Other,#horse riding
        "273" : ActivityType.Other,#Kite surf
        "280" : ActivityType.Other,#sailbard
        "360" : ActivityType.Other,#BMX"
        "374" : ActivityType.Other,#Skate board
        "260" : ActivityType.Other,#Aquagym
        "45" : ActivityType.Other,#Martial arts
        "335" : ActivityType.Other,#Badminton
        "10" : ActivityType.Other,#Basketball
        "35" : ActivityType.Other,#Boxe
        "13" : ActivityType.Other,#Football
        "18" : ActivityType.Other,#Handball
        "20" : ActivityType.Other,#Hockey
        "284" : ActivityType.Other,#diving
        "398" : ActivityType.Other,#rower machine
        "27" : ActivityType.Other,#Rugby
        "357" : ActivityType.Other,#Tennis
        "32" : ActivityType.Other,#Volleyball
        "399" : ActivityType.Other,#Run & Bike
        "105" : ActivityType.Other,#Yoga
        "354" : ActivityType.Other,#Squash
        "358" : ActivityType.Other,#Table tennis
        "7" : ActivityType.Other,#paragliding
        "400" : ActivityType.Other,#Stand Up Paddle
        "340" : ActivityType.Other,#Padel
        "326" : ActivityType.Other,#archery
        "366" : ActivityType.Other#Yatching
    }
    
    _unitMap = {
        "duration": "24",
        "distance": "5",
        "kcal" : "23",
        "speedaverage" : "9",
        "hrcurrent" : "1",
        "speedcurrent" : "6"
    }

    SupportedActivities = list(_activityTypeMappings.keys())

    def __init__(self):
        return None

    def UserUploadedActivityURL(self, uploadId):
        return "https://www.decathloncoach.com/fr-FR/portal/activities/%d" % uploadId


    def WebInit(self):
        params = {
                  'client_id':DECATHLONCOACH_CLIENT_ID,
                  'response_type':'code',
                  'redirect_uri':WEB_ROOT + reverse("oauth_return", kwargs={"service": "decathloncoach"})}
        self.UserAuthorizationURL = self.OauthEndpoint +"/oauth/authorize?" + urlencode(params)

    def _apiHeaders(self, serviceRecord):
        return {"Authorization": "access_token " + serviceRecord.Authorization["OAuthToken"]}

    def RetrieveAuthorizationToken(self, req, level):
        code = req.GET.get("code")
        params = {"grant_type": "authorization_code", "code": code, "client_id": DECATHLONCOACH_CLIENT_ID, "client_secret": DECATHLONCOACH_CLIENT_SECRET, "redirect_uri": WEB_ROOT + reverse("oauth_return", kwargs={"service": "decathloncoach"})}
 
        response = requests.get(self.accountOauth + "/accessToken", params=params)
        if response.status_code != 200:
            raise APIException("Invalid code")
        data = response.json()
        refresh_token = data["access_token"]
        # Retrieve the user ID, meh.
        id_resp = requests.get( self.OauthEndpoint + "/api/me?access_token=" + data["access_token"])
        return (id_resp.json()["ldid"], {"RefreshToken": refresh_token})

    def RevokeAuthorization(self, serviceRecord):
        resp = requests.get(self.OauthEndpoint + "/logout?access_token="+serviceRecord.Authorization["RefreshToken"])
        if resp.status_code != 204 and resp.status_code != 200:
            raise APIException("Unable to deauthorize DecathlonCoach auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass


    def _getAuthHeaders(self, serviceRecord=None):
        response = requests.get( self.OauthEndpoint + "/api/me?access_token="+serviceRecord.Authorization["RefreshToken"])
        if response.status_code != 200:
            if response.status_code >= 400 and response.status_code < 500:
                raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
            raise APIException("Could not retrieve refreshed token %s %s" % (response.status_code, response.text))
        requestKey = response.json()["requestKey"]
        return {"Authorization": "Bearer %s" % requestKey, 'User-Agent': 'Python Tapiriik Hub' , 'X-Api-Key':DECATHLONCOACH_API_KEY}
        
        
    def _parseDate(self, date):
        #model '2017-12-01T12:00:00+00:00'
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S+%Z").replace(tzinfo=pytz.utc)
        

    def DownloadActivityList(self, svcRecord, exhaustive=False):
        activities = []
        exclusions = []

        now = datetime.now()
        prev = now - timedelta(6*365/12)
        
        period = []
        
        aperiod = "%s%02d-%s%02d"% ( prev.year , prev.month, now.year , now.month)
        period.append(aperiod)
        
        if exhaustive:
            for _ in range(20):
                now = prev
                prev = now - timedelta(6*365/12)
                aperiod = "%s%02d-%s%02d"% (prev.year, prev.month, now.year, now.month)
                period.append(aperiod)
        
        for dateInterval in period:
            headers = self._getAuthHeaders(svcRecord)
            resp = requests.get(self.ApiEndpoint + "/users/" + str(svcRecord.ExternalID) + "/activities.xml?date=" + dateInterval, headers=headers)
            if resp.status_code == 400:
                logger.info(resp.content)
                raise APIException("No authorization to retrieve activity list", block = True, user_exception = UserException(UserExceptionType.Authorization, intervention_required = True))
            if resp.status_code == 401:
                    logger.info(resp.content)
                    raise APIException("No authorization to retrieve activity list", block = True, user_exception = UserException(UserExceptionType.Authorization, intervention_required = True))
            if resp.status_code == 403:
                    logger.info(resp.content)
                    raise APIException("No authorization to retrieve activity list", block = True, user_exception = UserException(UserExceptionType.Authorization, intervention_required = True))
    
    
    
    
            root = xml.fromstring(resp.content)
      
            logger.info("\t\t nb activity : " + str(len(root.findall('.//ID'))))
      
            for ride in root.iter('ACTIVITY'):
    
                activity = UploadedActivity()
                activity.TZ = pytz.timezone("UTC")  

                startdate =  ride.find('.//STARTDATE').text + ride.find('.//TIMEZONE').text
                datebase = parse(startdate)
                
    
                activity.StartTime = datebase#pytz.utc.localize(datebase)
                
                activity.ServiceData = {"ActivityID": ride.find('ID').text, "Manual": ride.find('MANUAL').text}
                
                logger.info("\t\t DecathlonCoach Activity ID : " + ride.find('ID').text)
    
    
                if ride.find('SPORTID').text not in self._reverseActivityTypeMappings:
                    exclusions.append(APIExcludeActivity("Unsupported activity type %s" % ride.find('SPORTID').text, activity_id=ride.find('ID').text, user_exception=UserException(UserExceptionType.Other)))
                    logger.info("\t\tDecathlonCoach Unknown activity, sport id " + ride.find('SPORTID').text+" is not mapped")
                    continue
    
                activity.Type = self._reverseActivityTypeMappings[ride.find('SPORTID').text]
    
                for val in ride.iter('VALUE'):
                    if val.get('id') == self._unitMap["duration"]:
                        activity.EndTime = activity.StartTime + timedelta(0, int(val.text))
                    if val.get('id') ==  self._unitMap["distance"]:
                        activity.Stats.Distance = ActivityStatistic(ActivityStatisticUnit.Meters, value=int(val.text))
                    if val.get('id') ==  self._unitMap["kcal"]:
                        activity.Stats.Energy = ActivityStatistic(ActivityStatisticUnit.Kilocalories, value=int(val.text))
                    if val.get('id') ==  self._unitMap["speedaverage"]:
                        meterperhour = int(val.text)
                        meterpersecond = meterperhour/3600
                        activity.Stats.Speed = ActivityStatistic(ActivityStatisticUnit.MetersPerSecond, avg=meterpersecond, max= None)
    
                if ride.find('LIBELLE').text == "" or ride.find('LIBELLE').text is None:
                    txtdate = startdate.split(' ')
                    activity.Name = "Sport DecathlonCoach " + txtdate[0]
                else:
                    activity.Name = ride.find('LIBELLE').text
                
                activity.Private = False
                activity.Stationary = ride.find('MANUAL').text
                activity.GPS = ride.find('ABOUT').find('TRACK').text
                activity.AdjustTZ()
                activity.CalculateUID()
                activities.append(activity)

        return activities, exclusions



    def DownloadActivity(self, svcRecord, activity):
        activityID = activity.ServiceData["ActivityID"]

        logger.info("\t\t DC LOADING  : " + str(activityID))

        headers = self._getAuthHeaders(svcRecord)
        resp = requests.get(self.ApiEndpoint + "/activity/"+activityID+"/fullactivity.xml", headers = headers)
        if resp.status_code == 401:
            raise APIException("No authorization to download activity", block = True, user_exception = UserException(UserExceptionType.Authorization, intervention_required = True))

        try:
            root = xml.fromstring(resp.content)
        except:
            raise APIException("Stream data returned from DecathlonCoach is not XML")


        lap = Lap(stats = activity.Stats, startTime = activity.StartTime, endTime = activity.EndTime) 
        activity.Laps = [lap]
        lap.Waypoints = []
        
        activity.GPS = False


        #work on date
        startdate =  root.find('.//STARTDATE').text
        timezone =  root.find('.//TIMEZONE').text
        datebase = parse(startdate+timezone)


        for pt in root.iter('LOCATION'):
            wp = Waypoint()
            
            delta = int(pt.get('elapsed_time'))
            formatedDate = datebase + timedelta(seconds=delta)


            wp.Timestamp = formatedDate#self._parseDate(formatedDate.isoformat())
            
            
            wp.Location = Location()
            wp.Location.Latitude = float(pt.find('LATITUDE').text[:8])
            wp.Location.Longitude = float(pt.find('LONGITUDE').text[:8])
            activity.GPS = True
            wp.Location.Altitude = int(pt.find('ELEVATION').text[:8])

            #get the HR value in the Datastream node and measures collection
            for hr in root.iter('MEASURE'):
                if pt.get('elapsed_time') == hr.get('elapsed_time'):
                    for measureValue in hr.iter('VALUE'):
                        if measureValue.get('id') == "1":
                            wp.HR = int(measureValue.text)
                            break
                    break
            
            lap.Waypoints.append(wp)
        activity.Stationary = len(lap.Waypoints) == 0

        return activity

    
    def UploadActivity(self, svcRecord, activity):
        logger.info("UPLOAD To DecathlonCoach Activity tz " + str(activity.TZ) + " dt tz " + str(activity.StartTime.tzinfo) + " starttime " + str(activity.StartTime))
        
        #XML build
        root = etree.Element("ACTIVITY")
        header = etree.SubElement(root, "HEADER")
        etree.SubElement(header, "NAME").text = activity.Name
        etree.SubElement(header, "DATE").text = str(activity.StartTime).replace(" ","T") 
        duration = int((activity.EndTime - activity.StartTime).total_seconds())
        etree.SubElement(header, "DURATION").text =  str(duration)
        
        etree.SubElement(header, "SPORTID").text = self._activityTypeMappings[activity.Type]
        
        etree.SubElement(header, "LDID").text = str(svcRecord.ExternalID)
        etree.SubElement(header, "MANUAL", attrib=None).text = "true"

        summary = etree.SubElement(root,"SUMMARY")
        dataSummaryDuration = etree.SubElement(summary, "VALUE")
        dataSummaryDuration.text = str(int((activity.EndTime - activity.StartTime).total_seconds()))
        dataSummaryDuration.attrib["id"] = self._unitMap["duration"]
    
        if activity.Stats.Distance.Value is not None:
            dataSummaryDistance = etree.SubElement(summary, "VALUE")
            dataSummaryDistance.text = str((int(activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value)))
            dataSummaryDistance.attrib["id"] = self._unitMap["distance"]
        
        if activity.Stats.Energy.Value is not None:
            dataSummaryKcal = etree.SubElement(summary, "VALUE")
            dataSummaryKcal.text = str((int(activity.Stats.Energy.asUnits(ActivityStatisticUnit.Kilocalories).Value)))       
            dataSummaryKcal.attrib["id"] = self._unitMap["kcal"]
            
            
        #Speed average, We accept meter/hour
        if activity.Stats.Speed.Average is not None:
            dataSummarySpeedAvg = etree.SubElement(summary, "VALUE")
            speed_kmh = activity.Stats.Speed.asUnits(ActivityStatisticUnit.KilometersPerHour).Average
            speed_mh = 1000 * speed_kmh
            
            dataSummarySpeedAvg.text = str((int(speed_mh)))       
            dataSummarySpeedAvg.attrib["id"] = self._unitMap["speedaverage"]
        

        datameasure = etree.SubElement(root, "DATA")                                         
        for lap in activity.Laps:
            for wp in lap.Waypoints:
                if wp.HR is not None or wp.Speed is not None or wp.Distance is not None or wp.Calories is not None:
                    oneMeasureLocation = etree.SubElement(datameasure, "MEASURE")
                    oneMeasureLocation.attrib["elapsed_time"] = str(duration - int((activity.EndTime - wp.Timestamp).total_seconds()))
                    if wp.HR is not None:
                        measureHR = etree.SubElement(oneMeasureLocation, "VALUE")
                        measureHR.text = str(int(wp.HR))
                        measureHR.attrib["id"] =  self._unitMap["hrcurrent"]
                    if wp.Speed is not None:
                        measureSpeed = etree.SubElement(oneMeasureLocation, "VALUE")
                        measureSpeed.text = str(int(wp.Speed*3600))
                        measureSpeed.attrib["id"] = self._unitMap["speedcurrent"]
                    if wp.Calories is not None:
                        measureKcaletree = etree.SubElement(oneMeasureLocation, "VALUE")
                        measureKcaletree.text = str(int(wp.Calories))
                        measureKcaletree.attrib["id"] =  self._unitMap["kcal"] 
                    if wp.Distance is not None:
                        measureDistance = etree.SubElement(oneMeasureLocation, "VALUE")
                        measureDistance.text = str(int(wp.Distance))
                        measureDistance.attrib["id"] =  self._unitMap["distance"] 
        
        
        if len(activity.GetFlatWaypoints()) > 0:
            if activity.GetFlatWaypoints()[0].Location.Latitude is not None:
                track = etree.SubElement(root, "TRACK")
                tracksummary = etree.SubElement(track, "SUMMARY")
                etree.SubElement(tracksummary, "LIBELLE").text = ""
                tracksummarylocation = etree.SubElement(tracksummary, "LOCATION")
                tracksummarylocation.attrib["elapsed_time"] = "0"
                etree.SubElement(tracksummarylocation, "LATITUDE").text = str(activity.GetFlatWaypoints()[0].Location.Latitude)[:8]
                etree.SubElement(tracksummarylocation, "LONGITUDE").text = str(activity.GetFlatWaypoints()[0].Location.Longitude)[:8]
                etree.SubElement(tracksummarylocation, "ELEVATION").text = "0"
        
                etree.SubElement(tracksummary, "DISTANCE").text = str(int(activity.Stats.Distance.asUnits(ActivityStatisticUnit.Meters).Value))
                etree.SubElement(tracksummary, "DURATION").text = str(int((activity.EndTime - activity.StartTime).total_seconds()))
                etree.SubElement(tracksummary, "SPORTID").text = "121"
                etree.SubElement(tracksummary, "LDID").text = str(svcRecord.ExternalID)

                
                for wp in activity.GetFlatWaypoints():
                    if wp.Location is None or wp.Location.Latitude is None or wp.Location.Longitude is None:
                        continue  # drop the point
                    #oneLocation = etree.SubElement(track, "LOCATION")
                    oneLocation = etree.SubElement(track,"LOCATION")
                    oneLocation.attrib["elapsed_time"] = str(duration - int((activity.EndTime - wp.Timestamp).total_seconds()))
                    etree.SubElement(oneLocation, "LATITUDE").text = str(wp.Location.Latitude)[:8]
                    etree.SubElement(oneLocation, "LONGITUDE").text = str(wp.Location.Longitude)[:8]
                    if wp.Location.Altitude is not None:
                        etree.SubElement(oneLocation, "ELEVATION").text = str(int(wp.Location.Altitude))
                    else:
                        etree.SubElement(oneLocation, "ELEVATION").text = "0"
    
        activityXML = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")

        headers = self._getAuthHeaders(svcRecord)
        upload_resp = requests.post(self.ApiEndpoint + "/activity/import.xml", data=activityXML, headers=headers)
        if upload_resp.status_code != 200:
            raise APIException("Could not upload activity %s %s" % (upload_resp.status_code, upload_resp.text))
        
        upload_id = None    

        try:
            root = xml.fromstring(upload_resp.content)
            upload_id =  root.find('.//ID').text
        except:
            raise APIException("Stream data returned is not XML")

        return upload_id


    def DeleteCachedData(self, serviceRecord):
        cachedb.decathloncoach_cache.remove({"Owner": serviceRecord.ExternalID})
        cachedb.decathloncoach_activity_cache.remove({"Owner": serviceRecord.ExternalID})

    
    def DeleteActivity(self, serviceRecord, uploadId):
        headers = self._getAuthHeaders(serviceRecord)
        del_res = requests.delete(self.ApiEndpoint + "/activity/+d/summary.xml" % uploadId , headers=headers)
        del_res.raise_for_status()
