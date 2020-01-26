# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # WRC Scraper  2020

import requests
import json
import pandas as pd
from pandas.io.json import json_normalize

# Cache results in text notebook
import requests_cache
requests_cache.install_cache('wrc_cache',
                             backend='sqlite',
                             expire_after=300)

s = requests.Session()
s.get('https://www.wrc.com')

# + tags=["active-ipynb"]
# # TO DO 
# # There is also an: activeSeasonId":19
# # IS there something we can get there?
# -

#Is this URL constant or picked up relative to each rally?
URL='https://www.wrc.com/ajax.php?contelPageId=176146'


# +
def _getActiveRally(_url=None):
    """Get active rally details."""
    if not _url:
        _url = 'https://www.wrc.com/ajax.php?contelPageId=171091'
    
    args= {"command":"getActiveRally","context":None}
    r = s.post(_url, data=json.dumps(args))
    event = json_normalize(r.json()).drop(columns='eventDays')
    days =  json_normalize(r.json(), 'eventDays').drop(columns='spottChannel.assets')
    channels = json_normalize(r.json(), ['eventDays', 'spottChannel','assets'])
    return (event, days, channels)

def getActiveRallyBase():
    """Get active rally using alt_url """
    event, days, channels = _getActiveRally()
    return (event, days, channels)


# + tags=["active-ipynb"]
# event, days, channels = getActiveRallyBase()
# display(event.head())
# display(days.head())
# display(channels.head())
# display(event.columns)

# +
#Raw https://webappsdata.wrc.com/srv API?
#Need to create separate package to query that API
#Season info
#_url = 'https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22Season%22&maxdepth=1' 
#r = s.get(_url)
#json_normalize(r.json())
# -

def getCurrentSeasonEvents():
    """Get events for current season"""
    _url='https://www.wrc.com/ajax.php?contelPageId=181782'
    #There seems to be a second UTL giving same data?
    #_url='https://www.wrc.com/ajax.php?contelPageId=183400'
    args = {"command":"getActiveSeason","context":None}
    r = s.post(_url, data=json.dumps(args))
    current_season_events = json_normalize(r.json(), ['rallyEvents', 'items'], meta='seasonYear').drop(columns='eventDays')
    eventdays = json_normalize(r.json(), ['rallyEvents', 'items', 'eventDays']).drop(columns='spottChannel.assets')
    eventchannel = json_normalize(r.json(), ['rallyEvents', 'items', 'eventDays', 'spottChannel','assets'])
    return (current_season_events, eventdays, eventchannel)



# + tags=["active-ipynb"]
# current_season_events, eventdays, eventchannel = getCurrentSeasonEvents()
# display(current_season_events.head())
# display(eventdays.head())
# display(eventchannel.head())
# display(current_season_events.columns)
# -

# ## getItinerary

def getActiveRally():
    """Get active rally details."""
    event, days, channels = _getActiveRally(URL)
    return (event, days, channels)


# + tags=["active-ipynb"]
# event, days, channels = getActiveRally()
# display(event)
# display(days)
# display(channels.head())
# -

#This seems to work with sdbRallyId=None, returning active rally?
def getItinerary(sdbRallyId=None):
    """Get itinerary details for specified rally."""
    args = {"command":"getItinerary",
            "context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    
    if not r.text or r.text=='null':
        return None
    
    itinerary = json_normalize(r.json()).drop(columns='itineraryLegs')
    legs = json_normalize(r.json(),'itineraryLegs' )
    if not legs.empty:
        legs = legs.drop(columns='itinerarySections')
        sections = json_normalize(r.json(),['itineraryLegs', 'itinerarySections'] ).drop(columns=['controls','stages'])
        controls = json_normalize(r.json(),['itineraryLegs', 'itinerarySections', 'controls' ] )
        stages = json_normalize(r.json(),['itineraryLegs', 'itinerarySections', 'stages' ] )
    else:
        legs = sections = controls = stages = None
    return (itinerary, legs, sections, controls, stages)


# + tags=["active-ipynb"]
# sdbRallyId = 100
# itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
# display(itinerary.head())
# display(legs.head())
# display(sections.head())
# display(controls.head())
# display(stages.head())
# -

def getStartlist(startListId):
    """Get a startlist given startlist ID."""
    args={'command': 'getStartlist',
          'context': {'activeItineraryLeg': { 'startListId': startListId} }}
    r = s.post(URL, data=json.dumps(args))
    
    if not r.text or r.text=='null':
        return None
    
    startList = json_normalize(r.json()).drop(columns='startListItems')
    startListItems = json_normalize(r.json(), 'startListItems')
    
    return (startList,startListItems)
    


# + tags=["active-ipynb"]
# startListId = 451
# startList,startListItems = getStartlist(startListId)
# display(startList.head())
# display(startListItems.head())
# -

def getCars(sdbRallyId):
    """Get cars for a specified rally."""
    args = {"command":"getCars","context":{"sdbRallyId":100}}
    r = s.post(URL, data=json.dumps(args))
    if not r.text or r.text=='null':
        return None
    
    cars = json_normalize(r.json()).drop(columns='eventClasses')
    classes = json_normalize(r.json(), 'eventClasses')
    return (cars, classes)


# + tags=["active-ipynb"]
# cars, classes = getCars(sdbRallyId)
# display(cars.head())
# display(classes.head())
# cars.head().columns
# -

def getRally(sdbRallyId):
    """Get rally details for specified rally."""
    args = {"command":"getRally","context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    rally = json_normalize(r.json()).drop(columns=['eligibilities','groups'])
    eligibilities = json_normalize(r.json(),'eligibilities')
    groups = json_normalize(r.json(),'groups')
    return (rally, eligibilities, groups)


# + tags=["active-ipynb"]
# rally, eligibilities, groups = getRally(sdbRallyId)
# display(rally.head())
# display(eligibilities.head())
# display(groups.head())
# -

def getOverall(sdbRallyId, stageId):
    """Get overall standings for specificed rally and stage."""
    args = {"command":"getOverall","context":{"sdbRallyId":sdbRallyId,
                                              "activeStage":{"stageId":stageId}}}
    r = s.post(URL, data=json.dumps(args))
    overall = json_normalize(r.json())
    return overall


# + tags=["active-ipynb"]
# stageId = 1528
# overall = getOverall(sdbRallyId, stageId)
# overall.head()
# -

def getSplitTimes(sdbRallyId,stageId):
    """Get split times for specified rally and stage."""
    args = {"command":"getSplitTimes",
            "context":{"sdbRallyId":sdbRallyId, "activeStage":{"stageId":stageId}}}
    r = s.post(URL, data=json.dumps(args))
    splitPoints = json_normalize(r.json(),'splitPoints')
    entrySplitPointTimes = json_normalize(r.json(), 'entrySplitPointTimes').drop(columns='splitPointTimes')
    splitPointTimes = json_normalize(r.json(), ['entrySplitPointTimes','splitPointTimes'])
    return (splitPoints, entrySplitPointTimes, splitPointTimes)


# + tags=["active-ipynb"]
# splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
# display(splitPoints.head())
# display(entrySplitPointTimes.head())
# display(splitPointTimes.head())
# -

def getStageTimes(sdbRallyId,stageId):
    """Get stage times for specified rally and stage"""
    args = {"command":"getStageTimes",
            "context":{"sdbRallyId":sdbRallyId,
                       "activeStage":{"stageId":stageId}}}
    r = s.post(URL, data=json.dumps(args))
    stagetimes = json_normalize(r.json())
    return stagetimes


# + tags=["active-ipynb"]
# stagetimes = getStageTimes(sdbRallyId,stageId)
# stagetimes.head()
# -

def getStagewinners(sdbRallyId):
    """Get stage winners for specified rally."""
    args = {"command":"getStagewinners",
            "context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    stagewinners = json_normalize(r.json())
    return stagewinners


# + tags=["active-ipynb"]
# stagewinners = getStagewinners(sdbRallyId)
# stagewinners.head()

# +
COLS_PENALTIES=['penaltyId','controlId','entryId','penaltyDurationMs','penaltyDuration','reason']
               
def getPenalties(sdbRallyId):
    """Get penalties for specified rally."""
    args = {"command":"getPenalties",
            "context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    
    if not r.text:
        return pd.DataFrame(columns=COLS_PENALTIES)
    
    penalties = json_normalize(r.json())
    return penalties


# + tags=["active-ipynb"]
# penalties = getPenalties(sdbRallyId)
# penalties.head()

# +
COLS_RETIREMENT = ['retirementId','controlId','entryId','reason','retirementDateTime','retirementDateTimeLocal','status']

def getRetirements(sdbRallyId):
    """Get retirements for specified rally."""
    args = {"command":"getRetirements",
            "context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    
    if not r.text:
        return pd.DataFrame(columns=COLS_RETIREMENT)
    
    retirements = json_normalize(r.json())
    return retirements


# -

retirements = getRetirements(sdbRallyId)
retirements.head()

SEASON_URL = 'https://www.wrc.com/ajax.php?contelPageId=186641'


WRC2 
{"command":"getSeasonCategory","context":{"seasonCategory":"46"}}
WRC3 
{"command":"getSeasonCategory","context":{"seasonCategory":"49"}}
JWRC
{"command":"getSeasonCategory","context":{"seasonCategory":"58"}}


def getSeasonCategory(seasonCategory): 
    """??"""
    args = {"command":"getSeasonCategory",
            "context":{"seasonCategory":seasonCategory}}
    r = s.post(SEASON_URL, data=json.dumps(args))
    return json_normalize(r.json())


# + tags=["active-ipynb"]
# SEASON_CATEGORIES = {'WRC':"35", "WRC2":"46", "WRC3":"49","JWRC":"58"}
#
# seasonCategory = SEASON_CATEGORIES['JWRC']
# getSeasonCategory(seasonCategory)

# +
# TO DO  - following is WRC;
# TO DO championship_activeExternalId lookup from getSeasonCategory()
def _getChampionshipId(typ='drivers'):
    championship_activeExternalId = {'drivers':37,
                                    'codrivers':38,
                                    'manufacturers':39}
    activeExternalId = championship_activeExternalId[typ]
    return activeExternalId

def getChampionship(typ='drivers'):
    """Get Championship details for ??"""
    args = {"command":"getChampionship",
            "context":{"season":{"externalId":6},
                       "activeExternalId":_getChampionshipId(typ)}}
    r = s.post(SEASON_URL, data=json.dumps(args))
    
    if not r.text:
        return None
    
    championship = json_normalize(r.json()).drop(columns=['championshipRounds','championshipEntries'])
    championshipRounds = json_normalize(r.json(), 'championshipRounds' )
    championshipEntries = json_normalize(r.json(), 'championshipEntries')
    return (championship, championshipRounds, championshipEntries)

# + tags=["active-ipynb"]
# championship, championshipRounds, championshipEntries = getChampionshipDrivers('manufacturers')
# display(championship)
# display(championshipRounds.head())
# display(championshipEntries.head())
# -



def getChampionshipStandings():
    """??"""

    args = {"command":"getChampionshipStandings",
            "context":{
                       "season":{"externalId":6,
                                 },
                       "activeExternalId":"38"}}
    r = s.post(SEASON_URL, data=json.dumps(args))
    
    if not r.text:
        return None
    championship_standings = json_normalize(r.json(),'entryResults', meta='championshipId').drop(columns='roundResults')
    round_results = json_normalize(r.json(),['entryResults', 'roundResults'])

    return (championship_standings, round_results)


# + tags=["active-ipynb"]
# championship_standings, round_results = getChampionshipStandings()
# display(championship_standings.head())
# display(round_results.head())
# -



def getChampionshipStandingsLive():
    """Get Championship details for ??"""
    args = {"command":"getChampionshipStandingsLive",
            "context":{"sdbRallyId":100,"activeSeasonId":19}}
    r = s.post(URL, data=json.dumps(args))
    championship = json_normalize(r.json())
    return championship


# + tags=["active-ipynb"] run_control={"marked": false}
# championship = getChampionshipStandingsLive()
# championship

# +
# TO DO - define a class for each table
import warnings


class WRCRally_sdb:
    """Base class for things with an sdbRallyId"""
    def __init__(self, sdbRallyId=None, live=False, nowarn=True):
        if not nowarn and not sdbRallyId:
            warnings.warn("sdbRallyId should really be set...")
        self.sdbRallyId = sdbRallyId or None
        


# -

class WRCItinerary(WRCRally_sdb):
    """Class for WRC2020 Itinerary."""
    def __init__(self, sdbRallyId=None, live=False):
        """Initialise itinerary class."""
        WRCRally_sdb.__init__(self, sdbRallyId, live)
        
        self.itinerary=None
        self.legs=None
        self.sections=None
        self.controls=None
        self.stages=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
        
    def fetchData(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        sdbRallyId = sdbRallyId or None
        
        itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
        self.itinerary, self.legs, self.sections, self.controls, self.stages = itinerary, legs, sections, controls, stages
        


WRCItinerary(sdbRallyId=100).legs


class WRCCars(WRCRally_sdb):
    """Class for WRC2020 Cars table."""
    def __init__(self, sdbRallyId=None, live=False):  
        WRCRally_sdb.__init__(self, sdbRallyId, live)
        
        self.cars=None
        self.classes=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
            
    def fetchData(self, sdbRallyId=None):
        cars, classes = getCars(sdbRallyId)
        self.cars, self.classes = cars, classes


WRCCars()


class WRCStartlist():
    """Class for WRC2020 Startlist table."""
    def __init__(self, startListId=None):
        self.startListId = startListId or None
        
        if not self.startListId:
            warnings.warn("startListId should really be set..")
        
        if self.startListId:
            self.fetchData(startListId)
            
    def fetchData(self, startListId=None):
        startList,startListItems = getStartlist(startListId)
        self.startList, self.startListItems = startList,startListItems


WRCStartlist()


class WRCActiveRally(WRCRally_sdb):
    """Class for the active rally."""
    def __init__(self, live=False ):
        WRCRally_sdb.__init__(self, None, live, nowarn=True)

        self.live = live
        self.fetchData()
        
    def fetchData(self):
        event, days, channels = getActiveRally()
        self.event, self.days, self.channels = event, days, channels

        #np.int64 is not JSON serialisable
        self.sdbRallyId = int(event.loc[0,'id'])

        self.name = event.loc[0,'name']


rally, eligibilities, groups = getRally(sdbRallyId)
splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
stagetimes = getStageTimes(sdbRallyId,stageId)
stagewinners = getStagewinners(sdbRallyId)
penalties = getPenalties(sdbRallyId)
retirements = getRetirements(sdbRallyId)
championship = getChampionship()
championship = getChampionshipStandingsLive()


# +
#This class will contain everything about a single rally
class WRCRally(WRCRally_sbd):
    """Class for a rally - stuff where sdbRallyId is required."""
    def __init__(self, sdbRallyId=None, live=False, ):
        WRCRally_sbd.__init__(self, sdbRallyId, live)
        
        self.live = live
        self.itinerary = None
        self.startlistId = None
        self.activerally = None

        
    def _checkRallyId(self, sdbRallyId=None):
        """Return a rally ID or lookup active one."""
        sdbRallyId = sdbRallyId or self.sdbRallyId
        if not sdbRallyId:
            self.activerally = WRCActiveRally()
            self.sdbRallyId = self.activerally.sdbRallyId
        return self.sdbRallyId
    
    
    def getItinerary(self):
        """Get itinerary.
           If rally not known, use active rally.
           Also set a default startlistId."""
        self._checkRallyId()
        
        _i = self.itinerary = WRCItinerary(self.sdbRallyId)
        
        #Set a default startlistId value if required
        if not self.startlistId and _i and not _i.legs.empty :
            self.startlistId = int(_i.legs.loc[0,'startListId'])
            
        return (_i.itinerary, _i.legs, _i.sections, _i.controls, _i.stages)
 
    def getCars(self):
        """Get cars for a rally.
           If no rally provided, use current one."""
        self._checkRallyId()
        
        _c = self.cars = WRCCars(self.sdbRallyId)
        
        return (_c.cars, _c.classes)


        
    def _checkStartlistId(self, startlistId=None):
        """Return a startlistId or look one up."""
        startlistId = startlistId or self.startlistId
        if not startlistId:
            print('setting')
            if not self.itinerary:
                self.getItinerary()
            print('cc',int(self.itinerary.legs.loc[0,'startListId']))
            self.startlistId = int(self.itinerary.legs.loc[0,'startListId'])
        return self.startlistId
        
       
        
    def getStartlist(self, startlistId=None):
        """Get startlist.
           If no startlistId provided, try to find a default."""
        
        self._checkStartlistId(startlistId)
        
        _s = self.startlist = WRCStartlist(self.startlistId)
      
        return (_s.startList, _s.startListItems)
    
    
    
    

# +
# NEXT TO DO - active rally class
# -

zz = WRCRally()
zz.getStartlist()


# TO DO - need a more gernal season events class?
# If, that is, we can we look up arbtrary season events...
class WRCCurrentSeasonEvents:
    """Class for Season events."""
    def __init__(self ):
        self.current_season_events, self.eventdays, self.eventchannel = getCurrentSeasonEvents()



# +
#This class needs renaming...
#What does it actually represent? An event? A live event? A set of events?
class WRC2020(WRCRally):
    """Class for WRC data scrape using 2020 API."""

    def __init__(self, sdbRallyId=None, live=False):
        WRCRally.__init__(self, sdbRallyId, live)
        
        self.live = live
        self.currentseasonevents = None
        
        
    def getCurrentSeasonEvents(self):
        """Get Current season events."""
        if not self.currentseasonevents:
            _cse = self.currentseasonevents = WRCCurrentSeasonEvents()
        return (_cse.current_season_events, _cse.eventdays, _cse.eventchannel)


    
    
    
    

# + tags=["active-ipynb"]
# wrc=WRC2020()
# wrc.getCurrentSeasonEvents()
# wrc.currentseasonevents.current_season_events
# -

wrc.getStartlist()

# + tags=["active-ipynb"]
# wrc.itinerary.sections

# + tags=["active-ipynb"] hideCode=true
# itinerary, legs, sections, controls, stages = wrc.getItinerary()
# startList,startListItems = getStartlist(startListId)
# cars, classes = getCars(sdbRallyId)
# #rally, eligibilities, groups = getRally(sdbRallyId)
# #overall = getOverall(sdbRallyId, stageId)
# #splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
# #stagetimes = getStageTimes(sdbRallyId,stageId)
# #stagewinners = getStagewinners(sdbRallyId)
# #penalties = getPenalties(sdbRallyId)
# #retirements = getRetirements(sdbRallyId)
# #championship = getChampionship()
# #championship = getChampionshipStandingsLive()

# + tags=["active-ipynb"]
# current_season_events, eventdays, eventchannel = wrc.getCurrentSeasonEvents()

# + tags=["active-ipynb"]
# event, days, channels = wrc.getActiveRally()
#

# + tags=["active-ipynb"]
# event

# + tags=["active-ipynb"]
# wrc.sdbRallyId
# -


