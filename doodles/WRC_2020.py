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
        itinerary = legs = sections = controls = stages = None
        return (itinerary, legs, sections, controls, stages)
    
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
        startList = startListItems = None
        return (startList,startListItems)
    
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
        cars = classes = None
        return (cars, classes)
    
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
# -

# Should we return empty dataframes with appropriate columns, or `None`?
#
# An advantage of returning an empty dataframe with labelled columns is that we can also use the column value list as a test of a returned column.
#
# We need to be consistent so we can have a common, consistent way of dealing with empty responses. This means things like `is None` or `pd.DataFrame().empty` both have to be handled.

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


#How can we look these up?
SEASON_CATEGORIES = {'WRC':"35", "WRC2":"46", "WRC3":"49","JWRC":"58"}


def getSeasonCategory(seasonCategory=SEASON_CATEGORIES['WRC']): 
    """??"""
    args = {"command":"getSeasonCategory",
            "context":{"seasonCategory":seasonCategory}}
    r = s.post(SEASON_URL, data=json.dumps(args))
    if not r.text:
        return None
    return json_normalize(r.json())


# + tags=["active-ipynb"]
# getSeasonCategory()

# + tags=["active-ipynb"]
# SC_COLS = ['id','externalIdDriver','externalIdCoDriver','externalIdManufacturer']
#
# def getChampionshipCodes():
#     """Create dataframe of external championship IDs."""
#     champs=pd.DataFrame()
#
#     for sc in SEASON_CATEGORIES:
#         seasonCategory = SEASON_CATEGORIES[sc]
#
#         champs = champs.append(getSeasonCategory(seasonCategory)[SC_COLS])
#
#     champs.set_index('id', inplace=True)
#     champs.rename(columns={'externalIdDriver':'drivers',
#                            'externalIdCoDriver':'codrivers',
#                            'externalIdManufacturer':'manufacturers'},
#                  inplace=True)
#     return champs
#

# + tags=["active-ipynb"]
# getChampionshipCodes()

# + run_control={"marked": false}
class SeasonBase:
    """Base class for things to do with seasons."""
    def __init__(self, season_external_id=None, autoseed=False):
        self.season_external_id = season_external_id or None
        if not self.season_external_id and autoseed:
            self._check_season_external_id()
            
    def _check_season_external_id(self):
        """Check that season_external_id exists and if not, get one."""
        if not hasattr(self,'season_external_id') or not self.season_external_id:
            #Get current one from active rally
            #It's also available from current_season_events
            event, days, channels = getActiveRallyBase()
            self.event, self.days, self.channels = event, days, channels
            #The returned np.int64 is not JSON serialisable
            self.season_external_id = int(event.loc[0,'season.externalId'])

# TO DO
class Championship(SeasonBase):       
    """Class for championship."""
    def __init__(self ):
        SeasonBase.__init__(self)



# + tags=["active-ipynb"]
# SeasonBase(autoseed=True).season_external_id

# +
def _getChampionshipId(category='WRC', typ='drivers'):
    """Look up external ids for championship by category and championship."""
    champs=getChampionshipCodes()
    championship_activeExternalId = champs.to_dict(orient='index')[int(SEASON_CATEGORIES[category])]
    activeExternalId = championship_activeExternalId[typ]
    return activeExternalId

def getChampionship(category='WRC',typ='drivers',
                    season_external_id=None, ):
    """Get Championship details for specified category and championship.
       If nor season ID is provided, use the external seasonid from the active rally. """
    
    season_external_id = SeasonBase(season_external_id, autoseed=True).season_external_id
    args = {"command":"getChampionship",
            "context":{"season":{"externalId":season_external_id},
                       "activeExternalId":_getChampionshipId(category,typ)}}
    
    r = s.post(SEASON_URL, data=json.dumps(args))

    if not r.text:
        championship = championshipRounds = championshipEntries = None
        return (championship, championshipRounds, championshipEntries)
    
    championship = json_normalize(r.json()).drop(columns=['championshipRounds','championshipEntries'])
    championshipRounds = json_normalize(r.json(), 'championshipRounds' )
    championshipEntries = json_normalize(r.json(), 'championshipEntries')
    return (championship, championshipRounds, championshipEntries)


# + tags=["active-ipynb"]
# (championship, championshipRounds, championshipEntries) = getChampionship('JWRC', 'drivers')
# display(championship)
# display(championshipRounds.head())
# display(championshipEntries.head())

# + tags=["active-ipynb"]
# getChampionshipCodes().to_dict(orient='index')#[int(SEASON_CATEGORIES['JWRC'])]
# -

def getChampionshipStandings(category='WRC',typ='drivers',
                             season_external_id=None, ):
    """Get championship standings."""
    season_external_id = SeasonBase(season_external_id, autoseed=True).season_external_id
    args = {"command":"getChampionshipStandings",
            "context":{
                       "season":{"externalId":season_external_id,
                                 },
                       "activeExternalId":_getChampionshipId(category,typ)}}
    r = s.post(SEASON_URL, data=json.dumps(args))
    
    if not r.text:
        championship_standings = round_results = None
        return (championship_standings, round_results)
    championship_standings = json_normalize(r.json(),'entryResults', meta='championshipId').drop(columns='roundResults')
    round_results = json_normalize(r.json(),['entryResults', 'roundResults'])

    return (championship_standings, round_results)



# + tags=["active-ipynb"]
# championship_standings, round_results = getChampionshipStandings()
# display(championship_standings.head())
# display(round_results.head())

# +
# TO DO - define a class for each table
import warnings


class WRCRally_sdb:
    """Base class for things with an sdbRallyId.
       Can also help find an active sdbRallyId"""
    def __init__(self, sdbRallyId=None, live=False,
                 autoseed=False, nowarn=True,):
        if not nowarn and not sdbRallyId:
            warnings.warn("sdbRallyId should really be set...")
        
        self.sdbRallyId = sdbRallyId or None
        
        if autoseed:
            self._checkRallyId(sdbRallyId)
    
    def _checkRallyId(self, sdbRallyId=None):
        """Return a rally ID or lookup active one."""
        sdbRallyId = sdbRallyId or self.sdbRallyId
        if not hasattr(self, 'sdbRallyId') or self.itinerary
        if not sdbRallyId:
            self.activerally = WRCActiveRally()
            self.sdbRallyId = self.activerally.sdbRallyId
        return self.sdbRallyId


# -

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


# + tags=["active-ipynb"]
# WRCActiveRally()

# + tags=["active-ipynb"]
# zz = WRCRally_sdb(autoseed=True)
# print(zz.sdbRallyId)
# -

# We use the `.fetchData()` method so as to ry not to be greedy. This way, we can define a class and start to work towards only grabbling the data if we need it.

class WRCRetirements(WRCRally_sdb):
    """Class for retirements"""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise retirements class."""
        WRCRally_sdb.__init__(self, sdbRallyId, live, autoseed)
            
        self.retirements=None
        
        if self.sdbRallyId:
            self.fetchData(self.sdbRallyId)
        
    def fetchData(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self.retirements = getRetirements(self.sdbRallyId)
        


# + tags=["active-ipynb"]
# zz=WRCRetirements(autoseed=True)
# zz.retirements.head(3)
# -

class WRCPenalties(WRCRally_sdb):
    """Class for penalties"""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise penalties class."""
        WRCRally_sdb.__init__(self, sdbRallyId, live, autoseed)
            
        self.penalties=None
        
        if self.sdbRallyId:
            self.fetchData(self.sdbRallyId)
        
    def fetchData(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self.penalties = getPenalties(self.sdbRallyId)
        


# + tags=["active-ipynb"]
# zz=WRCPenalties(autoseed=True)
# zz.penalties.head(3)

# +
#rally, eligibilities, groups = getRally(sdbRallyId)
#splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
#stagetimes = getStageTimes(sdbRallyId,stageId)
#stagewinners = getStagewinners(sdbRallyId)
#championship = getChampionship()
#championship = getChampionshipStandings()
# -

class WRCItinerary(WRCRally_sdb):
    """Class for WRC2020 Itinerary."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise itinerary class."""
        WRCRally_sdb.__init__(self, sdbRallyId, live, autoseed)
        
        self.itinerary=None
        self.legs=None
        self.sections=None
        self.controls=None
        self.stages=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
        
    def fetchData(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        
        itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
        self.itinerary, self.legs, self.sections, self.controls, self.stages = itinerary, legs, sections, controls, stages
        


# + tags=["active-ipynb"]
# print(WRCItinerary(autoseed=True).sdbRallyId)

# + tags=["active-ipynb"]
# WRCItinerary(sdbRallyId=100).legs
# -

class WRCStartlist():
    """Class for WRC2020 Startlist table."""
    def __init__(self, startlistId=None, autoseed=True):
        self.startListId = startlistId or None
        
        if not self.startListId:
            warnings.warn("startListId should really be set..")
        
        if self.startListId or autoseed:
            self.fetchData(startListId)
    
    def _checkStartListId(self, startListId=None):
        """Return a startlistId or look one up."""
        self.startListId = startListId or self.startListId
        if not self.startListId:
            if not hasattr(self, 'itinerary') or not self.itinerary:
                self.itinerary = WRCItinerary(autoseed=True)
                self.sdbRallyId = self.itinerary.sdbRallyId
            self.startListId = int(self.itinerary.legs.loc[0,'startListId'])
        return self.startListId
        
        
    def fetchData(self, startListId=None):
        self._checkStartListId(startListId)
        startList,startListItems = getStartlist(self.startListId)
        self.startList, self.startListItems = startList,startListItems


# + tags=["active-ipynb"]
# WRCStartlist(autoseed=True).startList
# -

class WRCCars(WRCRally_sdb):
    """Class for WRC2020 Cars table."""
    def __init__(self, sdbRallyId=None, live=False):  
        WRCRally_sdb.__init__(self, sdbRallyId, live)
        
        self.cars=None
        self.classes=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
            
    def fetchData(self, sdbRallyId=None):
        self._checkRallyId(sdbRallyId)
        cars, classes = getCars(sdbRallyId)
        self.cars, self.classes = cars, classes


WRCCars()


# +
#This class will contain everything about a single rally
class WRCRally(WRCRally_sdb):
    """Class for a rally - stuff where sdbRallyId is required."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False ):
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, live=live,
                             autoseed=autoseed)
        
        self.live = live
        self.itinerary = None
        self.startListId = None
        self.activerally = None
    
    
    def getItinerary(self):
        """Get itinerary.
           If rally not known, use active rally.
           Also set a default startListId."""
        
        _i = self.itinerary = WRCItinerary(self.sdbRallyId)
        
        #Set a default startListId value if required
        if not self.startListId and _i and _i.legs and not _i.legs.empty :
            self.startListId = int(_i.legs.loc[0,'startListId'])
            
        return (_i.itinerary, _i.legs, _i.sections, _i.controls, _i.stages)
 
    def getCars(self):
        """Get cars for a rally.
           If no rally provided, use current one."""
        
        _c = self.cars = WRCCars(self.sdbRallyId)
        
        return (_c.cars, _c.classes)
       
        
    def getStartlist(self, startListId=None):
        """Get startlist.
           If no startListId provided, try to find a default."""
        
        _s = self.startlist = WRCStartlist(self.startListId)
      
        return (_s.startList, _s.startListItems)
    
    
    def getPenalties(self):
        """Get penalties."""
        
        self._penalties = WRCPenalties(self.sdbRallyId)
        self.penalties = self._penalties.penalties
        return self.penalties
     
        
    def getRetirements(self):
        """Get retirements."""
        
        self._retirements = WRCRetirements(self.sdbRallyId)
        self.retirements = self._retirements.retirements
        return self.retirements
     
    
    
    

# +
# NEXT TO DO - active rally class

# + tags=["active-ipynb"]
# zz = WRCRally()
# zz.getRetirements()
# -

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


