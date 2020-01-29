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
#
#
# *TO DO  - add in database elements. Schema will be in `wrcResults2020.sql`*
#
# *Add in `asyncio` scheduler elements.*

from WRC_2020_scraper import *


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
        sdbRallyId = int(sdbRallyId) or self.sdbRallyId
        if not hasattr(self, 'sdbRallyId') or not self.sdbRallyId:
            self.activerally = WRCActiveRally()
            self.sdbRallyId = self.activerally.sdbRallyId
            self.name = self.activerally.name
        return self.sdbRallyId


# -

class WRCActiveRally(WRCRally_sdb):
    """Class for the active rally."""
    def __init__(self, live=False ):
        WRCRally_sdb.__init__(self, live=live, nowarn=True)

        self.live = live
        self.fetchData()
        
    def fetchData(self):
        event, days, channels = getActiveRally()
        self.event, self.days, self.channels = event, days, channels

        #np.int64 is not JSON serialisable
        self.sdbRallyId = int(event.loc[0,'id'])

        self.name = event.loc[0,'name']


# + tags=["active-ipynb"]
# WRCActiveRally().sdbRallyId

# + tags=["active-ipynb"]
# zz = WRCRally_sdb(autoseed=True)
# print(zz.sdbRallyId)
# -

# We use the `.fetchData()` method so as to ry not to be greedy. This way, we can define a class and start to work towards only grabbling the data if we need it.

class WRCRetirements(WRCRally_sdb):
    """Callable class for retirements"""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise retirements class."""
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed)
            
        self.retirements=None
        
        if self.sdbRallyId:
            self.fetchData(self.sdbRallyId)
        
    def fetchData(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self.retirements = getRetirements(self.sdbRallyId)
    
    def __call__(self):
        return self.retirements


# + tags=["active-ipynb"]
# zz=WRCRetirements(autoseed=True)
# zz.retirements.head(3)
# -

class WRCPenalties(WRCRally_sdb):
    """Callable class for penalties."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise penalties class."""
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed)
            
        self.penalties=None
        
        if self.sdbRallyId:
            self.fetchData(self.sdbRallyId)

    
    def fetchData(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self.penalties = getPenalties(self.sdbRallyId)
    
    def __call__(self):
        return self.penalties

# + tags=["active-ipynb"]
# zz=WRCPenalties(autoseed=True)
# zz.penalties.head(3)

# + tags=["active-ipynb"]
# zz.name
# -

zz().head(2)


class WRCItinerary(WRCRally_sdb):
    """Class for WRC2020 Itinerary."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise itinerary class."""
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed)
        
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
        
        itinerary, legs, sections, controls, stages = getItinerary(self.sdbRallyId)
        self.itinerary, self.legs, self.sections, self.controls, self.stages = itinerary, legs, sections, controls, stages
        


# + tags=["active-ipynb"]
# print(WRCItinerary(autoseed=True).sdbRallyId)

# + tags=["active-ipynb"]
# WRCItinerary(sdbRallyId=100).legs
# -

class WRCStartlist():
    """Class for WRC2020 Startlist table."""
    def __init__(self, startListId=None, autoseed=True):
        self.startListId = startListId or None
        
        if not self.startListId:
            warnings.warn("startListId should really be set..")
        
        if self.startListId or autoseed:
            self.fetchData(self.startListId)
    
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
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):  
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed)
        
        self.cars=None
        self.classes=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
            
    def fetchData(self, sdbRallyId=None):
        self._checkRallyId(sdbRallyId)
        cars, classes = getCars(self.sdbRallyId)
        self.cars, self.classes = cars, classes


# + tags=["active-ipynb"]
# WRCCars(autoseed=True)
# -

class WRCRally(WRCRally_sdb):
    """Class for WRC2020 Rally table."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):  
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed)
        
        self.rally=None
        self.eligibilities=None
        self.groups=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
            
    def fetchData(self, sdbRallyId=None):
        self._checkRallyId(sdbRallyId)
        cars, classes = getCars(sdbRallyId)
        rally, eligibilities, groups = getRally(self.sdbRallyId)
        self.rally, self.eligibilities = rally, eligibilities


zz=WRCRally(autoseed=True)
zz.eligibilities


# +
# TO DO
#splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
#stagetimes = getStageTimes(sdbRallyId,stageId)
#stagewinners = getStagewinners(sdbRallyId)
#championship = getChampionship()
#championship = getChampionshipStandings()

# +
#paths=[]
#parts=attr.split('.')
#for i, _ in enumerate(parts):
#    if i:
#        paths.append('.'.join(parts[:-i]))
#paths.reverse()

def _checkattr(obj,attr):
    """Check an object exists and is set to a non-null value."""
    
    #TO DO  - support attributes done a path, checking each step in turn
    
    if hasattr(obj,attr):
        if isinstance(obj.attr, pd.DataFrame):
            return not obj.attr.empty
        elif obj.attr:
            return True
        
    return False


# -

class WRCRally_stage(WRCRally_sdb):
    """Base class for things with a stageId.
       Can also help find a stageId list for a given rally."""
    def __init__(self, sdbRallyId=None, stageId=None, live=False,
                 autoseed=False, nowarn=True,):
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed, nowarn=False)
        
        if not nowarn and not sdbRallyId:
            warnings.warn("sdbRallyId should really be set...")
        if not nowarn and not stageId:
            warnings.warn("stageId should really be set...")
        
        self.sdbRallyId = sdbRallyId or None
        self.stageId = stageId or None
        
        if autoseed:
            self._rallyStages(self.sdbRallyId, self.stageId)
    
    def _rallyStages(self, sdbRallyId=None, stageId=None):
        """Return a stageId list or lookup list of active rally."""
        stageId = stageId or self.stageId
        
        #Have we got an sdbRallyId?
        if not hasattr(self, 'sdbRallyId') or not self.sdbRallyId:
            self.activerally = WRCActiveRally()
            self.sdbRallyId = self.activerally.sdbRallyId
            self.name = self.activerally.name
        
        #The stages are in the stages return value from the itinerary
        #itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
        if not hasattr(self, 'itinerary') or not _checkattr(self,'itinerary.stages'):
            self.itinerary = WRCItinerary(self.sdbRallyId, autoseed=True)
        
        _ccols=['code']+(list(set(self.itinerary.controls.columns) - set(self.itinerary.stages.columns)))
        return (self.sdbRallyId, self.itinerary.stages.merge(self.itinerary.controls[_ccols], on='code'))


zz=WRCRally_stage(autoseed=True)
zz.itinerary.stages # stages / controls
zz._rallyStages()[1]



class WRCSplitTimes(WRCRally_sdb):
    """Class for WRC2020 SplitTimes table."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):  
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId,
                              live=live, autoseed=autoseed)
        
        self.rally=None
        self.eligibilities=None
        self.groups=None
        
        if self.sdbRallyId:
            self.fetchData(sdbRallyId)
            
    def fetchData(self, sdbRallyId=None):
        self._checkRallyId(sdbRallyId)
        cars, classes = getCars(sdbRallyId)
        rally, eligibilities, groups = getRally(self.sdbRallyId)
        self.rally, self.eligibilities = rally, eligibilities







# In the following class:
#
#     - `fetchX` will grab a copy from the WRC API;
#     - `getX` will try to use a copy on the event object, and if it doesnlt exist, try to fetch it before returning it;

#This class will contain everything about a single rally
class WRCEvent(WRCRally_sdb):
    """Class for a rally - stuff where sdbRallyId is required."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False, slurp=False):
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, live=live,
                             autoseed=autoseed)
        
        self.live = live
        self.itinerary = None
        self.startListId = None
        self.activerally = None
    
        if slurp:
            self.rallyslurper()
    
    def _setItinerary(self):
        """Set itinerary.
           If rally not known, use active rally.
           Also set a default startListId."""
        
        self.itinerary = WRCItinerary(self.sdbRallyId, autoseed=True)
        
        _i = self.itinerary
        #If necessary, set a default sdbRallyId
        if not self.sdbRallyId:
             self.sdbRallyId = _i.sdbRallyId
                
        #Set a default startListId value if required
        if not self.startListId and _i and hasattr(_i,'legs') and not _i.legs.empty:
            self.startListId = int(_i.legs.loc[0,'startListId'])

        #return (_i.itinerary, _i.legs, _i.sections, _i.controls, _i.stages)
        
    def _checkItinerary(self):
        """Check itinerary.
           If rally not known, use active rally.
           Also set a default startListId."""
        
        _itinerary_items = ['itinerary', 'legs', 'sections', 'controls', 'stages']
        if not any([hasattr(self, i) for i in _itinerary_items]):
            _setItinerary(self)
        return 
            
    def fetchItinerary(self):
        """Get itinerary.
           If rally not known, use active rally.
           Also set a default startListId."""
        self._setItinerary(self)
        return (self.itinerary, self.legs, self.sections, self.controls, self.stages)
 
    def fetchCars(self):
        """Get cars for a rally.
           If no rally provided, use current one."""
        
        _c = self.cars = WRCCars(self.sdbRallyId)
        
        return (_c.cars, _c.classes)
       
        
    def fetchStartlist(self, startListId=None):
        """Get startlist.
           If no startListId provided, try to find a default."""
        
        _s = self.startlist = WRCStartlist(self.startListId)
      
        return (_s.startList, _s.startListItems)
    
    
    def fetchPenalties(self):
        """Get penalties."""
        
        self.penalties = WRCPenalties(self.sdbRallyId)()
        return self.penalties
     
        
    def fetchRetirements(self):
        """Get retirements."""
        
        self.retirements = WRCRetirements(self.sdbRallyId)()
        return self.retirements
     
    
    def rallyslurper(self):
        """Grab everything..."""
        self.getItinerary()
        self.getCars()
        self.getStartlist()
        self.getPenalties()
        self.getRetirements()
    

# +
# NEXT TO DO - active rally class

# + tags=["active-ipynb"]
# #zz = WRCEvent(slurp=True)

# + tags=["active-ipynb"]
# zz = WRCEvent(autoseed=True)
# zz.getPenalties()
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
class WRC2020(WRCEvent):
    """Class for WRC data scrape using 2020 API."""

    def __init__(self, sdbRallyId=None, live=False):
        WRCEvent.__init__(self, sdbRallyId, live)
        
        self.live = live
        self.currentseasonevents = None
        
    
    def fetchCurrentSeasonEvents(self):
        """Fetch current season events."""
        self.currentseasonevents = WRCCurrentSeasonEvents()
        
    def getCurrentSeasonEvents(self):
        """Get Current  = season events."""
        _current_season_sevents_attrs = ['currentseasonevents', 'current_season_events',
                                         'eventdays', 'eventchannel' ]
        if not any([hasattr(self,a) for a in _current_season_sevents_attrs]) or not self.currentseasonevents:
            _cse = self.currentseasonevents = WRCCurrentSeasonEvents()
        else:
            _cse = self.currentseasonevents
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


