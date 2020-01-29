# -*- coding: utf-8 -*-
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
# *TO DO  - add in database elements. Schema will be in `wrcResults2020.sql`.*
#
# There may be duplication of data across various objects in the final class. This is for convenience as much as anything, trying to preserve both captures of the original data with combined or derived data, as well as trying to put the data in places we can easily find it. Consistency in a live setting is where this may fall apart!
#
# *Add in `asyncio` scheduler elements to call WRC API regularly. Avoid race conditions by scheduling items together in the same scheduled event if they compete for the same write resource.*

# + tags=["active-ipynb"]
# %load_ext autoreload
# %autoreload 2
# -

from WRC_2020_scraper import *
from WRC_2020_scraper import getSeasonCategories


# +

# TO DO - this should go into a general utils package
def _jsInt(val):
    """Ensure we have a JSON serialisable value for an int.
       The defends against non-JSON-serialisable np.int64."""
    try:
        val = int(val)
    except:
        val = None
        
    return val


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
        objattr = getattr(obj, attr)
        if isinstance(objattr, pd.DataFrame):
            return not objattr.empty
        elif objattr:
            return True
        
    return False


# +
# TO DO - define a class for each table
import warnings

#https://stackoverflow.com/questions/28208949/log-stack-trace-for-python-warning/29567225
import traceback

_formatwarning = warnings.formatwarning


# -

# The following generates stacktrace warnings useful when debugging in IPython notebook kernel environment.

# +
def formatwarning_tb(*args, **kwargs):
    s = _formatwarning(*args, **kwargs)
    
    _tb = traceback.format_stack()
    useful=False
    tb=[]
    for i in _tb:
        if 'ipython-input' in i:
            tb.append(i)
    s += ''.join(tb[:-1])
    return s

warnings.formatwarning = formatwarning_tb
#logging.captureWarnings(True)

    
class IPythonWarner:
    """Tools for reporting warnings."""
    def __init__(self,nowarn=False):
        self.nowarn = nowarn or None
    
    def warner(self, cond, msg, nowarn=None):
        """Test a condition and if True display a warning message."""
        if nowarn is None:
            nowarn=self.nowarn
            
        if not nowarn and cond:
            warnings.warn(msg)
        


# +
# TO DO

# how about a SqliteDB class that can provde db methods. Use sqlite_utils.
# upserter is most important, so can save each table: upsert(table, df, key)
# upserter may require col renaming or additional cols to df before upsert
# -

class WRCBase(IPythonWarner):
    "Base class for all WRC stuff."
    def __init__(self, nowarn=True):
        IPythonWarner.__init__(self, nowarn=nowarn)
        
    def _null():
        pass


# + run_control={"marked": false}
class WRCSeasonBase(WRCBase):
    """Base class for things related to with seasons."""
    def __init__(self, season_external_id=None, autoseed=False, nowarn=True):
        WRCBase.__init__(self, nowarn=nowarn)
        
        self.season_external_id = _jsInt(season_external_id)
        if not self.season_external_id and autoseed:
            self._check_season_external_id()
            
    def _check_season_external_id(self, season_external_id=None):
        """Check that season_external_id exists and if not, get one."""
        self.season_external_id =  _jsInt(season_external_id) or self.season_external_id
        if not _checkattr(self,'season_external_id'):
            #Get current one from active rally
            #It's also available from current_season_events
            event, days, channels = getActiveRally()
            self.event, self.days, self.channels = event, days, channels
            #The returned np.int64 is not JSON serialisable
            self.season_external_id = _jsInt(event.loc[0,'season.externalId'])



# + tags=["active-ipynb"]
# WRCSeasonBase(autoseed=True)#.season_external_id
# -

class WRCSeasonCategories(WRCSeasonBase):
    def __init__(self, autoseed=False, nowarn=True):
        WRCSeasonBase.__init__(self, nowarn=nowarn)
        
        self.season_categories = None
        self.championship_codes = None
        
        if autoseed:
            self.fetchSeasonCategories()
            
    def fetchSeasonCategories(self):
        self.season_categories = getSeasonCategories()
        self.championship_codes = getSeasonChampionshipCodes()


# + tags=["active-ipynb"]
# zz = WRCSeasonCategories()
# zz.fetchSeasonCategories()
# zz.championship_codes
# -

class WRCChampionship(WRCSeasonCategories):       
    """Class for championship."""
    def __init__(self, category='WRC', typ='drivers',
                 season_external_id = None,
                 autoseed=False, nowarn=True ):
        WRCSeasonCategories.__init__(self, autoseed=False, nowarn=True)

        self.championships = {}
        
        if autoseed:
            fetchChampionship(self, category=category, typ=typ,
                              season_external_id=season_external_id)
        
    def fetchChampionship(self, category='WRC',typ='drivers', season_external_id=None):
        self._check_season_external_id(season_external_id)
        _c = self.championships
        if category not in _c:
            _c[category] = {}
        _cc = _c[category]
        if typ not in _cc:
            _cc[typ] =  {}
        _cct = _cc[typ]
        (_cct['championship'], _cct['championshipRounds'], \
         _cct['championshipEntries']) = getChampionship(category=category,
                                                        typ=typ, season_external_id=season_external_id)


# + tags=["active-ipynb"]
# zz = WRCChampionship()
# zz.fetchChampionship()
# -

class WRCChampionshipStandings(WRCSeasonCategories):       
    """Standings for a championship."""
    def __init__(self, category='WRC', typ='drivers',
                 season_external_id = None,
                 autoseed=False, nowarn=True ):
        WRCSeasonCategories.__init__(self, autoseed=False, nowarn=True)
        
        self.championship_standings = {}
        
        if autoseed:
            fetchChampionshipStandings(self,category=category, typ=typ,
                                       season_external_id=season_external_id)
            
        
    def fetchChampionshipStandings(self, category='WRC',typ='drivers', season_external_id=None):
        self._check_season_external_id(season_external_id)
        _c = self.championship_standings
        if category not in _c:
            _c[category] = {}
        _cc = _c[category]
        if typ not in _cc:
            _cc[typ] =  {}
        _cct = _cc[typ]
        (_cct['championship_standings'], \
         _cct['round_results']) = getChampionshipStandings(category=category,
                                                           typ=typ, season_external_id=season_external_id)

# + tags=["active-ipynb"]
# zz = WRCChampionshipStandings()
# zz.fetchChampionshipStandings('JWRC')
# zz.fetchChampionshipStandings('WRC')
# zz.championship_standings
# -



# TO DO - need a more general season events class?
# If, that is, we can we look up arbitrary season events...
class WRCActiveSeasonEvents:
    """Class for Season events."""
    def __init__(self, autoseed=False):
        
        if autoseed:
            self.fetchActiveSeasonEvents()
            
    def fetchActiveSeasonEvents(self):
        self.current_season_events, self.eventdays, self.eventchannel = getActiveSeasonEvents()



# + tags=["active-ipynb"]
# WRCActiveSeasonEvents(autoseed=True).current_season_events.head()
# -

class WRCRally_sdb(WRCBase):
    """Base class for things with an sdbRallyId.
       Can also help find an active sdbRallyId"""
    def __init__(self, sdbRallyId=None,
                 autoseed=False, nowarn=True,):
        WRCBase.__init__(self, nowarn=nowarn)
        
        self.warner(not sdbRallyId, "sdbRallyId should really be set...")
        
        self.sdbRallyId = _jsInt(sdbRallyId)
        
        if autoseed:
            self._checkRallyId(sdbRallyId)
    
    def _checkRallyId(self, sdbRallyId=None):
        """Return a rally ID or lookup active one."""
        
        sdbRallyId = _jsInt(sdbRallyId) or self.sdbRallyId
        if not hasattr(self, 'sdbRallyId') or not self.sdbRallyId:
            self.activerally = WRCActiveRally()
            self.sdbRallyId = self.activerally.sdbRallyId
            self.name = self.activerally.name
        return self.sdbRallyId


# + tags=["active-ipynb"]
# WRCRally_sdb(nowarn=False).nowarn
# -

class WRCLive(WRCBase):
    """Base class for live rallies."""
    def __init__(self, live=False):
        WRCBase.__init__(self)
        
        self.live = live


class WRCActiveRally(WRCRally_sdb, WRCLive):
    """Class for the active rally."""
    def __init__(self, live=False ):
        WRCRally_sdb.__init__(self, nowarn=True)
        WRCLive.__init__(self, live=live)

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
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
            
        self.retirements=None
        
        if self.sdbRallyId:
            self.fetchRetirements(self.sdbRallyId)
        
    def fetchRetirements(self, sdbRallyId=None):
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
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
            
        self.penalties=None
        
        if self.sdbRallyId:
            self.fetchPenalties(self.sdbRallyId)

    
    def fetchPenalties(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self.penalties = getPenalties(self.sdbRallyId)
    
    def __call__(self):
        return self.penalties


# + tags=["active-ipynb"]
# zz=WRCPenalties(autoseed=True)
# zz.penalties.head(3)
# -

class WRCStagewinners(WRCRally_sdb):
    """Callable class for penalties."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise penalties class."""
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
            
        self.stagewinners=None
        
        if self.sdbRallyId:
            self.fetchStagewinners(self.sdbRallyId)

    
    def fetchStagewinners(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self.stagewinners = getStagewinners(self.sdbRallyId)
    
    def __call__(self):
        return self.stagewinners

# + tags=["active-ipynb"]
# zz=WRCStagewinners()
# zz.fetchStagewinners()
# zz.stagewinners.head()
# -



class WRCItinerary(WRCRally_sdb, WRCLive):
    """Class for WRC2020 Itinerary."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        """Initialise itinerary class."""
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
        WRCLive.__init__(self, live=live)
        
        self.itinerary=None
        self.legs=None
        self.sections=None
        self.controls=None
        self.stages=None
        
        if self.sdbRallyId:
            self.fetchItinerary(sdbRallyId)
    
    def _checkItinerary(self):
        """Check itinerary.
           If rally not known, use active rally.
           Also set a default startListId."""
        
        _itinerary_items = ['itinerary', 'legs', 'sections', 'controls', 'stages']
        if not any([hasattr(self, i) for i in _itinerary_items]):
            self.fetchItinerary()

    
    def fetchItinerary(self, sdbRallyId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self._checkItinerary()
        self.itinerary, self.legs, self.sections, self.controls, self.stages = getItinerary(self.sdbRallyId)
        
        _ccols=['code']+(list(set(self.controls.columns) - set(self.stages.columns)))
        self.richstages=self.stages.merge(self.controls[_ccols], on='code')
            

# + tags=["active-ipynb"]
# print(WRCItinerary(autoseed=True).sdbRallyId)

# + tags=["active-ipynb"]
# WRCItinerary(sdbRallyId=100).legs
# -

class WRCStartlist(WRCLive):
    """Class for WRC2020 Startlist table."""
    def __init__(self, startListId=None, live=False, autoseed=True, nowarn=False):
        WRCLive.__init__(self, live=live)
        
        self.startListId = _jsInt(startListId)
        
        self.startList = None
        self.startListItems = None
        
        if not nowarn and not self.startListId:
            warnings.warn("startListId should really be set..")
        
        if self.startListId or autoseed:
            self.fetchStartList(self.startListId)
    
    def _checkStartListId(self, startListId=None):
        """Return a startlistId or look one up."""
        self.startListId = startListId or self.startListId
        if not self.startListId:
            if not _checkattr(self, 'itinerary'):
                self.itinerary = WRCItinerary(autoseed=True)
                self.sdbRallyId = self.itinerary.sdbRallyId
            self.startListId = int(self.itinerary.legs.loc[0,'startListId'])
        return self.startListId
        
        
    def fetchStartList(self, startListId=None):
        self._checkStartListId(startListId)
        startList,startListItems = getStartlist(self.startListId)
        self.startList, self.startListItems = startList,startListItems


# + tags=["active-ipynb"]
# zz = WRCStartlist(autoseed=True)
# zz.fetchStartList()
# getStartlist(451)
# -

class WRCCars(WRCRally_sdb):
    """Class for WRC2020 Cars table."""
    def __init__(self, sdbRallyId=None, live=False, autoseed=False):  
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
        
        self.cars=None
        self.classes=None
        
        if self.sdbRallyId:
            self.fetchCars(sdbRallyId)
            
    def fetchCars(self, sdbRallyId=None):
        self._checkRallyId(sdbRallyId)
        cars, classes = getCars(self.sdbRallyId)
        self.cars, self.classes = cars, classes


# + tags=["active-ipynb"]
# WRCCars(autoseed=True)
# -

class WRCRally(WRCRally_sdb):
    """Class for WRC2020 Rally table. This gives external ids."""
    def __init__(self, sdbRallyId=None, autoseed=False):  
        WRCRally_sdb.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
        
        self.rally=None
        self.eligibilities=None
        self.groups=None
        
        if self.sdbRallyId:
            self.fetchRally(sdbRallyId)
            
    def fetchRally(self, sdbRallyId=None):
        print('fetching')
        self._checkRallyId(sdbRallyId)
        (self.rally, self.eligibilities, self.groups) = getRally(self.sdbRallyId)
        
    # TO DO - define iterators?


zz=WRCRally(autoseed=True)
display(zz.rally)
zz.eligibilities


# +

# TO DO - have a check stages function to get some data...

class WRCRally_stages(WRCItinerary):
    """Class referring to all rally stages."""
    def __init__(self, sdbRallyId=None, live=False,
                 autoseed=False, nowarn=True,):
        WRCItinerary.__init__(self, sdbRallyId=None, live=False, autoseed=autoseed)
        
        self.sdbRallyId = _jsInt(sdbRallyId)
        
        if autoseed:
            self._checkStages(self.sdbRallyId)

    def _checkStages(self, sdbRallyId=None):
        """Return a stages list or lookup list for active rally."""
        #Have we got an sdbRallyId?
        if not hasattr(self, 'sdbRallyId') or not self.sdbRallyId:
            self.activerally = WRCActiveRally()
            self.sdbRallyId = self.activerally.sdbRallyId
            self.name = self.activerally.name

        self._checkItinerary()
        
    def lastCompletedStage(self):
        # need to check etc
        return self.stages[self.stages['status']=='Completed'].iloc[-1]['stageId']
    
    def stagesIterator(sdbRallyId=None):
        pass


# -

zz=WRCRally_stages(autoseed=True)
zz.richstages.head() # stages / controls
#zz._checkStages()[1].head()
#zz.lastCompletedStage()

# +
# Does this actually do anything other than possible checks?

class WRCRally_stage(WRCRally_stages):
    """Base class for things with a stageId.
       Can also help find a stageId list for a given rally."""
    def __init__(self, sdbRallyId=None, stageId=None, live=False,
                 autoseed=False, nowarn=True):
        WRCRally_stages.__init__(self, sdbRallyId=sdbRallyId,
                                 live=live, autoseed=autoseed, nowarn=nowarn)
        
        if not nowarn:
            if not sdbRallyId:
                warnings.warn("sdbRallyId should really be set...")
            if not stageId:
                warnings.warn("stageId should really be set...")

        stageId = _jsInt(stageId)
        
        if autoseed:
            fetchData(self.sdbRallyId, stageId)
 
    def _checkStageId(self, sdbRallyId=None, stageId=None, fallback='lastCompleted'):
        """Return a stage ID or lookup a current one."""
        
        self._checkRallyId(sdbRallyId)
        
        stageId = _jsInt(stageId)
        
        #Obe methid for finding a stage ID - last completed
        if not stageId:
            if 'lastCompleted':
                stageId  = self.lastCompletedStage()
            #What else? Most recent still running, else lastCompleted?
        

        #sdbRallyId = sdbRallyId or self.sdbRallyId
        #if not hasattr(self, 'sdbRallyId') or not self.sdbRallyId:
        #    self.activerally = WRCActiveRally()
        #    self.sdbRallyId = self.activerally.sdbRallyId
        #    self.name = self.activerally.name
        #return self.sdbRallyId
        pass


# + tags=["active-ipynb"]
# zz=WRCRally_stage()
# -

# # TO DO
#
# For things built on `WRCRally_stage` class, we need a way of iterating over all stages. Ideally, one way, as a method in `WRCRally_stage`.
#
# The `stages` table from `itinerary` is probably best to go with?

class WRCOverall(WRCRally_stage):
    """Class for overall stage table."""
    def __init__(self, sdbRallyId=None, stageId=None, live=False,
                 autoseed=False, nowarn=True):
        WRCRally_stage.__init__(self, sdbRallyId=sdbRallyId, stageId=stageId,
                                live=live, autoseed=autoseed, nowarn=nowarn)
        
        self.overall={}

        if stageId:
            self.fetchOverall(self.sdbRallyId, stageId)

        if autoseed:
            pass

    def fetchOverall(self, sdbRallyId=None, stageId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self._checkStageId(self.sdbRallyId, stageId)

        if stageId:
            self.overall[stageId] = getOverall(self.sdbRallyId, stageId)
    
    def __call__(self):
        return self.overall


# + tags=["active-ipynb"]
# zz=WRCOverall(stageId = 1528)
# zz.fetchOverall()
# zz.overall
# -

class WRCStageTimes(WRCRally_stage):
    """Class for stage times table."""
    def __init__(self, sdbRallyId=None, stageId=None, live=False,
                 autoseed=False, nowarn=True):
        WRCRally_stage.__init__(self, sdbRallyId=sdbRallyId, stageId=stageId,
                                live=live, autoseed=autoseed, nowarn=nowarn)
                         
        self.stagetimes={}

        if stageId:
            self.fetchStageTimes(self.sdbRallyId, stageId)

        if autoseed:
            pass

    def fetchStageTimes(self, sdbRallyId=None, stageId=None):
        """Fetch the data from WRC API."""
        self._checkRallyId(sdbRallyId)
        self._checkStageId(self.sdbRallyId, stageId)
        
        if stageId:
            self.stagetimes[stageId] = getStageTimes(self.sdbRallyId, stageId)
    
    def __call__(self):
        return self.stagetimes


# + tags=["active-ipynb"]
# zz=WRCStageTimes(stageId = 1528)
# zz.fetchStageTimes()
# zz.stagetimes
# -

class WRCSplitTimes(WRCRally_stage):
    """Class for SplitTimes stage table."""
    def __init__(self, sdbRallyId=None, stageId=None, live=False, autoseed=False):  
        WRCRally_stage.__init__(self, sdbRallyId=sdbRallyId, stageId=stageId,
                                 live=live, autoseed=autoseed)
        
        self.splitPoints = {}
        self.entrySplitPointTimes ={}
        self.splitPointTimes = {}
        
        if stageId:
            self.fetchSplitTimes(self.sdbRallyId, stageId)

        if autoseed:
            pass
            
    def fetchSplitTimes(self, sdbRallyId=None, stageId=None):
        self._checkRallyId(sdbRallyId)
        self._checkStageId(self.sdbRallyId, stageId)
        if stageId:
            (self.splitPoints[stageId], self.entrySplitPointTimes[stageId], \
             self.splitPointTimes[stageId]) = getSplitTimes(self.sdbRallyId, stageId)


# + tags=["active.ipynb"]
zz=WRCSplitTimes(stageId = 1528)
zz.fetchSplitTimes()
zz.entrySplitPointTimes

# -





# TO DO - think about sqlite export.

# + run_control={"marked": false}
def WRCdatagetter(func):
    """Decorator to run a passed in function then return a response.
       Originally included other logic..."""
    def call(self):
        (attrs, func2) = func(self)
        if not attrs:
            return None
        if isinstance(attrs, str):
            attrs=[attrs]
        #Don't call the function if all the attributes that would be set
        # by calling it are already set.
        if not all([_checkattr(self,a) for a in attrs]):
            func2()
        if len(attrs)>1:
            return tuple(getattr(self,a) for a in attrs)
        return getattr(self,attrs[0])
    return call

        
#This class will contain everything about a single rally
class WRCEvent(WRCItinerary, WRCCars, WRCPenalties, WRCRetirements, WRCStartlist,
               WRCRally, WRCStagewinners, WRCOverall, WRCStageTimes, WRCSplitTimes ):
    """Class for a rally event.
       Can be used to contain all the timing results data from a WRC rally weekend."""
    def __init__(self, sdbRallyId=None, stageId=None, live=False, autoseed=False, slurp=False):
        WRCItinerary.__init__(self, sdbRallyId=sdbRallyId, live=live,
                             autoseed=autoseed)
        WRCCars.__init__(self, sdbRallyId=sdbRallyId, live=live,
                             autoseed=autoseed)
        WRCPenalties.__init__(self, sdbRallyId=sdbRallyId, live=live,
                             autoseed=autoseed)
        WRCRetirements.__init__(self, sdbRallyId=sdbRallyId, live=live,
                             autoseed=autoseed)
        WRCStartlist.__init__(self, startListId=None, live=live,
                             autoseed=autoseed, nowarn=True)
        WRCRally.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
        WRCStagewinners.__init__(self, sdbRallyId=sdbRallyId, autoseed=autoseed)
        WRCOverall.__init__(self, sdbRallyId=sdbRallyId, stageId=stageId, autoseed=autoseed)
        WRCStageTimes.__init__(self, sdbRallyId=sdbRallyId, stageId=stageId, autoseed=autoseed)
        WRCSplitTimes.__init__(self, sdbRallyId=sdbRallyId, stageId=stageId, autoseed=autoseed)
        
        if slurp:
            self.rallyslurper()

    

    def getRally(self):
        """Get external rally details."""
        
        attrs=['rally','eligibilities', 'groups']
        if not all([_checkattr(self,a) for a in attrs]):
            print('hello')
            self.fetchRally()
        return tuple(getattr(self,a) for a in attrs)
    

    def getItinerary(self):
        """Get itinerary.
           If rally not known, use active rally.
           Also set a default startListId."""
        self.fetchItinerary()
        attrs = ['itinerary', 'legs', 'sections','controls', 'stages']
        return tuple(getattr(self,a) for a in attrs)
       

    def getStartlist(self, startListId=None):
        """Get startlist.
           If no startListId provided, try to find a default."""
        self.fetchStartList()
        attrs=['startList', 'startListItems']
        return tuple(getattr(self,a) for a in attrs)

    def getPenalties(self):
        """Get penalties."""
        self.fetchPenalties()
        return self.penalties
    
    def getRetirements(self):
        """Get retirements."""
        self.fetchRetirements()
        return self.retirements

    
    def getStagewinners(self):
        """Get stagewinners"""
        self.fetchStagewinners()
        return self.stagewinners
                
                
    def getCars(self):
        """Get cars."""
        self.fetchCars()
        attrs=['cars', 'classes']
        return tuple(getattr(self,a) for a in attrs)

    #TO DO - different decorator
    def getOverall(self, stageId=None):
        """Get Overall."""
        attrs = ['overall']
        self._checkRallyId()
        self._checkStages()
            
        self.fetchOverall(self.sdbRallyId, stageId)
        
        return self.overall
    
    
    def getStageTimes(self, stageId=None):
        """Get StageTimes."""
        attrs = ['stagetimes']
        self._checkRallyId()
        self._checkStages()
            
        self.fetchStageTimes(self.sdbRallyId, stageId)
        return self.stagetimes

    def getSplitTimes(self, stageId=None):
        """Get SplitTimes."""
        attrs = ['splitPoints','entrySplitPointTimes','splitPointTimes']
        self._checkRallyId()
        self._checkStages()
        
        self.fetchSplitTimes(self.sdbRallyId, stageId)

        return (self.splitPoints, self.entrySplitPointTimes, self.splitPointTimes)

    
    def rallyslurper(self):
        """Grab everything..."""
        self.getItinerary()
        self.getCars()
        self.getStartlist()
        self.getPenalties()
        self.getRetirements()
    

# + tags=["active-ipynb"]
# zz=WRCEvent()
# zz.getRally()
# -

zz.getRally()

attrs=['rally','eligibilities', 'groups']
all([_checkattr(zz,a) for a in attrs])


# +
#This class needs renaming...
#What does it actually represent? An event? A live event? A set of events?

# TO DO - this presumably is wrong if we call in in 2021?
class WRC2020(WRCActiveSeasonEvents, WRCEvent):
    """Class for WRC data scrape using 2020 API."""

    def __init__(self, sdbRallyId=None, live=False, autoseed=False):
        WRCActiveSeasonEvents.__init__(self, autoseed=autoseed)
        WRCEvent.__init__(self, sdbRallyId, live)
        
        self.live = live
        
        
    def getActiveSeasonEvents(self):
        """Get active (current) season events."""
        _current_season_events_attrs = ['current_season_events',
                                         'eventdays', 'eventchannel' ]
        if not any([hasattr(self,a) for a in _current_season_events_attrs]) or not _checkattr(self,'current_season_events'):
            self.fetchActiveSeasonEvents()
        return (self.current_season_events, self.eventdays, self.eventchannel)


    
    
    
    
# -

zz = WRC2020().getActiveSeasonEvents()
#zz

# + tags=["active-ipynb"]
# #zz = WRCEvent(slurp=True)

# + tags=["active-ipynb"]
# zz = WRCEvent(autoseed=True)
# zz.getPenalties()

# + tags=["active-ipynb"]
# wrc=WRC2020()
# wrc.getActiveSeasonEvents()
# wrc.activeseasonevents.current_season_events
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
# current_season_events, eventdays, eventchannel = wrc.getActiveSeasonEvents()

# + tags=["active-ipynb"]
# event, days, channels = wrc.getActiveRally()
#

# + tags=["active-ipynb"]
# event

# + tags=["active-ipynb"]
# wrc.sdbRallyId
# -


