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

# # Functions for grabbing data from WRC API
#
# This package contains a range of functions for grabbing and parsing live timing results data from the WRC website via a simple JSON API that is used to generate the official WRC live timing results web pages.
#
#
# TO DO - consider a scraper class with a requests session embedded in it.

import requests
import warnings
import json
import pandas as pd
from pandas.io.json import json_normalize

# Cache results in text notebook
import requests_cache
requests_cache.install_cache('wrc_cache',
                             backend='sqlite',
                             expire_after=30000)


# + tags=["active-ipynb"]
# # TO DO 
# # There is also an: activeSeasonId":19
# # IS there something we can get there?
# -

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
def _isnull(obj):
    """Check an object is null."""
    if isinstance(obj, pd.DataFrame):
        return obj.empty
    elif isinstance(obj, str) and obj.lower()=='null':
        return True
    elif obj:
        return False
    return True

def _notnull(obj):
    """Check an object is not null."""
    return not _isnull(obj)

def _checkattr(obj,attr):
    """Check an object exists and is set to a non-null value."""
    
    #TO DO  - support attributes done a path, checking each step in turn
    
    if hasattr(obj,attr):
        objattr = getattr(obj, attr)
        return _notnull(objattr)
        
    return False


# -

#Is this URL constant or picked up relative to each rally?
URL='https://www.wrc.com/ajax.php?contelPageId=176146'


# +
def _getresponse(_url, args, ss={'conn':None}, secondtry=False):
    """Simple function to get response from a post request."""
    r=None
    if ss['conn'] is None or secondtry:
        try:
            ss['conn'] = requests.Session()
            ss['conn'].get('https://www.wrc.com')
        except:
            return None
        
    try:
        r = ss['conn'].post(_url, data=json.dumps(args))
    except: #requests.exceptions.ConnectionError:
        if not secondtry:
            #If there's an error, try once again
            try:
                _getresponse(_url, args, secondtry=True)
            except:
                return None
        else:
            return None
            
    return r

def _get_and_handle_response(_url, args, func, nargs=1, raw=False, renamecols=None,
                             extracols=None, dropcols=None):
    """Make a request to the API and then return a raw string
       or parse the response with a provided parser function."""
   
    def _add_cols(response, extracols):
        """Add extra columns to each dataframe."""
        if _isnull(response):
            return
        
        dupes=set(response.columns).intersection(extracols.keys())

        if dupes:
            warnings.warn(f"Trying to add pre-existing cols: {', '.join(dupes)}")
        for k in extracols:
            response[k]=extracols[k]
        
                          
    r =  _getresponse(_url,args)
    if raw or not callable(func):
        return r.text
    

    #Make sure we return the desired number of None items in a tuple as a null response
    if not r or r is None or not r.text or r.text=='null':
        return tuple([None for i in range(nargs)])
    
    response = func(r)
    
    if renamecols is None:
        renamecols={}
    if extracols is None:
        extracols={}
        
    #The dataframe type check can help if we have the wrong number of args
    #Could display a warning that the nargs is incorrect if so
    if nargs==1 or isinstance(response, pd.DataFrame):
        if renamecols:
            response.rename(columns=renamecols, inplace=True)
        if extracols:
            _add_cols(response, extracols)
        if dropcols:
            response.drop(columns=dropcols,inplace=True, errors='ignore')
    else:
        for i in range(nargs):
            if renamecols:
                _cols = set(response[i].columns).intersection(renamecols.keys())
                #couldn't we just errors='ignore?
                response[i].rename(columns={k:renamecols[k] for k in _cols}, inplace=True)
            if extracols:
                _add_cols(response[i], extracols)
            if dropcols:
                response[i].drop(columns=dropcols,inplace=True, errors='ignore')
    
    return response
# -

ACTIVE_RALLY_URL = 'https://www.wrc.com/ajax.php?contelPageId=171091'


# +
def _parseActiveRally(r):
    """Parse active rally response."""
    event = json_normalize(r.json()).drop(columns='eventDays')
    days =  json_normalize(r.json(), 'eventDays').drop(columns='spottChannel.assets')
    channels = json_normalize(r.json(), ['eventDays', 'spottChannel','assets'])
    return (event, days, channels)

def getActiveRally(_url=None, raw=False, func=_parseActiveRally):
    """Get active rally details."""
    
    if not _url:
        _url = ACTIVE_RALLY_URL  
    args= {"command":"getActiveRally","context":None}
    
    return _get_and_handle_response(_url, args, func, nargs=3, raw=raw, dropcols='winner.driverImageFormats')



# + tags=["active-ipynb"]
# event, days, channels = getActiveRally() #also works with passing URL
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

# + run_control={"marked": false}
CURRENT_SEASON_URL = 'https://www.wrc.com/ajax.php?contelPageId=181782'


# +
def _parseActiveSeasonEvents(r):
    """Parse current season events response."""
    current_season_events = json_normalize(r.json(), ['rallyEvents', 'items'], meta='seasonYear').drop(columns='eventDays')
    eventdays = json_normalize(r.json(), ['rallyEvents', 'items', 'eventDays']).drop(columns='spottChannel.assets')
    eventchannel = json_normalize(r.json(), ['rallyEvents', 'items', 'eventDays', 'spottChannel','assets'])
    return (current_season_events, eventdays, eventchannel)

# TO DO - can we get events for other seasons?
def getActiveSeasonEvents(raw=False, func=_parseActiveSeasonEvents):
    """Get events for current season"""
    _url=CURRENT_SEASON_URL
    #There seems to be a second UTL giving same data?
    #_url='https://www.wrc.com/ajax.php?contelPageId=183400'
    args = {"command":"getActiveSeason","context":None}

    return _get_and_handle_response(_url, args, func, nargs=3, raw=raw)#, dropcols='winner.driverImageFormats')
        


# + tags=["active-ipynb"]
# current_season_events, eventdays, eventchannel = getActiveSeasonEvents()
# display(current_season_events.head())
# display(eventdays.head())
# display(eventchannel.head())
# display(current_season_events.columns)
# eventchannel.columns
# -

# ## getItinerary

# +
#This seems to work with sdbRallyId=None, returning active rally?

def _parseItinerary(r):
    """Parse itinerary response."""
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

def getItinerary(sdbRallyId=None, raw=False, func=_parseItinerary):
    """Get itinerary details for specified rally."""
    
    if not sdbRallyId:
        event, days, channels = getActiveRally()
        sdbRallyId = int(event.loc[0,'id'])
    
    args = {"command":"getItinerary",
            "context":{"sdbRallyId":_jsInt(sdbRallyId)}}
    if sdbRallyId:
        extracols = {'rallyid':sdbRallyId}
    else:
        extracols ={}
    # TO DO - could we annotate with a looked up rally id?
    #Presumably from eg getActiveRally()? Or more generally ActiveSeasonEvents
    return _get_and_handle_response(URL, args, func, nargs=5, raw=raw, extracols = extracols)



# + tags=["active-ipynb"]
# sdbRallyId = 100
# itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
# display(itinerary.head())
# display(legs.head())
# display(sections.head())
# display(controls.head())
# display(stages.head())

# +
def _parseStartlist(r):
    """Parse raw startlist response."""
    startList = json_normalize(r.json()).drop(columns='startListItems')
    startListItems = json_normalize(r.json(), 'startListItems')
    
    return (startList,startListItems)


# TO DO - so can get a startlist for a not active leg?

def getActiveLegStartlist(startListId, raw=False, func=_parseStartlist):
    """Get a startlist for the active itinerary leg."""
    args={'command': 'getStartlist',
          'context': {'activeItineraryLeg': { 'startListId': startListId} }}

    return _get_and_handle_response(URL, args, func, nargs=2, raw=raw)


def getStartlist(startListId,typ='activeleg', raw=False):
    """Get a generic startlist."""
    if typ=='activeleg':
        return getActiveLegStartlist(startListId)
    
    return (None, None)


# + tags=["active-ipynb"]
# startListId = 451
# getStartlist(startListId)

# + tags=["active-ipynb"]
#
# startList,startListItems = getActiveLegStartlist(startListId)
# display(startList.head())
# display(startListItems.head())

# +
def _parseCars(r):
    """Parser for raw cars response."""
    cars = json_normalize(r.json()).drop(columns='eventClasses')
    classes = json_normalize(r.json(), 'eventClasses', meta='entryId')
    return (cars, classes)

def getCars(sdbRallyId, raw=False, func=_parseCars):
    """Get cars for a specified rally."""
    args = {"command":"getCars","context":{"sdbRallyId":_jsInt(sdbRallyId)}}
    
    return _get_and_handle_response(URL, args, func, nargs=2, raw=raw)


# + tags=["active-ipynb"]
# cars, classes = getCars(sdbRallyId)
# display(cars.head())
# display(classes.head())
# cars.head().columns

# +
def _parseRally(r):
    """Parser for raw rally response."""
    rally = json_normalize(r.json()).drop(columns=['eligibilities','groups'])
    eligibilities = json_normalize(r.json(),'eligibilities', meta='rallyId')
    eligibilities.rename(columns={0:'category'}, inplace=True)
    groups = json_normalize(r.json(),'groups', meta='rallyId')
    return (rally, eligibilities, groups)

def getRally(sdbRallyId, raw=False, func=_parseRally):
    """Get rally details for specified rally."""
    args = {"command":"getRally","context":{"sdbRallyId":_jsInt(sdbRallyId)}}

    return _get_and_handle_response(URL, args, func, nargs=3, raw=raw,
                                    renamecols={'rallyId':'externalIdRally','eventId':'externalIdEvent'},
                                    extracols = {'sdbRallyId':sdbRallyId})


# + tags=["active-ipynb"]
# rally, eligibilities, groups = getRally(sdbRallyId)
# display(rally.head())
# display(eligibilities.head())
# display(groups.head())

# +
def _parseOverall(r):
    """Parser for raw overall response."""
    overall = json_normalize(r.json())
    return overall

def getOverall(sdbRallyId, stageId, raw=False, func=_parseOverall):
    """Get overall standings for specified rally and stage."""
    args = {"command":"getOverall","context":{"sdbRallyId":_jsInt(sdbRallyId),
                                              "activeStage":{"stageId":_jsInt(stageId)}}}

    return _get_and_handle_response(URL, args, func, nargs=1, raw=raw, extracols={'stageId':stageId})


# + tags=["active-ipynb"]
# stageId = 1528
# overall = getOverall(sdbRallyId, stageId)
# overall.head()

# +
def _parseSplitTimes(r):
    """Parser for raw splittimes response."""
    splitPoints = json_normalize(r.json(),'splitPoints')
    entrySplitPointTimes = json_normalize(r.json(), 'entrySplitPointTimes', meta='stageId').drop(columns='splitPointTimes')
    splitPointTimes = json_normalize(r.json(), ['entrySplitPointTimes','splitPointTimes'], meta='stageId')
    return (splitPoints, entrySplitPointTimes, splitPointTimes)

def getSplitTimes(sdbRallyId,stageId, raw=False, func=_parseSplitTimes):
    """Get split times for specified rally and stage."""
    args = {"command":"getSplitTimes",
            "context":{"sdbRallyId":_jsInt(sdbRallyId),
                       "activeStage":{"stageId":_jsInt(stageId)}}}

    return _get_and_handle_response(URL, args, func, nargs=3, raw=raw)


# + tags=["active-ipynb"]
# splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
# display(splitPoints.head())
# display(entrySplitPointTimes.head())
# display(splitPointTimes.head())

# +
def _parseStageTimes(r):
    """Parser for raw stagetimes response."""
    stagetimes = json_normalize(r.json())
    return stagetimes

def getStageTimes(sdbRallyId, stageId, raw=False, func=_parseStageTimes):
    """Get stage times for specified rally and stage"""
    args = {"command":"getStageTimes",
            "context":{"sdbRallyId":_jsInt(sdbRallyId),
                       "activeStage":{"stageId":_jsInt(stageId)}}}

    return _get_and_handle_response(URL, args, func, nargs=1, raw=raw)


# + tags=["active-ipynb"]
# stagetimes = getStageTimes(sdbRallyId,stageId)
# stagetimes.head()

# + run_control={"marked": false}
def _parseStagewinners(r):
    """Parser for raw stagewinners response."""
    stagewinners = json_normalize(r.json())
    return stagewinners

def getStagewinners(sdbRallyId, raw=False, func=_parseStagewinners):
    """Get stage winners for specified rally."""
    args = {"command":"getStagewinners",
            "context":{"sdbRallyId":_jsInt(sdbRallyId)}}

    return _get_and_handle_response(URL, args, func, nargs=1, raw=raw)


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
#COLS_PENALTIES=['penaltyId','controlId','entryId','penaltyDurationMs','penaltyDuration','reason']

def _parsePenalties(r):
    """Parser for raw penalties response."""
    penalties = json_normalize(r.json())
    return penalties

def getPenalties(sdbRallyId, raw=False, func=_parsePenalties):
    """Get penalties for specified rally."""
    args = {"command":"getPenalties",
            "context":{"sdbRallyId":_jsInt(sdbRallyId)}}
    
    return _get_and_handle_response(URL, args, func, nargs=1, raw=raw)


# + tags=["active-ipynb"]
# penalties = getPenalties(sdbRallyId)
# penalties.head()

# + run_control={"marked": false}
#COLS_RETIREMENT = ['retirementId','controlId','entryId','reason','retirementDateTime','retirementDateTimeLocal','status']

def _parseRetirements(r):
    """Parser for raw retirements response."""   
    retirements = json_normalize(r.json())
    return retirements

def getRetirements(sdbRallyId, raw=False, func=_parseRetirements):
    """Get retirements for specified rally."""
    args = {"command":"getRetirements",
            "context":{"sdbRallyId":_jsInt(sdbRallyId)}}

    return _get_and_handle_response(URL, args, func, nargs=1, raw=raw)


# + tags=["active-ipynb"]
# retirements = getRetirements(sdbRallyId)
# retirements.head()
# -

SEASON_URL = 'https://www.wrc.com/ajax.php?contelPageId=186641'


#How can we look these up?
SEASON_CATEGORIES = {'WRC':"35", "WRC2":"46", "WRC3":"49","JWRC":"58"}


# +
def _parseSeasonCategory(r):
    """Parser for raw season category response."""
    season_category = json_normalize(r.json())
    return season_category

def getSeasonCategory(seasonCategory=SEASON_CATEGORIES['WRC'], raw=False, func=_parseSeasonCategory): 
    """Get championships in season category."""
    args = {"command":"getSeasonCategory",
            "context":{"seasonCategory":seasonCategory}}

    return _get_and_handle_response(SEASON_URL, args, func, nargs=1, raw=raw)



# + tags=["active-ipynb"]
# getSeasonCategory()

# +
# TO DO - what about other seasons?

def getSeasonCategories(seasonCategories=None):
    """Create dataframe of external season categoties."""
    champs=pd.DataFrame()

    if seasonCategories is None:
        for sc in SEASON_CATEGORIES:
            seasonCategory = SEASON_CATEGORIES[sc]

            champs = champs.append(getSeasonCategory(seasonCategory))#[SC_COLS])
    champs.reset_index(inplace=True, drop=True)
    return champs



# + tags=["active-ipynb"]
# getSeasonCategories()

# +
SC_COLS = ['id', 'category.name','externalIdDriver','externalIdCoDriver','externalIdManufacturer']

def getSeasonChampionshipCodes():
    """Convenience function to get chmapionship codes in an easily retrieved way."""
    champs = getSeasonCategories()[SC_COLS]
    champs.rename(columns={'externalIdDriver':'drivers',
                           'externalIdCoDriver':'codrivers',
                           'externalIdManufacturer':'manufacturers'},
                  inplace=True)
    return champs


# + tags=["active-ipynb"]
# getSeasonChampionshipCodes()

# +

def _getChampionshipId(category='WRC', typ='drivers'):
    """Look up external ids for championship by category and championship."""
    champs=getSeasonChampionshipCodes()
    championship_activeExternalId = champs.set_index('id').to_dict(orient='index')[int(SEASON_CATEGORIES[category])]
    activeExternalId = championship_activeExternalId[typ]
    return activeExternalId

def _getSeasonId(): 
    event, days, channels = getActiveRally()
    return int(event.loc[0,'season.externalId'])

def _parseChampionship(r):
    """Parser for raw championship response."""
    championship = json_normalize(r.json()).drop(columns=['championshipRounds','championshipEntries'])
    championshipRounds = json_normalize(r.json(), 'championshipRounds' )
    championshipEntries = json_normalize(r.json(), 'championshipEntries')
    return (championship, championshipRounds, championshipEntries)
    
def getChampionship(category='WRC',typ='drivers', season_external_id=None, 
                    raw=False, func=_parseChampionship):
    """Get Championship details for specified category and championship.
       If nor season ID is provided, use the external seasonid from the active rally. """
    
    season_external_id = _getSeasonId()
    args = {"command":"getChampionship",
            "context":{"season":{"externalId":season_external_id},
                       "activeExternalId":_getChampionshipId(category,typ)}}

    return _get_and_handle_response(SEASON_URL, args, func, nargs=3, raw=raw)


# + tags=["active-ipynb"]
# (championship, championshipRounds, championshipEntries) = getChampionship()
# display(championship)
# display(championshipRounds.head())
# display(championshipEntries.head())

# + tags=["active-ipynb"]
# getSeasonCategories().to_dict(orient='index')#[int(SEASON_CATEGORIES['JWRC'])]

# +
def _parseChampionshipStandings(r):
    """Parser for raw champioship standings response."""
    championship_standings = json_normalize(r.json(),'entryResults', meta='championshipId')
    if not championship_standings.empty:
        championship_standings.drop(columns='roundResults', inplace=True)
        round_results = json_normalize(r.json(),['entryResults', 'roundResults'])
    else:
        round_results=pd.DataFrame()
    return (championship_standings, round_results)
    
def getChampionshipStandings(category='WRC',typ='drivers', season_external_id=None, 
                             raw=False, func=_parseChampionshipStandings ):
    """Get championship standings."""
    season_external_id = _getSeasonId()
    args = {"command":"getChampionshipStandings",
            "context":{
                       "season":{"externalId":season_external_id,
                                 },
                       "activeExternalId":_getChampionshipId(category,typ)}}

    return _get_and_handle_response(SEASON_URL, args, func, nargs=2, raw=raw, extracols={'category':category,
                                                                                         'championship':typ})


# + tags=["active-ipynb"]
# championship_standings, round_results = getChampionshipStandings('WRC')
# display(championship_standings.head())
# display(round_results.head())
# -

