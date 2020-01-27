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
def _getresponse(_url, args):
    """Simple function to get response from a post request."""
    r = s.post(_url, data=json.dumps(args))
    return r

def _get_and_handle_response(_url, args, func, nargs=1, raw=False):
    """Make a request to the API and then return a raw string
       or parse the response with a provided parser function."""
    r =  _getresponse(_url,args) 

    if raw or not callable(func):
        return r.text
    
    if not r.text or r.text=='null':
        return tuple([None for i in range(nargs)])
    
    return func(r)


# -

ACTIVE_RALLY_URL = 'https://www.wrc.com/ajax.php?contelPageId=171091'


# +
def _parseActiveRally(r):
    """Parse active rally response."""
    event = json_normalize(r.json()).drop(columns='eventDays')
    days =  json_normalize(r.json(), 'eventDays').drop(columns='spottChannel.assets')
    channels = json_normalize(r.json(), ['eventDays', 'spottChannel','assets'])
    return (event, days, channels)

def getActiveRallyBase(_url=None, raw=False, func=_parseActiveRally):
    """Get active rally details."""
    
    if not _url:
        _url = ACTIVE_RALLY_URL  
    args= {"command":"getActiveRally","context":None}
    
    return _get_and_handle_response(_url, args, func, raw)



# + tags=["active-ipynb"]
# event, days, channels = getActiveRallyBase() #also works with passing URL
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
def _parseCurrentSeasonEvents(r):
    """Parse current season events response."""
    current_season_events = json_normalize(r.json(), ['rallyEvents', 'items'], meta='seasonYear').drop(columns='eventDays')
    eventdays = json_normalize(r.json(), ['rallyEvents', 'items', 'eventDays']).drop(columns='spottChannel.assets')
    eventchannel = json_normalize(r.json(), ['rallyEvents', 'items', 'eventDays', 'spottChannel','assets'])
    return (current_season_events, eventdays, eventchannel)

def getCurrentSeasonEvents(raw=False, func=_parseCurrentSeasonEvents):
    """Get events for current season"""
    _url=CURRENT_SEASON_URL
    #There seems to be a second UTL giving same data?
    #_url='https://www.wrc.com/ajax.php?contelPageId=183400'
    args = {"command":"getActiveSeason","context":None}

    return _get_and_handle_response(_url, args, func, nargs=3, raw=raw)
        


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
    event, days, channels = getActiveRallyBase(URL)
    return (event, days, channels)


# + tags=["active-ipynb"]
# event, days, channels = getActiveRally()
# display(event)
# display(days)
# display(channels.head())

# +
#This seems to work with sdbRallyId=None, returning active rally?

def _parseItinerary(r):
    """Parse itninerary response."""
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
    args = {"command":"getItinerary",
            "context":{"sdbRallyId":sdbRallyId}}
    
    return _get_and_handle_response(URL, args, func, nargs=5, raw=raw)



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

def getStartlist(startListId, raw=False, func=_parseStartlist):
    """Get a startlist given startlist ID."""
    args={'command': 'getStartlist',
          'context': {'activeItineraryLeg': { 'startListId': startListId} }}

    return _get_and_handle_response(URL, args, func, nargs=2, raw=raw)


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


# + tags=["active-ipynb"]
# retirements = getRetirements(sdbRallyId)
# retirements.head()
# -

SEASON_URL = 'https://www.wrc.com/ajax.php?contelPageId=186641'


#How can we look these up?
SEASON_CATEGORIES = {'WRC':"35", "WRC2":"46", "WRC3":"49","JWRC":"58"}


def getSeasonCategory(seasonCategory=SEASON_CATEGORIES['WRC']): 
    """Get championships in season category."""
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
