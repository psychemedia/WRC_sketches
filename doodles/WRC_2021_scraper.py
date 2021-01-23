# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
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

# + tags=["active-ipynb"]
# %load_ext autoreload
# %autoreload 2

# + tags=["active-ipynb"]
# %load_ext pycodestyle_magic
# #%flake8_on --ignore D100
# -

import requests
import warnings
import json
import pandas as pd
from pandas import json_normalize

# Cache results in text notebook
import requests_cache
requests_cache.install_cache('wrc_cache',
                             backend='sqlite',
                             expire_after=300)

# + tags=["active-ipynb"]
# # TO DO
# # There is also an: activeSeasonId":19
# # Is there something we can get there?

# +
# URL Types
API_BASE_URL = "https://api.wrc.com"
API_RALLY_RESULTS = f'{API_BASE_URL}/results-api/rally-event'
URLS = { 'active': '{API_BASE_URL}/sdb/rallyevent/active',
    "season_categories": '{API_BASE_URL}/sdb/categories/season/{SEASON_ID}',
        "cars": '{API_RALLY_RESULTS}/{RALLY_ID}/cars',
        "groups": '{API_RALLY_RESULTS}/{RALLY_ID}',
        'startlist':'{API_RALLY_RESULTS}/{RALLY_ID}/start-list-external/{startListId}',
                     "itinerary": '{API_RALLY_RESULTS}/{RALLY_ID}/itinerary',
                    "retirements": '{API_RALLY_RESULTS}/{RALLY_ID}/retirements',
                    "penalties": '{API_RALLY_RESULTS}/{RALLY_ID}/penalties',
                    "stagewinners": '{API_RALLY_RESULTS}/{RALLY_ID}/stage-winners',
                    "splittimes": '{API_RALLY_RESULTS}/{RALLY_ID}/split-times/stage-external/{STAGE_ID}',
                    "stagetimes": '{API_RALLY_RESULTS}/{RALLY_ID}/stage-times/stage-external/{STAGE_ID}',
                    "overall": '{API_RALLY_RESULTS}/{RALLY_ID}/stage-result/stage-external/{STAGE_ID}',
        'drivers':'https://api.wrc.com/results-api/championship/season/{SEASON_ID}/season-category/{SEASON_CATEGORY}/driver',
              'manufacturers':'https://api.wrc.com/results-api/championship/season/{SEASON_ID}/season-category/{SEASON_CATEGORY}/manufacturer',
        'co-drivers':'https://api.wrc.com/results-api/championship/season/{SEASON_ID}/season-category/{SEASON_CATEGORY}/co-driver',
 
       }
# active https://api.wrc.com/sdb/rallyevent/active
# itinerary https://api.wrc.com/results-api/rally-event/1695/itinerary
# retirements https://api.wrc.com/results-api/rally-event/1695/retirements
# penalties https://api.wrc.com/results-api/rally-event/1695/penalties
# stagewinners https://api.wrc.com/results-api/rally-event/1695/stage-winners
# splittimes https://api.wrc.com/results-api/rally-event/1695/split-times/stage-external/1698
# stagetimes https://api.wrc.com/results-api/rally-event/1695/stage-times/stage-external/1698
# overall https://api.wrc.com/results-api/rally-event/1695/stage-result/stage-external/1698
# championship result https://api.wrc.com/results-api/championship/season/140/season-category-name/wrc/driver 
#https://api.wrc.com/results-api/championship/season/140/season-category/442/driver
#https://api.wrc.com/results-api/rally-event/1695/start-list-external/511

#https://api.wrc.com/results-api/rally-event/{RALLY_ID}/result
# -

from WRCUtils2021 import _isnull, _notnull, _checkattr, _jsInt, listify


# +
# Is this URL constant or picked up relative to each rally?
# Where was this originally found?
# What is that pages supposed to look like????
#URL = 'https://www.wrc.com/ajax.php?contelPageId=176146'

# +
def _format_url(_url, path_args=None):
    """Format a URL."""
    path_args = {} if not path_args else path_args
    path_args['API_BASE_URL'] = API_BASE_URL
    path_args['API_RALLY_RESULTS'] = API_RALLY_RESULTS
    return URLS[_url].format(**path_args)

def _getresponse(_url, args=None, ss={'conn': None}, secondtry=False, typ='get',
                 path_args=None):
    """Get response from a post request."""
    args = {} if args is None else args
    r = None
    if ss['conn'] is None or secondtry:
        try:
            ss['conn'] = requests.Session()
            ss['conn'].get('https://www.wrc.com')
        except:
            return None
        
    try:
        if typ=='post':
            r = ss['conn'].post(_url, data = json.dumps(args))
        else:
            if _url in URLS:
                _url = _format_url(_url, path_args)
            print(_url)
            r = ss['conn'].get(_url, params = json.dumps(args))
    except:
        #requests.exceptions.ConnectionError:
        if not secondtry:
            #If there's an error, try once again
            try:
                _getresponse(_url, args, secondtry = True)
            except:
                return None
        else:
            return None
            
    return r


# + tags=["active_ipynb"]
_format_url('overall', {'RALLY_ID':1, 'STAGE_ID':2})


# -

def _get_and_handle_response(_url, func, args=None, nargs=1, path_args=None,
                             raw=False, renamecols=None,
                             extracols=None, dropcols=None, typ='get'):
    """
    Make request to WRC API.

    Return a raw string or parse the response
    with a provided parser function.
    """
    def _add_cols(response, extracols):
        """Add extra columns to each dataframe."""
        if _isnull(response):
            return

        dupes = set(response.columns).intersection(extracols.keys())

        if dupes:
            warnings.warn(f"Trying to add pre-existing cols: {', '.join(dupes)}")
        for k in extracols:
            response[k] = extracols[k]
    
    r = _getresponse(_url, args, typ=typ, path_args=path_args)
    if raw or not callable(func):
        return r.text

    # Make sure we return the desired number of None items
    # in a tuple as a null response
    if not r or r is None or not r.text or r.text == 'null':
        return tuple([None for i in range(nargs)])
        
    response = func(r)

    if renamecols is None:
        renamecols = {}
    if extracols is None:
        extracols = {}

    # The dataframe type check can help if we have the wrong number of args
    # Could display a warning that the nargs is incorrect if so
    if nargs == 1 or isinstance(response, pd.DataFrame):
        if renamecols:
            response.rename(columns=renamecols, inplace=True)
        if extracols:
            _add_cols(response, extracols)
        if dropcols:
            response.drop(columns=dropcols, inplace=True, errors='ignore')
    else:
        for i in range(nargs):
            if renamecols:
                _cols = response[i].columns
                _cols = set(_cols).intersection(renamecols.keys())
                # Couldn't we just errors='ignore?
                response[i].rename(columns={k: renamecols[k] for k in _cols},
                                   inplace=True, errors='ignore')
            if extracols:
                _add_cols(response[i], extracols)
            if dropcols:
                response[i].drop(columns=dropcols,
                                 inplace=True, errors='ignore')

    return response


# +
#OLD_ACTIVE_RALLY_URL = 'https://www.wrc.com/ajax.php?contelPageId=171091'
# -

r = _getresponse('active')
r.json()


# +
def _parseActiveRally(r):
    """Parse active rally response."""
    event = json_normalize(r.json()).drop(columns='eventDays')
    days = json_normalize(r.json(),
                          'eventDays').drop(columns='spottChannel.assets')
    channels = json_normalize(r.json(),
                              ['eventDays', 'spottChannel', 'assets'])
    return (event, days, channels)


# TO DO  - doesnlt this work any more?
def getActiveRally(raw=False, func=_parseActiveRally):
    """Get active rally details."""

    return _get_and_handle_response('active', func, nargs=3, raw=raw,
                                    dropcols='winner.driverImageFormats')




# + tags=["active-ipynb"]
# event, days, channels = getActiveRally()  # Also works with passing URL
# display(event.head())
# display(days.head())
# display(channels.head())
# display(event.columns)

# + tags=["active-ipynb"]
# event.loc[0].to_dict()

# +
# Raw https://webappsdata.wrc.com/srv API?
# Need to create separate package to query that API
# Season info
# _url = 'https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22Season%22&maxdepth=1' 
# r = s.get(_url)
# json_normalize(r.json())

# + run_control={"marked": false}
# 2021 season
CURRENT_SEASON_URL = 'https://api.wrc.com/contel-page/83388/calendar/season/140'
CURRENT_SEASON_ID = '140' # TO DO - where can we look this up?


# +
def _parseActiveSeasonEvents(r):
    """Parse current season events response."""
    current_season_events = json_normalize(r.json(),
                                           ['rallyEvents', 'items'],
                                           meta='seasonYear').drop(columns='eventDays')
    eventdays = json_normalize(r.json(),
                               ['rallyEvents', 'items',
                                'eventDays'])#.drop(columns='spottChannel.assets')
    # TO DO -  has this disappeared?
    eventchannel = None
    #eventchannel = json_normalize(r.json(),
    #                              ['rallyEvents', 'items', 'eventDays',
    #                               'spottChannel', 'assets'])
    return (current_season_events, eventdays, eventchannel)


# TO DO - can we get events for other seasons?
def getActiveSeasonEvents(raw=False, func=_parseActiveSeasonEvents):
    """Get events for current season."""
    _url = CURRENT_SEASON_URL
    # There seems to be a second UTL giving same data?
    # _url='https://www.wrc.com/ajax.php?contelPageId=183400'
    #args = {"command": "getActiveSeason", "context": None}

    return _get_and_handle_response(_url, func, nargs=3, raw=raw)
    # dropcols='winner.driverImageFormats')



# +
#getActiveSeasonEvents(raw=True)

# + tags=["active-ipynb"]
# current_season_events, eventdays, eventchannel = getActiveSeasonEvents()
# display(current_season_events.head())
# display(eventdays.head())
# #display(eventchannel.head())
# display(current_season_events.columns)
# #eventchannel.columns

# + tags=["active-ipynb"]
# current_season_events.iloc[0].to_dict()
# -

# ## getItinerary

# +
# https://api.wrc.com/results-api/rally-event/1695/itinerary

# +
# This seems to work with sdbRallyId=None, returning active rally?

def _parseItinerary(r):
    """Parse itinerary response."""
    itinerary = json_normalize(r.json()).drop(columns='itineraryLegs')
    legs = json_normalize(r.json(), 'itineraryLegs')
    if _notnull(legs):
        legs = legs.drop(columns='itinerarySections')
        sections = json_normalize(r.json(),
                                  ['itineraryLegs', 'itinerarySections']).drop(columns=['controls', 'stages'])
        controls = json_normalize(r.json(),
                                  ['itineraryLegs', 'itinerarySections', 'controls'],
                                  meta=[['itineraryLegs', 'itineraryLegId'],
                                         ['itineraryLegs', 'startListId']])
        controls.rename(columns={'itineraryLegs.itineraryLegId': 'itineraryLegId',
                                 'itineraryLegs.startListId': 'startListId'}, inplace=True)
        stages = json_normalize(r.json(),
                                ['itineraryLegs', 'itinerarySections', 'stages'],
                                meta=[['itineraryLegs', 'itineraryLegId'],
                                         ['itineraryLegs', 'startListId']])
        stages.rename(columns={'itineraryLegs.itineraryLegId': 'itineraryLegId',
                               'itineraryLegs.startListId': 'startListId'}, inplace=True)
    else:
        legs = sections = controls = stages = None
    return (itinerary, legs, sections, controls, stages)


def getItinerary(sdbRallyId=None, raw=False, func=_parseItinerary):
    """Get itinerary details for specified rally."""
    if not sdbRallyId:
        event, days, channels = getActiveRally()
        sdbRallyId = int(event.loc[0, 'id'])

    args = {"command": "getItinerary",
            "context": {"sdbRallyId": _jsInt(sdbRallyId)}}

    if sdbRallyId:
        extracols = {'rallyid': sdbRallyId}
    else:
        extracols = {}
    # TO DO - could we annotate with a looked up rally id?
    #Presumably from eg getActiveRally()? Or more generally ActiveSeasonEvents

    return _get_and_handle_response('itinerary', func, nargs=5,
                                    path_args={'RALLY_ID': _jsInt(sdbRallyId)},
                                    raw=raw, extracols = extracols)



# -

_parseItinerary(requests.get('https://api.wrc.com/results-api/rally-event/1695/itinerary'))

# + tags=["active-ipynb"]
# event, days, channels = getActiveRally()

# + tags=["active-ipynb"]
# sdbRallyId = event.loc[0]['id']
# itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
# display(itinerary.head())
# display(legs.head())
# display(sections.head())
# display(controls.head())
# display(stages.head())
# -

itinerary, legs, sections, controls, stages = getItinerary()
legs


# + tags=["active-ipynb"]
# legs.where(legs['status']=='Running').last_valid_index()
# ix = legs.where(legs['status']=='Completed').last_valid_index()
# if ix and ix!=0:
#     legs.loc[ix]
# -

def getCurrentLeg(legs=None):
    """Get the current running leg, or the next leg to run."""
    # TO DO - need to know what the values of status are
    
    if _isnull(legs):
        # TO DO - if the class calls this, the data is obtained but not returned
        itinerary, legs, sections, controls, stages = getItinerary()
    
    _running = legs.where(legs['status']=='Running').last_valid_index()
    if not _running:
        # TO DO - need to check to run
        _running = legs.where(legs['status']=='ToRun').last_valid_index()
        
    if not _running:
        _running = legs.where(legs['status']=='Completed').last_valid_index()

    if _running is not None:
        return legs.loc[_running]
        
    return None


# + tags=["active-ipynb"]
# getCurrentLeg()
# -

RALLY_ID = getCurrentLeg()['rallyid']
RALLY_ID


def getStageDetails(stageNum, stages=None):
    """Get stage details from stage number (eg SS1)."""
    if _isnull(stages):
        # TO DO - if the class calls this, the data is obtained but not returned
        itinerary, legs, sections, controls, stages = getItinerary()
    if isinstance(stageNum, str) and stageNum.startswith('SS'):
        pass
    elif _jsInt(stageNum):
        stageNum = f'SS{stageNum}'
    else:
        stageNum = None

    stages_idx = stages.where(stages['code']==stageNum).last_valid_index()
    
    if stages_idx is not None:
        return stages.loc[stages_idx]

    return None


# + tags=["active-ipynb"]
# getStageDetails('SS2')  # also accepts: '2', 2

# +
def _parseStartlist(r):
    """Parse raw startlist response."""
    if not r.json():
        return (pd.DataFrame(), pd.DataFrame())
    startList = json_normalize(r.json()).drop(columns='startListItems')
    startListItems = json_normalize(r.json(), 'startListItems')

    return (startList, startListItems)

def getStartlistId(stage='', startListId=None, legs=None, stages=None):
    """Get a generic startListId."""
    # We essentially hack the precedence ordering
    # TO DO - we should warn from this
    # If passed something as first parameter (stage) that is actually 
    # a startListId and we have no startListId, use _stage as startListId
    _stage = _jsInt(stage)
    if startListId is None and _stage and legs and _stage in legs['startListId']:
        startListId = _stage

    # If we don't have a valid startListId, try to finesse one from stage
    if _isnull(_jsInt(startListId)) or not (legs and _jsInt(startListId)
                                            and _jsInt(startListId) in legs['startListId']):
        # If the startListId is a str, is it a stage designator?
        if isinstance(startListId, str) and startListId.startswith('SS'):
            stage = startListId
        if stage and isinstance(stage, str) and stage.lower().startswith('current'):
            startListId = getCurrentLeg(legs=legs)['startListId']
        elif stage:
            stage_details = getStageDetails(startListId, stages=stages)
            if _notnull(stage_details):
                startListId = stage_details['startListId']
        if not startListId:
            startListId = getCurrentLeg(legs=legs)['startListId']
    return startListId


def getStartlist(stage='', startListId=None, legs=None, stages=None,
                 raw=False, func=_parseStartlist):
    """Get a generic startlist."""
    startListId = getStartlistId(stage=stage, startListId=startListId,
                                 legs=legs, stages=stages)

    # TO DO - would it be neater to pass args like this?
    args = {'command': 'getStartlist',
            'context': {'activeItineraryLeg': {'startListId': _jsInt(startListId)}}}
    
    path_args={'RALLY_ID':RALLY_ID, 'startListId':_jsInt(startListId) }

    return _get_and_handle_response('startlist', func, nargs=2, raw=raw, path_args=path_args)


# + tags=["active-ipynb"]
# #getStartlist('SS4')[0]#[1].head()
# #getStartlist(469)[0], getStartlistId(startListId='SS4')#(startListId='SS4')
# getStartlistId(startListId='SS2')

# + tags=["active-ipynb"]
# startList,startListItems = getStartlist(startListId=511)
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
    path_args = {"RALLY_ID": _jsInt(sdbRallyId)}
    return _get_and_handle_response('cars',  func, nargs=2, raw=raw, path_args=path_args)


# + tags=["active-ipynb"]
# cars, classes = getCars(sdbRallyId)
# display(cars.head())
# display(classes.head())
# cars.head().columns

# +
def _parseRally(r):
    """Parser for raw rally response."""
    rally = json_normalize(r.json()).drop(columns=['eligibilities', 'groups'])
    eligibilities = json_normalize(r.json(), 'eligibilities', meta='rallyId')
    eligibilities.rename(columns={0: 'category'}, inplace=True)
    groups = json_normalize(r.json(), 'groups', meta='rallyId')
    return (rally, eligibilities, groups)


def getRally(sdbRallyId, raw=False, func=_parseRally):
    """Get rally details for specified rally."""
    args = {"command": "getRally",
            "context": {"sdbRallyId": _jsInt(sdbRallyId)}}

    return _get_and_handle_response('groups', func, nargs=3, raw=raw,
                                    renamecols={'rallyId': 'externalIdRally',
                                                'eventId': 'externalIdEvent'},
                                    extracols={'sdbRallyId': sdbRallyId}, path_args={'RALLY_ID':_jsInt(sdbRallyId)})


# + tags=["active-ipynb"]
# # https://api.wrc.com/results-api/rally-event/1695 but this seems incomplete maybe aligns to entries?
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
    args = {"command": "getOverall",
            "context": {"sdbRallyId": _jsInt(sdbRallyId),
                        "activeStage": {"stageId": _jsInt(stageId)}}}
    
    return _get_and_handle_response('overall', func, nargs=1, path_args={'RALLY_ID':_jsInt(sdbRallyId),
                                                                        'STAGE_ID': _jsInt(stageId)},
                                    raw=raw, extracols={'stageId': stageId})


# + tags=["active-ipynb"]
# stageId = getStageDetails('SS2')['stageId']
# overall = getOverall(sdbRallyId, stageId)
# overall.head()

# +
def _parseSplitTimes(r):
    """Parser for raw splittimes response."""
    splitPoints = json_normalize(r.json(), 'splitPoints')
    entrySplitPointTimes = json_normalize(r.json(),
                                          'entrySplitPointTimes',
                                          meta='stageId')
    splitPointTimes = json_normalize(r.json(),
                                     ['entrySplitPointTimes', 'splitPointTimes'],
                                     meta='stageId')
    if _notnull(splitPointTimes):
        entrySplitPointTimes.drop(columns='splitPointTimes', inplace=True)

    return (splitPoints, entrySplitPointTimes, splitPointTimes)


def getSplitTimes(sdbRallyId, stageId,
                  raw=False, func=_parseSplitTimes):
    """Get split times for specified rally and stage."""
    args = {"command": "getSplitTimes",
            "context": {"sdbRallyId": _jsInt(sdbRallyId),
                        "activeStage": {"stageId": _jsInt(stageId)}}}

    return _get_and_handle_response('splittimes', func, nargs=3, raw=raw, path_args={"RALLY_ID": _jsInt(sdbRallyId),
                        "STAGE_ID": _jsInt(stageId)})


# + tags=["active-ipynb"]
# splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId, stageId)
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
    args = {"command": "getStageTimes",
            "context": {"sdbRallyId": _jsInt(sdbRallyId),
                        "activeStage": {"stageId": _jsInt(stageId)}}}

    return _get_and_handle_response('stagetimes', func, nargs=1, raw=raw,path_args={"RALLY_ID": _jsInt(sdbRallyId),
                        "STAGE_ID": _jsInt(stageId)} )


# + tags=["active-ipynb"]
# stagetimes = getStageTimes(sdbRallyId, stageId)
# stagetimes.head()

# + run_control={"marked": false}
def _parseStagewinners(r):
    """Parser for raw stagewinners response."""
    stagewinners = json_normalize(r.json())
    return stagewinners

 
def getStagewinners(sdbRallyId, raw=False, func=_parseStagewinners):
    """Get stage winners for specified rally."""
    args = {"command": "getStagewinners",
            "context": {"sdbRallyId": _jsInt(sdbRallyId)}}

    return _get_and_handle_response('stagewinners', func, nargs=1, raw=raw, path_args={"RALLY_ID": _jsInt(sdbRallyId)})

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
# COLS_PENALTIES=['penaltyId','controlId','entryId','penaltyDurationMs','penaltyDuration','reason']


def _parsePenalties(r):
    """Parser for raw penalties response."""
    penalties = json_normalize(r.json())
    return penalties


def getPenalties(sdbRallyId, raw=False, func=_parsePenalties):
    """Get penalties for specified rally."""
    args = {"command": "getPenalties",
            "context": {"sdbRallyId": _jsInt(sdbRallyId)}}

    return _get_and_handle_response('penalties', func, nargs=1, raw=raw,
                                    path_args={"RALLY_ID": _jsInt(sdbRallyId)})


# + tags=["active-ipynb"]
# penalties = getPenalties(sdbRallyId)
# penalties.head()

# + run_control={"marked": false}
# COLS_RETIREMENT = ['retirementId','controlId','entryId','reason','retirementDateTime','retirementDateTimeLocal','status']

def _parseRetirements(r):
    """Parser for raw retirements response."""
    retirements = json_normalize(r.json())
    return retirements


def getRetirements(sdbRallyId, raw=False, func=_parseRetirements):
    """Get retirements for specified rally."""
    args = {"command": "getRetirements",
            "context": {"sdbRallyId": _jsInt(sdbRallyId)}}

    return _get_and_handle_response('retirements',  func, nargs=1, raw=raw,
                                   path_args={"RALLY_ID": _jsInt(sdbRallyId)})


# + tags=["active-ipynb"]
# retirements = getRetirements(sdbRallyId)
# retirements.head()
# -

SEASON_URL = 'https://www.wrc.com/ajax.php?contelPageId=186641'


#We can look these up
SEASON_CATEGORIES = {'WRC':"442", "WRC2":"453", "WRC3":"459", "JWRC":"58"}


# +
#https://api.wrc.com/sdb/season-category/442 eg wrc
#https://api.wrc.com/sdb/season-category/453 eg wrc2
# https://api.wrc.com/sdb/season-category/459 wrc3
# https://api.wrc.com/results-api/championship/season/140/season-category/442/driver
# https://api.wrc.com/results-api/championship/season/140/season-category/453/driver
#https://api.wrc.com/results-api/championship/season/140/season-category/453/manufacturer
# https://api.wrc.com/results-api/championship/season/140/season-category/453/co-driver

# +
# TO DO - what about other seasons?

def _parseSeasonCategories(r):
    """Parser for season categories response."""
    retirements = json_normalize(r.json(), 'items')
    return retirements

def getSeasonCategories(season_id=None, raw=False, func=_parseSeasonCategories):
    """Create dataframe of external season categories."""
    if season_id is None:
        season_id =  CURRENT_SEASON_ID
    return _get_and_handle_response('season_categories',  func, nargs=1, raw=raw,
                                    renamecols={'externalIds.Driver':'drivers',
                                                'externalIds.Co-Driver': 'codrivers',
                                                'externalIds.Manufacturer': 'manufacturers'},
                                   path_args={"SEASON_ID": season_id})



# + tags=["active-ipynb"]
# getSeasonCategories()

# +
def _getChampionshipId(category='WRC', typ='drivers'):
    """Look up external ids for championship by category and championship."""
    champs = getSeasonCategories()
    #championship_activeExternalId = champs.set_index('id').to_dict(orient='index')[int(SEASON_CATEGORIES[category])]
    #activeExternalId = championship_activeExternalId[typ]
    #return activeExternalId


def _getSeasonId():
    event, days, channels = getActiveRally()
    return int(event.loc[0, 'season.externalId'])


def _parseChampionship(r):
    """Parser for raw championship response."""
    championship = json_normalize(r.json()).drop(columns=['championshipRounds',
                                                          'championshipEntries'])
    championshipRounds = json_normalize(r.json(), 'championshipRounds')
    championshipEntries = json_normalize(r.json(), 'championshipEntries')
    return (championship, championshipRounds, championshipEntries)


def getChampionship(seasonCategory='WRC',
                      championship='drivers', season_id=None,
                    raw=False, func=_parseChampionship):
    """
    Get Championship details for specified category and championship.

    If nor season ID is provided, use the external seasonid from the active rally.
    """
    season_external_id = _getSeasonId()

    if season_id is None:
        season_id =  CURRENT_SEASON_ID
    return _get_and_handle_response(championship, func, nargs=3, raw=raw,
                                    path_args ={'SEASON_ID': season_id ,
                                                'SEASON_CATEGORY':SEASON_CATEGORIES[seasonCategory]})


    # to do - championship table decodes cols in other tables
                                                

# + tags=["active-ipynb"]
# (championship, championshipRounds, championshipEntries) = getChampionship()
# display(championship)
# display(championshipRounds.head())
# display(championshipEntries.head())

# + tags=["active-ipynb"]
# championshipRounds.columns

# + tags=["active-ipynb"]
# getSeasonCategories().to_dict(orient='index')  # [int(SEASON_CATEGORIES['JWRC'])]

# +
# TO DO - DONE UP TO HERE

# +
def _parseChampionshipStandings(r):
    """Parser for raw champioship standings response."""
    championship_standings = json_normalize(r.json(),
                                            'entryResults',
                                            meta='championshipId')
    if not championship_standings.empty:
        championship_standings.drop(columns='roundResults', inplace=True)
        round_results = json_normalize(r.json(),
                                       ['entryResults', 'roundResults'])
    else:
        round_results = pd.DataFrame()
    return (championship_standings, round_results)


def getChampionshipStandings(category='WRC', typ='drivers',
                             season_external_id=None,
                             raw=False, func=_parseChampionshipStandings):
    """Get championship standings."""
    season_external_id = _getSeasonId()
    #args = {"command": "getChampionshipStandings",
     #       "context": {"season": {"externalId": season_external_id},
      #                  "activeExternalId": _getChampionshipId(category, typ)}}

    return _get_and_handle_response(SEASON_URL, func, nargs=2,
                                    raw=raw, extracols={'category': category,
                                                        'championship': typ})


# + tags=["active-ipynb"]
# championship_standings, round_results = getChampionshipStandings('WRC')
# display(championship_standings.head())
# display(round_results.head())
# -


