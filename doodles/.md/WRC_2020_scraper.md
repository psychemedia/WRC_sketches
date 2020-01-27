---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
```

```python
# Cache results in text notebook
import requests_cache
requests_cache.install_cache('wrc_cache',
                             backend='sqlite',
                             expire_after=300)
```

```python
s = requests.Session()
s.get('https://www.wrc.com')
```

```python
#Is this URL constant or picked up relative to each rally?
URL='https://www.wrc.com/ajax.php?contelPageId=176146'
```

```python
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
```

```python tags=["active-ipynb"]
event, days, channels = getActiveRallyBase()
display(event.head())
display(days.head())
display(channels.head())
display(event.columns)
```

```python
#Raw https://webappsdata.wrc.com/srv API?
#Need to create separate package to query that API
#Season info
#_url = 'https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22Season%22&maxdepth=1' 
#r = s.get(_url)
#json_normalize(r.json())
```

```python
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

```

```python tags=["active-ipynb"]
current_season_events, eventdays, eventchannel = getCurrentSeasonEvents()
display(current_season_events.head())
display(eventdays.head())
display(eventchannel.head())
display(current_season_events.columns)
```

## getItinerary

```python
def getActiveRally():
    """Get active rally details."""
    event, days, channels = _getActiveRally(URL)
    return (event, days, channels)
```

```python tags=["active-ipynb"]
event, days, channels = getActiveRally()
display(event)
display(days)
display(channels.head())
```

```python
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
```

```python tags=["active-ipynb"]
sdbRallyId = 100
itinerary, legs, sections, controls, stages = getItinerary(sdbRallyId)
display(itinerary.head())
display(legs.head())
display(sections.head())
display(controls.head())
display(stages.head())
```

```python
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
    
```

```python tags=["active-ipynb"]
startListId = 451
startList,startListItems = getStartlist(startListId)
display(startList.head())
display(startListItems.head())
```

```python
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
```

```python tags=["active-ipynb"]
cars, classes = getCars(sdbRallyId)
display(cars.head())
display(classes.head())
cars.head().columns
```

```python
def getRally(sdbRallyId):
    """Get rally details for specified rally."""
    args = {"command":"getRally","context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    rally = json_normalize(r.json()).drop(columns=['eligibilities','groups'])
    eligibilities = json_normalize(r.json(),'eligibilities')
    groups = json_normalize(r.json(),'groups')
    return (rally, eligibilities, groups)
```

```python tags=["active-ipynb"]
rally, eligibilities, groups = getRally(sdbRallyId)
display(rally.head())
display(eligibilities.head())
display(groups.head())
```

```python
def getOverall(sdbRallyId, stageId):
    """Get overall standings for specificed rally and stage."""
    args = {"command":"getOverall","context":{"sdbRallyId":sdbRallyId,
                                              "activeStage":{"stageId":stageId}}}
    r = s.post(URL, data=json.dumps(args))
    overall = json_normalize(r.json())
    return overall
```

```python tags=["active-ipynb"]
stageId = 1528
overall = getOverall(sdbRallyId, stageId)
overall.head()
```

```python
def getSplitTimes(sdbRallyId,stageId):
    """Get split times for specified rally and stage."""
    args = {"command":"getSplitTimes",
            "context":{"sdbRallyId":sdbRallyId, "activeStage":{"stageId":stageId}}}
    r = s.post(URL, data=json.dumps(args))
    splitPoints = json_normalize(r.json(),'splitPoints')
    entrySplitPointTimes = json_normalize(r.json(), 'entrySplitPointTimes').drop(columns='splitPointTimes')
    splitPointTimes = json_normalize(r.json(), ['entrySplitPointTimes','splitPointTimes'])
    return (splitPoints, entrySplitPointTimes, splitPointTimes)
```

```python tags=["active-ipynb"]
splitPoints, entrySplitPointTimes, splitPointTimes = getSplitTimes(sdbRallyId,stageId)
display(splitPoints.head())
display(entrySplitPointTimes.head())
display(splitPointTimes.head())
```

```python
def getStageTimes(sdbRallyId,stageId):
    """Get stage times for specified rally and stage"""
    args = {"command":"getStageTimes",
            "context":{"sdbRallyId":sdbRallyId,
                       "activeStage":{"stageId":stageId}}}
    r = s.post(URL, data=json.dumps(args))
    stagetimes = json_normalize(r.json())
    return stagetimes
```

```python tags=["active-ipynb"]
stagetimes = getStageTimes(sdbRallyId,stageId)
stagetimes.head()
```

```python
def getStagewinners(sdbRallyId):
    """Get stage winners for specified rally."""
    args = {"command":"getStagewinners",
            "context":{"sdbRallyId":sdbRallyId}}
    r = s.post(URL, data=json.dumps(args))
    stagewinners = json_normalize(r.json())
    return stagewinners
```

```python tags=["active-ipynb"]
stagewinners = getStagewinners(sdbRallyId)
stagewinners.head()
```

Should we return empty dataframes with appropriate columns, or `None`?

An advantage of returning an empty dataframe with labelled columns is that we can also use the column value list as a test of a returned column.

We need to be consistent so we can have a common, consistent way of dealing with empty responses. This means things like `is None` or `pd.DataFrame().empty` both have to be handled.

```python

```
