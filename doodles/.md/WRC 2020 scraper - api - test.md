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

# WRC Scraper  2020

```python
import requests
import pandas as pd
from pandas.io.json import json_normalize
```

```python
URL='https://www.wrc.com/ajax.php?contelPageId=176146'
```

```python
s = requests.Session()
s.get('https://www.wrc.com')
```

## getItinerary

```python
args=json.loads('{"command":"getStartlist","context":{"sdbRallyId":100,"activeSeasonId":19,"getSeasonCategoryField":"getExternalIdDriver","activeStage":{"stageId":1528,"eventId":124,"number":1,"name":"Malijai - Puimichel (Live TV)","distance":17.47,"status":"Running","stageType":"SpecialStage","timingPrecision":"Tenth","locked":false,"code":"SS1"},"activeEligibility":null,"activeItineraryLeg":{"itinerarySections":[{"controls":[{"controlId":6539,"eventId":124,"stageId":null,"type":"TimeControl","code":"TC0","location":"Monaco","timingPrecision":"Minute","distance":0,"targetDuration":null,"targetDurationMs":null,"firstCarDueDateTime":"2020-01-23T16:00:00","firstCarDueDateTimeLocal":"2020-01-23T17:00:00+01:00","status":"Completed","controlPenalties":"All","roundingPolicy":"NoRounding","locked":false},{"controlId":6543,"eventId":124,"stageId":null,"type":"TimeControl","code":"TC0A","location":"Tyre Fitting Zone IN","timingPrecision":"Minute","distance":166.33,"targetDuration":"02:45:00","targetDurationMs":9900000,"firstCarDueDateTime":"2020-01-23T18:45:00","firstCarDueDateTimeLocal":"2020-01-23T19:45:00+01:00","status":"Completed","controlPenalties":"All","roundingPolicy":"NoRounding","locked":false},{"controlId":6541,"eventId":124,"stageId":null,"type":"TimeControl","code":"TC0B","location":"Tyre Fitting Zone OUT","timingPrecision":"Minute","distance":0.35,"targetDuration":"00:15:00","targetDurationMs":900000,"firstCarDueDateTime":"2020-01-23T19:00:00","firstCarDueDateTimeLocal":"2020-01-23T20:00:00+01:00","status":"Completed","controlPenalties":"All","roundingPolicy":"NoRounding","locked":false},{"controlId":6593,"eventId":124,"stageId":1528,"type":"TimeControl","code":"TC1","location":"Malijai","timingPrecision":"Minute","distance":17.08,"targetDuration":"00:35:00","targetDurationMs":2100000,"firstCarDueDateTime":"2020-01-23T19:35:00","firstCarDueDateTimeLocal":"2020-01-23T20:35:00+01:00","status":"Running","controlPenalties":"All","roundingPolicy":"NoRounding","locked":false},{"controlId":6592,"eventId":124,"stageId":1528,"type":"StageStart","code":"SS1","location":"Malijai - Puimichel (Live TV)","timingPrecision":"Minute","distance":17.47,"targetDuration":"00:03:00","targetDurationMs":180000,"firstCarDueDateTime":"2020-01-23T19:38:00","firstCarDueDateTimeLocal":"2020-01-23T20:38:00+01:00","status":"Running","controlPenalties":"None","roundingPolicy":"RoundToClosestMinute","locked":false},{"controlId":6591,"eventId":124,"stageId":1528,"type":"FlyingFinish","code":"SF1","location":"Malijai - Puimichel (Live TV)","timingPrecision":"Tenth","distance":null,"targetDuration":null,"targetDurationMs":null,"firstCarDueDateTime":null,"firstCarDueDateTimeLocal":null,"status":"Running","controlPenalties":"None","roundingPolicy":"NoRounding","locked":false},{"controlId":6590,"eventId":124,"stageId":1538,"type":"TimeControl","code":"TC2","location":"Bayons","timingPrecision":"Minute","distance":83.29,"targetDuration":"01:45:00","targetDurationMs":6300000,"firstCarDueDateTime":"2020-01-23T21:23:00","firstCarDueDateTimeLocal":"2020-01-23T22:23:00+01:00","status":"ToRun","controlPenalties":"All","roundingPolicy":"NoRounding","locked":false},{"controlId":6589,"eventId":124,"stageId":1538,"type":"StageStart","code":"SS2","location":"Bayons - Bréziers","timingPrecision":"Minute","distance":25.49,"targetDuration":"00:03:00","targetDurationMs":180000,"firstCarDueDateTime":"2020-01-23T21:26:00","firstCarDueDateTimeLocal":"2020-01-23T22:26:00+01:00","status":"ToRun","controlPenalties":"None","roundingPolicy":"RoundToClosestMinute","locked":false},{"controlId":6588,"eventId":124,"stageId":1538,"type":"FlyingFinish","code":"SF2","location":"Bayons - Bréziers","timingPrecision":"Tenth","distance":null,"targetDuration":null,"targetDurationMs":null,"firstCarDueDateTime":null,"firstCarDueDateTimeLocal":null,"status":"ToRun","controlPenalties":"None","roundingPolicy":"NoRounding","locked":false},{"controlId":6542,"eventId":124,"stageId":null,"type":"TimeControl","code":"TC2A","location":"Technical Zone IN ","timingPrecision":"Minute","distance":25.61,"targetDuration":"01:00:00","targetDurationMs":3600000,"firstCarDueDateTime":"2020-01-23T22:26:00","firstCarDueDateTimeLocal":"2020-01-23T23:26:00+01:00","status":"ToRun","controlPenalties":"All","roundingPolicy":"NoRounding","locked":false},{"controlId":6536,"eventId":124,"stageId":null,"type":"TimeControl","code":"TC2B","location":"Technical Zone OUT - Flexi-Service IN","timingPrecision":"Minute","distance":0.15,"targetDuration":"00:10:00","targetDurationMs":600000,"firstCarDueDateTime":"2020-01-23T22:36:00","firstCarDueDateTimeLocal":"2020-01-23T23:36:00+01:00","status":"ToRun","controlPenalties":"None","roundingPolicy":"NoRounding","locked":false},{"controlId":6546,"eventId":124,"stageId":null,"type":"TimeControl","code":"TC2C","location":"Flexi-Service OUT - Parc Fermé IN","timingPrecision":"Minute","distance":1.3,"targetDuration":"00:48:00","targetDurationMs":2880000,"firstCarDueDateTime":"2020-01-23T23:24:00","firstCarDueDateTimeLocal":"2020-01-24T00:24:00+01:00","status":"ToRun","controlPenalties":"None","roundingPolicy":"NoRounding","locked":false}],"stages":[{"stageId":1528,"eventId":124,"number":1,"name":"Malijai - Puimichel (Live TV)","distance":17.47,"status":"Running","stageType":"SpecialStage","timingPrecision":"Tenth","locked":false,"code":"SS1"},{"stageId":1538,"eventId":124,"number":2,"name":"Bayons - Bréziers","distance":25.49,"status":"ToRun","stageType":"SpecialStage","timingPrecision":"Tenth","locked":false,"code":"SS2"}],"itinerarySectionId":637,"itineraryLegId":273,"order":1,"name":"Section 1"}],"itineraryLegId":273,"itineraryId":240,"startListId":451,"name":"Thursday 23rd January","legDate":"2020-01-23","order":1,"status":"Running"}}}')
args['context']['activeItineraryLeg']
```

```python
args={'command': 'getStartlist','context': {'activeItineraryLeg': { 'startListId': 451} }}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getItinerary","context":{"sdbRallyId":100}}#,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
r.text
```

```python
json_normalize(r.json())
```

```python
args = {"command":"getActiveRally","context":None}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
json_normalize(r.json()).columns
```

```python
args = {"command":"getCars","context":{"sdbRallyId":100}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getRally","context":{"sdbRallyId":100}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getOverall","context":{"sdbRallyId":100, "activeStage":{"stageId":1528}}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getSplitTimes","context":{"sdbRallyId":100, "activeStage":{"stageId":1528}}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())

```

```python
args = {"command":"getStageTimes","context":{"sdbRallyId":100,
                                             "activeStage":{"stageId":1528}}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
json_normalize(r.json(),'splitPoints')
```

```python
# NOT THERE? also getChampionshipStandingsLive
args = {"command":"getSeasonDetails","context":{"sdbRallyId":100,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
# also getChampionshipStandingsLive ??
args = {"command":"getChampionship","context":{"sdbRallyId":100,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python run_control={"marked": false}
#TO DO  NOT THERE? - guessing this exists
args = {"command":"getChampionshipResults","context":{"sdbRallyId":100,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getPenalties","context":{"sdbRallyId":100,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getRetirements","context":{"sdbRallyId":100,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python
args = {"command":"getStagewinners","context":{"sdbRallyId":100,"activeSeasonId":19}}
r = s.post('https://www.wrc.com/ajax.php?contelPageId=176146', data=json.dumps(args))
json_normalize(r.json())
```

```python

```
