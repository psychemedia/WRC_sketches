---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.1'
      jupytext_version: 1.2.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

```python
import notebookimport

sr = __import__("Charts - Stage Results")
```

```python
import os
import sqlite3
import pandas as pd
```

```python tags=["active-ipynb"]
#dbname='wrc18.db'
YEAR=2019
dbname='../../wrc-timing/finland19.db'
conn = sqlite3.connect(dbname)
rally='Finland'
rebase = 'NEU'
rebase = ''
```

```python tags=["active-ipynb"]
pd.read_sql('SELECT * FROM event_metadata',conn).columns
```

```python tags=["active-ipynb"]
#pd.read_sql('SELECT DISTINCT(code) FROM itinerary_controls',conn)
pd.read_sql('SELECT DISTINCT code, stageId FROM itinerary_stages',conn)
```

```python tags=["active-ipynb"]
pd.read_sql('SELECT DISTINCT status FROM itinerary_controls',conn)['status'].to_list()
```

```python
def stageDistances(conn):
    q='''
        SELECT *
        FROM itinerary_controls itc
        JOIN championship_events ce ON itc.eventId=ce.eventId
        JOIN itinerary_sections isc ON itc.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
        JOIN itinerary_legs il ON isc.itineraryLegId=il.itineraryLegId
        WHERE ce.`country.name`="{rally}" AND strftime('%Y', startDate)='{year}' 
                AND firstCarDueDateTimeLocal NOT NULL ORDER BY firstCarDueDateTimeLocal 
        '''.format(rally=rally, year=YEAR)
    _tmp = pd.read_sql(q,conn)
    return _tmp[_tmp['type']=='StageStart'].set_index('code')

```

```python tags=["active-ipynb"]
yy = stageDistances(conn)
yy.head()
```

```python tags=["active-ipynb"]
#Distance still to run
yy.loc['SS2':,:]['distance'].sum()
```

```python tags=["active-ipynb"]
yy.loc['SS2':,:].head()
```

```python tags=["active-ipynb"]
yy.loc['SS5']
```

```python
def distToRun(nextStage):
    ''' Return the distance still to run over the rest of the rally. '''
    return distances.loc[nextStage:,:]['distance'].values.sum()

def stageDist(stage):
    ''' Return the distance to run over one or more stages. '''
    if isinstance(stage,list):
        return distances.loc[stage,:]['distance'].sum()
    return distances.loc[stage,:]['distance']

```

```python tags=["active-ipynb"]
distances = stageDistances(conn)
stageDist('SS1'), stageDist(['SS1','SS2'])
```

```python tags=["active-ipynb"]
distToRun('SS6')
```

```python tags=["active-ipynb"]
q="SELECT * FROM stage_times_overall WHERE stageId=1125 ORDER BY totalTimeMs LIMIT 10 ;"
pd.read_sql(q,conn)
```

```python tags=["active-ipynb"]
stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages='SS5').set_index('drivercode')
stagerank_overall
```

```python tags=["active-ipynb"]
stagerank_overall.columns
```

```python tags=["active-ipynb"]
stagerank_overall['snum']
```

```python tags=["active-ipynb"]
stagerank_overall['totalTimeS'] = stagerank_overall['totalTimeMs']/1000
```

```python tags=["active-ipynb"]
stagerank_overall[['entrant.name', 'totalTime','totalTimeS']]
```

```python tags=["active-ipynb"]
stagerank_overall.loc['MEE','totalTimeS']
```

```python tags=["active-ipynb"]
#rebase
stagerank_overall['totalTimeS'] - stagerank_overall.loc['MEE','totalTimeS']
```

```python
def requiredPace(nextstage, times,  rebase=None, allrally=True):
    ''' Pace required on competitive distance remaining.
        We can report this as the number of seconds required per km,
        but not as a speed unless we have a target speed.
    '''
    
    if allrally and not isinstance(nextstage,list):
        dist = distToRun(nextstage)
        print('Distance to run starting at {}: {} km'.format(nextstage, dist))
    else:
        #Find the pace required to recoup on the next stage.
        dist = stageDist(nextstage)
        print('Distance to run on {}: {} km'.format(nextstage, dist))
    
    times['totalTimeS'] = times['totalTimeMs']/1000
    if rebase is None:
        timedelta =  -times['totalTimeS']- times.iloc[0]['totalTimeS']
    else:
        timedelta = -(times['totalTimeS'] - times.loc[rebase,'totalTimeS'])
        
    #print('Time to make up (s): {}'.format(timedelta))
        
    return pd.DataFrame({'timedelta_s':timedelta, 's_per_km':timedelta / dist})
```

```python tags=["active-ipynb"]
stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages='SS3').set_index('drivercode')
requiredPace(['SS4','SS5'], stagerank_overall,'LAP', allrally=False )
```

```python tags=["active-ipynb"]
stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages='SS1').set_index('drivercode')
requiredPace('SS2', stagerank_overall,'MEE' )

#Positive means driver has to drive that much faster in km/
```

```python
def paceReport(stage, rebase=None):
    ''' Time gained / lost per km on a stage. '''
    stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='stage', stages=stage).set_index('drivercode')
    dist = stageDist(stage)
    print('Stage dist is {} km'.format(dist))
    if rebase is None:
        return (stagerank_overall['elapsedDurationMs'] / 1000) / dist
    else:
        return ((stagerank_overall['elapsedDurationMs'] - stagerank_overall.loc[rebase,'elapsedDurationMs']) / 1000) / dist
    
#If we don't rebase, the series gives the time (in s) per km


#Also need to be able to rebase relative to:
# - stage winner
# - overall leader going in to stage?
# - overall leader at end of stage?
```

```python tags=["active-ipynb"]
#Pace report - time gained / lost on stage in seconds per km 
paceReport('SS7', rebase='TÄN')
```

```python tags=["active-ipynb"]
paceReport('SS11')
```

```python tags=["active-ipynb"]
type(paceReport('SS1', rebase='OGI'))
```

Maybe create a table with row per driver and column per stage showing time gained / lost in seconds per km to each other driver on that stage. Display as heatmap.

```python
def multiStagePaceReport(stages, rebase=None):
    report = pd.DataFrame()
    for stage in stages:
        report[stage] = paceReport(stage, rebase=rebase)
    return report
```

```python tags=["active-ipynb"]
driver='LAT'

tmp =  multiStagePaceReport(['SS{}'.format(i) for i in range(1,24)], driver )
tmp
```

```python
from dakar_utils import moreStyleDriverSplitReportBaseDataframe
from IPython.display import HTML
```

```python tags=["active-ipynb"]


pd.set_option('precision', 1)

s2 = moreStyleDriverSplitReportBaseDataframe(-tmp.T,'')
display(HTML(s2))
```

```python tags=["active-ipynb"]
import dakar_utils as dakar

#The negative map means we get times as the rebased driver is concerned...
s2 = moreStyleDriverSplitReportBaseDataframe(-tmp.loc[['TÄN', 'LAP', 'LAT', 'MIK', 'OGI', 'BRE', 'NEU', 'SUN', 'GRE',
       ],:],'')
display(HTML(s2))

dakar.getTablePNG(s2, fnstub='pace_{}_'.format(driver),scale_factor=2)
```

```python tags=["active-ipynb"]
s2 = moreStyleDriverSplitReportBaseDataframe(tmp.T,'')
display(HTML(s2))
```

Need some simple functions (they may already exist) for things like:

- driverID / code ordered by overall position rank at end of stage N.


## Widgetise

Make some controls for this...

```python

```
