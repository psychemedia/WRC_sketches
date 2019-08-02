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

```python
if __name__=='__main__':
    #dbname='wrc18.db'
    YEAR=2019
    dbname='chile19.db'
    conn = sqlite3.connect(dbname)
    rally='Chile'
    rebase = 'LOE'
    rebase = ''
```

```python
#pd.read_sql('SELECT DISTINCT(code) FROM itinerary_controls',conn)
pd.read_sql('SELECT DISTINCT code, stageId FROM itinerary_stages',conn)
```

```python
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

```python
yy = stageDistances(conn)
yy.head()
```

```python
#Distance still to run
yy.loc['SS2':,:]['distance'].sum()
```

```python
yy.loc['SS2':,:].head()
```

```python
yy.loc['SS5']
```

```python
def distToRun(nextStage):
    return distances.loc[nextStage:,:]['distance'].values.sum()

def stageDist(stage):
    if isinstance(stage,list):
        return distances.loc[stage,:]['distance'].sum()
    return distances.loc[stage,:]['distance']

```

```python
distances = stageDistances(conn)
stageDist('SS1'), stageDist(['SS1','SS2'])
```

```python
distToRun('SS6')
```

```python
q="SELECT * FROM stage_times_overall WHERE stageId=1125 ORDER BY totalTimeMs LIMIT 10 ;"
pd.read_sql(q,conn)
```

```python
stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages='SS5').set_index('drivercode')
stagerank_overall
```

```python
stagerank_overall.columns
```

```python
stagerank_overall['snum']
```

```python
stagerank_overall['totalTimeS'] = stagerank_overall['totalTimeMs']/1000
```

```python
stagerank_overall[['entrant.name', 'totalTime','totalTimeS']]
```

```python
stagerank_overall.loc['MEE','totalTimeS']
```

```python
#rebase
stagerank_overall['totalTimeS'] - stagerank_overall.loc['MEE','totalTimeS']
```

```python
def requiredPace(nextstage, times,  rebase=None, allrally=True):
    ''' Pace required on competitive distance remaining.
        We can report this as the number of seconds required per km,
        but not as a speed unless we have a target speed.
    '''
    
    if allrally and not isinstance(nextstage,list)
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

```python
stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages='SS3').set_index('drivercode')
requiredPace(['SS4','SS5'], stagerank_overall,'LAP', allrally=False )
```

```python
stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages='SS1').set_index('drivercode')
requiredPace('SS2', stagerank_overall,'MEE' )

#Positive means driver has to drive that much faster in km/
```

```python
def paceReport(stage, rebase=None):
    ''' Time gained / lost per km on a stage. '''
    stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall', stages=stage).set_index('drivercode')
    dist = stageDist(stage)
    print('Stage dist is {} km'.format(dist))
    if rebase is None:
        return (stagerank_overall['stageTimeMs'] / 1000) / dist
    else:
        return ((stagerank_overall['stageTimeMs'] - stagerank_overall.loc[rebase,'stageTimeMs']) / 1000) / dist
    
    
```

```python
#Pace report - time gained / lost on stage in seconds per km 
paceReport('SS1', rebase='OGI')
```

```python

```

```python

```
