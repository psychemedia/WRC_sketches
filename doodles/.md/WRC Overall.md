---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.0rc1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# WRC Overall

Generate a graphic enriched tabel summarising rally evolution over multiple stages, rebased to a specific driver.

```python
if __name__=='__main__':
    %load_ext autoreload
    %autoreload 2
    
import notebookimport
sr = __import__("Charts - Stage Results")

```

```python
# TO DO
# do a step line chart for relative / rebased positions ahead / behind
```

```python
import pandas as pd

from IPython.display import HTML

import dakar_utils as dakar
from dakar_utils import moveColumn, sparkline2, sparklineStep, moreStyleDriverSplitReportBaseDataframe

import sqlite3
```

```python
if __name__=='__main__':
    dbname2='../../wrc-timing/finland19.db'
    conn2 = sqlite3.connect(dbname2)

    c2 = conn2.cursor()

```

```python
from IPython.display import HTML
```

```python
if __name__=='__main__':
    #We can't use codes becuase they are not unique
    #We need to use entryId
    q= 'SELECT entryId, `driver.code` AS Code FROM startlists'
    codes = pd.read_sql(q,conn2).set_index('entryId')
    codes.head()
```

```python
def _rebaseTimes(times, bib=None):
    ''' Rebase times relative to specified driver. '''
    #SHould we rebase against entryId, so need to lool that up. In which case, leave index as entryId
    if bib is None: return times
    #bibid = codes[codes['Code']==bib].index.tolist()[0]
    return times - times.loc[bib]
```

```python
if __name__=='__main__':
    REBASER=306
```

```python

```

```python
#Should set this from current year?
#YEAR is used in a function def - reset that to None and handle?
YEAR=2019
if __name__=='__main__':
    #For WRC
    rc='RC1'
    rally='Finland'
    typ='overall'
    wREBASE='TÄN'
```

### Day Based Reporting

How do we limit the report to just show the stages on a particular day, or particular loop?

```python
#Based on a function in Itinerary Basics
def dbGetSSitinerary(conn, rally, year=YEAR):
    ''' Get dataframe containing time control details for a specified rally. '''
    q='''
    SELECT il.name AS date, itc.*, ce.timeZoneOffset,
         isc.itinerarySectionId, isc.name AS section, isc.`order`
    FROM itinerary_controls itc
    JOIN championship_events ce ON itc.eventId=ce.eventId
    JOIN itinerary_sections isc ON itc.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
    JOIN itinerary_legs il ON isc.itineraryLegId=il.itineraryLegId
    WHERE ce.`country.name`="{rally}" AND strftime('%Y', startDate)='{year}'
            AND firstCarDueDateTimeLocal NOT NULL 
            AND itc.type='StageStart'
            ORDER BY firstCarDueDateTimeLocal 
    '''.format(rally=rally, year=year)
    time_controls = pd.read_sql(q,conn)
    time_controls['firstCarDueDateTimeLocal']=pd.to_datetime(time_controls['firstCarDueDateTimeLocal'])
    return time_controls
```

Create a day index so that we can limit reports to show a particular day, set of days, or up to and including a particular day.

We could also support reporting by a section selection.

```python
def listify(items):
    ''' Turn an argument to a list. '''
    return [] if items is None else items if isinstance(items, list) else [items]


def getStagesByDay(daynums=None, sections=None):
    ''' Return the stages for a given day, days, section or sections. '''
    daynums = listify(daynums)
    sections = listify(sections)
    
    schedule = dbGetSSitinerary(conn2,rally)
    #The grouper will return a group ID, but not in order?
    #schedule['daynum'] = schedule.groupby('date').grouper.label_info
    #https://stackoverflow.com/a/41638343/454773
    schedule['index'] = schedule[['date']].merge( schedule.drop_duplicates( 'date' ).reset_index(), on='date' )['index'].rank(method='dense').astype(int)
    tmp = schedule[['date','code','section','order','index']]
    if daynums:
        tmp = tmp[tmp['index'].isin(daynums)]
    if sections:
        tmp = tmp[tmp['order'].isin(sections)]
    
    return tmp

```

```python
if __name__=='__main__':
    getStagesByDay(daynums=3)
```

```python
#We can optimise this function so that we only generate charts for specified rows
def _gapToLeaderBar(Xtmpq, typ, stages=None, milliseconds=True, items=None):
    if milliseconds:
        Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    k = '{}GapToLeader'.format(typ)
    Xtmpq[k] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
    
    #Chart generation is the slow step, so only do it where we need it
    if items is None:
        Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    else:
        #Use loc for index vals, iloc for row number
        Xtmpq[k].loc[items] = Xtmpq[k].loc[items].apply(sparkline2, typ='bar', dot=True)
    return Xtmpq 

def gapToLeaderBar2(Xtmpq, typ, stages=None, milliseconds=True, items=None):
    Xtmpq = Xtmpq[['entryId','snum', 'diffFirstMs']].pivot(index='entryId',columns='snum',values='diffFirstMs')
    return _gapToLeaderBar(Xtmpq, typ, stages, milliseconds, items)
```

<!-- #raw -->
#Need to deprecate this...
def gapToLeaderBar(conn, rally, rc, typ, stages=None, milliseconds=True):
    Xtmpq = sr.dbGetStageRank(conn, rally, rc, typ, stages)#.head()
    Xtmpq = Xtmpq[['entryId','snum', 'diffFirstMs']].pivot(index='entryId',columns='snum',values='diffFirstMs')
    if milliseconds:
        Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    k = '{}GapToLeader'.format(typ)
    Xtmpq[k] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
    Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    return Xtmpq 
<!-- #endraw -->

```python
def gapBar(df):
    ''' Bar chart showing rebased gap at each stage. '''
    col='Gap'
    df[col] = df[[c for c in df.columns if c.startswith('SS_') and c.endswith('_overall')]].values.tolist()
    df[col] = df[col].apply(lambda x: [-y for y in x])
    df[col] = df[col].apply(sparkline2, typ='bar', dot=False)
    return df
```

```python
if __name__=='__main__':
    display(sr.dbGetStageRank(conn2, rally, rc, typ, None).head())
    print(sr.dbGetStageRank(conn2, rally, rc, typ, None).columns)
```

```python
if __name__=='__main__':
    display(sr.dbGetStageRank(conn2, rally, rc, typ, None)[['entryId','snum', 'position']].pivot(index='entryId',columns='snum',values='position'))
```

Chart generation is slow. Is this in the pivot steps, perhaps?

```python
#def positionStep(conn, rally, rc, typ, stages=None):
def _positionStep(Xtmpq, typ, stages=None):
    Xtmpq.columns = ['SS_{}_{}_pos'.format(c, typ) for c in Xtmpq.columns]
    k = '{}Position'.format(typ)
    Xtmpq[k] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
    Xtmpq[k] = Xtmpq[k].apply(sparklineStep)
    return Xtmpq 

def positionStep(Xtmpq, typ, stages=None):
    #Xtmpq = sr.dbGetStageRank(conn, rally, rc, typ, stages)#.head()
    Xtmpq = Xtmpq[['entryId','snum', 'position']].pivot(index='entryId',columns='snum',values='position')
    return  _positionStep(Xtmpq, typ, stages)

# TO DO - this is really clunky; need a better way
def overallAtLastStage(conn, rally, rc, typ, stages=None):
    ''' Get overall rank associated with last stage in table. '''
    Xtmpq = sr.dbGetStageRank(conn, rally, rc, typ, stages)#.head()
    Xtmpq = Xtmpq[['entryId','snum', 'position']].pivot(index='entryId',columns='snum',values='position')
    last = Xtmpq.columns
    return Xtmpq[[last[-1]]]
    

def generateOverallResultsChartable(conn, rally, rc, rebase=None, stages=None, days=None, sections=None):
    ''' Generate overall results table for a particular event. '''
    
    if days:
        stages = listify(stages) + getStagesByDay(daynums=days)['code'].tolist()
    
    if sections:
        stages = listify(stages) + getStagesByDay(sections=sections)['code'].tolist()
    
    #Using the codes list as a base, add in the position step chart showing overall position evolution
    #Note that there may be duplicate codes
    #wrc = pd.merge(codes, positionStep(conn, rally, rc, 'overall', stages=stages)[['overallPosition']], left_index=True, right_index=True)
    _stages_overall = sr.dbGetStageRank(conn, rally, rc, 'overall', stages)
    wrc = pd.merge(codes, positionStep(_stages_overall, 'overall', stages)[['overallPosition']], left_index=True, right_index=True)
    
    #Add in bar chart showing gap to leader at each stage
    #wrc = pd.merge(wrc, gapToLeaderBar(conn, rally, rc, 'overall', stages), left_index=True, right_index=True)
    wrc = pd.merge(wrc, gapToLeaderBar2(_stages_overall, 'overall', stages), left_index=True, right_index=True)
    
    moveColumn(wrc, 'overallGapToLeader', right_of='overallPosition')
    
    
    wrc['Pos'] = overallAtLastStage(conn, rally, rc, typ, stages)
    moveColumn(wrc, 'Pos', right_of='overallGapToLeader')
    #By this point it seems we may have introduced a duplicate? But how?
    #Add in step chart for stage ranks
    #wrc = pd.merge(wrc, positionStep(conn, rally, rc, 'stage', stages)[['stagePosition']], left_index=True, right_index=True)
    _stages_stage = sr.dbGetStageRank(conn, rally, rc, 'stage', stages)
    wrc = pd.merge(wrc, positionStep(_stages_stage, 'stage', stages)[['stagePosition']], left_index=True, right_index=True)

    #Add in bar chart for gap to stage leader
    #wrc = pd.merge(wrc, gapToLeaderBar(conn, rally, rc, 'stage', stages), left_index=True, right_index=True)
    wrc = pd.merge(wrc, gapToLeaderBar2(_stages_stage, 'stage', stages), left_index=True, right_index=True)
    wrc.rename(columns={'stageGapToLeader':'stageWinnerGap'},inplace=True)
    moveColumn(wrc, 'stageWinnerGap', right_of='stagePosition')


    wrc = wrc.sort_values('Pos', ascending=True)
    
    #At this point the index is the entryId
    #We can't really set the index on Code because there may be duplicate code values
    wrc=wrc.set_index('Code', drop=True)
    
    #Some tidying up if we have stages in the db but no results...
    wrc=wrc.dropna(how='all', axis='columns')
    
    cols = [c for c in wrc.columns if c.startswith('SS')]
      
    #We need to always rebase to make sure the stage bars are correct
    #if None rebase to the overall leader at last stage?
    if rebase is not None:
        #This will break if we provide one of the duplicate Code values...
        #If not, we should be okay...
        wrc[cols] = -wrc[cols].apply(_rebaseTimes, bib=rebase, axis=0)
    
    #Add in bar chart showing gap relative to other cars from rebased car
    #This needs to be done after rebasing
    wrc = gapBar(wrc)
    moveColumn(wrc, 'Gap', left_of='stagePosition')
    
    return wrc

   
```

```python
if __name__=='__main__':
    tmp = generateOverallResultsChartable(conn2, rally, rc, rebase=wREBASE, days=1, sections=3)
    tmp                
 
```

```python
if __name__=='__main__':
    wREBASE='LAT'
    tmp = generateOverallResultsChartable(conn2, rally, rc, rebase=wREBASE, stages=None)#[9])
    #tmp = generateOverallResultsChartable(conn2, rally, rc, rebase=wREBASE, days=4)#[9])
    
    #Throws an error if days is too many

```

```python
if __name__=='__main__':
    s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')

    #Introduce a dot marker to highlight winner
    display(HTML(s2))
    dakar.getTablePNG(s2, fnstub='overall_{}_'.format(wREBASE),scale_factor=2)

```


```python
if __name__=='__main__':
    tmp.index
```

```python
if __name__=='__main__':
    !ls -al testpng/
```

## Ultimate Margins

...aka *time left on table*...


Total time lost for driver $i$ over first $N$ stages is ${}_{N}\Delta_i = \sum_{s=1}^N \Delta_{i,s}$ where $\Delta_{i,s}=t_{i,w}-t_{w,s}$ and $t_{i,s}$ is the time on stage $s$ for driver $i$ and $t_{w,s}$ is time on stage $s$ for the stage winner, $w$. We then plot $y={}_{N}\Delta_i$ against $x=s:1..N$ for driver $i$.

For an even more exacting metric, we might also look at the deltas for time taken between each split.

We can also look at turning that into a percentage, *cf.* Formula One 107% times.

For example:${}_N\nabla_{i} = \frac{\sum_{s=1}^N t_{s,i}}{\sum_{s=1}^N t_{s,w}}$ and again plot $y={}_{N}\nabla_i$ against $x=s:1..N$ for driver $i$.

Note that this gives meaning to "giving 110%" in a roundabout sort of way. A driver might be running at 105% winner time in early stages, then improve to bring this down to 103%..

105 -> 103 means you give E.105 = 103: E = 103/105. To make that 100, (103/105) * (1/E) * 100 = 100. So "gave 110%" is (100 * oldPerCent/newPercent)

Alternatively... overall leader does time 110, driver does 105. So driver does P * 105 = 110, P = 110 /105 = overall / driver ? I prefer old/new formualtion -  relative to driver? If he gave it 100%, his 105% behind at start of stage would be 105% behind at end of stage?


We can also look to adding lower margins to table, e.g. searching for `max(positive delta)` to find the amount of time lost to the leader on each stage.


Leader rebasing: also consider dynamic / leader rebasing; eg rebaser kernel is `{'SS1':'LOE','SS2':'NEU', etc...}` then get times for each of those to rebase against.


We need smoething along the lines of:

# ```
#ultimate times to rebase against
for each stage:
    for each sector:
        get min(sector_time) where sector_time>0

# ```

We can then rebase each driver's splits against these times.

For a cruder metric based just on the stage time, we can simply find the faset stage time in each stage to rebase against.

```python

```

```python
if __name__=='__main__':
    #Need a WRC query for this
    data
    #cols SS, Overall position, Stage position, with a driver index
```

```python
if __name__=='__main__':
    wrc.plot(x='SS_1_overall',drawstyle="steps-mid",linestyle=':')
    plt.gca().invert_yaxis()
```

```python

```
