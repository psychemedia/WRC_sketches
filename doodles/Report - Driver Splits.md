---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.0'
      jupytext_version: 0.8.6
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Driver Splits

Script to rebase the split times for a stage and display them relative to a specified driver.

The intention is to generate a report on a stage that is meaningful to a specified driver.

Ideally the report should:

- show where the driver finished on the stage (stage rank)
- show the running stage delta at each split compared to each other driver
- show the extent to which a driver gained or lost time on each split compared to each other driver
- show the start order (so that this can be related to stage rank)
- identify the overall position at the end of the stage for each driver
- show whether overall positions were gained or lost after the stage (not implemented yet; need a +=- column)


Ideally, we'd use drivercodes, but these are not necessarily unique. For example, there are duplicates in RC2.

```python
if __name__=='__main__':
    %load_ext autoreload
    %autoreload 2
```

```python
import notebookimport

if __name__=='__main__':
    typ = 'overall' #this defines ???
    #rebase='overallleader' #TO DO
    rebase='OGI'#'PAD'
    MAXINSPLITDELTA=20 #set xlim on the within split delta
    ss='SS3'
    
    #The drivercode inbuilds some intelligence
    drivercode=rebase
```

```python
sr = __import__("Charts - Stage Results")
ssd = __import__("Charts - Split Sector Delta")
```

```python
#!pip3 install pytablewriter
```

Set up a connection to a simple SQLite database, and specify some metadata relating to the actual rally we are interested in.

```python
import os
import sqlite3
import pandas as pd
import pytablewriter
import six
from numpy import NaN

#dbname='wrc18.db'
#dbname='france18.db'
#conn = sqlite3.connect(dbname)

if __name__=='__main__':
    #dbname='wrc18.db'
    dbname='sweden19.db'
    conn = sqlite3.connect(dbname)
    rally='Sweden'
    rc='RC1'
    year=2019
    #ss='SS4'
```

```python
if __name__=='__main__':
    #This doesn't appear to be used elsewhere in this notebook
    #May support logic for checking stage status?
    stagedetails = sr.dbGetRallyStages(conn, rally).sort_values('number')
    stagedetails.head()
```

```python
if __name__=='__main__':
    #Let's see what data is available to us in the stagerank_overall table
    stagerank_overall = sr.getEnrichedStageRank(conn, rally, rc=rc, typ='overall')
    print(stagerank_overall.columns)
    display(stagerank_overall.head())
```

```python
if __name__=='__main__':
    #Get the total stage time for specified driver on each stage
    #We can then subtract this from each driver's time to get their times as rebased delta times
    #  compared to the the specified driver
    rebaser = stagerank_overall[stagerank_overall['drivercode']==drivercode][['code','totalTimeMs']].set_index('code').to_dict(orient='dict')['totalTimeMs']
    display(rebaser)
```

```python
def rebaseOverallRallyTime(stagerank_overall, drivercode):
    ''' Rebase overall stage rank relative to a specified driver. '''
    #Get the time for each stage for a particular driver
    rebaser = stagerank_overall[stagerank_overall['drivercode']==drivercode][['code','totalTimeMs']].set_index('code').to_dict(orient='dict')['totalTimeMs']
    #The stagerank_overall['code'].map(rebaser) returns the total time for each stage achieved by the rebase driver
    # stagerank_overall['code'] identifies the stage
    #Subtract this rebase time from the overall stage time for each driver by stage
    stagerank_overall['rebased'] = stagerank_overall['totalTimeMs'] - stagerank_overall['code'].map(rebaser)
    return stagerank_overall
```

```python
if __name__=='__main__':
    #Preview the stagerank_overall contents for a particular stage
    display(stagerank_overall[stagerank_overall['code']==ss][['drivercode','position','totalTimeMs','code']])
```

```python
def rebased_stage_stagerank(conn,rally,ss,drivercode,rc='RC1',typ='overall'):
    ''' Calculate the rebased time for each driver, in a specified stage (ss),
        relative to a specified driver (drivercode).
        Returns columns: ['position','totalTimeMs','code','rebased','Overall Time']
    '''
    stagerank_overall = sr.getEnrichedStageRank(conn, rally, rc=rc, typ=typ)
    zz=rebaseOverallRallyTime(stagerank_overall, drivercode)#, ss)
    #Get the rebased times for a particular stage
    #The position corresponds to either overall or stage pos
    zz=zz[zz['code']==ss][['drivercode','position','totalTimeMs','code', 'rebased']].set_index('drivercode')
    #Scale down the time from milliseconds to seconds
    zz['Overall Time']=-zz['rebased']/1000
    return zz
```

```python
if __name__=='__main__':
    zz=rebased_stage_stagerank(conn,rally,ss, drivercode, rc)
    display(zz)
```

```python
if __name__=='__main__':
    display(stagerank_overall.columns)
```

```python
if __name__=='__main__':
    #Preview a long format dataframe describing position and stage code for a specified driver
    #This appears not be be referenced anywhere else in this notebook
    stagerank_stage = sr.getEnrichedStageRank(conn, rally, rc=rc, typ='stage')
    stagerank_stage[stagerank_stage['drivercode']==rebase][['position','code']]
```

```python
if __name__=='__main__':
    sr.dbGetStageRank(conn, rally, rc, 'overall', stages='SS8').columns
```

```
'elapsedDuration', 'elapsedDurationMs', 'entryId', 'splitDateTime',
       'splitDateTimeLocal', 'splitPointId', 'splitPointTimeId',
       'stageTimeDuration', 'stageTimeDurationMs', 'startDateTime',
       'startDateTimeLocal', 'stageId', 'class', 'code', 'distance', 'name',
       'drivercode', 'elapsedDurationS'
```

```python
if __name__=='__main__':
    #If there are no splits, ssd.dbGetSplits should optionally get the overall times from elsewhere as a single split
    splits = ssd.dbGetSplits(conn,rally,ss,rc)#, forcesingle=True)

    elapseddurations=ssd.getElapsedDurations(splits)
    display(elapseddurations.head())
```

```python
def getRoadPosition(conn,rally,rc='RC1',stages=None):
    ''' Get road position for each driver for a given stage.
    
        NOTE:
        The start time is only available from stages with split times recorded.
        We can't get road position for stages with no splits.
    
    '''
    
    #TO DO - this doesn't seem to work on stage with no splits?
    roadPos=sr.dbGetStageStart(conn, rally, rc, stages)
    roadPos=roadPos[['drivercode','startDateTime','startpos']]
    roadPos.columns=['drivercode','startDateTime','Road Position']
    roadPos = roadPos.set_index('drivercode')
    return roadPos

```

```python
if __name__=='__main__':
    roadPos = getRoadPosition(conn,rally,rc,ss)
    display(roadPos)
```

```
if __name__=='__main__':
    #Preview splits for a couple of drivers
    display(splits[splits['drivercode'].isin(['PAD','NEU'])])
```

```python
def waypoint_rank(splitdurations, on='section',by='elapsedDurationS'):
    ''' Return rank at each waypoint. '''
    splitdurations['split_pos'] = splitdurations.groupby(on)[by].rank(method='min',na_option='keep')#.astype(int)
    
    #For diff to first, do we want first at each waypoint or first at end of stage?
    #Use diff to driver in first at each waypoint
    splitdurations['gapToStageLeader'] = splitdurations[by] - splitdurations.groupby(on)[by].transform('min')
    #For each group, rebase relative to that time
    return splitdurations

```

```python
if __name__=='__main__':
    rebasedelapseddurations = ssd.rebaseElapsedDurations(elapseddurations, drivercode)
    #This returns columns of the form: drivercode	elapsedDurationS	startDateTime	section	rebased
    #If there are no splits, this is currently an empty dataframe
    rebasedelapseddurations = waypoint_rank(rebasedelapseddurations,by = 'elapsedDurationS')
    display(rebasedelapseddurations.head())
```

```python
if __name__=='__main__':
    rebasedelapseddurations
```

```python
from dakar_utils import sparklineStep, sparkline2, moveColumn

def pivotRebasedElapsedDurations(rebasedelapseddurations, ss):
    ''' Pivot rebased elapsed durations (that is, deltas relative target).
        Rows give stage delta at each split for a specific driver.
        
        Returns columns of the form: ['1','2','3','SS9 Overall']
    '''
    if rebasedelapseddurations.empty:
        return pd.DataFrame(columns=['drivercode']).set_index('drivercode')
    
    rbe=-rebasedelapseddurations.pivot('drivercode','section','rebased')
    
    
    #TO DO: DRY stuff here - mungeForSparkLine function, maybe?
    #Add in a bar chart to identify evolving gap at each waypoint
    #Gap refers to the difference between driver
    col='Rebase Gap'
    rbe[col] = rbe[[c for c in rbe.columns ]].values.tolist()
    rbe[col] = rbe[col].apply(lambda x: [-y for y in x])
    rbe[col] = rbe[col].apply(sparkline2, typ='bar', dot=False)
    
    rbe.columns=list(rbe.columns)[:-2]+['{} Overall'.format(ss), 'Rebase Gap']
    rbe=rbe.sort_values(rbe.columns[-2],ascending = False)
    
    rbe2=rebasedelapseddurations.pivot('drivercode','section','split_pos')
    rbe2.columns=['{}_pos'.format(i) for i in rbe2.columns]
    col='Waypoint Rank'
    rbe2[col] = rbe2[[c for c in rbe2.columns ]].values.tolist()
    rbe2[col] = rbe2[col].apply(lambda x: [-y for y in x])
    rbe2[col] = rbe2[col].apply(sparklineStep)
    rbe = pd.merge(rbe,rbe2[col],left_index=True,right_index=True)
    
    rbe2=rebasedelapseddurations.pivot('drivercode','section','gapToStageLeader')
    rbe2.columns=['{}_pos'.format(i) for i in rbe2.columns]
    col='gapToStageLeader'
    rbe2[col] = rbe2[[c for c in rbe2.columns ]].values.tolist()
    rbe2[col] = rbe2[col].apply(lambda x: [-y for y in x])
    rbe2[col] = rbe2[col].apply(sparkline2, typ='bar', dot=True)
    rbe = pd.merge(rbe,rbe2[col],left_index=True,right_index=True)

    return rbe

if __name__=='__main__':
    rbe = pivotRebasedElapsedDurations(rebasedelapseddurations, ss)
```

```python
if __name__=='__main__':
    display(rbe)
```

```python
#https://pandas.pydata.org/pandas-docs/stable/style.html
def color_negative(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    if isinstance(val, str): return ''
    elif val and (isinstance(val,int) or isinstance(val,float)):
        color = 'green' if val < 0 else 'red' if val > 0  else 'black'
    else:
        color='white'
    return 'color: %s' % color
```

```python
if __name__=='__main__':
    #test of applying style to pandas dataframe
    #Is this really fown to pandas to fail gracefully if df is empty??
    s = rbe.style.applymap(color_negative)
    display(s)
```

```python
# TO DO:
# - calculate stage position at each split
# - calculate rank within that sector
```

```python
if __name__=='__main__':
    #splitdurations are the time in each sector (time take to get from one split to the next)
    #But what if there are no splits? We get an empty dataframe...
    splitdurations = ssd.getSplitDurationsFromSplits(conn,rally,ss,rc)
    #splitdurations = waypoint_rank(splitdurations, 'section','stageTimeDurationMs' )
    display(splitdurations)#.head()
```

```python
if __name__=='__main__':
    #This will be an empty dataframe if there are no splits
    rebasedSplits = ssd.rebaseSplitDurations(splitdurations, drivercode)
    display(rebasedSplits.head())
```

```python
if __name__=='__main__':
    #preview what's available as a splitduration
    display(splitdurations[splitdurations['drivercode'].isin( ['PAD','NEU'])])
```

```python
def pivotRebasedSplits(rebasedSplits):
    ''' For each driver row, find the split. '''
    
    #If there are no splits...
    if rebasedSplits.empty:
        return pd.DataFrame(columns=['drivercode']).set_index('drivercode')
    
    rbp=-rebasedSplits.pivot('drivercode','section','rebased')
    rbp.columns=['D{}'.format(c) for c in rbp.columns]
    rbp.sort_values(rbp.columns[-1],ascending =True)
    return rbp

if __name__=='__main__':
    rbp = pivotRebasedSplits(rebasedSplits)
    display(rbp)
```

```python
if __name__=='__main__':
    #Just remind ourselves of what is available in the road position data
    display(roadPos)
```

```python
def getDriverSplitReportBaseDataframe(rbe,rbp, zz, roadPos, stageresult, ss):
    #TO DO: return empty w/ proper colnames
    if roadPos.empty: return pd.DataFrame()
    ''' Create a base dataframe for the rebased driver split report. '''
    
    stageresult.columns = ['drivercode','Stage Rank']
    rb2 = pd.merge(rbe,stageresult.set_index('drivercode'),left_index=True, right_index=True)

    rb2=pd.merge(rb2,zz[['position','Overall Time']],left_index=True, right_index=True)
    rb2.rename(columns={'position': 'Overall Position'}, inplace=True)
    
    #The following is calculated rather than being based on the actual timing data / result for the previous stage
    #Would be better to explicitly grab data for previous stage, along with previous ranking
    #display(rb2[['Overall Time','{} Overall'.format(ss)]])
    rb2['Previous'] =  rb2['Overall Time'] - rb2['{} Overall'.format(ss)]
    #Related to this, would be useful to have an overall places gained / lost column
    
    rb2=pd.merge(rb2,rbp,left_index=True, right_index=True)
    rb2=pd.merge(rb2,roadPos[['Road Position']],left_index=True, right_index=True)
    cols=rb2.columns.tolist()
    #Reorder the columns - move Road Position to first column
    rb2=rb2[[cols[-1]]+cols[:-1]]
    
    #reorder cols
    prev = rb2['Previous']
    rb2.drop(labels=['Previous'], axis=1,inplace = True)
    rb2.insert(1, 'Previous', prev)
    
    moveColumn(rb2,'Waypoint Rank',right_of='Previous')
    moveColumn(rb2,'Rebase Gap',right_of='Waypoint Rank')
    #The following line is not correctly locating... it's offsetting by 1 pos to right?
    moveColumn(rb2,'gapToStageLeader',right_of='Overall Position')
    
    
    return rb2

if __name__=='__main__':
    stageresult=sr.getEnrichedStageRank(conn, rally, stages=ss, rc=rc,typ='stage')[['drivercode','position']]
    rb2=getDriverSplitReportBaseDataframe(rbe,rbp, zz, roadPos, stageresult, ss)
    display(rb2)
```

```python
if __name__=='__main__':
    display(rb2.dtypes)
```

```python
#There seems to be missing tenths?
#Elapsed durations are provided in milliseconds. Need to round correctly to tenths?
#Elapsed times grabbed from ssd.dbGetSplits(conn,rally,ss,rc)

def cleanDriverSplitReportBaseDataframe(rb2, ss):
    ''' Tidy up the driver split report dataframe, replacing 0 values with NaNs that can be hidden.
        Check column names and data types. '''
    
    #TO DO: set proper colnames
    if rb2.empty: return rb2
    
    rb2=rb2.replace(0,NaN)
    #rb2=rb2.fillna('') #This casts columns containing NA to object type which means we can't use nan processing
    
    rb2['Road Position']=rb2['Road Position'].astype(float)
    return rb2

def __styleDriverSplitReportBaseDataframe(rb2, ss):
    ''' Test if basic dataframe styling.
        DEPRECATED. '''
    s=rb2.fillna('').style.applymap(color_negative,
                                    subset=[c for c in rb2.columns if isinstance(c, int) and c not in ['Overall Position', 'Road Position']])
    #data.style.applymap(highlight_cols, subset=pd.IndexSlice[:, ['B', 'C']])

    s.set_caption("{}: running split times and deltas within each split.".format(ss))
    return s
    
if __name__=='__main__':
    rb2c = cleanDriverSplitReportBaseDataframe(rb2.copy(), ss)
    s = __styleDriverSplitReportBaseDataframe(rb2c, ss)
    
```

```python
from IPython.core.display import HTML

if __name__=='__main__':
    html=s.render()
    display(HTML(html))
```

```python
from math import nan
def bg_color(s):
    ''' Set background colour sensitive to time gained or lost.
    '''
    attrs=[]
    for _s in s:
        if _s < 0:
            attr = 'background-color: green; color: white'
        elif _s > 0: 
            attr = 'background-color: red; color: white'
        else:
            attr = ''
        attrs.append(attr)
    return attrs
```

```python
import seaborn as sns

def moreStyleDriverSplitReportBaseDataframe(rb2,ss, caption=None):
    ''' Style the driver split report dataframe. '''
    
    if rb2.empty: return ''
        
    def _subsetter(cols, items):
        ''' Generate a subset of valid columns from a list. '''
        return [c for c in cols if c in items]
    
    
    #https://community.modeanalytics.com/gallery/python_dataframe_styling/
    # Set CSS properties for th elements in dataframe
    th_props = [
      ('font-size', '11px'),
      ('text-align', 'center'),
      ('font-weight', 'bold'),
      ('color', '#6d6d6d'),
      ('background-color', '#f7f7f9')
      ]

    # Set CSS properties for td elements in dataframe
    td_props = [
      ('font-size', '11px')
      ]

    # Set table styles
    styles = [
      dict(selector="th", props=th_props),
      dict(selector="td", props=td_props)
      ]
    
    #Define colour palettes
    #cmg = sns.light_palette("green", as_cmap=True)
    #The blue palette helps us scale the Road Position column
    # This may help us to help identify any obvious road position effect when sorting stage times by stage rank
    cm=sns.light_palette((210, 90, 60), input="husl",as_cmap=True)

    s2=(rb2.style
        .background_gradient(cmap=cm, subset=_subsetter(rb2.columns, ['Road Position']))
        .applymap(color_negative,
                  subset=[c for c in rb2.columns if isinstance(c, int) and c not in ['Overall Position', 'Road Position']])
        .highlight_min(subset=_subsetter(rb2.columns, ['Overall Position']), color='lightgrey')
        .highlight_max(subset=_subsetter(rb2.columns, ['Overall Time']), color='lightgrey')
        .highlight_max(subset=_subsetter(rb2.columns, ['Previous']), color='lightgrey')
        .apply(bg_color,subset=_subsetter(rb2.columns, ['{} Overall'.format(ss),'{} Overall*'.format(ss), 'Overall Time', 'Previous']))
        .bar(subset=[c for c in rb2.columns if str(c).startswith('D')], align='zero', color=[ '#5fba7d','#d65f5f'])
        .set_table_styles(styles)
        
        #.format({'total_amt_usd_pct_diff': "{:.2%}"})
       )
    
    if caption is not None:
        s2.set_caption(caption)

    #nan issue: https://github.com/pandas-dev/pandas/issues/21527
    return s2.render().replace('nan','')

if __name__=='__main__':
    rb2c = cleanDriverSplitReportBaseDataframe(rb2.copy(), ss)
    s2 = moreStyleDriverSplitReportBaseDataframe(rb2c, ss)
    display(HTML(s2))
```

```python
if __name__=='__main__':
    sr.dbGetStageRank(conn, rally, rc, typ='stage', stages=ss)[['position','drivercode','classrank']]
#'overall':'stage_times_overall', 'stage_times_overall':'stage_times_overall',
#              'stage':'stage_times_stage', 'stage_times_stage':'stage_times_stage'
#sr.getEnrichedStageRank(conn, rally, typ=typ)
```

```python

```

```python
if __name__=='__main__':
    sr.getDriverCodeBy(conn, rally, ss,'stage')
```

```python
if __name__=='__main__':
    ss
```

```python
if __name__=='__main__':
    sr.getEnrichedStageRank(conn, rally, stages=ss,rc=rc,typ='stage')
```

```python
if __name__=='__main__':
    rebased_stage_stagerank(conn,rally,ss,drivercode,rc=rc, typ='overall')
```

```python
def getDriverStageReport(conn, rally, ss, drivercode, rc='RC1', typ='overall', order=None, caption=None):
    ''' Generate a dataframe to report overall stage result. '''
    #'Previous',' SS9 Overall', 'Overall Position'	'Overall Time'; stage position by sort order
    
    if order is None: order='stage'
    #change cols depending on what report / sort order ie. remove redundant col
    
    #Get the overall results, rebased
    zz = rebased_stage_stagerank(conn,rally,ss,drivercode,rc=rc, typ='overall')
    zz.rename(columns={'position':'Overall Position'}, inplace=True)
    
    #Get stage result - does it need to be enriched?
    stageresult=sr.getEnrichedStageRank(conn, rally, stages=ss,rc=rc, typ='stage')

    stagerebaser = stageresult[stageresult['drivercode']==drivercode][['code','elapsedDurationMs']].set_index('code').to_dict(orient='dict')['elapsedDurationMs']
    #The stagerank_overall['code'].map(rebaser) returns the total time for each stage achieved by the rebase driver
    # stagerank_overall['code'] identifies the stage
    #Subtract this rebase time from the overall stage time for each driver by stage

    stcol='{} Time'.format(ss)
    sdeltacol='{} Overall'.format(ss)
    stageresult[sdeltacol] = -(stageresult['elapsedDurationMs'] - stageresult['code'].map(stagerebaser))
    stageresult=stageresult[['drivercode', 'position','elapsedDuration', sdeltacol,'elapsedDurationMs']]
    stageresult.columns=['drivercode', 'Stage Rank',stcol, sdeltacol,'stageDurationMs']
    stageresult[stcol] = stageresult[stcol].str.replace('00000','')
    
    combined = pd.merge(zz,stageresult, on='drivercode' )
    
    combined[sdeltacol] = combined[sdeltacol]/1000
    combined['Previous'] = (combined['Overall Time']-combined[sdeltacol])
    
    _tmp=combined[['drivercode','Previous','Stage Rank',stcol,sdeltacol,'Overall Position','Overall Time']].replace(0,NaN).set_index('drivercode')

    if order=='overall':
        combined=combined.sort_values('Overall Position', ascending=True)
    elif order=='previous':
        combined=combined.fillna(0).sort_values('Previous', ascending=False).replace(0,NaN)
    elif order=='stage':
        combined=combined.sort_values('Stage Rank', ascending=True)
    else:
        #Default is stage order
        combined=combined.sort_values('Stage Rank', ascending=True)
    
    s2 = moreStyleDriverSplitReportBaseDataframe(_tmp, ss, caption)
    return s2

if __name__=='__main__':
    s2=getDriverStageReport(conn, rally, ss, drivercode)
    display(HTML(s2))
```

```python
def getDriverSplitsReport(conn, rally, ss, drivercode, rc='RC1', typ='overall', 
                          order=None, caption=None, bars=True, dropcols=None):
    ''' Generate dataframe report relative to a given driver on a given stage.
            order: sorts table according to: overall | previous | roadpos
            
        At the moment, the splits reporter doesn't report anything if there are no splits.
        In this case, default to a simple overal stage (without splits) reporter table.
    '''
    
    dropcols = [] if dropcols is None else dropcols
    #TO DO - this needs to fail gracefully if there are no splits
    
    #Allow the drivercode to be relative to a position
    #if drivercode=='firstonroad':
        #allow things like onroad1, onroad2?
    #    drivercode=
    #elif drivercode=='previousfirst':
        #allow things like previous1, previous2?
    #    drivercode = 
    #elif drivercode = 'stagewinner':
        #allowthings like stage1, stage2?
    #    drivercode = 
    
    
    #Get the overall results, rebased
    zz = rebased_stage_stagerank(conn,rally,ss,drivercode,rc=rc, typ=typ)
    
    #Get the road position
    roadPos = getRoadPosition(conn,rally,rc,ss)
    if roadPos.empty:
        #Should we automatically offer the stagetable report as an alternative
        return getDriverStageReport(conn, rally, ss, drivercode, rc=rc, order=order, caption=caption)
    
    #Get the splits
    splits = ssd.dbGetSplits(conn,rally,ss,rc)
    elapseddurations=ssd.getElapsedDurations(splits)
    
    #Rebase the split elapsed durations
    rebasedelapseddurations = ssd.rebaseElapsedDurations(elapseddurations, drivercode)
    rebasedelapseddurations = waypoint_rank(rebasedelapseddurations,by = 'elapsedDurationS')
    rbe = pivotRebasedElapsedDurations(rebasedelapseddurations, ss)
    
    #splitdurations are the time in each sector (time take to get from one split to the next)
    splitdurations = ssd.getSplitDurationsFromSplits(conn,rally,ss,rc)
    rebasedSplits = ssd.rebaseSplitDurations(splitdurations, drivercode)

    rbp = pivotRebasedSplits(rebasedSplits)

    #Get stage result to merge in stage position
    stageresult=sr.getEnrichedStageRank(conn, rally, rc=rc, stages=ss,typ='stage')[['drivercode','position']]

    rb2=getDriverSplitReportBaseDataframe(rbe, rbp, zz, roadPos, stageresult, ss)
    rb2 = cleanDriverSplitReportBaseDataframe(rb2, ss)
    if not bars:
        rb2=rb2.drop([c for c in rb2.columns if str(c).startswith('D')], axis=1)
        
    if ss=='SS1':
        rb2['Previous']=NaN

    if order=='overall':
        rb2=rb2.sort_values('Overall Position', ascending=True)
        #Remove the redundant column
        rb2=rb2.drop(['Overall Position'], axis=1)
        #rb2=rb2.rename(columns={'Overall Position':'{} Overall*'.format(ss)})
    elif order=='previous':
        rb2=rb2.fillna(0).sort_values('Previous', ascending=False).replace(0,NaN)
        #rb2 = rb2.rename(columns={'Previous':'Previous*'})
    elif order=='roadpos':
        rb2=rb2.sort_values('Road Position', ascending=True)
        #rb2 = rb2.rename(columns={'Road Position':'Road Position*'})
    elif order=='stage':
        rb2.sort_values('Stage Rank', ascending=True)
        #Remove the redundant column
        rb2=rb2.drop(['Stage Rank'], axis=1)
    else:
        #Default is stage order
        rb2.sort_values('Stage Rank', ascending=True)
        #Remove the redundant column
        rb2=rb2.drop(['Stage Rank'], axis=1)
        rb2 = rb2.rename(columns={'{} Overall'.format(ss):'{} Overall*'.format(ss)})

    if caption =='auto':
        caption = 'Rebased stage split times for {}{}.'.format('{}, '.format(drivercode), ss)

    dc = [c for c in dropcols if c in rb2.columns]
    rb2 = rb2.drop(columns=dc)
        
    #s = styleDriverSplitReportBaseDataframe(rb2, ss)
    s2 = moreStyleDriverSplitReportBaseDataframe(rb2,ss, caption)
    return s2

if __name__=='__main__':
    s2 = getDriverSplitsReport(conn, rally, ss, drivercode, rc, typ)#, caption='auto')
    display(HTML(s2))
```

```python
if __name__=='__main__':
    ss='SS16'
    d='MIK'
    s2 = getDriverSplitsReport(conn, rally, ss, d, rc, typ)
    display(HTML(s2))
```

```
if __name__=='__main__':
    s2 = getDriverSplitsReport(conn, rally, 'SS3', 'LAT', rc, typ, 'overall')
    display(HTML(s2))
```

```
if __name__=='__main__':
    s2 = getDriverSplitsReport(conn, rally, 'SS11', 'PAD', rc, typ, 'previous')
    display(HTML(s2))
```

```
#Replace by Dakar tools
import os
import time
from selenium import webdriver


def getTableImage(url, fn='dummy_table', basepath='.', path='.', delay=5, height=420, width=800):
    ''' Render HTML file in browser and grab a screenshot. '''
    #should be a tmp file?
    #fname='testmap.html'
    #tmpurl='file://{path}/{mapfile}'.format(path=os.getcwd(),mapfile=fn)
    #folium_map.save(fn)
    browser = webdriver.Chrome()
    browser.set_window_size(width, height)
    browser.get(url)
    #Give the map tiles some time to load
    time.sleep(delay)
    imgpath='{}/{}.png'.format(path,fn)
    imgfn = '{}/{}'.format(basepath, imgpath)
    imgfile = '{}/{}'.format(os.getcwd(),imgfn)
    browser.save_screenshot(imgfile)
    browser.quit()
    os.remove(imgfile.replace('.png','.html'))
    #print(imgfn)
    return imgpath


def getTablePNG(tablehtml,basepath='.', path='testpng', fnstub='testhtml'):
    ''' Save HTML table as file. '''
    if not os.path.exists(path):
        os.makedirs('{}/{}'.format(basepath, path))
    fn='{cwd}/{basepath}/{path}/{fn}.html'.format(cwd=os.getcwd(), basepath=basepath, path=path,fn=fnstub)
    tmpurl='file://{fn}'.format(fn=fn)
    with open(fn, 'w') as out:
        out.write(tablehtml)
    return getTableImage(tmpurl, fnstub, basepath, path)
    #print(tmpurl)

    

if __name__=='__main__':
    getTablePNG(s2)
```

```python
if __name__=='__main__':
    from dakar_utils import getTablePNG
    getTablePNG(s2, fnstub='stage_{}_{}'.format(ss,d),scale_factor=5)
```

```
if __name__=='__main__':
    #Testing
    !pip3 install renderer
    #!pip3 install simple-settings
    #!pip3 install --upgrade git+https://github.com/istresearch/phantom-snap.git
    #!pip3 install phantom-snap

    from phantom_snap.settings import PHANTOMJS
    from phantom_snap.phantom import PhantomJSRenderer
    from phantom_snap.imagetools import save_image

    config = {
        'executable': '/usr/local/bin/phantomjs',
        'args': PHANTOMJS['args'] + ['--disk-cache=false', '--load-images=true']
    }
    r = PhantomJSRenderer(config)

    url = 'http://raallydatajunkie.com'
    html = s2

    try:
        page = r.render(url=url, html=html, img_format='PNG')
        save_image('.', page)
    finally:
        r.shutdown(15)
```

```python
if __name__=='__main__':
    s2 = getDriverSplitsReport(conn, rally, 'SS10', 'PAD', rc, typ, 'roadpos')
    display(HTML(s2))
```

```python
if __name__=='__main__':
    s2 = getDriverSplitsReport(conn, rally, 'SS20', 'TÄN', rc, typ,'stage')
    display(HTML(s2))
```

```python
if __name__=='__main__':
    s2 = getDriverSplitsReport(conn, rally, 'SS18', 'OGI', rc, typ)
    display(HTML(s2))
```

Problem with the bars is that the range is different in each column; ideally we want the same range in each column; could do this with two dummy rows to force max and min values?

```python
if __name__=='__main__':
    #Example for pandas issue https://github.com/pandas-dev/pandas/issues/21526
    import pandas as pd
    import numpy as np
    
    df=pd.DataFrame({'x1':list(np.random.randint(-10,10,size=10))+[-500,1000, -1000],
               'y1':list(np.random.randint(-5,5,size=13)),'y2':list(np.random.randint(-2,3,size=13)) })
    
    display(df.style.bar( align='zero', color=[ '#5fba7d','#d65f5f']))
```

```python
if __name__=='__main__':
    #clip lets us set a max limiting range although it means we lose the actual value?
    df['x2']= df['x1'].clip(upper=10, lower=-10)
    display(df.style.bar( align='zero', color=[ '#d65f5f','#5fba7d']))
```

```python
if __name__=='__main__':
    #for pandas 0.24 ? https://github.com/pandas-dev/pandas/pull/21548
    df['x2']= df['x1'].clip(upper=10, lower=-10)
    #Set axis=None for table wide range?
    #display(df.style.bar( align='zero', axis=None, color=[ '#d65f5f','#5fba7d']))
    
```

```python

```

```python

```
