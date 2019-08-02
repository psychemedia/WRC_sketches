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

# Charts - Stage Results

This notebook separates out code for generating *stage results* charts. Updates to the chart scripts will be made to this notebook.

```python
import sqlite3
import pandas as pd
```

```python
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.dates as mdates
```

```python
if __name__=='__main__':
    %matplotlib inline
```

```python
if __name__=='__main__':
    dbname='../../wrc-timing/finland19.db'
    conn = sqlite3.connect(dbname)

    q="SELECT name FROM sqlite_master WHERE type = 'table';"
    display(pd.read_sql(q,conn))
```

```python
if __name__=='__main__':
    year=2019
    rc='RC2'
    ss='SS4'
    rally='Finland'

    typ='stage_times_stage' #stage_times_stage stage_times_overall
    typ='stage_times_overall'
```

## Stage Results

```python
if __name__=='__main__':
    q='''
    SELECT st.*, sc.name as class, i.code, i.distance, i.name, CAST(REPLACE(code,'SS','') AS INTEGER) snum,
    sl.`driver.code` drivercode, sl.`entrant.name`
    FROM {typ} st INNER JOIN itinerary_stages i ON st.stageId = i.stageId
    INNER JOIN startlist_classes sc ON sc.entryid = st.entryId 
    INNER JOIN championship_events ce ON i.eventId=ce.eventId
    INNER JOIN startlists sl ON sl.entryId=sc.entryId
    WHERE sc.name="{rc}" AND ce.`country.name`="{rally}" ORDER BY snum
    '''.format(rc=rc,rally=rally, typ=typ)
    stagerank=pd.read_sql(q,conn)
    
    display( stagerank )
    
    stagerank['classrank'] = stagerank.groupby(['snum'])['position'].rank(method='dense').astype(int)

    display(stagerank.head(3))
```

We could further extend the above function to limit the query to return just the rows related to one or more specified stages.

```python
if __name__=='__main__':
    #Get differences in position and time to leader cf previous stage
    stagerank['gainedPos'] = stagerank.groupby(['drivercode'])['position'].diff()<0 
    stagerank['gainedTime'] = stagerank.groupby(['drivercode'])['diffFirstMs'].diff()<0 
```

```python
if __name__=='__main__':
    #An initial sketch chart, based on a race position chart
    #Number labels record the overall position, the y-value records the class position
    # Group properties
    gc = stagerank
    gcSize = len(gc['entryId'].unique()) #number of entries in class

    rankpos='position'
    #rankpos='classrank'

    # Create plot
    fig, ax = plt.subplots(figsize=(15,gcSize/2))

    #Set blank axes
    ax.xaxis.label.set_visible(False)
    ax.get_yaxis().set_ticklabels([])
    ax.set_yticks([]) 
    ax.set_xticks([]) 

    lhoffset=-1.95
    rhoffset=+0.5

    #labeler
    addlabels = True
    pinklabel= lambda x: int(x['position'])
    #pinklabel= lambda x: int(x['classrank'])
    #pinklabel= lambda x: x['drivercode']

    ylabel = lambda x: x['drivercode']
    ylabel = lambda x: int(x['position'])
    ylabel = lambda x: int(x['classrank'])
    ylabel = lambda x: '{} ({})'.format(x['drivercode'], int(x['classrank']))

    gcSize=10

    # Rally properties
    smin = gc['snum'].min() # min stage number
    smax = gc['snum'].max() # max stage number

    # Define a dummy rank to provide compact ranking display
    # This is primarily for RC1 which we expect to hold the top ranking positions overall
    gc['xrank']= (gc[rankpos]>gcSize)
    gc['xrank']=gc.groupby('snum')['xrank'].cumsum()
    gc['xrank']=gc.apply(lambda row: row[rankpos] if row[rankpos]<=gcSize  else row['xrank'] +gcSize, axis=1)

    #Base plot - line chart showing position of each driver against stages
    gc.groupby('entryId').plot(x='snum',y='xrank',ax=ax,legend=None)

    #Label cars ranked outside the group count - primarily for RC1
    if addlabels:
        for i,d in gc[gc['xrank']>gcSize].iterrows():
            ax.text(d['snum']-0.1, d['xrank'], pinklabel(d),
                    bbox=dict(  boxstyle='round,pad=0.3',color='pink')) #facecolor='none',edgecolor='black',

    #Label driver names at left hand edge
    for i,d in gc[gc['snum']==1].iterrows():
        ax.text(smin+lhoffset, d['xrank'], ylabel(d))

    #Label driver names at right hand edge
    for i,d in gc[gc['snum']==smax].iterrows():
        ax.text(smax+rhoffset, d['xrank'], ylabel(d))

    # Label x-axis stage numbers
    ax.set_xticks(stagerank['snum'].unique()) # choose which x locations to have ticks

    #Rotate the xticklabels to make them easier to read
    ax.set_xticklabels(stagerank['code'].unique(),rotation = 45, ha="right")

    plt.gca().invert_yaxis()

    #Hide outer box
    plt.box(on=None)
```

```python
#need a view that shows start number on stage versus final position on stage?
```

```python
if __name__=='__main__':
    q='''
    SELECT il.legDate, il.name AS date, il.startListId, il.status,
        isc.itineraryLegId, isc.itinerarySectionId, isc.name AS section, isc.`order`,
        ist.* FROM championship_events ce 
    JOIN itinerary_event ie ON ce.eventId = ie.eventId 
    JOIN itinerary_legs il ON ie.itineraryId=il.itineraryId
    JOIN itinerary_sections isc ON il.itineraryLegId=isc.itineraryLegId
    JOIN itinerary_stages ist ON ist.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
    WHERE ce.`country.name`="{rally}" ORDER BY isc.`order`
    '''.format(rally=rally)
    rally_stages = pd.read_sql(q,conn)
    display(rally_stages.head(3))
```

```python
if __name__=='__main__':
    display(rally_stages.groupby('section')['number'].max())

```

```python
if __name__=='__main__':
    bottom_offset, top_offset = ax.get_ylim()
```

```python
if __name__=='__main__':
    #ll=len(stagerank['drivercode'].unique())

    #top_offset and bottom_offset set above

    for i,d in rally_stages.iterrows():
        #Show stage distance
        ax.text(i+1,bottom_offset+0.5,'{}km'.format(d['distance']),
                horizontalalignment='center',fontsize=10, rotation=45)
        #Show stage name
        ax.text(i+1,top_offset-4,'{}'.format(d['name']),
                horizontalalignment='center',fontsize=10, rotation=45)
    #Separate out the separate sections
    for i in rally_stages.groupby('section')['number'].max()[:-1]:
        ax.plot((i+0.5,i+0.5), (top_offset-0.5,bottom_offset+0.5), color='lightgrey', linestyle='dashed')
    fig
```

### Make  Functions For That...

```python
def dbGetRallyStages(conn, rally):
    q='''
    SELECT il.legDate, il.name AS date, il.startListId, il.status,
        isc.itineraryLegId, isc.itinerarySectionId, isc.name AS section, isc.`order`,
        ist.* FROM championship_events ce 
    JOIN itinerary_event ie ON ce.eventId = ie.eventId 
    JOIN itinerary_legs il ON ie.itineraryId=il.itineraryId
    JOIN itinerary_sections isc ON il.itineraryLegId=isc.itineraryLegId
    JOIN itinerary_stages ist ON ist.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
    WHERE ce.`country.name`="{rally}" ORDER BY isc.`order`
    '''.format(rally=rally)
    rally_stages = pd.read_sql(q,conn)

    return rally_stages
```

```python
#stage_times_stage, stage_times_overall

def varasint(cand, retval=''):
    ''' Function to determine if we can cast something as an int.
        If we can, do so and return it, otherwise return retval, or, if retval=='asis', the original value. '''

    #Take the opportunity to de-SS if it's a possible SS identified stage
    cand = cand.strip().replace('SS','') if isinstance(cand, str) else cand
    
    retval = int(cand) if (int(cand) if isinstance(cand, str) and cand.isdigit() else False) or isinstance(cand, int) else retval
    if retval=='asis':
        return cand
    return retval

def _qInIntList(items, col, conj='AND', pattern='{}'):
    ''' Generate a SQL clause that tests whether a column value is in a list of values. '''
    items = [varasint(items)] if isinstance(items, (str, int)) else [i for i in [varasint(s) for s in items] if isinstance(i,int)]
    items = " {conj} {col} IN ({s}) ".format( conj=conj, col = col, s=','.join([pattern.format(i) for i in items]) )
 
    return items


def dbGetStageRank(conn, rally, rc, typ='overall', stages=None):
    ''' Function to query the database and return stage ranks either for the stage, or overall.
        The query can be limited to returning the results for one or more stages.
        typ: overall | stage
        
        Note - the dataframes return different columns:
        
        stage: ['diffFirst', 'diffFirstMs', 'diffPrev', 'diffPrevMs', 'elapsedDuration',
       'elapsedDurationMs', 'entryId', 'position', 'source', 'stageId',
       'stageTimeId', 'status', 'class', 'code', 'distance', 'name', 'snum',
       'drivercode', 'entrant.name', 'classrank']
       
       overall: ['diffFirst', 'diffFirstMs', 'diffPrev', 'diffPrevMs', 'entryId',
       'penaltyTime', 'penaltyTimeMs', 'position', 'stageTime', 'stageTimeMs',
       'totalTime', 'totalTimeMs', 'stageId', 'class', 'code', 'distance',
       'name', 'snum', 'drivercode', 'entrant.name', 'classrank']
       
       This is far from ideal and is going to take some careful unpicking...
       One path to this would be to have a new combined function, dbGetCombinedStageRanks()
       with some new column names and switch to it one calling function at a time...
       
       RENAME as dbGetStageRankDEPRECATED and pass to it (for now) from dbGetCombinedStageRanks?
       
       TO DO: allow stage -1 for last stage?
    '''
    
    stagetyp={'overall':'stage_times_overall', 'stage_times_overall':'stage_times_overall',
              'stage':'stage_times_stage', 'stage_times_stage':'stage_times_stage'} 
    
    #Be tolerant of stages entered as SSN or N?
    stages = '' if stages is None else _qInIntList(stages, 'i.code', pattern="'SS{}'")
    
    q='''
    SELECT st.*, sc.name as class, i.code, i.distance, i.name, CAST(REPLACE(code,'SS','') AS INTEGER) snum,
    sl.`driver.code` drivercode, sl.`entrant.name`
    FROM {typ} st INNER JOIN itinerary_stages i ON st.stageId = i.stageId
    INNER JOIN startlist_classes sc ON sc.entryid = st.entryId 
    INNER JOIN championship_events ce ON i.eventId=ce.eventId
    INNER JOIN startlists sl ON sl.entryId=sc.entryId
    WHERE sc.name="{rc}" AND ce.`country.name`="{rally}" {stages} ORDER BY snum, position
    '''.format(rc=rc, rally=rally, typ=stagetyp[typ], stages=stages)
    stagerank=pd.read_sql(q,conn)

    #If we pass in an incorrect rally name, for example...
    if stagerank.empty:
        return stagerank
    
    #Ideally, we want this as an int, but if there are NaNs, it breaks
    #Are we making assumptions that this is in postion order?
    stagerank['classrank'] = stagerank.groupby(['snum'])['position'].rank(method='dense').astype(float)

    return stagerank
```

```python
def prevStage(ss):
    ''' Return previous stage number in similar format. '''
    
    prevssint = int(str(ss).lower().replace('ss',''))-1
    if isinstance(ss,str):
        if ss.startswith('SS'):
            return 'SS{}'.format(prevssint)
        return str(prevssint)
    return prevssint


#test:
#prevStage(11)==10
#prevStage('11')=='10'
#prevStage('SS11')=='SS10'
```

### Enrichers

Enrichers are things like story flags and story switches that can be used to trigger particular actions in a `pytracery` story:

- *flags*: Booleans that allow boolean decisions to be made to switch a story between two symbols;
- *switches*: enumerated sets of options that map onto `pytracery` symbols and can be used to select amongst several possible story directions.

```python
#For the enrichers, might it make sense to turn these into views?


#TO DO: should this be able to run separately on eg stage and class, or just on the combined?
def commonClassEnrichers(stagerank, suffix=''):
    classrankcol = 'classrank'+suffix
    poscol = 'position'+suffix
    stagerank[classrankcol] = stagerank.groupby(['snum'])[poscol].rank(method='dense').astype(float)
    return stagerank

def overallClassEnrichers(stagerank, suffix=''):
    #Ideally, we want this as an int, but if there are NaNs, it breaks
    classrankcol = 'classrank'+suffix
    poscol = 'position'+suffix
    #stagerank[classrankcol] = stagerank.groupby(['snum'])[poscol].rank(method='dense').astype(float)
    stagerank = commonClassEnrichers(stagerank, suffix)
    stagerank['gainedClassPos'] = stagerank.groupby(['drivercode'])[classrankcol].diff()<0 
    stagerank['gainedClassLead'] = (stagerank.groupby(['drivercode'])[classrankcol].diff()<0 ) & (stagerank[classrankcol]==1)
    stagerank['classPosDiff'] = stagerank.groupby(['drivercode'])[classrankcol].diff().fillna(0)
    stagerank['lostClassLead'] = (stagerank[classrankcol]!=1) & (stagerank.groupby(['drivercode'])[classrankcol].diff() == stagerank[classrankcol]-1)
    stagerank['retainedClassLead'] = ((stagerank[classrankcol] ==1) & (~stagerank['gainedClassLead']) & (stagerank['snum']!=1))
    return stagerank

#https://stackoverflow.com/a/35428677/454773
def _streak(x, poscol='position', pos=1):
    ''' Return streak length for each driver in a specified position. '''
    return x.groupby( (x[poscol] != pos).cumsum()).cumcount() +  ( (x[poscol] != pos).cumsum() == 0).astype(int) 

def firststreak(x, poscol='position'):
    ''' Return streak length for each driver in first position. '''
    x['firstinarow'] = _streak(x, poscol, 1)#x.groupby( (x[poscol] != pos).cumsum()).cumcount() +  ( (x[poscol] != pos).cumsum() == 0).astype(int) 
    return x

#Why duplicate this function???
#def winningstreakleg(x, poscol='position'):
#    x['legwinsinarow'] = x.groupby( (x[poscol] != 1).cumsum()).cumcount() +  ( (x[poscol] != 1).cumsum() == 0).astype(int) 
#    return x

def _debug_winningstreak(x, poscol='position'):
    x['notwin'] = (x[poscol] != 1)
    #The notwincumcount creates a new group for each not win
    #So if we have a streak of wins, the notwincumcount groups those together
    x['notwincumcount'] = x['notwin'].cumsum()
    x['startwithawin'] = (x['notwincumcount'] == 0).astype(int)
    #groupby.cumcount  - number each item in each group from 0 to the length of that group - 1.
    x['streakgroupwincount'] = x.groupby( 'notwincumcount' ).cumcount()

    x['winstreak'] = x.groupby( 'notwincumcount' ).cumcount() + x['startwithawin']
    return x

def stageWinEnrichers(combined, suffix=''):
    ''' Only works for the stage results df. '''
    poscol = 'position'+suffix
    combined['stagewin'] = combined[poscol]==1
    combined['stagewincount'] = combined.groupby(['drivercode'])['stagewin'].cumsum()
    combined = combined.groupby('drivercode').apply(firststreak, poscol=poscol)
    return combined

def stageOverallEnrichers(combined, suffix=''):
    ''' Enrichment regarding overall standing.
        Only works for overall df. '''
    #stagerank['OverallLead'] = 
    poscol='position'+suffix
    #These relate to the overall position
    combined['gainedOverallPos'] = combined.groupby(['drivercode'])[poscol].diff()<0 
    combined['gainedOverallLead'] = (combined.groupby(['drivercode'])[poscol].diff()<0 ) & (combined[poscol]==1)
    combined['overallPosDiff'] = combined.groupby(['drivercode'])[poscol].diff().fillna(0)
    combined['lostOverallLead'] = (combined[poscol]!=1) & (combined.groupby(['drivercode'])[poscol].diff() == combined[poscol]-1)
    combined['retainedOverallLead'] = ((combined[poscol] ==1) & (~combined['gainedOverallLead']) & (combined['snum']!=1))
    combined = combined.groupby('drivercode').apply(firststreak, poscol=poscol)
    #These relate to stage -which means we somehow need to get that data in here?
    #Or we bump it back to stage
    #combined['stagewin'] = combined['position_stage']==1
    #combined['stagewincount'] = combined.groupby(['drivercode'])['stagewin'].cumsum()
    #combined = combined.groupby('drivercode').apply(winningstreak, poscol='position_stage') 

    return combined
    
def timeEnrichers(stagerank, suffix=''):
    stagerank['gainedTime'+suffix] = stagerank.groupby(['drivercode'])['diffFirstMs'+suffix].diff()<=0     
    return stagerank
    
def dbGetCombinedStageRanks(conn, rally, rc='RC1', typ='combined', stages=None):
    ''' Get combined stage and overall results for each stage.
        The function can be called at various levels of richness:
        - minimal: drivercode, overall position, rank position, overall time, stage time;
        - TO DO: full: all columns;
        - just_stage: the original dbGetStageRank(typ='stage') [BACKWARD COMPATABILITY]
        - just_overall: the original dbGetStageRank(typ='overall') [BACKWARD COMPATABILITY]
    '''
    
    #Backwards compatability
    if typ!='combined':
        return dbGetStageRank(conn,rally,rc,typ=typ, stages=stages)
    
    #Better to do this as a view in SQL?
    _stage=dbGetStageRank(conn,rally,rc,typ='stage', stages=stages)
    _overall=dbGetStageRank(conn,rally,rc,typ='overall', stages=stages)
    commonCols=['entryId','entrant.name','drivercode','stageId', 'class', 'code','name','snum', 'distance']
    uniqueCols = ['elapsedDuration', 'elapsedDurationMs','penaltyTime', 'penaltyTimeMs',
                  'stageTime', 'stageTimeMs','totalTime', 'totalTimeMs', 'source', 'status']
    #_combined = pd.merge(_stage,_overall,on=commonCols,suffixes=('_stage','_overall'))
    _stage.columns=[c if c in commonCols+uniqueCols else c+'_stage' for c in _stage.columns]
    _overall.columns=[c if c in commonCols+uniqueCols else c+'_overall' for c in _overall.columns]
    _combined = pd.merge(_stage,_overall,on=commonCols)
    return _combined


#TO DO - when calling this, need to check that we call it with combined?
#Can we drop the typ?
def getEnrichedStageRank(conn,rally,rc='RC1',typ='combined',stages=None):
    ''' Add enrichment flags on combined stage and overall results for each stage. '''
    
    #Need to be careful here... if you pass in stages, then the overall is nonsense
    #This needs to be applied to a dataframe that has both stage and overall rank columns
    #  and the enrichers need to be applied to that.
    #Or we just enrich appropriately on a partial results dataframe (eg just stage, or just overall)
    
    #We can only add cols that are appropriate
    
    # TO DO - add logic that copes with passing in typ.endswith('stage') and typ.endswith('overall') arguments
    combined = dbGetCombinedStageRanks(conn, rally, rc, typ, stages)
    if typ.endswith('stage'):
        combined = commonClassEnrichers(combined, suffix='')
        combined = timeEnrichers(combined,suffix='')
        combined = stageWinEnrichers(combined,suffix='')
    elif typ.endswith('overall'):
        combined = overallClassEnrichers(combined,suffix='')
        combined = timeEnrichers(combined,suffix='')
        combined = stageOverallEnrichers(combined, suffix='')
    else:
        combined = overallClassEnrichers(combined,suffix='_overall')
        combined = stageWinEnrichers(combined, suffix='_stage')
        combined = stageOverallEnrichers(combined, suffix='_overall')
        combined = timeEnrichers(combined,suffix='_stage')
        combined = timeEnrichers(combined,suffix='_overall')
    return combined
```

```python
#getEnrichedStageRank(conn,rally).columns
```

```python
#getEnrichedStageRank(conn,rally,typ='stage').columns
```

```python
#getEnrichedStageRank(conn,rally,typ='overall').columns
```

```python
#Helper functions for finding out driver codes associated with driver positions

def prevStage(ss, stageformat='SS{}'):
    ''' Return previous stage number. '''
    
    #Need to think how we handle the first stage
    #Go defensive to allow passing: 10, '10', 'SS10'
    return stageformat.format(int(str(ss).replace('SS',''))-1)


def getDrivercodeByRoadPos(conn, rally, ss, pos=1, rc='RC1'):
    ''' Get drivercode of driver starting the stage in the specifed road position. '''
    #Allow negative road position?
    splits = ssd.dbGetSplits(conn,rally,ss,rc)
    ixpos = pos if pos<0 else pos-1
    return getRoadPosition(splits).iloc[ixpos].name

def getDrivercodeByStagePos(conn, rally, ss, pos=1, rc='RC1'):
    ''' Get drivercode of driver finishing in specified stage position. '''
    ixpos = pos if pos<0 else pos-1
    return dbGetStageRank(conn, rally, rc, 'stage', stages=ss).iloc[ixpos]['drivercode']

def getDrivercodeByPrevStagePos(conn, rally, ss, pos=1, rc='RC1'):
    ''' Get drivercode of driver finishing in specified stage position on previous stage. '''
    ixpos = pos if pos<0 else pos-1
    return dbGetStageRank(conn, rally, rc, 'stage', stages=prevStage(ss)).iloc[ixpos]['drivercode']

def getDrivercodeByOverallPos(conn, rally, ss, pos=1, rc='RC1'):
    ''' Get drivercode of driver in specified overall position at end of specified stage. '''
    ixpos = pos if pos<0 else pos-1
    return dbGetStageRank(conn, rally, rc, 'overall', stages=ss).iloc[ixpos]['drivercode']

def getDrivercodeByPrevOverallPos(conn, rally, ss, pos=1, rc='RC1'):
    ''' Get drivercode of driver who was in specified overall position at end of previous stage. '''
    ixpos = pos if pos<0 else pos-1
    return dbGetStageRank(conn, rally, rc, 'overall', stages=prevStage(ss)).iloc[ixpos]['drivercode']


def isStageWinner(conn, rally, ss, drivercode, rc='RC1'):
    ''' Return a boolean value identifying whether a specified driver won a specified stage. '''
    return drivercode == getDrivercodeByStagePos(conn, rally, ss, pos=1, rc='RC1')

#Example to show how we can put function references into a dict then apply them
#aaaa={'s1':str, 's2':abs}
#aaaa['s2'](-12)
    

def getDriverCodeBy(conn, rally, ss, ranktyp='overall', pos=None, rc='RC1' ):
    ''' Meta function to allow retrieval of drivercode using criterion passed as `ranktyp` variable. '''
    #Better to make a dict of function names?'pos'
    dcmap={'overall':{'f':getDrivercodeByOverallPos,'pos':1 if pos is None else pos,'ss':ss},
           'prevoverall':{'f':getDrivercodeByOverallPos,'pos':1 if pos is None else pos,'ss':prevStage(ss)},
           'stage':{'f':getDrivercodeByStagePos,'pos':1 if pos is None else pos,'ss':ss},
           'prevstage':{'f':getDrivercodeByStagePos,'pos':1 if pos is None else pos,'ss':prevStage(ss)},
           'roadpos':{'f':getDrivercodeByRoadPos,'pos':1 if pos is None else pos,'ss':ss}
          }
        
    return dcmap[ranktyp.lower()]['f'](conn, rally, ss, dcmap[ranktyp]['pos'], rc)
```

```python
#Again, for this like this, should we just start to turn them into views?

def dbGetStageStart(conn, rally, rc, stages=None):
    ''' Query the database and return in class stage start and end position.
        Can be limited to return results for one or more stages.
        
        NOTE: This only works if there are split times available.               
    '''
    
    stages = '' if stages is None else _qInIntList(stages, 'i.code', pattern="'SS{}'")
        
    q='''
    SELECT sp.stageId, sp.entryId, sl.`driver.code` drivercode, sp.startDateTime, i.eventId, sc.name,
    CAST(REPLACE(code,'SS','') AS INTEGER) snum, st.position, st.diffFirstMs, st.diffPrevMs
    FROM (SELECT DISTINCT stageId, entryId, startDateTime FROM split_times) AS sp 
    INNER JOIN itinerary_stages i ON sp.stageId = i.stageId
    INNER JOIN startlist_classes sc ON sc.entryid = sp.entryId AND sc.eventId=i.eventId
    INNER JOIN championship_events ce ON i.eventId=ce.eventId
    INNER JOIN startlists sl ON sl.entryId=sc.entryId AND sl.eventId=ce.eventId
    INNER JOIN stage_times_stage st ON st.entryId=sc.entryId AND st.stageId = i.stageId
    WHERE sc.name="{rc}" AND ce.`country.name`="{rally}" {stages} ORDER BY snum, sp.startDateTime
    '''.format(rc=rc, rally=rally, stages=stages)
    stagestart=pd.read_sql(q,conn)
    
    #Ideally, we want this as an int, but if there are NaNs, it breaks
    if not stagestart.empty:
        stagestart['startpos'] = stagestart.groupby(['snum'])['startDateTime'].cumcount()+1
        stagestart['classrank'] = stagestart.groupby(['snum'])['position'].rank(method='dense').astype(float)
    else:
        stagestart = pd.concat([stagestart,pd.DataFrame(columns=['startpos','classrank'])],sort=False)
    stagestart.drop(['position'], axis=1, inplace=True)
    return stagestart
```

```python
#https://blog.ouseful.info/2017/03/15/grouping-numbers-that-are-nearly-the-same-casual-clustering/
#https://stackoverflow.com/questions/14783947/grouping-clustering-numbers-in-python/14783998#14783998
def cluster(data, maxgap):
    '''Arrange data into groups where successive elements
       differ by no more than *maxgap*
 
        cluster([1, 6, 9, 100, 102, 105, 109, 134, 139], maxgap=10)
        [[1, 6, 9], [100, 102, 105, 109], [134, 139]]
 
        cluster([1, 6, 9, 99, 100, 102, 105, 134, 139, 141], maxgap=10)
        [[1, 6, 9], [99, 100, 102, 105], [134, 139, 141]]
 
    '''
    data.sort()
    groups = [[data[0]]]
    for x in data[1:]:
        if abs(x - groups[-1][-1]) <= maxgap:
            groups[-1].append(x)
        else:
            groups.append([x])
    return groups

def numclustergroup(x,col,maxgap, cname='clustergroup'):
    x[cname] = (x[col]>=maxgap).cumsum()
    return x

def getBattleGroup(df,groupcol=None,ordercol='position',diffcol='diffPrevMs',
                   window=1000, mingroupsize=3, cname='battlegroup'):
    if ordercol is not None:
        df=df.sort_values(ordercol)
    if not groupcol:
        df = numclustergroup(df,diffcol, window, cname)
        df = df.groupby(cname).filter(lambda x: len(x) >= mingroupsize)
    else:
        df = df.groupby(groupcol).apply(lambda x: numclustergroup(x,diffcol, window,cname))
        df = df.groupby([groupcol,cname]).filter(lambda x: len(x) >= mingroupsize)
    df=df.copy()
    df[cname]=df.groupby(cname).ngroup()
    df['battlepos']=df.groupby(cname)['classrank'].transform('min')
    return df
```

```python
if __name__=='__main__':
    df = dbGetStageStart(conn, rally, rc, stages=[7,8])
    df=getBattleGroup(df, groupcol='snum', diffcol='diffPrevMs', ordercol=['snum','classrank'], window=1500)
    #df['diffGroupLeadMs']=df.groupby('battlegroup')['diffFirstMs'].transform(lambda x: x - x.min())
    df['diffGroupLeadMs']=df.groupby('battlegroup')['diffPrevMs'].transform(lambda x: x.cumsum()-x.iloc[0])
    #df=getBattleGroup(df, diffcol='diffPrevMs', ordercol=['snum','classrank'], window=500)
    display(df)
```

```python
def stageAnnotation(ax, stages, rally_stages, prune=True):
    ''' Annotate stage chart with a description / summary of each stage. '''
    bottom_offset, top_offset = ax.get_ylim()
    stages = rally_stages[rally_stages['code'].isin(stages['code'].unique().tolist())] if prune else rally_stages
    for i,d in stages.iterrows():
        #Show stage distance
        ax.text(i+1,bottom_offset+0.5,'{}km'.format(d['distance']),
                horizontalalignment='center',fontsize=10, rotation=45)
        #Show stage name
        ax.text(i+1,top_offset-4,'{}'.format(d['name']),
                horizontalalignment='center',fontsize=10, rotation=45)
    #Separate out the separate sections
    for i in stages.groupby('section')['number'].max()[:-1]:
        ax.plot((i+0.5,i+0.5), (top_offset-0.5,bottom_offset+0.5), color='lightgrey', linestyle='dashed')
```

__Note: the following function is out of kilter with the narrative - it includes references to enrichments that are introduced later in the notebook.__

```python
#if there is only one stage, this chart errors
def plotStageProgressionChart(gc, lhoffset=-0.5,rhoffset=0.5,linecolor=None, stageAnnotate=True, prune=True,
                              deltalabels=False, progress=False, streak ='firstinarow', rally_stages=None,
                              rankpos='classrank', title=None, elaborate = True, **kwargs):
    ''' rankpos: classrank | position'''
    
    if len(gc['code'].unique())<=1:
        return None, None
    
    gc=gc.copy()
    #For a live stage or for non-finishers, drop the row?
    gc = gc.dropna(subset=['diffFirst'])
    if gc.empty: return None, None
    
    entrySize = len(gc['entryId'].unique()) #number of entries in class

    progress = progress and 'leggainedpos' in gc.columns and 'leggainedtime' in gc.columns
    #rankpos='position'
    #rankpos='classrank'

    # Rally properties
    smin = gc['snum'].min() # min stage number
    smax = gc['snum'].max() # max stage number
    
    # Create plot
    fig, ax = plt.subplots()#figsize=(smax-smin+4,entrySize))

    #Set blank axes
    ax.xaxis.label.set_visible(False)
    ax.get_yaxis().set_ticklabels([])
    ax.set_yticks([]) 
    ax.set_xticks([]) 

    #labeler
    addlabels = True
    pinklabel= lambda x: int(x['position'])
    #pinklabel= lambda x: int(x['classrank'])
    #pinklabel= lambda x: x['drivercode']

    ylabel = lambda x: x['drivercode']
    ylabel = lambda x: int(x['position'])
    ylabel = lambda x: int(x['classrank'])
    ylabel = lambda x: '{} ({})'.format(x['drivercode'], int(x['classrank'])) 
     
    def elaboratelabel(x):
        if x['snum']==1:
            return '{} ({})'.format(x['drivercode'], int(x['classrank']))
        elif x['snum']==smin or not progress:
            txt='{} ({}: {})'
        elif x['leggainedpos'] and x['leggainedtime']:
            txt='{} ($\mathbf{{{}}}$: $\mathbf{{{}}}$)' 
        elif x['leggainedpos']:
            txt='{} ($\mathbf{{{}}}$: {})'
        elif x['leggainedtime']:
            txt='{} ({}: $\mathbf{{{}}}$)'
        else:  txt='{} ({}: {})'
        if x is not None:
            if x['diffFirst'] is not None:
                txt = txt.format(x['drivercode'], int(x['classrank']),x['diffFirst'].replace('PT','+').replace('S',''))
            else: txt = txt.format(x['drivercode'], int(x['classrank']),x['diffFirst'])
        else: txt = 'oops?'
        return txt
    ylabel= lambda x:elaboratelabel(x)
    gcSize=10
    
    ax.set_xlim(smin-0.2,smax+0.2)

    # Define a dummy rank to provide compact ranking display
    # This is primarily for RC1 which we expect to hold the top ranking positions overall
    gc['xrank'] = (gc[rankpos]>gcSize)
    gc['xrank'] = gc.groupby('snum')['xrank'].cumsum()
    gc['xrank'] = gc.apply(lambda row: row[rankpos] if row[rankpos]<=gcSize  else row['xrank'] +gcSize, axis=1)

    #Base plot - line chart showing position of each driver against stages
    gc.groupby('entryId').plot(x='snum',y='xrank',ax=ax,legend=None, color=linecolor)

    #test - lead data
    #for i,d in gc.iterrows():
    if streak:
        #what are options here? firstinarow, ???
        for i,d in gc[gc['position']==1].iterrows():
            resp=d[streak]#d['lostClassLead']#'1' if d['gainedLead'] else '0'
            ax.text(d['snum'], d['xrank'], resp,
                   bbox=dict(  boxstyle='round,pad=0.3',color='lightgrey'),
                   horizontalalignment='center')
    
    #Label cars ranked outside the group count - primarily for RC1
    if addlabels:
        for i,d in gc[gc['xrank']>gcSize].iterrows():
            if deltalabels: col='lightgreen' if d['gainedTime'] else 'pink'
            else: col ='pink'
            ax.text(d['snum'], d['xrank'], pinklabel(d),
                    bbox=dict(  boxstyle='round,pad=0.3',color=col),
                    horizontalalignment='center') #facecolor='none',edgecolor='black',

    #Stage time gain / loss indicator
    if deltalabels:
        for i,d in gc.iterrows():
            col='green' if d['gainedTime'] else 'red'
            ax.plot(d['snum'], d['xrank'], 'o', color=col)
    
    
    #Label driver names at left hand edge
    for i,d in gc[gc['snum']==smin].iterrows():
        yl = ylabel(d) if elaborate else d['drivercode']
        ax.text(smin+lhoffset, d['xrank'], yl, horizontalalignment='right')

    #Label driver names at right hand edge
    for i,d in gc[gc['snum']==smax].iterrows():
        yl = ylabel(d) if elaborate else d['drivercode']
        ax.text(smax+rhoffset, d['xrank'], yl, horizontalalignment='left')

    # Label x-axis stage numbers
    ax.set_xticks(gc['snum'].unique()) # choose which x locations to have ticks
    #Style the rotation of the ticklabels
    ax.set_xticklabels(gc['code'].unique(),rotation = 45, ha="right")

    plt.gca().invert_yaxis()

    if stageAnnotate and not rally_stages.empty:
        stageAnnotation(ax, gc, rally_stages, prune)
    
    #Hide outer box
    plt.box(on=None)
    
    if title: plt.title(title)
        
    if kwargs is not None and 'filename' in kwargs:
        fig.savefig(kwargs['filename'], bbox_inches='tight')
     
    return fig,ax
```

```python
if __name__=='__main__':
    #rally_stages referenced from within a function
    #How are the stage names at the top triggered? stageAnnotate = True?
    rally_stages = dbGetRallyStages(conn, rally)
    plotStageProgressionChart( dbGetStageRank(conn, rally, rc), rally_stages = rally_stages, filename='TEST.png' );
```

```python
if __name__=='__main__':
    stagerank[stagerank['drivercode']=='NEU']
```

```python
if __name__=='__main__':
    stagerank = getEnrichedStageRank(conn,rally)
    stagerank.head(20)
```

```python
if __name__=='__main__':
    ssdemo = stagerank[stagerank['code'].isin(['SS2','SS3','SS4'])].copy()
    lgp=(ssdemo.groupby('drivercode')['classrank'].agg('first')-ssdemo.groupby('drivercode')['classrank'].agg('last'))>0
    ssdemo['leggainedpos']=ssdemo['drivercode'].map(lgp.to_dict())

    tgp=(ssdemo.groupby('drivercode')['diffFirstMs'].agg('first')-ssdemo.groupby('drivercode')['diffFirstMs'].agg('last'))>=0
    ssdemo['leggainedtime']=ssdemo['drivercode'].map(tgp.to_dict())
```

```python
#What does stageLeg mean? Several stages run together?
def stageLegEnrichers(legStagerank,sectionStages):
    lgp=(legStagerank.groupby('drivercode')['classrank'].agg('first')-legStagerank.groupby('drivercode')['classrank'].agg('last'))>0
    legStagerank['leggainedpos']=legStagerank['drivercode'].map(lgp.to_dict())

    tgp=(legStagerank.groupby('drivercode')['diffFirstMs'].agg('first')-legStagerank.groupby('drivercode')['diffFirstMs'].agg('last'))>=0
    legStagerank['leggainedtime']=legStagerank['drivercode'].map(tgp.to_dict())
    
    #How do we get the stagewin? This is based on stagerank_overall which does not have stage result?
    #legStagerank['legstagewincount'] = legStagerank.groupby(['drivercode'])['stagewin'].cumsum()
    #legStagerank = legStagerank.groupby('drivercode').apply(winningstreakleg) 
    return legStagerank

```

```python
if __name__=='__main__':
    ssdemo = stagerank[stagerank['code'].isin(['SS2','SS3','SS4'])].copy()
    ssdemo = stageLegEnrichers(ssdemo)
```

```python
if __name__=='__main__':
    fig, ax = plotStageProgressionChart( ssdemo, linecolor='lightgrey' , deltalabels=True, 
                                        progress=True, stageAnnotate=False);

    #fig, ax = plotStageProgressionChart( stagerank,deltalabels=True );
```

```python
if __name__=='__main__':
    pass
    #for a in ax.get_xticklabels(): print(a.get_text(), a.get_position())
```

```python
if __name__=='__main__':
    plotStageProgressionChart(gc[gc['snum']<5],prune=False);
```

```python

```
