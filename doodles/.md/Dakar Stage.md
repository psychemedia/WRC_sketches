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

# Rally Stage Report for Dakar Rally

Generate a rally report for a single stage on the Dakar Rally, showing progression across splits.


```python
%load_ext autoreload
%autoreload 2
```

```python
import Dakar_Rally_2020 as dr
```

```python
from Dakar_Rally_2020 import get_timing_data_long_timeInSplit, \
                                get_long_annotated_timing_data, TIME, RANK, \
                                rebaseWaypointTimes, pivotRebasedSplits, \
                                get_driver_data, cleanDriverSplitReportBaseDataframe, \
                                get_timing_data, insertColumn, moveColumn, \
                                get_ranking_data, getCurrPrevOverallRank

from dakar_utils import moreStyleDriverSplitReportBaseDataframe, \
                        sparkline2, sparklineStep, getTablePNG, pos1symbols
                                
import sqlite3
from sqlite_utils import Database
import pandas as pd
```

```python
dbname = 'dakar_2020.db'

conn = sqlite3.connect(dbname)
db = Database(conn)
```

```python
q="SELECT name FROM sqlite_master WHERE type = 'table';"
pd.read_sql(q, conn)
```

```python
#VTYPE = 'car' # car moto

```

```python
setups = {'sunderland':{'v':'moto','b':3},
          'alo':{'v':'car','b':310},
          'sainz':{'v':'car','b':305},
          'attiyah':{'v':'car', 'b':300},
          'price':{'v':'moto','b':1},
          'peterh':{'v':'car','b':302},
          'trucks':{'v':'truck','b':516},
          'sanz':{'v':'moto','b':14},
          'coronel':{'v':'car', 'b':347},
         }

def get_setup(n):
    return setups[n]['v'],setups[n]['b']
```

```python
#REBASER = 310 #Alonso - 310, Sainz 305 #sunderland
CHART_POSITIONS = 10 #None for all


MAXMISSING = 10
if (CHART_POSITIONS / 2) < MAXMISSING:
    MAXMISSING = int(CHART_POSITIONS / 2)
```

```python
STAGE = 12
VTYPE, REBASER = get_setup('trucks')
VTYPE, REBASER = get_setup('price')
```

```python
def col_replacer(row, col, replace):
    return row[col].replace(row[replace],'').strip()
```

```python
timing_data_long = get_long_annotated_timing_data(STAGE, VTYPE)[TIME]
timing_data_long_insplit = get_timing_data_long_timeInSplit(STAGE, VTYPE)
timing_data_long_min = rebaseWaypointTimes( timing_data_long , REBASER, 'TimeInS')
timing_data_long_min.head(3)
```

```python
rebaseWaypointTimes(timing_data_long_insplit,REBASER)
```

```python
if REBASER:
    rb2c = pivotRebasedSplits(rebaseWaypointTimes(timing_data_long_insplit,REBASER))
else:
    rb2c = pivotRebasedSplits(rebaseWaypointTimes(timing_data_long_insplit,timing_data_long_insplit['Pos'].iloc[0]))
    
rb2c = cleanDriverSplitReportBaseDataframe(rb2c, STAGE)
top10 = get_driver_data(STAGE, CHART_POSITIONS, VTYPE)

#rb2cTop10 = rb2c[rb2c.index.isin(top10['Bib'])]
rb2cTop10 = pd.merge(top10, rb2c, how='left', left_index=True, right_index=True)
rb2cTop10.head(2)
```

```python
#Need processing on the below - also change column order
newColOrder = rb2cTop10.columns[1:].tolist()+[rb2cTop10.columns[0]]
rb2cTop10=rb2cTop10[newColOrder]
rb2cTop10 = pd.merge(rb2cTop10, -timing_data_long_min.reset_index().pivot('Bib','Waypoint','rebased'),
                     how='left', left_index=True,right_index=True)


#Cast s to timedelta

#for c in [c for c in rb2cTop10.columns if c.startswith('0')]:
#    rb2cTop10[c]=rb2cTop10[c].apply(lambda x: pd.to_timedelta('{}00:00:{}'.format('-' if x<0 else '', '0' if pd.isnull(x) else abs(x))))

#Rename last column
rb2cTop10.rename(columns={rb2cTop10.columns[-1]:'Stage Overall'}, inplace=True)

rb2cTop10.dropna(how='all',axis=1,inplace=True)

#Drop very gappy waypoint cols
rb2cTop10.dropna(thresh=MAXMISSING,axis=1,inplace=True)

rb2cTop10#.head(3)
```

```python
from IPython.display import HTML
```

```python
s2 = moreStyleDriverSplitReportBaseDataframe(rb2cTop10, STAGE)
display(HTML(s2))
```

```python
rb2cTop10['test']= rb2cTop10[[c for c in rb2cTop10.columns if c.startswith(('0','1','Stage Overall'))]].values.tolist()
#Swap the sign of the values
rb2cTop10['test'] = rb2cTop10['test'].apply(lambda x: [-y for y in x])
rb2cTop10.head()
```

```python
brandcol = rb2cTop10.columns.get_loc("Brand")
rb2cTop10.insert(brandcol+1, 'Stage Gap', rb2cTop10['test'].apply(sparkline2,typ='bar'))
rb2cTop10.drop('test', axis=1, inplace=True)
rb2cTop10.columns
```

```python
display(HTML(rb2cTop10.head(3).style.render()))
```

```python
# get gap to leader at each split
tmp = get_timing_data(STAGE,vtype=VTYPE, timerank='gap', kind='full')[TIME].set_index('Bib')
tmp.dropna(thresh=MAXMISSING,axis=1,inplace=True)
tmp.head(3)
```

```python
cols = [c for c in tmp.columns if c.startswith(('0','1'))]
tmp[cols]  = tmp[cols].apply(lambda x: x.dt.total_seconds())
tmp['test']= tmp[[c for c in tmp.columns if (c.startswith(('0','1')) and 'dss' not in c)]].values.tolist()
tmp['test2']= tmp[[c for c in tmp.columns if '_pos' in c]+['Pos']].values.tolist()
#Want better rank higher up
tmp['test2'] = tmp['test2'].apply(lambda x: [-y for y in x])
tmp.head(3)

```

```python
rb3c = get_timing_data(STAGE,vtype=VTYPE, timerank='gap', kind='full')[TIME].set_index('Bib')
rb3c.dropna(thresh=MAXMISSING,axis=1,inplace=True)
rb3c.head(3)
```

```python
rb3cTop10 = pd.merge(rb2cTop10[[]], rb3c, how='left', left_index=True,right_index=True)

cols = [c for c in rb3cTop10.columns if c.startswith(('0','1'))]
#rb2cTop10[cols]  = rb2cTop10[cols].apply(lambda x: x.dt.total_seconds())

rb3cTop10['test2']= rb3cTop10[[c for c in rb3cTop10.columns if ('_pos' in c and 'dss' not in c)]+['Pos']].values.tolist()
#Want better rank higher up
rb3cTop10['test2'] = rb3cTop10['test2'].apply(lambda x: [-y if not pd.isnull(y) else float('NaN') for y in x ])
rb3cTop10['Waypoint Rank'] = rb3cTop10['test2'].apply(sparklineStep,figsize=(0.5, 0.5))
rb3cTop10.head(3)
```

```python
rb3cTop10['test2'].head(3)
```

```python
rb3cTop10['test2'].apply(pos1symbols)
```

```python
#Use a copy for nw, while testing
rb4cTop10 = rb2cTop10.copy()
rb4cTop10.columns
```

```python
#Create line chart for pos between each waypoint
q=f"SELECT Bib, WaypointOrder, WaypointPos FROM waypoints WHERE VehicleType='{VTYPE}' AND Stage={STAGE};"
tmp = pd.read_sql(q, conn).pivot(index='Bib',
                                 columns='WaypointOrder',
                                 values='WaypointPos')
tmp.dropna(thresh=MAXMISSING,axis=1,inplace=True)
tmp['waypos'] = tmp.values.tolist()
tmp['waypos'] = tmp['waypos'].apply(lambda x: [-y if not pd.isnull(y) else float('NaN') for y in x ])
tmp['waypos']
```

```python
tmp['Waypoint Pos'] = tmp['waypos'].apply(sparklineStep,figsize=(0.5, 0.5))
insertColumn(rb4cTop10, 'Waypoint Pos', tmp['Waypoint Pos'], right_of='Brand')
```

```python
rb3cTop10.columns
```

```python
#Get rid of NA cols - maybe do this on a threshold?
rb3cTop10.dropna(how='all',axis=1,inplace=True)

rb3cTop10['test']= rb3cTop10[[c for c in rb3cTop10.columns if (c.startswith(('0','1')) and 'dss' not in c)]].values.tolist()
rb3cTop10['test'] = rb3cTop10['test'].apply(lambda x: [-y if not pd.isnull(y) else float('NaN') for y in x ])
rb3cTop10['Gap to Leader'] =  rb3cTop10['test'].apply(sparkline2, 
                                                      figsize=(0.5, 0.5), 
                                                      dot=True,
                                                      colband=(('pink','lightgreen'),('r.','g.')))

#rb3cTop10.drop('test', axis=1, inplace=True)
#rb3cTop10.drop('test2', axis=1, inplace=True)

insertColumn(rb4cTop10, 'Gap to Leader', rb3cTop10['Gap to Leader'], right_of='Pos')
insertColumn(rb4cTop10, 'Waypoint Rank', rb3cTop10['Waypoint Rank'], left_of='Gap to Leader')
rb4cTop10.head(3)
```

```python
#Overall Position, Previous
tmp = get_ranking_data(STAGE, VTYPE,timerank='stage')[RANK].set_index('Bib')
tmp['Penalty'] = tmp['Penalty'].dt.total_seconds()
rb4cTop10 = pd.merge(rb4cTop10, tmp['Penalty'],
                     left_index=True, right_index=True)
rb4cTop10.head()
```

```python
rb4cTop10 = pd.merge(rb4cTop10,
                     getCurrPrevOverallRank(STAGE, VTYPE, rebase=REBASER)[['Overall Position',
                                                                            'Previous Overall Position',
                                                                           'Overall Gap', 'Previous']],
                     left_index=True, right_index=True)
moveColumn(rb4cTop10, 'Previous', left_of='Crew')
moveColumn(rb4cTop10, 'Previous Overall Position', left_of='Previous')
moveColumn(rb4cTop10, 'Overall Position', right_of='Pos')
moveColumn(rb4cTop10, 'Overall Gap', left_of='Pos')

rb4cTop10.head(3)
```

```python
rb4cTop10
```

```python
if STAGE==1:
    rb4cTop10.drop(['Previous Overall Position','Previous'], axis=1, inplace=True)
```

```python
moveColumn(rb4cTop10, 'Waypoint Rank', right_of='Overall Position')
moveColumn(rb4cTop10, 'Pos', left_of='Overall Gap')

rb4cTop10['Crew'] = rb4cTop10.apply(col_replacer,col='Crew',replace='Brand', axis=1)
rb4cTop10.rename(columns={'Brand':'Team'}, inplace=True)

s2 = moreStyleDriverSplitReportBaseDataframe(rb4cTop10, STAGE)

display(HTML(s2))
```

```python
if not CHART_POSITIONS:
    TYP='All'
else:
    TYP=f'top_{CHART_POSITIONS}'

img = getTablePNG(s2, fnstub=f'SS{STAGE}_{VTYPE}_{REBASER}_{TYP}')
img
```

If we lose a split, as in split 3 for SS1, then the D time for the next column is also broken. Need to fix that by calculating relative to split prior to omitted split. 

```python
from IPython.display import Image
Image(img)
```

```python

```

```python

```

```python

```
