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
#TO DO: need a filter for particular day
#eg day 1, day 2.
```

```python
import pandas as pd

from IPython.display import HTML

import dakar_utils as dakar
from dakar_utils import moveColumn, sparkline2, sparklineStep, moreStyleDriverSplitReportBaseDataframe

```

```python
import sqlite3

dbname='dakar_sql.sqlite'

conn = sqlite3.connect(dbname)

c = conn.cursor()

```

```python
dbname2='sweden19.db'
conn2 = sqlite3.connect(dbname2)

c2 = conn2.cursor()

```

```python
from IPython.display import HTML
```

```python
def _rebaseTimes(times, bib=None):
    if bib is None: return times
    return times - times.loc[bib]
```

```python
REBASER=306
```

```python

```

```python
q= 'SELECT entryId, `driver.code` AS Code FROM startlists'
codes = pd.read_sql(q,conn2).set_index('entryId')
codes.head()
```

```python
#For WRC
rc='RC1'
rally='Sweden'
typ='overall'
wREBASE='LOE'

def gapToLeaderBar(conn, rally, rc, typ):
    Xtmpq = sr.dbGetStageRank(conn, rally, rc, typ)#.head()
    Xtmpq = Xtmpq[['entryId','snum', 'diffFirstMs']].pivot(index='entryId',columns='snum',values='diffFirstMs')
    Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    k = '{}GapToLeader'.format(typ)
    Xtmpq[k]= Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
    Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    return Xtmpq 

def rebaserGap(df):
    ''' Bar chart showing rebased gap at each stage. '''
    col='Gap'
    df[col] = df[[c for c in df.columns if c.startswith('SS_') and c.endswith('_overall')]].values.tolist()
    df[col] = df[col].apply(lambda x: [-y for y in x])
    df[col] = df[col].apply(sparkline2, typ='bar', dot=False)
    return df
    
wrc = pd.merge(codes,gapToLeaderBar(conn2, rally, rc, 'overall'), left_index=True, right_index=True)

wrc = pd.merge(wrc,gapToLeaderBar(conn2, rally, rc, 'stage'), left_index=True, right_index=True)

wrc.head(10)
```

```python
def positionStep(conn, rally, rc, typ):
    Xtmpq = sr.dbGetStageRank(conn, rally, rc, typ)#.head()
    Xtmpq = Xtmpq[['entryId','snum', 'position']].pivot(index='entryId',columns='snum',values='position')
    Xtmpq.columns = ['SS_{}_{}_pos'.format(c, typ) for c in Xtmpq.columns]
    k = '{}Position'.format(typ)
    Xtmpq[k]= Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
    Xtmpq[k] = Xtmpq[k].apply(sparklineStep)
    return Xtmpq 

def overallAtLastStage(conn, rally, rc, typ):
    Xtmpq = sr.dbGetStageRank(conn, rally, rc, typ)#.head()
    Xtmpq = Xtmpq[['entryId','snum', 'position']].pivot(index='entryId',columns='snum',values='position')
    last = Xtmpq.columns
    return Xtmpq[[last[-1]]]
    
    
wrc =  pd.merge(wrc,positionStep(conn2, rally, rc, typ='stage')[['stagePosition']], left_index=True, right_index=True)
wrc =  pd.merge(wrc,positionStep(conn2, rally, rc, typ='overall')[['overallPosition']], left_index=True, right_index=True)
wrc['Pos'] = overallAtLastStage(conn2, rally, rc, typ)

wrc = wrc.sort_values('Pos', ascending=True)
wrc=wrc.set_index('Code',drop=True)
cols = [c for c in wrc.columns if c.startswith('SS')]
wrc.rename(columns={'stageGapToLeader':'stageWinnerGap'},inplace=True)
wrc[cols] = -wrc[cols].apply(_rebaseTimes,bib=wREBASE, axis=0)

wrc = rebaserGap(wrc)
```

```python
moveColumn(wrc, 'Gap',left_of='SS_1')
moveColumn(wrc, 'stagePosition',left_of='SS_1')
moveColumn(wrc, 'stageWinnerGap',left_of='SS_1')
moveColumn(wrc, 'overallPosition',left_of='SS_1_overall')
moveColumn(wrc, 'overallGapToLeader',left_of='SS_1_overall')
moveColumn(wrc, 'Pos',left_of='SS_1_overall') #pos=None, left_of=None, right_of=None)
wrc
```

```python
s2 = moreStyleDriverSplitReportBaseDataframe(wrc.fillna(0),'')

#Introduce a dot marker to highlight winner
display(HTML(s2))
dakar.getTablePNG(s2, fnstub='overall_{}_'.format(wREBASE),scale_factor=2)
```

## Ultimate Margins

...aka *time left on table*...


${}_{N}\Delta_i = \sum_{s=1}^N \Delta_{i,s}$ where $\Delta_{i,s}=t_{i,w}-t_{w,s}$ and $t_{i,s}$ is the time on stage $s$ for driver $i$ and $t_{w,s}$ is time on stage $s$ for the stage winner, $w$. We then plot $y={}_{N}\Delta_i$ against $x=s:1..N$ for driver $i$.

We can also look at turning that into a percentage, *cf.* Formula One 1075 times.

For example:${}_N\nabla_{i} = \frac{\sum_{s=1}^N t_{s,i}}{\sum_{s=1}^N t_{s,w}}$ and again plot $y={}_{N}\nabla_i$ against $x=s:1..N$ for driver $i$.

Note that this gives meaning to "giving 110%" in a roundabout sort of way. A driver might be running ar 105% winner time in early stages, then improve to bring this down to 103%..


We can also look to adding lower margins to table, e.g. searching for `max(positive delta)` to find the amount of time lost to the leader on each stage.


Leader rebasing: also consider dynamic / leader rebasing; eg rebaser kernel is `{'SS1':'LOE','SS2':'NEU', etc...}` then get times for each of those to rebase against.

```python

```

```python

```

```python
#Need a WRC query for this
data
#cols SS, Overall position, Stage position, with a driver index
```

```python
wrc.plot(x='SS_1_overall',drawstyle="steps-mid",linestyle=':')
plt.gca().invert_yaxis()
```

```python

```
