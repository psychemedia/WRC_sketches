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
%%capture
import notebookimport

ds= __import__("Report - Driver Splits")
```

```python
import sqlite3
from IPython.display import HTML
import pandas as pd

#Set near zero display to zero
#Doesn't have any effect?
pd.set_option('display.chop_threshold', 0.001)

dbname='testdata/finland19a.db'
conn = sqlite3.connect(dbname)
rally='Finland'
rc='RC1'
YEAR = year=2019
```

```python
q_drivers='SELECT `driver.code` AS code  FROM startlists st JOIN startlist_classes sc ON sc.entryid = st.entryid AND name="{}"'

q_classes='SELECT DISTINCT(name) AS name FROM startlists st JOIN startlist_classes sc ON sc.entryid = st.entryid'

q_stages= 'SELECT * FROM itinerary_stages WHERE status!="ToRun" ORDER BY Number'
```

```python
import dakar_utils as dakar
import ipywidgets as widgets
from ipywidgets import interact
```

## How About More Control?

What about we let the user decide what to allow in each chart?


THink about adding some pace notes to the table. For example, a column showing what the pace was on the current stage in terms of s / km gained or lost, the pace required in the next stage to compensate for the gap to other drivers from the rebased driver, the pace required over the rest of the stages in the loop, or over the next loop.

```python
classes2 = widgets.Dropdown(
    options=pd.read_sql(q_classes,conn)['name'].to_list(),
    value='RC1',
    description='Class:',
    disabled=False,
)

drivers2 = widgets.Dropdown(
    options=pd.read_sql(q_drivers.format(classes2.value),conn)['code'].to_list(),
    #value='NEU',
    description='Driver:',
    disabled=False,
)

stages2 = widgets.Dropdown(
    options=pd.read_sql(q_stages,conn)['code'].to_list(), description='Stage:', disabled=False)


def stage_chart2(rc,driver, stage, bars, roadPos, waypointRank, previous):
    dropcols=[]
    if not roadPos:
        dropcols.append('Road Position')
    if not waypointRank:
        dropcols.append('Waypoint Rank')
    if not previous:
        dropcols.append('Previous')
        
    s2 = ds.getDriverSplitsReport(conn, rally, stage, driver, rc,
                                  bars=bars, dropcols=dropcols)
    display(HTML(s2))

splitBars = widgets.Checkbox( value=True, description='Split bars:',
                           disabled=False )
    
roadPos = widgets.Checkbox( value=True, description='Road pos',
                           disabled=False )

waypointRank = widgets.Checkbox( value=True, description='Waypoint Rank',
                           disabled=False )

previous = widgets.Checkbox( value=True, description='Previous',
                           disabled=False )


def update_drivers2(*args):
    qdrivers = q_drivers.format(classes2.value)
    driverlist = pd.read_sql(qdrivers,conn)['code'].to_list()
    print(driverlist)
    drivers2.options = driverlist
    
classes2.observe(update_drivers2, 'value')


#Add sort order

interact(stage_chart2, rc=classes2, driver=drivers2,
         stage=stages2, bars=splitBars, roadPos=roadPos,
        waypointRank=waypointRank, previous=previous);
```

```python
import testRequiredPace as pace
```

```python
pace.conn = conn
pace.rally = rally
pace.YEAR = year

pace.distances = pace.stageDistances(conn)
```

```python
from dakar_utils import moreStyleDriverSplitReportBaseDataframe

driver='LAT'
tmp =  pace.multiStagePaceReport(['SS{}'.format(i) for i in range(1,24)], driver )

#The negative map means we get times as the rebased driver is concerned...
s2 = moreStyleDriverSplitReportBaseDataframe(-tmp,'')
display(HTML(s2))

```

```python

```

```python

```
