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

dbname='argentina19.db'
conn = sqlite3.connect(dbname)
rally='Argentina'
rc='RC1'
year=2019
```

```python
q_drivers='SELECT `driver.code` AS code  FROM startlists st JOIN startlist_classes sc ON sc.entryid = st.entryid AND name="{}"'

q_classes='SELECT DISTINCT(name) AS name FROM startlists st JOIN startlist_classes sc ON sc.entryid = st.entryid'

q_stages= 'SELECT * FROM itinerary_stages WHERE status!="ToRun" ORDER BY Number'
```

```python
def stage_chart(rc,driver, stage):
    s2 = ds.getDriverSplitsReport(conn, rally, stage, driver, rc, order='overall')
    display(HTML(s2))
```

We need to tweak the sort order (I'm not sure that the default is?)...

```python
import ipywidgets as widgets
from ipywidgets import interact

classes = widgets.Dropdown(
    options=pd.read_sql(q_classes,conn)['name'].to_list(),
    value='RC1', description='Class:', disabled=False )

drivers = widgets.Dropdown(
    options=pd.read_sql(q_drivers.format(classes.value),conn)['code'].to_list(),
    description='Driver:', disabled=False)

stages = widgets.Dropdown(
    options=pd.read_sql(q_stages,conn)['code'].to_list(), description='Stage:', disabled=False)

def update_drivers(*args):
    qdrivers = q_drivers.format(classes.value)
    driverlist = pd.read_sql(qdrivers,conn)['code'].to_list()
    print(driverlist)
    drivers.options = driverlist
    
classes.observe(update_drivers, 'value')

interact(stage_chart, rc=classes, driver=drivers, stage=stages);
```

```python
%%capture table
_
```

## How About More Control?

What about we let the user decide what to allow in each chart?

```
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
    options=q_stages, description='Stage:', disabled=False)


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


interact(stage_chart2, rc=classes2, driver=drivers2,
         stage=stages2, bars=splitBars, roadPos=roadPos,
        waypointRank=waypointRank, previous=previous);
```

## HTML Table to PNG

There is a jquery plugin for this - could we use this in the widget app to download the table as png?

- https://stackoverflow.com/questions/38425931/download-table-as-png-using-jquery/40644383#40644383
- https://tableexport.v5.travismclarke.com/#tableexport
- https://html2canvas.hertzen.com/ / https://github.com/niklasvh/html2canvas
- https://w3lessons.info/export-html-table-to-excel-csv-json-pdf-png-using-jquery/ export to pdf
- https://github.com/simonbengtsson/jsPDF-AutoTable


Maybe use this — https://github.com/CermakM/jupyter-require — to load in the js?

```python

```

```python

```
