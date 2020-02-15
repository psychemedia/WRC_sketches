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

# eWRC Rally Console

Simple notebook for generating eWRC results and charts.

```python
%load_ext autoreload
%autoreload 2
```

```python
from rallyview_charts import *
```

```python
rally_stub = '60140-rally-sweden-2020'
ewrc = EWRC(rally_stub, live=True)
```

```python
ewrc.get_entry_list()

import ipywidgets as widgets
from ipywidgets import interact
from IPython.display import Image

classes = widgets.Dropdown(
    #Omit car 0
    options=['All']+ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].dropna().unique().tolist(),
    value='All', description='Class:', disabled=False )


carNum = widgets.Dropdown(
    options=ewrc.carsInClass(classes.value),
    description='Car:', disabled=False)

def update_drivers(*args):
    carlist = ewrc.carsInClass(classes.value)
    carNum.options = carlist
    
classes.observe(update_drivers, 'value')
```

```python
ewrc.df_stages
```

```python
from ipywidgets import fixed
interact(rally_report2, ewrc=fixed(ewrc), cl=classes, carNum=carNum);
```

```python
off_the_pace_chart(ewrc, filename='testpng/offpace.png');
```

```python
paceReport(ewrc).head(10)
```

```python
tanak = '/entryinfo/60140-rally-sweden-2020/2494761/'

pace_map(ewrc, PACEMAX=2, stretch=True, rally_class='RC1',
         rebase=tanak, filename='testpng/pacemap.png');
```

```python

```
