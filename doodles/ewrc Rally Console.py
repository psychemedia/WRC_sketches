# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # eWRC Rally Console
#
# Simple notebook for generating eWRC results and charts.
#
# This notebook is intended to be a step towards a voila dashboard that will allow interactive exploration of the data views.

# %load_ext autoreload
# %autoreload 2

from rallyview_charts import *

# TO DO - we need to have a proper way of identifying and loading particular rallies
rally_stub = '60140-rally-sweden-2020'
#rally_stub='61968-snetterton-stages-2020/'
ewrc = EWRC(rally_stub, live=True)

# ## Rally Review Chart
#
# A widget driven view that allows you to generate a rally review chart, with optional selectors for rebasing and class filtering.

# +
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
# -

from ipywidgets import fixed
interact(rally_report2, ewrc=fixed(ewrc), cl=classes, carNum=carNum);

# ## Off the Pace Charts
#
# Generate an off-the-pace chart.
#
# TO DO - widgets for class and driver rebase selection.

# +
ostberg = '/entryinfo/60140-rally-sweden-2020/2498361/'
tanak = '/entryinfo/60140-rally-sweden-2020/2494761/'
evans = '/entryinfo/60140-rally-sweden-2020/2496932/'

rovanpera = '/entryinfo/60140-rally-sweden-2020/2496933/'
ogier='/entryinfo/60140-rally-sweden-2020/2496931/'
# -

off_the_pace_chart(ewrc, filename='testpng/offpace_rov.png', rebase=rovanpera, rally_class='all'); #rebase=rovanpera,

# ## Pace Map
#
# Generate pace maps.
#
# TO DO - widget selection for class and rebase. Further optional selector for comparison driver.
#
# TO DO  - broken for class RC1?

pace_map(ewrc, PACEMAX=1, stretch=True, rally_class='RC1', rebase=rovanpera,
         compared_with=ogier,
         filename='testpng/pacemap_rov_ogi.png');

pace_map(ewrc, PACEMAX=1, stretch=True, rally_class='RC2',
         rebase=ostberg, filename='testpng/pacemap_ost.png');

pace_map(ewrc, PACEMAX=1, stretch=True, rebase=rovanpera,
        compared_with=ogier, filename='testpng/pacemap_rov_ogi.png');

pace_map(ewrc, PACEMAX=1, stretch=True, rally_class='RC2',
         filename='testpng/pacemap_RC2.png');

tanak = '/entryinfo/60140-rally-sweden-2020/2494761/'
evans = '/entryinfo/60140-rally-sweden-2020/2496932/'

pace_map(ewrc, PACEMAX=1, stretch=True, rebase=tanak,
        compared_with=evans, filename='testpng/pacemap_tan_ev.png');

ewrc.get_stage_times()[0].head()


