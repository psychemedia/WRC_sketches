# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
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

import ipywidgets as widgets
from ipywidgets import interact
from IPython.display import Image
from ipywidgets import fixed

# +
# TO DO - we need to have a proper way of identifying and loading particular rallies
rally_stub = '60429-rally-guanajuato-mexico-2020'
rally_stub = '/61496-grossi-toidukaubad-viru-rally-2020'

rally_stub='61347-rali-de-castelo-branco-2020'
rally_stub='61092-rally-di-roma-capitale-2020/'
#rally_stub='61968-snetterton-stages-2020/'
rally_stub='66710-rally-di-alba-raplus-2020'
rally_stub='62487-rally-di-alba-2020'
rally_stub='61090-rally-liepaja-2020'
rally_stub='61498-redgrey-louna-eesti-ralli-2020'
rally_stub='61497-rally-estonia-2020'
rally_stub='60436-rally-turkey-marmaris-2020/'
rally_stub='63180-rally-fafe-montelongo-2020/'
rally_stub='60433-rally-italia-sardegna-2020/'
rally_stub='61095-rally-hungary-2020/'
rally_stub='61089-rally-islas-canarias-2020/' # Lots of retirements - good to test
#rally_stub='66881-aci-rally-monza-2020/' # GOod one to test - cancelled stages

rally_stub='41079-rallye-automobile-de-monte-carlo-2021/'
ewrc = EWRC(rally_stub, live=True)
ewrc.get_entry_list()

# + active=""
# from ewrc_api import search_events
#
#
# ewrc = None
#
# options = None
#
# rally = widgets.Text(
#     value='',
#     placeholder='Search term',
#     description='Search:',
#     disabled=False
# )
#
#
#
# if not options:
#     options = ['Not found']
# rallies = widgets.Select(
#     options=options,
#     # rows=10,
#     description='Rallies:',
#     disabled=False
# )
#
# def update_rallies(response):
#     events = search_events(response.new)
#     rallies.options = [(events[e], e) for e in events]
#     
# rally.observe(update_rallies, 'value')
# display(rally)
# display(rallies)
#
#
# button = widgets.Button(description="RallyView")
# display(button)
#
# def do_rally_view(b):
#     global ewrc
#     #This sets up the next set of widgets with the required rally event id
#     #display(rallies.value)
#     ewrc = EWRC(rallies.value, live=True)
#
# button.on_click(do_rally_view)

# +
#ewrc.stub
# -

# ## Rally Review Chart Direct
#
#
# This approach identifies drivers in classes / championships directly from the ewrc UI.

# +
#ewrc.base_championships

# +
class_options = [(e, ewrc.base_classes[e]) for e in ewrc.base_classes]
classes = widgets.Dropdown(
    options = class_options,
    #value='All',
    description='Class:', disabled=False )

class_options_rev_dict= {c[1]:c[0] for c in class_options}

# +
championship_options = [(e, ewrc.base_championships[e]) for e in ewrc.base_championships]
if not championship_options:
    championship_options = [('All','')]
    
championships = widgets.Dropdown(
    options = championship_options,
    #value='All',
    description='Championship:', disabled=False )

_ewrc = None
#Get drivers
def getDirectChampionshipDriverCarOptions():
    """Get drivers / entries for championship."""
    global _ewrc
    _ewrc = EWRC(ewrc.stub, path='&'.join([i for i in [championships.value, classes.value] if i]), live=True)
    _ewrc.rally_championship = '' if not championships.value else championships.value.split('=')[-1]
    _ewrc.rally_class = '' if not classes.value else classes.value.split('=')[-1]
    #print(_ewrc.path)
    _ewrc.get_entry_list()
    entries = _ewrc.df_entry_list.loc[:,['DriverName', 'carNum']]
    entries['DriverName'] = entries['carNum'] + ': '+entries['DriverName']
    return entries.to_records(index=False)


# -

championship_options_rev_dict= {c[1]:c[0] for c in championship_options}

ewrc.get_entry_list()
ewrc.df_entry_list

# +
carNum = widgets.Dropdown(
    options=[(e[0],e[1]) for e in getDirectChampionshipDriverCarOptions()],
    description='Car:', disabled=False)

  
def update_view(*args):
    global _ewrc
    _ewrc.get_stage_times()
    rally_report2(_ewrc, class_options_rev_dict[classes.value], carNum.value,
                      postfilter=_ewrc.df_entry_list['driverEntry'].to_list(),
                      rally_championship = championship_options_rev_dict[championships.value])
    #rally_report2(_ewrc, _ewrc.df_entry_list['driverEntry'].to_list(), carNum.value)
    #except:
    #    pass

def update_championship_drivers(*args):
    #carlist = ewrc.carsInClass(classes.value)
    carlist = getDirectChampionshipDriverCarOptions()
    carNum.options = [(e[0],e[1]) for e in carlist]

# If we update championship, we need to then update classes and then drivers
championships.observe(update_championship_drivers, 'value')
classes.observe(update_championship_drivers, 'value')
carNum.observe(update_view, 'value')

display(championships)
display(classes)
display(carNum)
# -

_ewrc.df_overall.head()

_ewrc.df_entry_list['driverEntry'].to_list()

carNum.value

rally_report2(ewrc, ewrc.df_entry_list['driverEntry'].to_list()[:10], carNum.value)

# +
#display(championships)
#display(classes)
#carNum
# -

# ## Rally Review Chart
#
# This approach displays results from the entry list; at the moment, grouping via the entry list across class / championship is a bit flaky.
#
# A widget driven view that allows you to generate a rally review chart, with optional selectors for rebasing and class filtering.

# +
ewrc.get_entry_list()

classes = widgets.Dropdown(
    #Omit car 0
    options=['All']+ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].dropna().unique().tolist(),
    value='All', description='Class:', disabled=False )


#ipywidget takes (label, value) 
def getDriverCarOptions(c):
    """Get drivers / entries for class."""
    entries = ewrc.df_entry_list[ewrc.df_entry_list['carNum'].isin(ewrc.carsInClass(c))][['DriverName', 'carNum']]
    entries['DriverName'] = entries['carNum'] + ': '+entries['DriverName']
    return entries.to_records(index=False)


# We really need to better sort these?
carNum = widgets.Dropdown(
    options=[(e[0],e[1]) for e in getDriverCarOptions(classes.value)],
    description='Car:', disabled=False)

#carNum = widgets.Dropdown(
#    options=ewrc.carsInClass(classes.value),
#    description='Car:', disabled=False)

def update_drivers(*args):
    #carlist = ewrc.carsInClass(classes.value)
    carlist = getDriverCarOptions(classes.value)
    carNum.options = [(e[0],e[1]) for e in carlist]
    
classes.observe(update_drivers, 'value')
# -

ewrc.df_allInOne.head().iloc[0]['overalltimes']

display(carNum)
display(classes)


# +
#classes.value, carNum.value

# +
#rally_report2(ewrc,'All', '1')

# +
# TO DO - note that cancelled stages are not shown
# Maybe they should be added in?

def rally_report3(ewrc,cl,carNum):
    rally_report2(ewrc,cl,carNum)

interact(rally_report3, ewrc=fixed(ewrc), cl=classes, carNum=carNum);
# -

# ## Off the Pace Charts
#
# Generate an off-the-pace chart.
#
# TO DO - widgets for class and driver rebase selection.

ostberg = '/entryinfo/60140-rally-sweden-2020/2498361/'
tanak = '/entryinfo/60429-rally-guanajuato-mexico-2020/2519743/'
evans = '/entryinfo/60429-rally-guanajuato-mexico-2020/2519738/'
neuville='/entryinfo/60429-rally-guanajuato-mexico-2020/2519741/'
rovanpera = '/entryinfo/60140-rally-sweden-2020/2496933/'
ogier='/entryinfo/41079-rallye-automobile-de-monte-carlo-2021/2872591/'
suninen='/entryinfo/60429-rally-guanajuato-mexico-2020/2519775/'
greensmith='/entryinfo/60429-rally-guanajuato-mexico-2020/2519771/'
tanak = '/entryinfo/61496-grossi-toidukaubad-viru-rally-2020/2721397/'
solberg='/entryinfo/61092-rally-di-roma-capitale-2020/2738871/'


ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].replace('nan',np.nan).dropna().unique().tolist()


ewrc.df_entry_list[ewrc.df_entry_list['carNum'].isin(ewrc.carsInClass('RC1'))][['DriverName', 'carNum', 'driverEntry']]

# +
import numpy as np

classes2 = widgets.Dropdown(
    #Omit car 0
    options=ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].replace('nan',np.nan).dropna().unique().tolist(),
    description='Class:', disabled=False )

def getDriverCarOptions2(c):
    """Get drivers / entries for class."""
    if c=='nan':
        return []
    entries = ewrc.df_entry_list[ewrc.df_entry_list['carNum'].isin(ewrc.carsInClass(c))][['DriverName', 'carNum', 'driverEntry']]
    entries['driverCar'] = entries['carNum'] + ': '+entries['DriverName']

    return entries[['driverCar','driverEntry']].to_records(index=False)

carNum2 = widgets.Dropdown(
    options=[(e[0],e[1]) for e in getDriverCarOptions2(classes2.value)],
    description='Car:', disabled=False)

#carNum = widgets.Dropdown(
#    options=ewrc.carsInClass(classes.value),
#    description='Car:', disabled=False)

def update_drivers2(*args):
    #carlist = ewrc.carsInClass(classes.value)
    carlist = getDriverCarOptions2(classes2.value)
    carNum2.options = [(e[0], e[1]) for e in carlist]
    
classes2.observe(update_drivers2, 'value')


# +
# off_the_pace_chart??

# +
# This chart drops cancelled stages (gives a bettter sense of the pace trend)

def offpacemapper(ewrc, rc, cn):
    # This may error for some drivers - need to suppress that...
    off_the_pace_chart(ewrc, filename='testpng/offpace_lead.png', size=5,
                   rebase=cn,
                   rally_class=rc);
    
interact(offpacemapper, ewrc=fixed(ewrc), rc=classes2, cn=carNum2);
# -

off_the_pace_chart(ewrc, filename='testpng/offpace_lead.png', size=5,
                   rebase=ogier,
                   rally_class='RC1'); #rebase=rovanpera,


# ## Pace Map
#
# Generate pace maps.
#
# TO DO - widget selection for class and rebase. Further optional selector for comparison driver.
#
# TO DO  - broken for class RC1?

# +
classes3 = widgets.Dropdown(
    #Omit car 0
    options=ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].replace('nan',np.nan).dropna().unique().tolist(),
    description='Class:', disabled=False )

carNum3 = widgets.Dropdown(
    options=[(e[0],e[1]) for e in getDriverCarOptions2(classes2.value)],
    description='Car:', disabled=False)

#carNum = widgets.Dropdown(
#    options=ewrc.carsInClass(classes.value),
#    description='Car:', disabled=False)

def update_drivers3(*args):
    #carlist = ewrc.carsInClass(classes.value)
    carlist = getDriverCarOptions2(classes3.value)
    carNum2.options = [(e[0],e[1]) for e in carlist]
    carNum3.options = [(e[0],e[1]) for e in carlist]

pacemax = widgets.IntSlider(min=0.0, max=5.0, value=2)
classes2.observe(update_drivers3, 'value')

# +
#ewrc.df_stages
#paceReport(ewrc,rebase='/entryinfo/61497-rally-estonia-2020/2751400/',rally_class='RC1')

# +
from ipywidgets import interact_manual

def pacemapper(ewrc, rc, cn, cn2, pacemax):
    pace_map(ewrc, filename='testpng/pacemap_lead.png',
             PACEMAX=pacemax,
             stretch=True,
                   rebase=cn,
             compared_with=cn2,
                   rally_class=rc);

interact(pacemapper, ewrc=fixed(ewrc), rc=classes3, cn=carNum2, cn2=carNum3, pacemax=pacemax);
# -

pace_map(ewrc, PACEMAX=0.5, stretch=True, rally_class='RC1',
         rebase=ogier,
         #rebase=tanak,
         compared_with='/entryinfo/41079-rallye-automobile-de-monte-carlo-2021/2872592/',
         filename='testpng/pacemap_lead.png');

pace_map(ewrc, PACEMAX=1, stretch=True, rebase=rovanpera,
        compared_with=ogier, filename='testpng/pacemap_rov_ogi.png');

pace_map(ewrc, PACEMAX=1, stretch=True, rally_class='RC2',
         filename='testpng/pacemap_RC2.png');

tanak = '/entryinfo/60140-rally-sweden-2020/2494761/'
evans = '/entryinfo/60140-rally-sweden-2020/2496932/'

pace_map(ewrc, PACEMAX=1, stretch=True, rebase=tanak,
        compared_with=evans, filename='testpng/pacemap_tan_ev.png');

ewrc.get_stage_times()[0].head(10)

# # Position Charts
#
# https://blog.ouseful.info/2017/01/25/a-first-attempt-at-wrangling-wrc-world-rally-championship-data-with-pandas-and-matplotlib/
#
#
# See if there is something we can reuse in `wrcx`.

zz = ewrc.get_stage_times()
len(zz)

zz[3]#.dtypes

# +
import matplotlib.pyplot as plt
 
rc1=zz[3].reset_index()
 
fig, ax = plt.subplots(figsize=(15,8))
ax.get_yaxis().set_ticklabels([])
rc1
rc1=rc1.melt(id_vars='entryId', var_name='stage', value_name='pos')
rc1.dropna().groupby('entryId').plot(x='stage',y='pos',ax=ax,legend=None);

# +
rc1['rank']=rc1.groupby('stage')['pos'].rank()
 
fig, ax = plt.subplots(figsize=(15,8))
ax.get_yaxis().set_ticklabels([])
rc1.groupby('entryId').plot(x='stage',y='rank',ax=ax,legend=None)
 
#Add some name labels at the start
rc1[rc1['stage']==1].apply(lambda x: ax.text(-0.5,x['rank'], x['entryId']), axis=1);


# +
RC1SIZE =10

#Reranking...
rc1['xrank']= (rc1['pos']>RC1SIZE)
rc1['xrank']=rc1.groupby('stage')['xrank'].cumsum()
rc1['xrank']=rc1.apply(lambda row: row['pos'] if row['pos']<=RC1SIZE else row['xrank'] +RC1SIZE, axis=1)
fig, ax = plt.subplots(figsize=(15,8))
ax.get_yaxis().set_ticklabels([])
rc1.groupby('entryId').plot(x='stage',y='xrank',ax=ax,legend=None)
 
#Name labels
rc1[rc1['stage']==1].apply(lambda x: ax.text(-0.5,x['xrank'], x['entryId']), axis=1);
rc1[rc1['stage']==10].apply(lambda x: ax.text(10.5,x['xrank'], x['entryId']), axis=1);

#for i,d in rc1[rc1['stage']==1].iterrows():
#    ax.text(-0.5, d.ix(i)['xrank'], d.ix(i)['entryId'])
#for i,d in rc1[rc1['stage']==17].iterrows():
#    ax.text(17.3, d.ix(i)['xrank'], d.ix(i)['entryId'])

# +
fig, ax = plt.subplots(figsize=(15,8))
ax.get_yaxis().set_ticklabels([])
rc1.groupby('entryId').plot(x='stage',y='xrank',ax=ax,legend=None);
 
#rc1[rc1['stage']==1].apply(lambda x: ax.text(-0.5,x['rank'], x['entryId']), axis=1);
#rc1[rc1['stage']==10].apply(lambda x: ax.text(10.5,x['rank'], x['entryId']), axis=1);

#for i,d in rc1[rc1['xrank']>RC1SIZE].iterrows():
#    ax.text(d.ix(i)['stage']-0.1, d.ix(i)['xrank'], d.ix(i)['pos'], bbox=dict( boxstyle='round,pad=0.3',color='pink')) #facecolor='none',edgecolor='black',
#Name labels
#for i,d in rc1[rc1['stage']==1].iterrows():
#    ax.text(-0.5, d.ix(i)['xrank'], d.ix(i)['driverName'])

for i,d in rc1[rc1['stage']==17].iterrows(): 
    ax.text(17.3, d.ix(i)['xrank'], d.ix(i)['driverName'])
#Flip the y-axis plt.gca().invert_yaxis()

# +
from ipywidgets import interact

def f(x=1, y='a'):
    print(x, y)


interact(f, x=2, y='b');


# +
def g(x=1, y='a', z=''):
    print(x, y, z)

interact(g, x=2, y='b');


# +
def g(x=1, y='a', z=None):
    print(x, y, z)

interact(g, x=2, y='b');


# +
#https://stackoverflow.com/a/57410205/454773
# #%pip install PyPDF4

def removeWatermark(wm_text, inputFile, outputFile):
    from PyPDF4 import PdfFileReader, PdfFileWriter
    from PyPDF4.pdf import ContentStream
    from PyPDF4.generic import TextStringObject, NameObject
    from PyPDF4.utils import b_

    with open(inputFile, "rb") as f:
        source = PdfFileReader(f, "rb")
        output = PdfFileWriter()

        for page in range(source.getNumPages()):
            page = source.getPage(page)
            content_object = page["/Contents"].getObject()
            content = ContentStream(content_object, source)

            for operands, operator in content.operations:
                if operator == b_("Tj"):
                    text = operands[0]

                    if isinstance(text, str) and text.startswith(wm_text):
                        operands[0] = TextStringObject('')

            page.__setitem__(NameObject('/Contents'), content)
            output.addPage(page)

        with open(outputFile, "wb") as outputStream:
            output.write(outputStream)

wm_text = 'Digitized by'
inputFile = r'/Users/tonyhirst/Downloads/talesandlegends_isleOfWight.pdf'
inputFile2 = r"/Users/tonyhirst/Downloads/output1.pdf"
removeWatermark(wm_text, inputFile, inputFile2)
wm_text = 'Google'
#inputFile = r'/Users/tonyhirst/Downloads/talesandlegends_isleOfWight.pdf'
outputFile = r"/Users/tonyhirst/Downloads/output.pdf"
removeWatermark(wm_text, inputFile2, outputFile)

# +
from PyPDF4 import PdfFileReader
from PyPDF4.pdf import ContentStream

f= open(inputFile, "rb")
source = PdfFileReader(f, "rb")
page = source.getPage(10)
content_object = page["/Contents"].getObject()
content = ContentStream(content_object, source)
print(content_object)
# -

page


