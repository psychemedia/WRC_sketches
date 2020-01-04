# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0rc1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Rallyview Charts
#
# Make a start on trying to pull out some standard charting components

# + tags=["active-ipynb"]
# %load_ext autoreload
# %autoreload 2

# + tags=["active-ipynb"]
# from ewrc_api import EWRC
# -

import dakar_utils as dakar
from dakar_utils import moveColumn, sparkline2, sparklineStep, moreStyleDriverSplitReportBaseDataframe


# ## Multi-processing support
#

# +
#Can we improve performance?
#https://www.ellicium.com/python-multiprocessing-pool-process/
from multiprocessing import cpu_count

num_cores = cpu_count()

#https://towardsdatascience.com/how-i-learned-to-love-parallelized-applies-with-python-pandas-dask-and-numba-f06b0b367138
# #!pip3 install dask
# #!pip3 install cloudpickle
from dask import dataframe as dd


# +
import io
import pandas as pd

#Dask may require dataframe spec
#But what about multi-index dataframes?
def schema_df_create(df):
    ''' Generate a an empty data frame according to the schema of a pre-existing data frame. '''
    
    idx_name = df.index.name
    idx_type = df.index.dtype
    
    cnames = [idx_name]+[c for c in df.columns]
    zz = pd.read_csv(io.StringIO(""),
                     names=cnames,
                     dtype=dict(zip(cnames,[idx_type]+df.dtypes.to_list())),
                     index_col=[idx_name])
    #print(zz.info())
    return zz

def schema_df_drop(df):
    ''' Create an empty dataframe with a schema of an existing dataframe
        by dropping all data from the existing dataframe.
    '''
    return df.drop(df.index)



# -

# ## Utils

# + active=""
# def _rebaseTimes(times, bib=None, basetimes=None):
#     ''' Rebase times relative to specified driver. '''
#     #Should we rebase against entryId, so need to lool that up. In which case, leave index as entryId
#     if bib is None and basetimes is None: return times
#     #bibid = codes[codes['Code']==bib].index.tolist()[0]
#     if bib is not None:
#         return times - times.loc[bib]
#     if times is not None:
#         return times - basetimes
#     return times
# -

from ewrc_api import _rebaseTimes

# ## Charts

# +
#These are in wo as well - should move to dakar utils


#TO DO - the chart should be separated out from the cols generator
# The chart function should return only the chart

#This has been changed from wo so as not to change polarity of the times
def _gapToLeaderBar(Xtmpq, typ, milliseconds=True, flip=True, items=None):
    if milliseconds:
        Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    _tmp='_tmp'
    k = '{}GapToLeader'.format(typ)
    Xtmpq[_tmp] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    flip = -1 if flip else 1
    Xtmpq[_tmp] = Xtmpq[_tmp].apply(lambda x: [flip * y for y in x])
    #Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    #Chart generation is the slow step, so only do it where we need it
    if items is None:
        #Xtmpq[k] = Xtmpq[_tmp].apply(sparkline2, typ='bar', dot=True)
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        Xtmpq[k]=dd.from_pandas(Xtmpq[_tmp],npartitions=num_partitions).map_partitions( lambda df : df.apply( lambda x : sparkline2(x, typ='bar', dot=True)), 
                                                                                       meta=pd.Series(dtype=object)).compute(scheduler='processes')

    else:
        #Ony generate charts for specified rows
        #Create a dummy col
        Xtmpq[k]='s'
        #Use loc for index vals, iloc for row number
        #If the items are passed as dataframe index, we need to convert to a list
        if isinstance(items,pd.core.frame.DataFrame):
            items = items.index.to_list()
        #Xtmpq.loc[items,k] = Xtmpq[_tmp].loc[items].apply(sparkline2, typ='bar', dot=True)
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        Xtmpq.loc[items,k]=dd.from_pandas(Xtmpq[_tmp].loc[items],npartitions=num_partitions).map_partitions( lambda df : df.apply( lambda x : sparkline2(x, typ='bar', dot=True)), 
                                                                                       meta=pd.Series(dtype=object)).compute(scheduler='processes')

    Xtmpq = Xtmpq.drop(_tmp, 1)
    return Xtmpq 

import time
def _positionStep(Xtmpq, typ, items=None):
    Xtmpq.columns = ['SS_{}_{}_pos'.format(c, typ) for c in Xtmpq.columns]
    k = '{}Position'.format(typ)
    _tmp='tmp'
    Xtmpq[_tmp] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[_tmp] = Xtmpq[_tmp].apply(lambda x: [-y for y in x])
    if items is None:
        t0 = time.time()
        #Xtmpq[k] = Xtmpq[_tmp].apply(sparklineStep)
        t1 = time.time()
        #print("Time to process without Dask {}".format(t1-t0))
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        t0 = time.time()
        Xtmpq[k]=dd.from_pandas(Xtmpq[_tmp],npartitions=num_partitions).map_partitions( lambda df : df.apply(sparklineStep), meta=pd.Series(dtype=object)).compute(scheduler='processes')
        t1 = time.time()
        #print("Time to process with Dask {}".format(t1-t0))
        #scheduler='single-threaded | threads | processes')
    else:
        Xtmpq[k]=''
        if isinstance(items,pd.core.frame.DataFrame):
            items = items.index.to_list() 
        #Xtmpq.loc[items, k]= Xtmpq[_tmp].loc[items].apply(sparklineStep)
        num_partitions =num_cores if num_cores<len(items) else len(items)
        Xtmpq.loc[items, k]=dd.from_pandas(Xtmpq[_tmp].loc[items],npartitions=num_partitions).map_partitions( lambda df : df.apply(sparklineStep), meta=pd.Series(dtype=object)).compute(scheduler='processes')
        
    Xtmpq = Xtmpq.drop(_tmp, 1)
    return Xtmpq 



# -

## gapBar looks simple? From wo: wo = __import__("WRC Overall")
def gapBar(df):
    ''' Bar chart showing rebased gap at each stage. '''
    col='Gap'
    df[col] = df[[c for c in df.columns if c.startswith('SS_') and c.endswith('_overall')]].values.tolist()
    df[col] = df[col].apply(lambda x: [-y for y in x])
    df[col] = df[col].apply(sparkline2, typ='bar', dot=False)
    return df


# ## Dummy Trial Data

# + tags=["active-ipynb"]
# rally_stub = '42870-rallye-automobile-de-monte-carlo-2018'
# #rally_stub='54762-corbeau-seats-rally-tendring-clacton-2019'
# ewrc=EWRC(rally_stub)

# + tags=["active-ipynb"]
# ewrc.get_stage_times()
# wREBASE=ewrc.df_stages.iloc[4].name

# + tags=["active-ipynb"]
# wREBASE
# -

# ## Pace
#
# Tools to support pace calculations.

# +
def distToRun(ewrc, nextStage, expand=False):
    ''' Return the distance still to run over the rest of the rally. '''
    ewrc.get_itinerary()
    if expand:
        #return ewrc.df_itinerary.loc[nextStage:,:]['Distance'].cumsum().round(2)
        return ewrc.df_itinerary.loc[nextStage:,:].loc[::-1, 'Distance'].cumsum().round(2)[::-1]
    return ewrc.df_itinerary.loc[nextStage:,:]['Distance'].values.sum().round(2)

def stageDist(ewrc, stage, expand=False, from_next=False):
    ''' Return the cumulative distance to run over one or more stages. '''
    ewrc.get_itinerary()
    if expand:
        #Give the accumulated distance over following stages
        return ewrc.df_itinerary.loc[stage:,:][int(from_next):]['Distance'].cumsum().round(2)
        #return ewrc.stage_distances.loc[stage:].cumsum().round(2)
    return ewrc.df_itinerary.loc[stage,:]['Distance'].sum().round(2)
    #return ewrc.stage_distances.loc[stage].sum().round(2)


# + tags=["active-ipynb"]
# stageDist(ewrc, 'SS3', expand=False), stageDist(ewrc, 'SS3', expand=True), \
# stageDist(ewrc, 'SS3', expand=True, from_next=True)

# + tags=["active-ipynb"]
# distToRun(ewrc, 'SS3'), distToRun(ewrc, 'SS3', expand=True)

# + tags=["active-ipynb"]
# stageDist(ewrc, ['SS1', 'SS2'])

# +
def _rebased_pace_times(ewrc,rebase):
    if rebase == 'overall_leader':
        _df = ewrc.df_stages_rebased_to_overall_leader
        rebase = None
    elif rebase is None or rebase == 'stage_winner':
        #We default to pace times rebased to stage winner
        _df =  ewrc.df_stages_rebased_to_stage_winner
        rebase = None
    else:
        _df = ewrc.df_stages
    return _df, rebase
    
def paceReport(ewrc, rebase=None):
    ''' Time gained / lost per km on a stage. '''

    ewrc.set_rebased_times()
    ewrc.get_itinerary()
    
    _df, rebase = _rebased_pace_times(ewrc,rebase)

    return (_df.apply(_rebaseTimes, bib=rebase, axis=0) / ewrc.stage_distances).round(3)
 


# + tags=["active-ipynb"]
# paceReport(ewrc, rebase='stage_winner').head()

# + tags=["active-ipynb"]
# paceReport(ewrc, rebase='overall_leader').head()

# + tags=["active-ipynb"]
# paceReport(ewrc, rebase='/entryinfo/42870-rallye-automobile-de-monte-carlo-2018/1642087/').head()

# +
def requiredStagePace(ewrc, stage, rebase=None, target_stage=None):
    ''' Pace required from a particular stage to level up
        by the end of each of the remaining stages or on a specified
        target stage.
        Report as the number of seconds required per km,
        but not as a speed unless we have a target speed.
    '''
    ewrc.set_rebased_times()
    ewrc.get_itinerary()
    
    #Get times for specified stage, rebased as necessary
    _df, rebase = _rebased_pace_times(ewrc, rebase)
    times = _df[int(str(stage).replace('SS',''))]
    
    # Get series of distances by stage to end of last stage
    if target_stage is None:
        dist = stageDist(ewrc, stage, expand=True, from_next=True)
        _pace = dist.apply(lambda x: (times / x).round(2)).T
        _pace.columns = [int(str(c).replace('SS','')) for c in _pace.columns]
        return _pace

    #What would it take to level up on just a specified stage
    dist = ewrc.df_itinerary.loc[target_stage]['Distance']
    #Return required pace
    return (times/dist).round(2)

def requiredPace(ewrc, rebase=None, within=None):
    ''' Pace required on competitive distance remaining.
        We can report this as the number of seconds required per km,
        but not as a speed unless we have a target speed.
    '''
    
    ewrc.set_rebased_times()
    ewrc.get_itinerary()
    
    if within=='remaining':
        #Calculate over the whole of the rally
        dist = ewrc.stage_distances.iloc[::-1].cumsum().iloc[::-1]
    else:
        #Find the pace required to recoup on the next stage.
        dist = ewrc.stage_distances
    
    if rebase is None:
        _df = ewrc.df_overall_rebased_to_leader
        rebase = None
    else:
        _df = ewrc.df_overall
    
    #Need to shift right, set col 1 to na, and drop last col
    return (_df.apply(_rebaseTimes, bib=rebase, axis=0).shift(axis=1) / dist).round(3)


# + tags=["active-ipynb"]
# requiredStagePace(ewrc, 'SS4', rebase=None, target_stage=None)

# + tags=["active-ipynb"]
# requiredStagePace(ewrc, 'SS4', rebase=None, target_stage='SS6')

# + tags=["active-ipynb"]
# requiredPace(ewrc).head()

# + tags=["active-ipynb"]
# requiredPace(ewrc, within='remaining').head()
# -

# ## Reporter



# +
def rally_report(ewrc, rebase, codes=None):
    ''' Generate a rally report.
        rebase: rebase times to a specified car or position:
            - overall_leader
            - stage_winner
        codes: a list of index codes, a rally class or list of rally classes
        '''
    #The codes let us filter  - should filter in this function really?
    
    df_allInOne, df_overall, df_stages, df_overall_pos = ewrc.get_stage_times()
    
    #codes provides the order and is taken from the stage order
    if codes is None:
        codes = pd.DataFrame(df_stages.index.tolist()).rename(columns={0:'entryId'}).set_index('entryId')
    else:
        codes = [codes] if isinstance(codes,str) else codes
        #Allowable codes
        _codes = set(df_allInOne.index.tolist())
        carNums=[]
        for _code in codes:
            if _code in ewrc.rally_classes:
                carNums = carNums + df_allInOne[df_allInOne['carNum'].isin(ewrc.carsInClass(_code))].index.tolist() 
        #This was supposed to alose let you add specific codes
        #  but seems to cause an error further on...
        #carNums = carNums + df_allInOne.loc[set(codes).intersection(_codes)].index.tolist() 
        codes = pd.DataFrame(carNums).rename(columns={0:'entryId'}).set_index('entryId')
    
    xcols = df_overall.columns
    
    #rebase is the index value
    #tmp = pd.merge(codes, df_rally_overall[['Class']], how='left', left_index=True, right_index=True)
    tmp = pd.merge(codes, ewrc.df_allInOne[['carNum']],
                   how='left', left_index=True, right_index=True)
    #tmp = pd.merge(tmp, df_entry_list[['Class','carNum']], how='left', on='carNum')
    #https://stackoverflow.com/a/11982843/454773
    df_entry_list = ewrc.get_entry_list()
    tmp = tmp.reset_index().merge(df_entry_list[['Class','carNum']],
                                  how="left", on='carNum').set_index('entryId')
    #print(tmp[-1:].index)
    #If we want the charts relative to overall,
    # we need to assemble them at least on cars ranked above lowest ranked car in codes
    
    #But we could optimise by getting rid of lower ranked cars
    #eg we can get the row index for a given index value as:
    #tmp.index.get_loc('/entryinfo/54762-corbeau-seats-rally-tendring-clacton-2019/2226942/')
    lastcar = tmp[-1:].index[0]
    #print(lastcar)
    overall_idx = df_overall.index.get_loc( lastcar )
    #Then slice to 1 past this for lowest ranked car in selection so we don't rebase irrelevant/lower cars
    
    #Also perhaps provide an option to generate charts just relative to cars identified in codes?
    
    #overallGapToLeader: bar chart showing overall gap to leader
    
    ewrc.set_rebased_times()
    #Following handled by: ewrc.set_rebased_times()
    #The overall times need rebasing to the overall leader at each stage
    #leaderTimes = df_overall.min()#iloc[0]
    #df_overall_rebased_to_leader = df_overall[xcols].apply(_rebaseTimes, basetimes=leaderTimes, axis=1)
    df_overall_rebased_to_leader = ewrc.df_overall_rebased_to_leader
    
    tmp = pd.merge(tmp,_gapToLeaderBar(-df_overall_rebased_to_leader[xcols][:(overall_idx+1)], 'overall', False, False, codes), left_index=True, right_index=True)
    #print(tmp[-1:].index)
    #overallPosition: step line chart showing evolution of overall position
    
    #We need to pass a position table in
    xx=_positionStep(df_overall_pos[xcols][:(overall_idx+1)], 'overall', codes)[['overallPosition']]
    tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

    
    # stageWinnerGap: bar chart showing gap to stage winner
    
    #Following handled by: ewrc.set_rebased_times()
    #The stage times need rebasing to the overall leader
    #Gap to overall leader
    #leaderStagetimes = df_stages.iloc[0]
    #df_stages_rebased_to_overall_leader = df_stages[xcols].apply(_rebaseTimes, basetimes=leaderStagetimes, axis=1)
    #Now rebase to the stage winner
    #df_stages_rebased_to_stage_winner = df_stages_rebased_to_overall_leader[xcols].apply(_rebaseTimes, basetimes=df_stages_rebased_to_overall_leader.min(), axis=1)
    df_stages_rebased_to_stage_winner = ewrc.df_stages_rebased_to_stage_winner
    
    #The gapToLeaderBar needs to return the gap to the stage winner
    tmp = pd.merge(tmp,_gapToLeaderBar(-df_stages_rebased_to_stage_winner[xcols][:(overall_idx+1)], 'stages', False, False, codes), left_index=True, right_index=True)
    #In the preview the SS_N_stages bars are wrong because we have not rebased yet
    tmp.rename(columns={'stagesGapToLeader':'stageWinnerGap'},inplace=True)

    # stagePosition: step chart showing stage positions
    df_stages_pos = df_stages.rank(method='min')
    df_stages_pos.columns = range(1,df_stages_pos.shape[1]+1)
    
    xx=_positionStep(df_stages_pos[xcols][:(overall_idx+1)], 'stages', codes)['stagesPosition']
    tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

    #Rebase
    cols = [c for c in tmp.columns if c.startswith('SS')]
    tmp[cols] = tmp[cols].apply(_rebaseTimes, bib=rebase, axis=0)

    # Gap: bar chart showing gap relative to rebased entry
    # This is just taken from the overall in the table
    #The gap should be ignored for the rebased driver?
    tmp = gapBar(tmp)
    #print('v',tmp[-1:].index)

    moveColumn(tmp, 'stageWinnerGap', right_of='overallPosition')
    moveColumn(tmp, 'stagesPosition', right_of='overallPosition')
    moveColumn(tmp, 'Gap', right_of='overallPosition')

    moveColumn(tmp, 'overallPosition', pos=0)
    moveColumn(tmp, 'overallGapToLeader', right_of='overallPosition')
    #print('w',tmp[-1:].index)
    
    df_rally_overall = ewrc.get_final()
    tmp = pd.merge(tmp, df_rally_overall[['Pos']], how='left', left_index=True, right_index=True)
    moveColumn(tmp, 'Pos', right_of='overallGapToLeader')
    moveColumn(tmp, 'Class', pos=0)
    #print('x',tmp[-1:].index)
    #tmp = pd.merge(tmp, df_rally_overall[['CarNum','Class Rank']], how='left', left_index=True, right_index=True)
    tmp = pd.merge(tmp, df_rally_overall[['Class Rank']], how='left', left_index=True, right_index=True)
    moveColumn(tmp, 'Class Rank', right_of='Class')
    moveColumn(tmp, 'carNum', pos=0)
    #disambiguate carnum
    tmp['carNum'] = '#'+tmp['carNum'].astype(str)
    tmp = tmp.rename(columns={'carNum': 'CarNum'})
    #print('y',tmp[-1:].index)
    
    tmp = pd.merge(tmp, df_allInOne[['driverNav','carModel']], how='left', left_index=True, right_index=True )
    moveColumn(tmp, 'driverNav', pos=1)
    moveColumn(tmp, 'carModel', pos=2)
    #is this the slow bit?
    #print('styling...')
    s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
    #print('...done')
    return tmp, s2
    



# +
# TO DO
# pace to stage winner  - bar chart cf. Gap
# call it: stagePace

# + tags=["active-ipynb"]
# from IPython.display import HTML
#
# tmp, s2 = rally_report(ewrc, wREBASE, codes=None)
# display(HTML(s2))

# + tags=["active-ipynb"]
# df_allInOne, df_overall, df_stages, df_overall_pos = ewrc.get_stage_times()

# + tags=["active-ipynb"]
# df_stages

# + tags=["active-ipynb"]
# ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].dropna().unique().tolist()

# + tags=["active-ipynb"] language="javascript"
# IPython.OutputArea.auto_scroll_threshold = 9999;

# + tags=["active-ipynb"]
# ewrc.get_entry_list()
#
# import ipywidgets as widgets
# from ipywidgets import interact
# from IPython.display import Image
#
# classes = widgets.Dropdown(
#     #Omit car 0
#     options=['All']+ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].dropna().unique().tolist(),
#     value='All', description='Class:', disabled=False )
#
#
# carNum = widgets.Dropdown(
#     options=ewrc.carsInClass(classes.value),
#     description='Car:', disabled=False)
#
# def update_drivers(*args):
#     carlist = carsInClass(classes.value)
#     carNum.options = carlist
#     
# classes.observe(update_drivers, 'value')
#
# def rally_report2( cl, carNum):
#     #rebase = df_rally_overall[df_rally_overall['CarNum']==carNum].index[0]
#     #carNums = df_rally_overall[df_rally_overall['CarNum'].isin(ewrc.carsInClass(cl))].index.tolist()
#     df=ewrc.df_allInOne
#     rebase =  df[df['carNum']==carNum].index[0]
#     #print(rebase)
#     #carNums = df[df['carNum'].isin(ewrc.carsInClass(cl))].index.tolist()
#     #codes = pd.DataFrame(carNums).rename(columns={0:'entryId'}).set_index('entryId')
#
#     #print(codes[-1:])
#     if cl=='All':
#         cl = None
#     tmp, s2 = rally_report(ewrc, rebase, codes = cl)
#     
#     #display(HTML(s2))
#     #rally_logo='<img width="100%" src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/CSRTC-Logo-Banner-2019-01-1920x600-e1527255159629.jpg"/>'
#     rally_logo=''
#     #rallydj_logo='<img style="float: left; src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/rallydj.png"/>'
#     #datasrc_logo='<img style="background-color:black;float: right;" src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/ewrcresults800.png"/>'
#     #bottom_logos='<div>'+rallydj_logo+datasrc_logo+'</div>'
#     footer='<div style="margin-top:50px;margin-bottom:20px">Results and timing data sourced from <em>ewrc-results.com</em>. Chart generated by <em>rallydatajunkie.com</em>.</div>'
#     #footer1=bottom_logos
#     inclass='' if cl=='All' else ' (Class {})'.format(cl)
#     title='<div><h1>Overall Results'+inclass+'</h1><p>Times rebased relative to car {}.</p></div>'.format(carNum)
#     
#     html='<div style="font-family:sans-serif;margin-top:10px;margin-bottom:10px"><div style="margin-top:10px;margin-bottom:50px;">'+rally_logo+'</div>'
#     html = html+'<div style="margin-left:20px;margin-right:20px;">'+title+s2+'</div>'+footer+'</div>'
#     
#     print('grabbing screenshot...')
#     _ = dakar.getTablePNG(html, fnstub='overall_{}_'.format(rebase.replace('/','_')),scale_factor=2)
#     print('...done')
#     display(Image(_))
#     print(_)
#     
# interact(rally_report2, cl=classes, carNum=carNum);

# + tags=["active-ipynb"]
# df = paceReport(ewrc, rebase='stage_winner')#.head()
# df.T.reset_index()
# -

import matplotlib.pyplot as plt

# +
# #%pip install adjustText
# -

#Create xmin and xmax vals for stage indicators by cumulative distance
xy = [_ for _ in zip(ewrc.stage_distances.cumsum().shift(fill_value=0).round(2), 
                     ewrc.stage_distances.cumsum().round(2)) ]
xy


# + tags=["active-ipynb"]
# # Generate a dataframe that allows us to plot values actross the cumulative distance
# dff=df.T.reset_index().melt(id_vars='index')
# dff = pd.merge(dff, ewrc.df_allInOne[['carNum']],
#                how='left', left_on='entryId', right_index=True)
# dff['entryId'] = dff['entryId'].astype('category')
#
# dff['x0'] = dff['index'].apply(lambda x: xy[x-1][0] )
# dff['x1'] = dff['index'].apply(lambda x: xy[x-1][1] )
# dff['xm'] = (dff['x0'] + dff['x1'])/2
# dff
# -

#https://gist.github.com/jakevdp/91077b0cae40f8f8244a
def discrete_cmap(N, base_cmap=None):
    """Create an N-bin discrete colormap from the specified input map"""

    # Note that if base_cmap is a string or None, you can simply do
    #    return plt.cm.get_cmap(base_cmap, N)
    # The following works for string, None, or a colormap instance:

    base = plt.cm.get_cmap(base_cmap)
    color_list = base(np.linspace(0, 1, N))
    cmap_name = base.name + str(N)
    return base.from_list(cmap_name, color_list, N)


# + tags=["active-ipynb"]
# _ymin = 0
#
# PACEMAX=10
#
# PACEMAX = PACEMAX+0.1
# N = len(set(dff['entryId'].cat.codes))+1
#
# lines = dff.apply(lambda x: [(x['x0'],x['value']),(x['x1'],x['value'])], axis=1).to_list()
# lines = [xy for xy in lines if (pd.notna(xy[0][1]) and pd.notna(xy[1][1]) 
#                                 and xy[0][1] <= PACEMAX)]
#
# lc = mc.LineCollection(lines, array=dff['entryId'].cat.codes,
#                        cmap=discrete_cmap(N, 'brg'), linewidths=2)
#
# fig, ax = plt.subplots(figsize=(12,8))
#
# plt.box(on=None)
# plt.grid(axis='y')
# ax.xaxis.set_ticks_position('none')
# ax.yaxis.set_ticks_position('none') 
#
#
# ax.add_collection(lc)
#
# #Add labels for each car
# for x, y, s in zip(dff['xm'], dff['value'], dff['carNum']):
#     if pd.notna(y) and y <= PACEMAX:
#         plt.text(x, y, s, size=10)
#         if y < _ymin:
#             _ymin = y
#             
# ax.figsize = (16,6)
# ax.autoscale()
# #ax.set_facecolor('xkcd:salmon')
# ax.margins(0.1)
#
# # TO DO - add lines to demarcate days
#
# plt.gca().invert_yaxis()
#
# ymax, ymin = ax.get_ylim()
# xmax, xmin = ax.get_xlim()
#
# _ymax = ymax if ymax < PACEMAX else PACEMAX
#
# #Add stage labels
# for _i, _xy in enumerate(xy):
#     plt.text((_xy[0]+_xy[1])/2, _ymin-0.5, _i+1, size=10,
#             bbox=dict(facecolor='red', alpha=0.5))
#
#
# for _x in ewrc.stage_distances.cumsum():
#     ax.axvline( x=_x, color='lightgrey', linestyle=':')
#
# ax.set_ylim( _ymax, _ymin-0.3 );

# +
from matplotlib import collections  as mc
from matplotlib import colors as mcolors
from matplotlib import patches
import numpy as np

def pace_map(ewrc, rebase='stage_winner', rally_class='all', PACEMAX = 2):
    
    def _pace_df(df):
        
        dff=df.T.reset_index().melt(id_vars='index')
        dff = pd.merge(dff, ewrc.df_allInOne[['carNum']],
                       how='left', left_on='entryId', right_index=True)
        dff['entryId'] = dff['entryId'].astype('category')

        dff['x0'] = dff['index'].apply(lambda x: xy[x-1][0] )
        dff['x1'] = dff['index'].apply(lambda x: xy[x-1][1] )
        dff['xm'] = (dff['x0'] + dff['x1'])/2
        return dff

    df = paceReport(ewrc, rebase=rebase)
    
    xy = [_ for _ in zip(ewrc.stage_distances.cumsum().shift(fill_value=0).round(2), 
                             ewrc.stage_distances.cumsum().round(2)) ]
    
    dff = _pace_df(df)
    
    _ymin = 0

    PACEMAX=10

    PACEMAX = PACEMAX+0.1
    N = len(set(dff['entryId'].cat.codes))+1

    lines = dff.apply(lambda x: [(x['x0'],x['value']),(x['x1'],x['value'])], axis=1).to_list()
    lines = [xy for xy in lines if (pd.notna(xy[0][1]) and pd.notna(xy[1][1]) 
                                    and xy[0][1] <= PACEMAX)]

    lc = mc.LineCollection(lines, array=dff['entryId'].cat.codes,
                           cmap=discrete_cmap(N, 'brg'), linewidths=2)

    fig, ax = plt.subplots(figsize=(12,8))

    plt.box(on=None)
    plt.grid(axis='y')
    ax.xaxis.set_ticks_position('none')
    ax.yaxis.set_ticks_position('none') 


    ax.add_collection(lc)

    #Add labels for each car
    for x, y, s in zip(dff['xm'], dff['value'], dff['carNum']):
        if pd.notna(y) and y <= PACEMAX:
            plt.text(x, y, s, size=10)
            if y < _ymin:
                _ymin = y

    ax.figsize = (16,6)
    ax.autoscale()
    #ax.set_facecolor('xkcd:salmon')
    ax.margins(0.1)

    # TO DO - add lines to demarcate days

    plt.gca().invert_yaxis()

    ymax, ymin = ax.get_ylim()
    xmax, xmin = ax.get_xlim()

    _ymax = ymax if ymax < PACEMAX else PACEMAX

    #Add stage labels
    for _i, _xy in enumerate(xy):
        plt.text((_xy[0]+_xy[1])/2, _ymin-0.5, _i+1, size=10,
                bbox=dict(facecolor='red', alpha=0.5))


    for _x in ewrc.stage_distances.cumsum():
        ax.axvline( x=_x, color='lightgrey', linestyle=':')

    ax.set_ylim( _ymax, _ymin-0.3 );
# -

pace_map(ewrc)

# +
# TO DO
# pace chart like the above but with annotations for a particular driver
# showing the pace deficit at which they would lose position to a particular person?
# pace delta map? pace leveller map? For a particular next stage...
# -

# ## Pace Leveller Map
#
# For a particular selected (rebased) drive, a chart like the above with red and green bands that show pace that would level a driver compared to the selected driver on the next stage or N stages.
#

# +
# Use: requiredStagePace()
