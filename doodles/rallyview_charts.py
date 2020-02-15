# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
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

# # Rallyview Charts
#
# Make a start on trying to pull out some standard charting components

# + tags=["active-ipynb"]
# %load_ext autoreload
# %autoreload 2
# -

from ewrc_api import EWRC

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
    Xtmpq[_tmp] = Xtmpq[[c for c in Xtmpq.columns]].values.tolist()
    flip = -1 if flip else 1
    Xtmpq[_tmp] = Xtmpq[_tmp].apply(lambda x: [flip * y for y in x])
    #Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    #Chart generation is the slow step, so only do it where we need it
    if items is None:
        #Xtmpq[k] = Xtmpq[_tmp].apply(sparkline2, typ='bar', dot=True)
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        Xtmpq[k] = dd.from_pandas(Xtmpq[_tmp], npartitions=num_partitions).map_partitions(lambda df: df.apply( lambda x : sparkline2(x, typ='bar', dot=True)), 
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
        Xtmpq.loc[items, k] = dd.from_pandas(Xtmpq[_tmp].loc[items],
                                          npartitions=num_partitions).map_partitions(lambda df: df.apply(lambda x : sparkline2(x, typ='bar', dot=True)), 
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
        Xtmpq[k]=dd.from_pandas(Xtmpq[_tmp],
                                npartitions=num_partitions).map_partitions(lambda df: df.apply(sparklineStep),
                                                                           meta=pd.Series(dtype=object)).compute(scheduler='processes')
        t1 = time.time()
        #print("Time to process with Dask {}".format(t1-t0))
        #scheduler='single-threaded | threads | processes')
    else:
        Xtmpq[k]=''
        if isinstance(items,pd.core.frame.DataFrame):
            items = items.index.to_list() 
        #Xtmpq.loc[items, k]= Xtmpq[_tmp].loc[items].apply(sparklineStep)
        num_partitions = num_cores if num_cores < len(items) else len(items)
        Xtmpq.loc[items, k] = dd.from_pandas(Xtmpq[_tmp].loc[items],
                                             npartitions=num_partitions).map_partitions(lambda df: df.apply(sparklineStep), meta=pd.Series(dtype=object)).compute(scheduler='processes')
        
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
# rally_stub='61961-mgj-engineering-brands-hatch-winter-stages-2020'
# rally_stub='59972-rallye-automobile-de-monte-carlo-2020'
# rally_stub='60500-visit-conwy-cambrian-rally-2020/'
# rally_stub = '60140-rally-sweden-2020'
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
# #2,3, 4, 8, 5, 6, 7, 16,17, 18
# ewrc.get_itinerary()
# ewrc.stage_distances, ewrc.stage_distances_all
# #ewrc.df_itinerary['Distance'][~ewrc.df_itinerary['Time'].str.contains('cancelled')]
#
# -

# If a stage is cancelled, we need to make sure we put an empty result in? Or patch the stage_distances.

# + tags=["active-ipynb"]
# stageDist(ewrc, 'SS3', expand=False), stageDist(ewrc, 'SS3', expand=True), \
# stageDist(ewrc, 'SS3', expand=True, from_next=True)

# + tags=["active-ipynb"]
# distToRun(ewrc, 'SS3'), distToRun(ewrc, 'SS3', expand=True)

# + tags=["active-ipynb"]
# stageDist(ewrc, ['SS1', 'SS2'])
# -

# also in EWRC class as df_inclass_cars
def _inclass_cars(ewrc, _df, rally_class='all', typ='entryId'):
    """Get cars in particular class."""
    if rally_class != 'all':
        _df = _df[_df.index.isin(ewrc.carsInClass(rally_class, typ=typ))]
    return _df


# +
def _rebased_pace_times(ewrc, rebase, rally_class='all'):
    
    if rebase == 'overall_leader':
        _df = ewrc.df_stages_rebased_to_overall_leader
        rebase = None
    elif rebase is None or rebase == 'stage_winner':
        #We default to pace times rebased to stage winner
        if rally_class == 'all':
            _df =  ewrc.df_stages_rebased_to_stage_winner
        else:
            _df = ewrc.get_class_rebased_times(rally_class='RC2')
        rebase = None
    else:
        _df = ewrc.df_stages
        
    _df = _inclass_cars(ewrc, _df, rally_class=rally_class, typ='entryId')

    _times = _df.apply(_rebaseTimes, bib=rebase, axis=0)
    _distances = ewrc.stage_distances
    
    # This correctly does the division based on index, but if we have cancelled stages
    # our indexes are currently mixed up. Some include the cancelled stage, others don't.
    _df = (_times / _distances[:len(_times)] ).round(3)
    
        
    return _df, rebase
    
def paceReport(ewrc, rebase=None, show=False, rally_class='all'):
    ''' Time gained / lost per km on a stage. '''

    ewrc.set_rebased_times()
    ewrc.get_itinerary()
        
    _df, rebase = _rebased_pace_times(ewrc, rebase, rally_class=rally_class)
    if show:
        display(_df)
        display(rebase)
    
    return _df.dropna(how='all', axis=1)
 


# + tags=["active-ipynb"]
# paceReport(ewrc, rally_class='RC1')

# + tags=["active-ipynb"]
# paceReport(ewrc, rebase='stage_winner').head(10)

# + tags=["active-ipynb"]
# paceReport(ewrc, rebase='overall_leader').head()

# + tags=["active-ipynb"]
# #paceReport(ewrc, rebase='/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465687/').head()
#
# paceReport(ewrc, rebase='/entryinfo/60500-visit-conwy-cambrian-rally-2020/2507438/', rally_class='BRCJNational').head()

# + tags=["active-ipynb"]
# ewrc.df_overall

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
# requiredStagePace(ewrc, 'SS3', rebase=None, target_stage=None)

# + tags=["active-ipynb"]
# requiredStagePace(ewrc, 'SS4', rebase=None, target_stage='SS6')

# + tags=["active-ipynb"]
# requiredPace(ewrc).head()

# + tags=["active-ipynb"]
# requiredPace(ewrc, within='remaining').head()
# -

# ## Reporter



def rally_report(ewrc, rebase, codes=None):
    ''' Generate a rally report.
        rebase: rebase times to a specified car or position:
            - overall_leader
            - stage_winner
        codes: a list of index codes, a rally class or list of rally classes
        '''
    
    #Check we have the entry list
    ewrc.get_entry_list()
    
    #The codes let us filter  - should filter in this function really?
    
    df_allInOne, df_overall, df_stages, df_overall_pos = ewrc.get_stage_times()
    
    #codes provides the order and is taken from the stage order
    if codes is None or codes=='All':
        codes = pd.DataFrame(df_stages.index.tolist()).rename(columns={0:'entryId'}).set_index('entryId')
    else:
        codes = [codes] if isinstance(codes,str) else codes
        #Allowable codes
        _codes = set(df_allInOne.index.tolist())
        carNums=[]
        for _code in codes:
            if _code in ewrc.rally_classes:
                carNums = carNums + df_allInOne[df_allInOne['carNum'].isin(ewrc.carsInClass(_code))].index.tolist() 
        #This was supposed to also let you add specific codes
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
    if not isinstance(overall_idx, int):
        overall_idx = len(df_overall)-1
    #Then slice to 1 past this for lowest ranked car in selection so we don't rebase irrelevant/lower cars
    
    #Also perhaps provide an option to generate charts just relative to cars identified in codes?
    
    #overallGapToLeader: bar chart showing overall gap to leader
    
    ewrc.set_rebased_times()
    #Following handled by: ewrc.set_rebased_times()
    #The overall times need rebasing to the overall leader at each stage
    #leaderTimes = df_overall.min()#iloc[0]
    #df_overall_rebased_to_leader = df_overall[xcols].apply(_rebaseTimes, basetimes=leaderTimes, axis=1)
    df_overall_rebased_to_leader = ewrc.df_overall_rebased_to_leader

    tmp = pd.merge(tmp,_gapToLeaderBar(-df_overall_rebased_to_leader[xcols][:(overall_idx+1)],
                                       'overall', False, False, codes),
                   left_index=True, right_index=True)
    #print(tmp[-1:].index)
    #overallPosition: step line chart showing evolution of overall position
    
    #We need to pass a position table in
    xx=_positionStep(df_overall_pos[xcols][:(overall_idx+1)], 
                     'overall', codes)[['overallPosition']]
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
    tmp = pd.merge(tmp,_gapToLeaderBar(-df_stages_rebased_to_stage_winner[xcols][:(overall_idx+1)], 'stages', False, False, codes),
                   left_index=True, right_index=True)
    #In the preview the SS_N_stages bars are wrong because we have not rebased yet
    tmp.rename(columns={'stagesGapToLeader':'stageWinnerGap'},inplace=True)

    # stagePosition: step chart showing stage positions
    df_stages_pos = df_stages.rank(method='min')
    df_stages_pos.columns = range(1,df_stages_pos.shape[1]+1)
    
    xx=_positionStep(df_stages_pos[xcols][:(overall_idx+1)],
                     'stages', codes)['stagesPosition']
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
    
    #There is no final result if we are in a rally
    #Need to handle this better
    #eg use a ranking from the latst stage?
    #need a function get_final_or_latest()?
    try:
        df_rally_overall = ewrc.get_final()
        tmp = pd.merge(tmp, df_rally_overall[['Pos']],
                       how='left', left_index=True, right_index=True)
        moveColumn(tmp, 'Pos', right_of='overallGapToLeader')
        moveColumn(tmp, 'Class', pos=0)
        #print('x',tmp[-1:].index)
        #tmp = pd.merge(tmp, df_rally_overall[['CarNum','Class Rank']], how='left', left_index=True, right_index=True)
        tmp = pd.merge(tmp, df_rally_overall[['Class Rank']],
                       how='left', left_index=True, right_index=True)
        moveColumn(tmp, 'Class Rank', right_of='Class')
        moveColumn(tmp, 'carNum', pos=0)
        #disambiguate carnum
        tmp['carNum'] = '#'+tmp['carNum'].astype(str)
        tmp = tmp.rename(columns={'carNum': 'CarNum'})
        #print('y',tmp[-1:].index)
    except:
        pass
    
    tmp = pd.merge(tmp, df_allInOne[['driverNav','carModel']],
                   how='left', left_index=True, right_index=True )
    moveColumn(tmp, 'driverNav', pos=1)
    moveColumn(tmp, 'carModel', pos=2)
    #is this the slow bit?
    #print('styling...')
    s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
    #print('...done')
    return tmp, s2


# + tags=["active-ipynb"]
# aa='/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465687/'
# ewrc.df_overall_rebased_to_leader.loc[aa]

# +
# TO DO
# pace to stage winner  - bar chart cf. Gap
# call it: stagePace

# + tags=["active-ipynb"]
# ewrc=EWRC(rally_stub, live=True)
# -

from IPython.display import HTML

# + tags=["active-ipynb"]
#
# wREBASE = '/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465681/'
# tmp, s2 = rally_report(ewrc, wREBASE, codes='RC1') #codes='all'
# display(HTML(s2))

# + tags=["active-ipynb"]
# ewrc.get_entry_list()
# ewrc.rally_classes

# + tags=["active-ipynb"]
# df_allInOne, df_overall, df_stages, df_overall_pos = ewrc.get_stage_times()

# + tags=["active-ipynb"]
# df_stages

# + tags=["active-ipynb"]
# ewrc.df_entry_list[ewrc.df_entry_list['CarNum']!='#0']['Class'].dropna().unique().tolist()

# + tags=["active-ipynb"] language="javascript"
# IPython.OutputArea.auto_scroll_threshold = 9999;

# +
from IPython.display import Image

def rally_report2(ewrc, cl, carNum):
    #rebase = df_rally_overall[df_rally_overall['CarNum']==carNum].index[0]
    #carNums = df_rally_overall[df_rally_overall['CarNum'].isin(ewrc.carsInClass(cl))].index.tolist()
    
    ewrc.get_stage_times()
    
    df = ewrc.df_allInOne
    rebase =  df[df['carNum']==carNum].index[0]
    #print(rebase)
    #carNums = df[df['carNum'].isin(ewrc.carsInClass(cl))].index.tolist()
    #codes = pd.DataFrame(carNums).rename(columns={0:'entryId'}).set_index('entryId')

    #print(codes[-1:])
    tmp, s2 = rally_report(ewrc, rebase, codes = cl)
    
    #display(HTML(s2))
    #rally_logo='<img width="100%" src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/CSRTC-Logo-Banner-2019-01-1920x600-e1527255159629.jpg"/>'
    rally_logo=''
    #rallydj_logo='<img style="float: left; src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/rallydj.png"/>'
    #datasrc_logo='<img style="background-color:black;float: right;" src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/ewrcresults800.png"/>'
    #bottom_logos='<div>'+rallydj_logo+datasrc_logo+'</div>'
    footer='<div style="margin-top:50px;margin-bottom:20px">Results and timing data sourced from <em>ewrc-results.com</em>. Chart generated by <em>rallydatajunkie.com</em>.</div>'
    #footer1=bottom_logos
    inclass='' if cl=='All' else ' (Class {})'.format(cl)
    title='<div><h1>Overall Results'+inclass+'</h1><p>Times rebased relative to car {}.</p></div>'.format(carNum)
    
    html='<div style="font-family:sans-serif;margin-top:10px;margin-bottom:10px"><div style="margin-top:10px;margin-bottom:50px;">'+rally_logo+'</div>'
    html = html+'<div style="margin-left:20px;margin-right:20px;">'+title+s2+'</div>'+footer+'</div>'
    
    print('grabbing screenshot...')
    _ = dakar.getTablePNG(html, fnstub='overall_{}_'.format(rebase.replace('/','_')),scale_factor=2)
    print('...done')
    display(Image(_))
    print(_)


# + tags=["active-ipynb"]
# from ipywidgets import fixed
#
# ewrc.get_entry_list()
#
# import ipywidgets as widgets
# from ipywidgets import interact
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
#     carlist = ewrc.carsInClass(classes.value)
#     carNum.options = carlist
#     
# classes.observe(update_drivers, 'value')

# + tags=["active-ipynb"]
# interact(rally_report2, ewrc=fixed(ewrc), cl=classes, carNum=carNum);

# + tags=["active-ipynb"]
# df = paceReport(ewrc, rebase='stage_winner')#.head()
# df.T.reset_index()
# -

import matplotlib.pyplot as plt


# +
# #%pip install adjustText

# + tags=["active-ipynb"]
# # Hack for sweden to cope with cancelled stage - need to address missing stage somehow
# #ewrc.get_itinerary()
# ewrc.stage_distances = ewrc.stage_distances[1:]
# ewrc.stage_distances

# + tags=["active-ipynb"]
# #Create xmin and xmax vals for stage indicators by cumulative distance
# xy = [_ for _ in zip(ewrc.stage_distances.cumsum().shift(fill_value=0).round(2), 
#                      ewrc.stage_distances.cumsum().round(2)) ]
# xy

# + tags=["active-ipynb"]
# # Generate a dataframe that allows us to plot values across the cumulative distance
# dff=df.T.reset_index().melt(id_vars='index')
# dff = pd.merge(dff, ewrc.df_allInOne[['carNum']],
#                how='left', left_on='entryId', right_index=True)
# dff['entryId'] = dff['entryId'].astype('category')
#
# dff['x0'] = dff['index'].apply(lambda x: xy[x-1][0] )
# dff['x1'] = dff['index'].apply(lambda x: xy[x-1][1] )
# dff['xm'] = (dff['x0'] + dff['x1'])/2
# dff

# + tags=["active-ipynb"]
# dff[dff['carNum']=='1'].iloc[0]['entryId']
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
# dff
# -

from matplotlib import collections  as mc
from matplotlib import colors as mcolors
from matplotlib import patches
import numpy as np

# + tags=["active-ipynb"]
# compared_with='/entryinfo/60140-rally-sweden-2020/2496932/'
# rebase='/entryinfo/60140-rally-sweden-2020/2494761/'
#
# df.T[compared_with]>df.T[rebase]

# + tags=["active-ipynb"]
# dff[dff['entryId']==compared_with]['value']

# + tags=["active-ipynb"]
# for i in zip(xy,df.T[compared_with]>df.T[rebase]):
#     print(i)

# + tags=["active-ipynb"]
# xy

# + tags=["active-ipynb"]
# import matplotlib.patches as patches

# + tags=["active-ipynb"]
#
#
# _ymin = 0
#
# PACEMAX=10
#
# PACEMAX = PACEMAX+0.1
#
# lines = dff.apply(lambda x: [(x['x0'],x['value']),
#                              (x['x1'],x['value'])], axis=1).to_list()
#
# _entries = [True if (pd.notna(xy[0][1]) and pd.notna(xy[1][1]) 
#                                 and xy[0][1] <= PACEMAX) else False for xy in lines]
# lines = [xy for xy in lines if (pd.notna(xy[0][1]) and pd.notna(xy[1][1]) 
#                                 and xy[0][1] <= PACEMAX)]
#
#
#
# #N = len(set(dff['entryId'].cat.codes))+1
# #lc = mc.LineCollection(lines, array=dff['entryId'].cat.codes,
# #                       cmap=discrete_cmap(N, 'brg'), linewidths=2)
#
# N = len(set(dff[_entries]['entryId'].cat.codes))+1
# print(N)
# lc = mc.LineCollection(lines, array=dff[_entries]['entryId'].cat.codes,
#                        cmap=discrete_cmap(N, 'brg'), linewidths=2)
#
#
# fig, ax = plt.subplots(figsize=(12,8))
#
# plt.box(on=None)
# plt.grid(axis='y')
# ax.xaxis.set_ticks_position('none')
# ax.yaxis.set_ticks_position('none') 
#
#
# narrow_compare=True
# if compared_with:
#     for i in zip(xy,df.T[compared_with]>df.T[rebase],
#                  dff[dff['entryId']==compared_with]['value']):
#         color = 'pink' if i[1] else 'lightgreen'
#         if narrow_compare:
#             rect = patches.Rectangle((i[0][0],0),i[0][1]-i[0][0],i[2],
#                                      color=color)
#             # Add the patch to the Axes
#             ax.add_patch(rect)
#         else:
#             ax.axvspan(i[0][0], i[0][1], alpha=0.5, color=color)
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
from matplotlib.ticker import MaxNLocator


def pace_map(ewrc, rebase='stage_winner',
             rally_class='all', PACEMAX = 2,  compared_with=None, narrow_compare=True,
             title=None, stretch=True, drop=False, filename=None):
    """Pace map chart."""
    
    def _pace_df(df):
        
        dff = df.T.reset_index().melt(id_vars='index')
        dff = pd.merge(dff, ewrc.df_allInOne[['carNum']],
                       how='left', left_on='entryId', right_index=True)
        dff['entryId'] = dff['entryId'].astype('category')

        dff['x0'] = dff['index'].apply(lambda x: xy[x-1][0] )
        dff['x1'] = dff['index'].apply(lambda x: xy[x-1][1] )
        dff['xm'] = (dff['x0'] + dff['x1'])/2
        return dff

    
    if title is None:
        title = f'Pace Report rebased to {rebase}'
        
    df = paceReport(ewrc, rebase=rebase, rally_class=rally_class)
    
    if stretch:
        xy = [_ for _ in zip(ewrc.stage_distances.cumsum().shift(fill_value=0).round(2), 
                                 ewrc.stage_distances.cumsum().round(2)) ]
    else:
        xy = [_ for _ in zip(range(ewrc.stage_distances.size),range(1,ewrc.stage_distances.size+1))]

    dff = _pace_df(df)
    
    
    #Need to tweak the whole chart to be able to show not run stages
    if drop:
        pass
        
    _ymin = 0

    # Set pacemax to be at list the min gap
    if PACEMAX < dff['value'].min():
        PACEMAX = dff['value'].min() * 1.5
        
    PACEMAX = PACEMAX+0.1

    lines = dff.apply(lambda x: [(x['x0'],x['value']),(x['x1'],x['value'])],
                                      axis=1).to_list()
    
    #This is part of a fudge to try to get categorical line coloring
    _entries = [True if (pd.notna(xy[0][1]) and pd.notna(xy[1][1]) 
                                and xy[0][1] <= PACEMAX) else False for xy in lines]

    lines = [xy for xy in lines if (pd.notna(xy[0][1]) and pd.notna(xy[1][1]) 
                                    and xy[0][1] <= PACEMAX)]

    if not lines:
        # Need to increase PACEMAX
        return

    #N = len(set(dff['entryId'].cat.codes))+1
    #lc = mc.LineCollection(lines, array=dff['entryId'].cat.codes,
    #                       cmap=discrete_cmap(N, 'brg'), linewidths=2)

    N = len(set(dff[_entries]['entryId'].cat.codes))+1
    #The lookup rebases arbitrary category codes into sequentially enumerated codes
    lookup = {j:i for i,j in enumerate(set(dff[_entries]['entryId'].cat.codes))}
    
    colormap = discrete_cmap(N, 'brg')
    lc = mc.LineCollection(lines, array=np.array([lookup[c] for c in dff[_entries]['entryId'].cat.codes]),
                           cmap=colormap, linewidths=2)

    fig, ax = plt.subplots(figsize=(12,8))

    plt.box(on=None)
    plt.grid(axis='y')
    ax.xaxis.set_ticks_position('none')
    ax.yaxis.set_ticks_position('none') 


    ax.add_collection(lc)

    #Add labels for each car - simple dodge to improve readability
    #Need to group by each x0, and then do left and right in stage rank order
    offset=True
    for x0, xm, y, s in zip(dff['x0'], dff['xm'], dff['value'], dff['carNum']):
        if pd.notna(y) and y <= PACEMAX and y >= -PACEMAX:
            offset = not offset
            plt.text((not offset)*x0+offset*(xm), y, s, size=10)
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
    _ymin = ymin if ymin > -PACEMAX else -PACEMAX
    
    #Add background colour to show +/- compared with another driver
    if compared_with:
        for i in zip(xy, df.T[compared_with]<0, dff[dff['entryId']==compared_with]['value']):
            color = 'pink' if i[1] else 'lightgreen'
            if narrow_compare:
                rect = patches.Rectangle((i[0][0],0),i[0][1]-i[0][0],i[2],
                                     color=color)
                # Add the patch to the Axes
                ax.add_patch(rect)
            else:
                ax.axvspan(i[0][0], i[0][1], alpha=0.5, color=color)


    #Add stage labels
    for _i, _xy in enumerate(xy):
        plt.text((_xy[0]+_xy[1])/2, _ymin-0.5, _i+1, size=10,
                bbox=dict(facecolor='red', alpha=0.5))


    #Add title
    plt.text(0, _ymin-0.7, title, size=10)
    
    if stretch:
        for _x in ewrc.stage_distances.cumsum():
            ax.axvline( x=_x, color='lightgrey', linestyle=':')
    else:
        for (_, _x) in xy:
            ax.axvline( x=_x, color='lightgrey', linestyle=':')

    ax.set_ylabel("Off the pace (s/km)")
    if stretch:
        ax.set_xlabel("Accumulated competitive distance (km)")
    else:
        #ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.xaxis.set_major_formatter(plt.NullFormatter())
        ax.set_xlabel(None)

    ax.set_ylim( _ymax, _ymin-0.3 )
    
    if filename:
        plt.savefig(filename)
        
    return ax

# + tags=["active-ipynb"]
# xy

# + tags=["active-ipynb"]
# #ewrc.stage_distances = ewrc.stage_distances[1:]
# tanak = '/entryinfo/60140-rally-sweden-2020/2494761/'
# ostberg = '/entryinfo/60140-rally-sweden-2020/2498361/'

# + tags=["active-ipynb"]
# pace_map(ewrc, PACEMAX=2, stretch=True, rally_class='RC2',
#          rebase=ostberg, filename='testpng/pacemap_ostberg.png');
#
# # TO DO  - need a 'class_winner' rebaser

# + tags=["active-ipynb"]
# pace_map(ewrc, PACEMAX=2, stretch=True, rebase='/entryinfo/60500-visit-conwy-cambrian-rally-2020/2507999/',
#         compared_with='/entryinfo/60500-visit-conwy-cambrian-rally-2020/2507438/')

# + tags=["active-ipynb"]
# pace_map(ewrc, PACEMAX=2, stretch=False)

# + tags=["active-ipynb"]
# evans='/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465687/'
# neuville = '/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465681/'
# pace_map(ewrc, rebase=neuville,
#          PACEMAX=1)

# +
# TO DO
# pace chart like the above but with annotations for a particular driver
# showing the pace deficit at which they would lose position to a particular person?
# pace delta map? pace leveller map? For a particular next stage...
# -

# ## Off the Pace Map
#
# Line chart showing accumulated time off from the stage winner.

# + tags=["active-ipynb"]
# ewrc.df_stages_rebased_to_stage_winner.head()

# + tags=["active-ipynb"]
#  ewrc.df_allInOne

# + run_control={"marked": false} tags=["active-ipynb"]
# #dff = ewrc.df_stages_rebased_to_stage_winner.head()
# #bib: overall_leader, stage_winner
# #gPace from PushingPace https://pushingpace.com/gpace/
# #"What is gPace? 
# # gPace is the time lost to the fastest theoretical time achievable.
# #In other words a theoretical rally time of the fastest stage times,
# #whoever set them. 
# #Further still it could include stage times made of the fastest splits.
# #Only one person has ever won every stage of a round of the WRC. 
# #Known as the GOAT or God, Sebastien Loeb achieved this 
# #on Tour de Corse in 2005. 
# #Therefore the ‘g’ of gPace is a small tribute to him.
# #Although it could also stand for ‘ghost pace’ 
# #as seen in many rally video games.
# #See rest of original post for more
# rebase='/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465687/' #evans
# #rebase='/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465681/'#neuville'
# #rebase='/entryinfo/59972-rallye-automobile-de-monte-carlo-2020/2465687/'
# rebase='/entryinfo/60500-visit-conwy-cambrian-rally-2020/2507999/'
#
# rebase = ewrc.df_allInOne.index.values[1]
# #pilot = 'Evans'
# rebase=None
# -



# + run_control={"marked": false}
def off_the_pace_chart(ewrc, rally_class='all',
                       stretch=True, figsize=(16,6), rebase=None,
                       filename=None, size=5):

    ewrc.get_itinerary()

    fig, ax = plt.subplots(figsize=(12,8))
    ax.figsize = figsize

    if rebase:
        dff = ewrc.df_stages
        dff = _inclass_cars(ewrc, dff, rally_class).head(size)
        dff.apply(_rebaseTimes, bib=rebase, axis=0)
        #Need to now subtract that driver's times
        dff = dff - dff.loc[rebase]
        pilot = rebase
    else:
        if rally_class == 'all':
            dff = ewrc.df_stages_rebased_to_stage_winner.head(size)
        else:
            dff = ewrc.get_class_rebased_times(rally_class).head(size)
        pilot = 'Each Stage Winner'

    dff = pd.merge(dff, ewrc.df_allInOne[['carNum']],
                           how='left', left_index=True, right_index=True)
    dff.set_index('carNum', drop=True, inplace=True)
    dff_cumsum = dff.cumsum(axis=1)


    dff_cumsumT=dff_cumsum.T

    if stretch:
        xy = [_ for _ in zip(ewrc.stage_distances.cumsum().shift(fill_value=0).round(2), 
                                 ewrc.stage_distances.cumsum().round(2)) ]
    else:
        xy = [_ for _ in zip(range(ewrc.stage_distances.size),range(1,ewrc.stage_distances.size+1))]


    _xy = [_x[1] for _x in xy[:len(dff_cumsum.T)]]  

    if stretch:      
        for _x in _xy[:-1]:
            plt.axvline(_x,linestyle='dotted')
        dff_cumsumT.index=_xy
        ax = dff_cumsumT.append(pd.Series(0, index=dff_cumsumT.columns,name=0)).sort_index().plot(ax=ax)

        _times = dff_cumsumT.append(pd.Series(0, index=dff_cumsumT.columns,name=0)).sort_index().to_dict()
        ax.set_xlabel("Accumulated Competitive Distance (km)")

        xlim = plt.xlim()
        ax.set_xlim(xlim[0], xlim[1]+1)

    else:
        ax = dff_cumsumT.plot(ax=ax)
        #ax.set_xlabel("Stage")
        ax.get_xaxis().set_visible(False)
        _times=dff_cumsumT.to_dict()

        for _x in _xy[:-1]:
            plt.axvline(_x,linestyle='dotted')

        xlim = plt.xlim()
        ax.set_xlim(xlim[0]-0.1, xlim[1]+0.1)

    for _car in _times:
        for _dist in _times[_car]:
            if _dist:
                ax.plot(_dist,_times[_car][_dist], marker='D',
                        markersize=3, mec='grey', c='grey') #


    #Add marker for stage winner
    # dff.min() gives the minimum (rebased) stage time (So need to cope w/ NA?)
    winner_stage = sorted(list(dff[dff==dff.min()].stack().index), key=lambda x: x[1])
    for (_x,_y) in winner_stage:
        if not stretch:
            ax.plot(_y, dff_cumsum.loc[_x,_y],
                    marker='D', markersize=2, c='red')
        else:
            ax.plot(xy[_y-1][1], dff_cumsumT.iloc[_y-1][winner_stage[_y-1][0]],
                    marker='D', markersize=2, c='red')



    plt.title(f'Off the ultimate pace chart (summed delta relative to {pilot})', pad=25)
    ax.set_ylabel(f"Off the summed stagetime pace relative to {pilot} (s)")

    #x-axis integers: https://stackoverflow.com/a/38096332/454773
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))


    #Add stage labels
    _ymin=0
    if stretch:
        for( _i, _xy) in  [(_i, _xy) for (_i, _xy) in enumerate(xy)][:len(winner_stage)]:
            plt.text((_xy[0]+_xy[1])/2, _ymin-5, _i+1, size=10,
                    bbox=dict(facecolor='red', alpha=0.5))
    else:
        ylim = plt.ylim()
        for( _i, _xy) in  [(_i, _xy) for (_i, _xy) in enumerate(xy)][:len(winner_stage)]:
            plt.text((_xy[0]+_xy[1])/2, ylim[1]+10, _i+1, size=10,
                    bbox=dict(facecolor='red', alpha=0.5))
        ax.xaxis.set_ticks_position('none')

    plt.axhline(0,linestyle='dotted')

    plt.box(on=None)
    #ax.yaxis.set_ticks_position('none') 

    plt.gca().invert_yaxis()
    
    if filename:
        plt.savefig(filename)

    return ax

# + tags=["active-ipynb"]
# off_the_pace_chart(ewrc, filename='testpng/offpace.png');

# + tags=["active-ipynb"]
#  ewrc.df_stages.head()

# + tags=["active-ipynb"]
# ewrc.df_allInOne
# -

# ## Pace Leveller Map
#
# For a particular selected (rebased) drive, a chart like the above with red and green bands that show pace that would level a driver compared to the selected driver on the next stage or N stages.
#
#
# Pace required to level up by end of Stage N from from stage M (s/km):

# + tags=["active-ipynb"]
# # Use: requiredStagePace()
# requiredStagePace(ewrc, 'SS9', rebase=None, target_stage=None).head(10)
# -



# ## Rally Strategy Simulator
#
# Slider widgets, one per stage, with assumed pace delta. Line chart shoiwn summed time delta over stages.

import ipywidgets as widgets

# + run_control={"marked": false} tags=["active-ipynb"]
# #Based on https://stackoverflow.com/q/48020345/454773
#
# _sliders = []
# _sliders_left = []
# _sliders_right = []
#
# sliders = {}
#
# for (_i, _stage) in enumerate(xy):
#     _sliders.append(widgets.FloatSlider(value=0,
#                                         min=-1.5, max=1.5,
#                                         description=f'SS{_i+1} ({ ewrc.stage_distances[_i+1]})'))
#
# colheight = len(xy)/2
# for (_i, _stage_slider) in enumerate(_sliders):
#     sliders[f'SS{_i+1}'] = _stage_slider
#     if _i < colheight:
#         _sliders_left.append(_stage_slider)
#     else:
#         _sliders_right.append(_stage_slider)
#
# left_box = widgets.VBox(_sliders_left)
# right_box = widgets.VBox(_sliders_right)
# ui = widgets.VBox([widgets.Label(value="Rally Strategist"),
#                    widgets.HBox([left_box, right_box])])
#
#
# def f(**kwargs):
#     _cumdelta=0
#     for x in kwargs:
#         _dist = ewrc.stage_distances[int(x.replace('SS',''))]
#         _delta = round(kwargs[x]*_dist,2)
#         _cumdelta = round(_cumdelta+_delta, 2)
#         print(x, _dist,
#               kwargs[x], _delta, _cumdelta )
#
# out = widgets.interactive_output(f, {f'SS{_i+1}':_s for (_i, _s) in enumerate(_sliders) })
#
#
# display(ui, out)
# -


