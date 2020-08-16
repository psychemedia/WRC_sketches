#import pandas as pd
#import pandas.io.html as ih

#import numpy as np
#import requests
#import requests_cache

#import re
#import random

#from statsmodels.graphics import utils

#import seaborn as sns
#import matplotlib.pyplot as plt
#import matplotlib.patches as patches
#import matplotlib.dates as mdates

#import inflect

import wrcX.core
import wrcX.core.data
from wrcX.core.enrichers import stageDesc, winningstreak


#Load base rally data and generic reports
from wrcX.io.loaders_wrc import getEntryList, getItinerary, getRetirements,getPenalties
#Load overall results
from wrcX.io.loaders_wrc import getOverall, getStageResults
#Load specific stage results
from wrcX.io.loaders_wrc import getSplitTimes,getSplitSectorTimes

from wrcX.reports.base import entryReport, retirementReport, penaltyReport, stageIntroReport
from wrcX.reports.utils import allstagedetails

from wrcX.charts.overall import basicStagePosChart, timePlot
from wrcX.charts.overall import chart_stage_sector_pos, chart_stage_sector_delta
from wrcX.charts.overall import chart_stage_split_pos, chart_stage_delta_s

from wrcX.reports import overall

from wrcX.core.logictables import stageflags

from pandas import merge

def liveupdate(base,minstage=wrcX.core.first_stage,forceReload = True):
    year=base['year']
    rallyid=base['rallyid']
    
    if 'cachedb' in base:
        wrcX.core.cachedb=base['cachedb']

    wrcX.core.cacheReset=forceReload
    wrcX.core.data.df_entry = getEntryList(year,rallyid)
    wrcX.core.data.driverDict=wrcX.core.data.df_entry[['driverName','carNo']].set_index('carNo').to_dict(orient='dict')['driverName']
    
    wrcX.core.data.df_itinerary=getItinerary(year,rallyid)
    wrcX.core.data.df_stagedesc=stageDesc()
    wrcX.core.first_stage=wrcX.core.data.df_stagedesc['stagenum'].min()
    wrcX.core.maxstages=wrcX.core.data.df_stagedesc['stagenum'].max()
    
    wrcX.core.data.df_retirements= getRetirements(year,rallyid)
    wrcX.core.data.df_penalties= getPenalties(year,rallyid)

    #Use the itinerary to load in results for stages that have completed
    #Need to set something up for stages running... eg to set a time to keep reloading?
    #stages = list(range(wrcX.core.first_stage,wrcX.core.maxstages+1))
    wrcX.core.stagesuptoandincluding = wrcX.core.data.df_stagedesc.completedstages.max()
    stages = list(range(minstage,wrcX.core.stagesuptoandincluding+1))

    wrcX.core.data.df_overall = getOverall(year,rallyid,stages)
    
    wrcX.core.data.df_stage, wrcX.core.data.df_overallpart = getStageResults(year,rallyid,stages)
    wrcX.core.data.df_stagepositions=allstagedetails(wrcX.core.data.df_stage)
    
    #if we have a fn to get a single stage from X_all, not from loader, need to rebase index into X_all as stage-first_stage
    wrcX.core.data.df_splitTimes_all =getSplitTimes( year,rallyid,stages)
    wrcX.core.data.df_splitSectorTimes_all = getSplitSectorTimes( year,rallyid,stages)
    
    wrcX.core.data.df_overallpositions=allstagedetails(wrcX.core.data.df_overallpart)
    
    g=stageflags(wrcX.core.data.df_overallpositions,wrcX.core.data.df_stagepositions, wrcX.core.data.df_itinerary)
    wrcX.core.data.g2=merge(g.reset_index(),wrcX.core.data.df_entry,on='carNo')
    wrcX.core.data.g2['stageNo']=wrcX.core.data.g2['stage'].str.replace('SS','').astype(int)

    wrcX.core.data.t_winstreak=wrcX.core.data.df_stage.groupby('driverName').apply(winningstreak)
    
def init(base,forceReload = False):
    year=base['year']
    rallyid=base['rallyid']
    
    if 'cachedb' in base:
        wrcX.core.cachedb=base['cachedb']
    
    liveupdate(base,forceReload=forceReload)
        

