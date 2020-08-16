import nbformat as nb
import nbformat.v4.nbbase as nb4

import wrcX.core
import wrcX.core.data as d
from wrcX.reports.utils import driverFromCar, addSlideComponent, df_md, stageStartOrder
from wrcX.reports.slides import *

import wrcX.reports.overall as r
import wrcX.reports.base as b
import wrcX.reports.tables as t

import os


def save_ax(ax,fn,path='images/',suffix='png'):
    fname='{}/{}.{}'.format(path.rstrip('/'),fn,suffix.strip('.'))
    fig=ax.get_figure()
    fig.savefig(fname)
    plt.close()
    return fname




def generator(rallydet, groupClass='RC1',imgpath='.'):
    #imgpath='images/{}_{}'.format(rallydet[0],rallydet[1])
    
    if not os.path.exists(imgpath):
        os.makedirs(imgpath)
    
    notebook=nb4.new_notebook()
    
    addSlideComponent(notebook,'# WRC Rally - {}, {}'.format(rallydet[2],rallydet[0]),'slide')
    
    for key,day in d.df_stagedesc[d.df_stagedesc['stage'].str.startswith('SS')].groupby('day'):
        for key2,section in day.groupby('section'):
            txt='# Day {} ({})\n'.format(key,'/'.join(day['date'].dt.strftime('%d/%m/%Y').unique())) + \
                '## Section {} ({})'.format(key2,'/'.join(section['partofday'].unique()))
            addSlideComponent(notebook,txt,'slide')

            #Within each section are stage rows
            for key,row in section.iterrows():
                stagenum=row['stagenum']
                stage=row['stage']
                
                if d.df_stagedesc.loc[key]['stagenum']> wrcX.core.stagesuptoandincluding : break
                currstages=list(range(wrcX.core.first_stage,stagenum+1))
                ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]
                df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]
                
                #Stage intro
                slide_stageIntro(notebook,stagenum,'slide')
                
                #Add Start Order
                slide_stageStartOrder(notebook,stageStartOrder(stagenum,groupClass))
                
                #Add stage report
                slide_stageReport(notebook,stagenum)

                #Add stage penalties report
                slide_stagePenaltiesReport(notebook,stagenum,groupClass)
                
                #Add overall retirements report
                slide_overallRetirementReport(notebook,stagenum,groupClass)
                
                #Add stage results table
                slide_stageResultsTable(notebook,stagenum)

                #Add overall rally positions at end of stage
                slide_overallPosAtEndOfStage(notebook,stagenum)
                
                #Chart stage split time rank
                slide_stageSplitTimeRank(notebook,stagenum,stage,
                                         groupClass=groupClass, df_splitTimes=df_splitTimes,
                                         styp='subslide',imgpath=imgpath)
                
                #Chart stage split time delta
                slide_stageSplitTimeDelta(notebook,stagenum,stage,
                                          groupClass=groupClass, df_splitTimes=df_splitTimes,
                                          styp='subslide',imgpath=imgpath)
                
                #Ultimate lap
                slide_ultimateStageReport(notebook,stagenum,ddf_sectors)
                                          
                #Poor start / poor end report
                slide_startEndSymbolicDynamicsReport(notebook,stagenum,ddf_sectors)
                
                #Chart stage sector delta
                slide_stageSectorDelta(notebook,stagenum,stage,
                              groupClass=groupClass,
                              df_splitTimes=df_splitTimes, ddf_sectors=ddf_sectors,
                              styp='subslide',imgpath=imgpath)
                
                #Stage split delta time
                
                
                #Overall rally positions at end of stage
                
                
                #After a first proper stage has been completed...
                if row['completedstages']>1:
                    pass
                
                    #Overall rank evolution
                    
                    
                    #Overall position at end of each stage
                    
                    
                    #Time to leader overall (s)
                    
                    
                    #Stage positions
                    
                    
                #After any stages have been completed
                if row['completedstages']>0:
                    pass
                
                    #Overall results
                    
                
        
    nbf='wrcTest-slides-{}.ipynb'.format(rallydet[2])
    nb.write(notebook,nbf)
    
    
def driverGenerator(rallydet,carNum,groupClass='RC1',imgpath=''):
    
    driver=driverFromCar(carNum)
    if not os.path.exists(imgpath):
        os.makedirs(imgpath)
    
    notebook=nb4.new_notebook()
    
    for key,day in d.df_stagedesc[d.df_stagedesc['stage'].str.startswith('SS')].groupby('day'):
        for key2,section in day.groupby('section'):
            txt='# Day {} ({})\n'.format(key,'/'.join(day['date'].dt.strftime('%d/%m/%Y').unique())) + \
                '## Section {} ({})'.format(key2,'/'.join(section['partofday'].unique()))
            addSlideComponent(notebook,txt,'slide')

            #Within each section are stage rows
            for key3,row in section.iterrows():
                stagenum=row['stagenum']
                stage=row['stage']
                
                print(d.df_stagedesc.loc[key3]['stagenum'])
                if d.df_stagedesc.loc[key3]['status']=='CANCELLED' : continue
                if d.df_stagedesc.loc[key3]['stagenum']> wrcX.core.stagesuptoandincluding : break
                
                ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]
                df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]

                # Stage intro
                slide_stageIntro(notebook,stagenum,'subslide')
                
                #Driver stage sectors summary
                ##reports.driver - report_driverStageSectors
                
                
                #Team table
                
                
                #Winner and ultimate table
                
                # Stage split time delta
                slide_stageSplitTimeDelta(notebook,stagenum,stage, rebase=carNum,
                                            keyHighlight=driverFromCar(carNum),groupClass=groupClass,
                                            display='title,chart,chelp,rebasedtable',
                                           styp='',imgpath=imgpath)
 
                # Stage sector delta time
                slide_stageSectorDelta(notebook,stagenum,stage,
                               rebase=carNum,groupClass=groupClass,
                              df_splitTimes=df_splitTimes, ddf_sectors=ddf_sectors,
                              #keyHighlight=str(carNum),
                              display='title,chart,chelp,rebasedtable,rebasedbar,table',styp='',imgpath=imgpath)  
                
   
    nbf='wrcTest-slides-car-{}-{}.ipynb'.format(carNum,rallydet[2])
    nb.write(notebook,nbf) 
