from wrcX.reports.utils import addSlideComponent, df_md, report_StageWinner, add_fig

#There is a trade off between loading data here and reusing it, and allowing charts to generate their own data from db data
import wrcX.core
import wrcX.core.data as d

import wrcX.reports.base as b
import wrcX.reports.tables as t
import wrcX.reports.symbolDynamics as sd
import wrcX.reports.overall as r
import wrcX.charts.overall as c
import wrcX.reports.utils as u


def slide_stageIntro(notebook,stagenum,styp=''):
    addSlideComponent(notebook,'{}\n\n*{}*\n'.format(b._stageIntroReport(stagenum),b._stagelen(stagenum)),styp)

def slide_stageStartOrder(notebook,df,groupClass='',styp=''):
    addSlideComponent(notebook,'## Start Order:\n' + df_md(df[['start', 'carNo', 'driverName',
                                                               'team', 'eligibility', 'groupClass']]),styp)
    
def slide_stageReport(notebook,stagenum,groupClass='',styp='slide'):
    txt=report_StageWinner(stagenum,[r.report_qa00(stagenum),r.report_qa01(stagenum),
                                     r.report_qa03(stagenum),r.report_qa04(stagenum),
                                     r.report_qa05(stagenum),r.report_qa08(stagenum),
                                     r.report_qa09(stagenum),r.report_qa10(stagenum)])
                
    addSlideComponent(notebook,'## Stage Report {}- Stage {}:\n\n{}'.format('({}) '.format(groupClass) if groupClass!='' else '',
                                                                            stagenum, txt),styp)
    
def slide_stageResultsTable(notebook,stagenum,groupClass='',styp='',lower=0,upper=10):
    html=t.table_stageResult(stagenum,groupClass=groupClass,lower=lower,upper=upper)
    addSlideComponent(notebook,'\n\n### Stage Results Table - Stage {}\n{}'.format(stagenum,html),styp)
    
def slide_overallPosAtEndOfStage(notebook,stagenum,groupClass='',styp='',lower=0,upper=10):
    addSlideComponent(notebook,'### Overall Rally Positions At End Of Stage - Stage {}'.format(stagenum),styp)
    html=t.table_overallEndOfStage(stagenum,groupClass=groupClass,lower=lower,upper=upper)
    addSlideComponent(notebook,'{}'.format(html))

    
def slide_stageSplitTimeRank(notebook,stagenum,stage,
                             groupClass='RC1',
                             df_splitTimes=None,
                             display='title,chart,chelp', styp='',imgpath='.'):
    def _title_display():
        txt='## {} - stage split time, rank position'.format(stage)
        addSlideComponent(notebook,txt)

    def _chart_display():
        fig=c.chart_stage_split_pos(d.df_entry,df_splitTimes, gc=groupClass )
        txt=add_fig(fig,'{}/chart_stage_split_pos_{}'.format(imgpath.rstrip('/'),stage))
        addSlideComponent(notebook,txt)
        
    def _chelp_display():
        txt='*This chart allows you to compare the __on stage__ performance of each driver. It displays the rank position of each driver at each split along the stage.*'
        addSlideComponent(notebook,txt)

    addSlideComponent(notebook,'',styp)
    
    if df_splitTimes is None:
        df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]
 
    for typ in [t.strip().lower() for t in display.split(',')]:
        if typ=='title': _title_display()
        elif typ=='chart': _chart_display()
        elif typ=='chelp': _chelp_display()


def slide_stageSectorDelta(notebook,stagenum,stage,
                              groupClass='RC1', eligibility=None,rebase=None,ylim=None,keyHighlight='leader',
                              df_splitTimes=None, ddf_sectors=None,xticklabels=None,
                              display='title,chart,chelp,table,bar',styp='',imgpath='.'):

    def _title_display(): 
        txt='## {} - stage sector delta time (seconds) compared to the stage sector time{}.'.format(stage,base)
        addSlideComponent(notebook,txt)
    
    def _chart_display():
        fig=c.chart_stage_sector_delta(dxs=dxs,driverOrder2=driverOrder2, gc= groupClass,
                                      rebase=rebase,ylim=ylim,keyHighlight=keyHighlight)
        txt=add_fig(fig,'{}/chart_stage_sector_delta_{}_{}'.format(imgpath.rstrip('/'),rebase,stage))
        addSlideComponent(notebook,txt)
        
    def _chelp_display():
        txt='*This chart allows you to compare the __on stage__ performance of each driver.'
        txt2= '' if rebase is None else base
        txt='{} It displays the "sector time" of each driver (the time between each split point along the stage){}.*'.format(txt,txt2)
        
        addSlideComponent(notebook,txt)

    def _table_display():
        txt=t.table_stageSectorDelta(stagenum,groupClass,ddf_sectors=ddf_sectors)
        addSlideComponent(notebook,txt)
        
    def _table_rebase_display():
        txt=t.table_stageSectorDeltaRebase(stagenum,rebase,groupClass=groupClass,ddf_sectors=ddf_sectors)
        addSlideComponent(notebook,txt) 
        
    def _chart_bar_display():
        fig=c.sectorDeltaBarPlot(d.df_entry,df_splitTimes,ddf_sectors)
        txt=add_fig(fig,'chart_stage_sector_delta_bar_{}_{}'.format(rebase,stage))
        addSlideComponent(notebook,txt)
        
    def _chart_rebase_bar_display():
        fig=c.sectorDeltaBarPlot(dxs=dxs,driverOrder2=driverOrder2,flip=True,sortTotal=True,rebase=rebase,gc=groupClass)
        txt=add_fig(fig,'{}/chart_stage_sector_delta_bar_{}_{}'.format(imgpath.rstrip('/'),rebase,stage))
        addSlideComponent(notebook,txt)

    
    addSlideComponent(notebook,'',styp)
    
    if df_splitTimes is None:
        df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]       
    if ddf_sectors is None:
        ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]
        
    if rebase is None:
        base=' of the first driver on stage'
    else:
        base=' rebased relative to {}'.format(u.driverFromCar(rebase))
    
    items=display.split(',')
    if 'chart' in items or 'rebasedbar' in items:
        dxs,driverOrder2=c._chart_stage_sector_delta_base(d.df_entry,df_splitTimes,
                                                         ddf_sectors,rebase=rebase,gc=groupClass,eligibility=eligibility)

    # TO DO - when rebasing, need to handle situations where target is NaT
    
    for typ in [t.strip().lower() for t in items]:
        if typ=='title': _title_display()
        elif typ=='chart': _chart_display()
        elif typ=='chelp': _chelp_display()
        elif typ=='table': _table_display()
        elif typ=='bar': _chart_bar_display()
        elif typ=='rebasedtable': _table_rebase_display()
        elif typ=='rebasedbar': _chart_rebase_bar_display()

            
def slide_stageSplitTimeDelta(notebook,stagenum,stage,
                              groupClass='RC1', rebase=None,ylim=None,keyHighlight='leader',
                              df_splitTimes=None,
                              display='title,chart,chelp,table',styp='',imgpath='.'):
    def _title_display():         
        txt='## {} - stage split delta time (seconds) compared to {}'.format(stage,base)
        addSlideComponent(notebook,txt)
    
    def _chart_display():
        fig=c.chart_stage_delta_s(d.df_entry,df_splitTimes,keyHighlight=keyHighlight,
                                  gc=groupClass,rebase=rebase,ylim=ylim)
        txt=add_fig(fig,'{}/chart_split_delta_{}_{}'.format(imgpath.rstrip('/'),rebase,stage))
        addSlideComponent(notebook,txt)
    
    def _chelp_display():
        txt='*This chart allows you to compare the __on stage__ performance of each driver relative to the split times {}.*'.format(base)
        if keyHighlight=='leader':
            txt=txt+'\n*The leader within the stage at each split point is highlighted.*'
        addSlideComponent(notebook,txt)

    def _table_display():
        txt=t.table_stageSplitTimeDelta(stagenum,groupClass)
        addSlideComponent(notebook,txt)
        
    def _table_rebase_display():
        txt=t.table_stageSplitTimeDeltaRebase(stagenum,rebase,groupClass=groupClass,df_splitTimes=df_splitTimes)
        addSlideComponent(notebook,txt)

    addSlideComponent(notebook,'',styp)
    
    if df_splitTimes is None:
        df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]

    if rebase is None:
        base='of the first driver on stage'
    else: base='rebased relative to {}'.format(u.driverFromCar(rebase))
   
    # TO DO - when rebasing, need to handle situations where target is NaT
        
    for typ in [t.strip().lower() for t in display.split(',')]:
        if typ=='title': _title_display()
        elif typ=='chart': _chart_display()
        elif typ=='chelp': _chelp_display()
        elif typ=='table': _table_display()
        elif typ=='rebasedtable': _table_rebase_display()

def slide_startEndSymbolicDynamicsReport(notebook,stagenum,ddf_sectors=None,groupClass='RC1',styp=''):
    if ddf_sectors is None:
        ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]

    patterns=sd.symReport1(ddf_sectors)
    txt= '{}'.format(' '.join(sd.report_poorStart(patterns)))
    txt= txt+'{}'.format(' '.join(sd.report_poorEnd(patterns)))
    addSlideComponent(notebook,txt,styp)

def slide_ultimateStageReport(notebook,stagenum,ddf_sectors=None,styp=''):
    if ddf_sectors is None:
        ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]
    txt=r.report_ultimateStage(stagenum=stagenum,ddf_sectors=ddf_sectors)
    addSlideComponent(notebook,txt,styp)
    
def slide_overallRetirementReport(notebook,stagenum,groupClass='RC1',styp=''):
    _txt=b.retirementReport(groupClass,stagenum)
    txt='### Overall Retirement Report ({} Competitors)\n'.format(groupClass)
    _txt=_txt if _txt !='' else '*None reported.*\n'
    txt=txt+_txt
    addSlideComponent(notebook,txt,styp)
    
def slide_stagePenaltiesReport(notebook,stagenum,groupClass='RC1',styp=''):
    _txt=b.penaltyReport(groupClass,stagenum)
    txt='### Stage Penalty Report ({} Competitors)\n'.format(groupClass)
    _txt=_txt if _txt !='' else '*None reported.*\n'
    txt=txt+_txt
    addSlideComponent(notebook,txt,styp)