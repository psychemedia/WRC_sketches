import wrcX.core
import wrcX.core.data as d
import wrcX.charts.overall as c

from pandas import merge
from wrcX.core.enrichers import addGroupClassFromCarNo
from wrcX.core.filters import groupClassFilter

def table_stageResult(stagenum,groupClass='',lower=0,upper=10):
    pretxt = d.df_stage[d.df_stage['stage']==stagenum]
    pretxt=groupClassFilter(pretxt,groupClass)
    
    if upper is None: upper=len(pretxt)
            
    tcols=['pos','carNo','driverName','time','diffPrev','diffFirst']
    if groupClass=='':
        tcols.append('groupClass')
        pretxt=addGroupClassFromCarNo(pretxt)
                                   
    pretxt=pretxt[tcols][lower:upper]
    pretxt['pos']=pretxt['pos'].fillna(0).astype(int).replace(0,'')
    txt=pretxt.set_index('pos').to_html()
    return txt


def table_overallEndOfStage(stagenum,groupClass='',lower=0,upper=10):
    df_overall=d.df_overall[d.df_overall['stage']==stagenum]
    pretxt=groupClassFilter(df_overall, groupClass)
    
    if upper is None: upper=len(pretxt)
    
    tcols=['pos','carNo','driverName','time','diffPrev','diffFirst']
    if groupClass=='':
        tcols.append('groupClass')
        pretxt=addGroupClassFromCarNo(pretxt)

    pretxt=pretxt[tcols][lower:upper]
    pretxt['pos']=pretxt['pos'].fillna(0).astype(int).replace(0,'')
    txt=pretxt.set_index('pos').to_html()
    return txt

def table_stageSectorDelta(stagenum,groupClass='',ddf_sectors=None):
    if ddf_sectors is None:
        ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]

    xcols=[c for c in ddf_sectors.columns if c.startswith('sector')]
    if len(xcols)==1: xcols=[]
    tcols=['start','carNo','driverName']+xcols+['time_stageTime']
    pretxt=ddf_sectors.sort_values('time_stageTime')[tcols]
    pretxt=groupClassFilter(pretxt,groupClass)
    
    unclassified=pretxt[pretxt['start'].isnull()]
    
    pretxt=pretxt[pretxt['start'].notnull()]
    pretxt['start']=pretxt['start'].astype(int)
    txt=pretxt.to_html()
    
    if len(unclassified): txt=txt+'\n#### Non-starters, non-finishers:'+unclassified.to_html(index=False)
    return txt

def table_stageSectorDeltaRebase(stagenum,rebase,groupClass=None,eligibility=None,df_splitTimes=None,ddf_sectors=None):

    if df_splitTimes is None:
        df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]       
    if ddf_sectors is None:
        ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]

    rebasedSectorTimes,y= c._chart_stage_sector_delta_base(wrcX.core.data.df_entry,df_splitTimes,
                                                         ddf_sectors,gc=groupClass,eligibility=eligibility,rebase=rebase)
    if rebasedSectorTimes.empty:
        return '\nINCOMPLETE STAGE - NO REBASING\n'+ groupClassFilter(ddf_sectors,groupClass).to_html(index=False)
    
    rebasedSectorTimes=groupClassFilter(rebasedSectorTimes,groupClass)
    
    rebasedSectorTimes=rebasedSectorTimes.pivot_table(index=['carNo','driverName'],
                                        columns='control',values='sectordelta_s').reset_index()#.sort_values('time_stageTime')
    tcols=[c for c in rebasedSectorTimes.columns if c.startswith('d_sector_')]
    rebasedSectorTimes['stagediff']=rebasedSectorTimes[tcols].sum(axis=1).round(1)
    
    txt=rebasedSectorTimes.merge(df_splitTimes[['carNo','time_stageTime']],
                                 on='carNo').sort_values('time_stageTime').to_html(index=False)
    return txt
    
    
def table_stageSplitTimeDelta(stagenum,groupClass='',df_splitTimes=None, df_stage=None):
    if df_splitTimes is None:
        df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]
    
    if df_stage is None:
        df_stage=d.df_stage[d.df_stage['stage']==stagenum]
    
    tcols=['start','carNo','driverName']+[c for c in df_splitTimes.columns if c.startswith('split_')]+['stageTime']
    pretxt=df_splitTimes.sort_values('time_stageTime')[tcols]
    pretxt=groupClassFilter(pretxt,groupClass)
    
    pretxt=pretxt.merge( df_stage[df_stage['stage']==stagenum][['carNo','diffFirst','diffPrev']],on='carNo')
    
    unclassified=pretxt[pretxt['start'].isnull()]
    
    pretxt=pretxt[pretxt['start'].notnull()]
    pretxt['start']=pretxt['start'].astype(int)
    cols=['carNo','driverName']+ [c for c in pretxt.columns if c.startswith('split_')]+['stageTime','diffFirst','diffPrev','groupClass']
    txt=pretxt[cols].to_html()
    
    if len(unclassified): txt=txt+'\n#### Non-starters, non-finishers:'+unclassified.to_html(index=False)
    return txt

def table_stageSplitTimeDeltaRebase(stagenum,rebase,groupClass='',df_splitTimes=None):
    if df_splitTimes is None:
        df_splitTimes=d.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]
    rebasedSplitTimes,y=c._chart_stage_delta_s_base(wrcX.core.data.df_entry, df_splitTimes, rebase=rebase)
    if rebasedSplitTimes.empty:
        return '\nINCOMPLETE STAGE - NO REBASING\n'+groupClassFilter(df_splitTimes,groupClass).to_html(index=False)
    
    rebasedSplitTimes=groupClassFilter(rebasedSplitTimes,groupClass)
        
    txt = rebasedSplitTimes.pivot_table(index=['carNo','driverName'],
                                        columns='control',
                                        values='delta_s').sort_values('time_stageTime').reset_index().to_html(index=False)
    return txt

