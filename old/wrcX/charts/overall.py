import wrcX.core
import wrcX.core.data

from statsmodels.graphics import utils

import seaborn as sns

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.dates as mdates

from pandas import melt,merge, DataFrame, notnull
from numpy import arange

from wrcX.core.filters import groupClassFilter
#-------
def split_plot3(grouped_x, keyOrder, ydim,labdim, xticklabels=None, ylabel=None,
            ax=None,label=False,keyHighlight=None,ylim=None):
    fig, ax = utils.create_mpl_ax(ax)
    start = 0
    ticks = []
    tmpxlabels = []
    for key in keyOrder:
        df=grouped_x.get_group(key)
        nobs = len(df)
        x_plot = arange(start, start + nobs)
        ticks.append(x_plot.mean())
        #Third parameter is color; 'k' is black
        ax.plot(x_plot, df[ydim], 'g', linestyle='--')

        #named colors: http://matplotlib.org/examples/color/named_colors.html
        highlightColors=['mistyrose','lightsalmon','salmon','tomato','orangered']
        baseColors=['silver','lightblue','paleturquoise','lightcyan','lightgreen']
        if key==keyHighlight:
            colors=highlightColors
        else:
            colors=baseColors
        for i in range(0,nobs):
            if ylim is not None and (df[ydim].iloc[i]<=ylim[0] or df[ydim].iloc[i]>=ylim[1]): continue
            if label:
                if nobs<=len(colors):
                    if keyHighlight=='leader':
                        if int(df[labdim].iloc[i])==1: color=highlightColors[i-nobs]
                        else: color=baseColors[i-nobs]
                    else:
                        color=colors[i-nobs]
                else: color='pink'
                ax.text(x_plot[i], df[ydim].iloc[i], int(df[labdim].iloc[i]),
                        bbox=dict(  boxstyle='round,pad=0.3',color=color))
            #elif df[ydim].iloc[i]>self.RC1SIZE:
            #    ax.text(x_plot[i]-1, df[ydim].iloc[i], int(df[labdim].iloc[i]),
            #            bbox=dict(  boxstyle='round,pad=0.3',color='pink')) #facecolor='none',edgecolor='black',
            else:
                ax.plot(x_plot[i], df[labdim].iloc[i], 'or')
        #ax.hlines(df.values.mean(), x_plot[0], x_plot[-1], colors='k')
        start += nobs
        tmpxlabels.append(key)

    if xticklabels is None:
        xticklabels=tmpxlabels
    elif isinstance(xticklabels, dict):
        xticklabels=[xticklabels[x] if x in xticklabels else x for x in tmpxlabels]
    ax.set_xticks(ticks)
    ax.set_xticklabels(xticklabels)
    if ylabel is not None: ax.set_ylabel(ylabel)
    ax.margins(.1, .05)
    return fig
#------

def rebaser(dt,carNo,sector=False):
    ''' Rebase the split times on a stage against the times for a particular driver '''

    if carNo=='' or carNo is None: 
        return dt
    else:
        carNo=str(int(carNo)) # go defensive on the carNo
        
    delta='delta' if not sector else 'sectordelta'
    delta_s='delta_s' if not sector else 'sectordelta_s'

    _dt=dt.copy()
    #If the car isn't there to rebase, don't rebase - return empty dataframe?
    if carNo not in _dt['carNo'].unique(): return DataFrame()#_dt
    # TO DO - also need to consider where car is there but not all split times are recorded
    # Try this for now - rebase to cols that do exist, otherwise drop cols
    rebaseableTimes=_dt[_dt['carNo']==carNo]['control'].unique().tolist()
    _dt=_dt[_dt['control'].isin(rebaseableTimes)]
    #HACK for now; TO DO - catch this properly
    try:
        rebasetimes={control:_dt[(_dt['carNo']==carNo) & (_dt['control']==control)][delta_s].iloc[0] for control in _dt['control'].unique()}
    except: return _dt
    _dt[delta_s]=_dt[delta_s]-_dt['control'].map(rebasetimes)
    rebasetimes={control:_dt[(_dt['carNo']==carNo) & (_dt['control']==control)][delta].iloc[0] for control in _dt['control'].unique()}
    _dt[delta]=_dt[delta]-_dt['control'].map(rebasetimes)
    return _dt


    
#-------

def _chart_stage_sector_delta_base_core(df_entry,df_splitTimes,
                                   ddf_sectors,gc='RC1',eligibility=None,rebase=None):

    gc1=groupClassFilter(ddf_sectors,gc,eligibility)

    driverOrder=df_splitTimes.sort_values('start')['carNo']
    tcols=[c for c in ddf_sectors.columns if c.startswith('d_')]

    #dxs=gc1.ix[:,['carNo','driverName']+tcols]
    dxs=gc1.loc[:][['carNo','driverName']+tcols]
    dxs=melt(dxs, id_vars=['carNo','driverName'], var_name='control', value_vars=tcols, value_name='sectordelta')
    dxs=dxs[(dxs['sectordelta'].notnull()) & (dxs['sectordelta']!='') ]
    if len(dxs)==0: return DataFrame(),[]
    dxs['sectordelta_s']=dxs['sectordelta'].apply(lambda x: x.total_seconds())
    dxs=dxs[dxs['sectordelta_s']<1000]
    #This is the overall rank - need to be consistent with other ranking?
    dxs['spos']=dxs.groupby('control')['sectordelta_s'].rank().astype(int)
    driverOrder2=[d for d in driverOrder if d in dxs['carNo'].unique()]
    return dxs,driverOrder2

def _chart_stage_sector_delta_base(df_entry,df_splitTimes,
                                   ddf_sectors,gc='RC1',eligibility=None,rebase=None):
    dxs,driverOrder2=_chart_stage_sector_delta_base_core(df_entry,df_splitTimes,
                                   ddf_sectors,gc,eligibility,rebase)
    if rebase is not None: dxs=rebaser(dxs,rebase,sector=True)
    return dxs,driverOrder2

def _chart_stage_delta_s_base(df_entry,df_splitTimes,gc='RC1',eligibility=None,rebase=None):

    gc1=groupClassFilter(df_splitTimes,gc,eligibility)
    cols=[c for c in df_splitTimes.columns if c.startswith('time_split_')]
    
    #HACK - TO DO - HANDLE PROPERLY TO ALLOW PARTIAL RESULTS
    #If there is a NaT in a cell in a time column, the whole column is dropped in the charting?
    #Hack is to remove rows that do not have a complete complement of times
    gc1=gc1.dropna(subset=[c for c in gc1.columns if c.startswith('time_')])
    
    for col in cols:
        s=col.split('_')[-1]
        gc1['rank_{}'.format(s)]=gc1.groupby('stage')['time_split_{}'.format(s)].rank()
    gc1['rank']=gc1.groupby('stage')['time_stageTime'].rank()

    driverOrder=gc1.sort_values('start')['driverName']

    _tcols=[t for t in gc1.columns if t.startswith('td_split_')]
    tcols=_tcols+['time_stageTime']

    dt=gc1.ix[:,['carNo','driverName']+tcols]
    dt['time_stageTime']=dt['time_stageTime']-dt.iloc[0]['time_stageTime']
    dt=melt(dt, id_vars=['carNo','driverName'], var_name='control', value_vars=tcols, value_name='delta')

    dt['delta_s']=dt['delta'].apply(lambda x: x.total_seconds() if notnull(x) else None )

    #This is the RC1 rank - should we use the overall rank?
    dt['spos']=dt.groupby('control')['delta_s'].rank()

    dt=dt.dropna(subset=['spos','delta_s'])
    
    #Need to handle outliers?
    #dt=dt[dt['delta_s']<1000]
    
    driverOrder2=[d for d in driverOrder if d in dt['driverName'].unique()]

    if rebase is not None: dt=rebaser(dt,rebase)
    return dt, driverOrder2
    
#------

def chart_stage_delta_s(df_entry,df_splitTimes,gc='RC1',
                        eligibility=None,keyHighlight=None,rebase=None,ylim=None):
    dt, driverOrder2 = _chart_stage_delta_s_base(df_entry,df_splitTimes,gc=gc,
                                                 eligibility=eligibility,rebase=rebase)
    #We can actually tune this for different chart types
    fig, ax = plt.subplots(figsize=(15,8))
    if dt.empty: return fig
    
    split_plot3(dt[['delta_s','spos']].groupby(dt['driverName']),driverOrder2,'delta_s','spos',
                ax=ax,label=True,keyHighlight=keyHighlight,ylim=ylim)
    ax.set_xticklabels(ax.xaxis.get_majorticklabels(), rotation=90)
    
    if ylim is not None: ax.set_ylim(ylim)

    return fig
    

def chart_stage_split_pos(df_entry,df_splitTimes,gc='RC1',eligibility=None,keyHighlight=None, ylim=None):
    dxs,driverOrder2=_chart_stage_delta_s_base(df_entry,df_splitTimes,
                                               gc=gc,eligibility=eligibility)
    fig, ax = plt.subplots(figsize=(15,8))
    if dxs.empty: return fig
    
    split_plot3(dxs[['driverName','spos']].groupby('driverName'),
                driverOrder2,'spos','spos',ax=ax,label=True,keyHighlight=keyHighlight)

    ax.set_xticklabels(ax.xaxis.get_majorticklabels(), rotation=90)
    
    if ylim is not None: ax.set_ylim(ylim)
        
    return fig

def chart_stage_sector_delta(df_entry=None,df_splitTimes=None,ddf_sectors=None,dxs=None,driverOrder2=None,gc='RC1',
                             eligibility=None,keyHighlight=None,rebase=None,xticklabels=None,ylim=None):
    if dxs is None:
        dxs,driverOrder2=_chart_stage_sector_delta_base(df_entry,df_splitTimes,
                                                         ddf_sectors,gc=gc,eligibility=eligibility,rebase=rebase)
    fig, ax = plt.subplots(figsize=(15,8))
    
    if driverOrder2==[] or dxs.empty: return fig
    
    if xticklabels is None: xticklabels= wrcX.core.data.driverDict

    split_plot3(dxs[['carNo','driverName','sectordelta_s','spos']].groupby('carNo'),
                     driverOrder2,'sectordelta_s','spos',
                     xticklabels=xticklabels,ax=ax,label=True,keyHighlight=keyHighlight,ylim=ylim)
    ax.set_xticklabels(ax.xaxis.get_majorticklabels(), rotation=90)
    
    if ylim is not None: ax.set_ylim(ylim)
        
    return fig

def chart_stage_sector_pos(df_entry=None,df_splitTimes=None,ddf_sectors=None,dxs=None,driverOrder2=None,gc='RC1',
                           eligibility=None,keyHighlight=None,xticklabels=None):
    if dxs is None:
        dxs,driverOrder2=_chart_stage_sector_delta_base(df_entry,df_splitTimes,
                                                         ddf_sectors,gc=gc,eligibility=eligibility)
    fig, ax = plt.subplots(figsize=(15,8))
    if dxs.empty: return fig
    
    if xticklabels is None: xticklabels= wrcX.core.data.driverDict
    split_plot3(dxs[['carNo','driverName','spos']].groupby('carNo'),
                     driverOrder2,'spos','spos',
                     xticklabels=xticklabels,ax=ax,label=True,keyHighlight=keyHighlight)

    ax.set_xticklabels(ax.xaxis.get_majorticklabels(), rotation=90)
    return fig

#--------


def basicStagePosChart(gc='RC1',eligibility=None,maxstages=None,stagemax=None,stagemin=None):
    df_overall = wrcX.core.data.df_overall[:]
    
    maxstages=df_overall['stage'].max() if maxstages is None else maxstages
    stagemin=df_overall['stage'].min() if stagemin is None else stagemin
    stagemax=maxstages if stagemax is None else stagemax
    gc1=df_overall[(df_overall['groupClass']==gc) & (df_overall['stage']<=stagemax)].reset_index(drop=True)

    gcSize=len(gc1[(gc1['groupClass']==gc) & (gc1['stage']==1)]) #should really do this from entry

    gc1['xrank']= (gc1['pos']>gcSize)
    gc1['xrank']=gc1.groupby('stage')['xrank'].cumsum()
    gc1['xrank']=gc1.apply(lambda row: row['pos'] if row['pos']<=gcSize  else row['xrank'] +gcSize, axis=1)

    fig, ax = plt.subplots(figsize=(15,8))
    ax.get_yaxis().set_ticklabels([])
    gc1.groupby('driverName').plot(x='stage',y='xrank',ax=ax,legend=None);

    for i,d in gc1[gc1['xrank']>gcSize].iterrows():
        ax.text(d.loc(i)['stage'], d.loc(i)['xrank'], int(d.loc(i)['pos']),
                bbox=dict(  boxstyle='round,pad=0.3',color='pink')) #facecolor='none',edgecolor='black',

    plt.xlim(stagemin-1, maxstages+0.9) #max stages

    for i,d in gc1[gc1['stage']==stagemin].iterrows():
        ax.text(stagemin-0.95, d.loc(i)['xrank'], d.loc(i)['driverName'])
    for i,d in gc1[gc1['stage']==maxstages].iterrows():
        ax.text(maxstages+0.1, d.loc(i)['xrank'], d.loc(i)['driverName'])

    #If we have a large offset for names ensure we don't display non-stage numbers
    fig.canvas.draw()
    labels = [item.get_text() for item in ax.get_xaxis().get_majorticklabels()]
    labels = [l if (l!='' and int(float(l)) >= stagemin and int(float(l)) <= maxstages) else '' for l in labels ]
    ax.set_xticklabels(labels)
    
    plt.gca().invert_yaxis()
    return plt

def timePlot(typ='td_diffFirst',max_mult=25,max_xs=900,gc="RC1",eligibility=None,
         maxstages=None,stagemax=None ):
    df_entry = wrcX.core.data.df_entry[:]
    df_overall = wrcX.core.data.df_overall[:]
    
    maxstages=df_overall['stage'].max() if maxstages is None else maxstages
    stagemax=maxstages if stagemax is None else stagemax
    gc1=df_overall[(df_overall['groupClass']==gc) & (df_overall['stage']<=stagemax)].reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(15,8))
    #ax.get_yaxis().set_ticklabels([])
    gc1['td_time_s']=gc1['td_time'].dt.total_seconds()
    gc1[typ+'_s']=gc1[typ].fillna(0).dt.total_seconds()

    max_s=gc1[gc1['stage']==max(gc1['stage'])][typ+'_s'].max()
    max_s= max_s if max_s<max_xs else max_xs
    if max_mult is not None: max_s= max_s if max_s<(max_mult*stagemax) else max_mult*stagemax
    ax.set_ylim([0,max_s])

    gc1.groupby('driverName').plot(x='stage',y=typ+'_s', ax=ax,legend=None);
    plt.xlim(1, maxstages) #max stages
    for i,d in gc1[(gc1['stage']==max(gc1['stage'])) & (gc1[typ+'_s'] < max_s)].iterrows():
        ax.text(max(gc1['stage'])+0.3, d.loc(i)[typ+'_s'], d.loc(i)['driverName'])
    return plt;


def sectorDeltaBarPlot(df_entry=None,df_splitTimes=None,ddf_sectors=None,dxs=None,driverOrder2=None,
                       flip=False,sortTotal=True,title='Delta by sector', gc='RC1',eligibility=None,rebase=None):
    if dxs is None:
        dxs,driverOrder2=_chart_stage_sector_delta_base(df_entry,df_splitTimes,
                                                         ddf_sectors,gc=gc,eligibility=eligibility,rebase=rebase)
    fig, ax = plt.subplots(figsize=(15,8))
    if dxs.empty: return fig
    
    dxsp=dxs.pivot('driverName','control','sectordelta_s')
    
    if sortTotal:
        tcols=[c for c in dxsp.columns if c.startswith('d_sector_')]
        dxsp['stagediff']=dxsp[tcols].sum(axis=1).round(1)
        dxsp=dxsp.sort_values('stagediff')[tcols]
        
    if rebase is not None: title='{} (relative to car {})'.format(title,rebase)

    
    dxsp.plot(kind='barh',title=title,ax=ax)
    
    
    #Also try to position the legend hopefully into whitespace
    xmin, xmax = ax.get_xlim()
    if flip:
        ax.invert_xaxis()
        if abs(xmin)<=abs(xmax):
            ax.legend(loc='upper left')
        else: ax.legend(loc='lower right')
    else:
        if abs(xmin)<=abs(xmax):
            ax.legend(loc='upper right')
        else: ax.legend(loc='lower left')
            
    ax.invert_yaxis()
        
    plt.ylabel('')
    plt.xlabel('Sector Delta (s)')
    return plt;

