import random
#import nbformat as nb
import nbformat.v4.nbbase as nb4

from pandas import DataFrame, concat, melt, merge, isnull

import io
import pytablewriter
writer = pytablewriter.MarkdownTableWriter()

import wrcX.core
import wrcX.core.data
from wrcX.core.enrichers import addGroupClassFromCarNo, addTeamFromCarNo

import matplotlib.pyplot as plt

def save_fig(fig,fn,path='./',suffix='png'):
    fname='{}/{}.{}'.format(path.rstrip('/'),fn,suffix.strip('.'))
    fig.savefig(fname)
    plt.close()
    return fname

def add_fig(ax,fn,path='./',suffix='png'):
    figp=save_fig(ax,fn,path,suffix)
    embed='![]({})'.format(figp)
    return embed

def df_md(df):
    writer.from_dataframe(df)
    writer.stream = io.StringIO()
    writer.write_table()
    return writer.stream.getvalue()


def addSlideComponent(notebook, content, styp=''):
    if styp in ['slide','fragment','subslide']: styp={"slideshow": {"slide_type":styp}}
    else: styp={}
    notebook.cells.append(nb4.new_markdown_cell(content, metadata=styp))

def listrand(l):
    return l[random.randint(0,len(l)-1)]

def allstagedetails(df):
    cols=['SS{}'.format(x) for x in df['stage'].unique()]
    details=DataFrame(columns=cols)

    for car in df['carNo'].unique():
        tmp=DataFrame(columns=cols)
        for stage in df['stage'].unique():
            tmp['SS{}'.format(stage)]=df[(df['carNo']==car) & (df['stage']==stage)].set_index('carNo')['pos']
        details=concat([details,tmp])
        details.index.name='carNo'
    return details

def driverFromCar(carNo):
    return wrcX.core.data.df_entry[wrcX.core.data.df_entry['carNo']==str(int(carNo))]['driverName'].iloc[0]

def getTeamFromCarNo(carNo):
    return wrcX.core.data.df_entry[wrcX.core.data.df_entry['carNo']==str(int(carNo))]['team'].iloc[0]

def stageStartOrder(stagenum,groupClass=''):
    df=wrcX.core.data.df_splitTimes_all[stagenum-int(wrcX.core.first_stage)]
    if groupClass!='':
        df=addGroupClassFromCarNo(df)
        df=df[df['groupClass']==groupClass]
    return df

#https://stackoverflow.com/a/8907269/454773
def strfdelta(tdelta, fmt="{hours}{minutes}:{seconds}"):
    if isnull(tdelta): return ''
    d = {}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["hours"]='' if d["hours"]==0 else '{}:'.format(d['hours'])
    d["minutes"], d["seconds"] = divmod(rem, 60)
    d["minutes"]='{0:02d}'.format(d["minutes"])
    d['seconds']='{0:04.1f}'.format(d['seconds'] +tdelta.microseconds/1000000)
    return fmt.format(**d)


def _melter(logicTested,newcolname):
    return melt(logicTested.reset_index(),id_vars='carNo',var_name='stage',
                       value_vars=logicTested.columns.tolist(),value_name=newcolname)

def df_md(df):
    writer.from_dataframe(df)
    writer.stream = io.StringIO()
    writer.write_table()
    return writer.stream.getvalue()

def report_StageWinner(stagenum,reps):
    txt=''
    for rep in reps:
        tmp=rep[0] if len(rep) else ''
        txt='{}{}'.format(txt,tmp)
        if txt!='': break
    return txt