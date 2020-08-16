from wrcX.reports.utils import listrand

from pandas import merge, read_sql, notnull

import random
import re

from pandasql import sqldf
#pysqldf = lambda q: sqldf(q, locals())

import wrcX.core.data

import inflect
p = inflect.engine()

import warnings
warnings.filterwarnings('ignore',message=r".*the.*timedelta.*")

def getStageWinCount(stage,pre=', ',post='',numtoword=True,pronoun='his'):
    q="SELECT COUNT(*) as cnt FROM g2 WHERE stageNo<={stage} and g2.won=1 \
    and g2.carNo in (SELECT carNo from g2 where stageNo={stage} and won=1)".format(stage=stage)
    dds= sqldf(q,{'g2':wrcX.core.data.g2})
    if post=='' and random.random()<0.1: post=' of the rally'
    cnt=dds.iloc[0]['cnt']
    if cnt>0:
        t = p.number_to_words(p.ordinal(cnt)) if numtoword else p.ordinal(cnt)
        txt='{pre}{pronoun} {cnt} stage win{post}'.format(cnt=t,pre=pre,post=post,pronoun=pronoun)
    else: txt=''
    return  txt

def getConsecutiveStageWinCount(stage,pre=', ',post='',numtoword=True,pronoun='his'):
    q="SELECT winstreak from t_winstreak where stage={stage} and t_winstreak.carNo in (SELECT carNo from g2 where stageNo={stage} and won=1)".format(stage=stage)
    dds=sqldf(q,{'g2':wrcX.core.data.g2,'t_winstreak':wrcX.core.data.t_winstreak})
    cnt=dds.iloc[0]['winstreak']
    txt ='their {} consecutive stage win,'.format(p.number_to_words(p.ordinal(cnt))) if cnt>1 else ''
    return txt
    
def _fullAttrib(driverName):
    q="SELECT driverName, coDriverName, team, car, driverName_nationality, coDriverName_nationality \
        FROM df_entry WHERE driverName='{}'".format(driverName)
    return sqldf(q,{'df_entry':wrcX.core.data.df_entry}).to_dict(orient='records')

#Build in nationality map - but FCO uses alpha2 country code so need an alpha2-alpha3 lookup?
#https://country.register.gov.uk/
#eg http://www.nationsonline.org/oneworld/country_code_list.htm ?Official opendata one anywhere?

def fullAttrib(driverName):
    d=_fullAttrib(driverName)[0]
    d['cd']= 'co-driver ' if not random.randint(0,4) else ''
    t=listrand([' in the {car}', ' for {team}', ' for {team} in the {car}', ' ({team})'])
    txt='{driverName} ({driverName_nationality}) and {cd}{coDriverName} ({coDriverName_nationality})'
    txt = txt+t if random.randint(0,4) else "{team}'s "+txt
    txt=txt.format(**d)
    return txt

def _carRetired(carNo,stage):
    if wrcX.core.data.empty: return False
    q="SELECT * \
        FROM df_retirements WHERE carNo={} AND stagenumber={}".format(carNo,stage)
    return sqldf(q,{'df_retirements':wrcX.core.data.df_retirements}).to_dict(orient='records')
#if _carRetired(carNo,stage):print('retired')


###----- REPORTS -------

def report_ultimateStage(stagenum=None,ddf_sectors=None):
    if ddf_sectors is None:
        ddf_sectors=wrcX.core.data.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]
        
    cc=[c for c in ddf_sectors.columns if c.startswith('overall_sector')]
    ultimate=ddf_sectors[:1].set_index('driverName')[cc].apply(max,axis=1)
    if len(cc)>1 and len(ultimate[ultimate==1]):
        txt='{} drove an __ultimate stage__, recording the fastest time *between* each timing point on the stage.'.format(ultimate.index[0])
    else: txt=''
    return txt


def report_qa00(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)
    
    q="SELECT g2a.stageNo as stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage , \
        da.time AS time, db.diffFirst AS  diff \
        FROM g2 AS g2a JOIN g2 AS g2b JOIN df_stage AS db JOIN df_stage AS da  \
        WHERE g2a.won=1 AND g2b.second=1 \
        AND g2a.stage=g2b.stage {stager} \
        AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage  \
        AND g2b.stage=(SELECT basestage FROM df_itinerary WHERE status='COMPLETED' ORDER BY basestage ASC LIMIT 1) \
        ".format(stager=stager)

    def _reporter(row):
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        txt='In stage {stage}, {lead} took the stage, and the lead of the rally, with a time of {time}, {diff} ahead of second on stage {second}.'.format(**row)
        return txt

    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage, 'df_itinerary':wrcX.core.data.df_itinerary })
    return df.apply(_reporter,axis=1)


def report_qa01(stage=None, groupClass=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)
    
    q="SELECT g2a.stageNo as stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage , \
        da.time AS time, db.diffFirst AS diff \
        FROM g2 AS g2a JOIN g2 AS g2b join df_stage AS db JOIN df_stage AS da \
        WHERE g2a.won=1 AND g2a.retainedlead=1 AND g2b.second=1 \
        AND g2a.stage=g2b.stage {stager} \
        AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
        ".format(stager=stager)
    
    def _reporter(row):
        row['nthstagewin']=getStageWinCount(row['stageNo'],pre='',post='',pronoun='their')
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        row['winstreak']=getConsecutiveStageWinCount(row['stageNo'],pronoun='their')
        txt='In stage {stage}, {lead} continued to lead the rally with {nthstagewin},{winstreak} in a time of {time}.\n\n{second} were placed second on the stage, {diff} behind.'.format(**row)
        return txt

    #If groupClass is set, we should invoke a different g2? Also need to think how to handle the time diffs?
    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)

def report_qa03(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)
    
    q="SELECT g2a.stageNo AS stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage , \
        da.time AS time, db.diffFirst AS  diff \
        FROM g2 AS g2a JOIN g2 AS g2b JOIN df_stage AS db JOIN df_stage as da \
        WHERE g2a.won=1 AND g2a.gainedlead=1 AND g2b.second=1 AND g2b.lostlead=1 \
        AND g2a.stage=g2b.stage {stager} \
        AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
        ".format(stager=stager)
    
    def _reporter(row):
        row['nthstagewin']=getStageWinCount(row['stageNo'],pre='',pronoun='their')
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        row['winstreak']=getConsecutiveStageWinCount(row['stageNo'],post=', ',pronoun='their')
        txt='In stage {stage}, in a time of {time}, {lead} took {nthstagewin},{winstreak} as well as overall lead of the rally.\n\nHe was {diff} ahead of previous rally leader {second}, placed second on the stage.'.format(**row)
        return txt

    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)

def report_qa04(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)

    q="SELECT g2a.stageNo AS stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage, \
       g2x.driverName AS prevlead, db.diffFirst AS diff, dx.diffFirst AS diff2, da.time AS time \
       FROM g2 AS g2a JOIN g2 AS g2b JOIN g2 AS g2x \
       JOIN df_stage AS dx JOIN df_stage AS db JOIN df_stage AS da \
       WHERE g2a.won=1 AND g2b.second=1 AND g2x.lostlead=1 AND g2a.gainedlead=1 \
       AND g2a.stage=g2b.stage {stager} AND g2x.stageNo=dx.stage \
       AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
       AND g2a.stage=g2x.stage AND g2x.carNo!=g2b.carNo AND g2x.carNo!=g2a.carNo  \
       AND dx.carNo=g2x.carNo AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage AND g2x.stageNo=dx.stage \
       ".format(stager=stager)
    
    def _reporter(row):
        row['nthstagewin']=getStageWinCount(row['stageNo'],pre=', and ',post=',',pronoun='their')
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        row['prevlead']=fullAttrib(row['prevlead'])
        txt='In stage {stage}, {lead} took the stage{nthstagewin} in {time}, {diff} ahead of {second}.\n\nAs well as taking the stage, his {diff2} gain over {prevlead}, the rally leader going into the stage, meant {lead} also took overall lead of the rally.'.format(**row)
        return txt
 
    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)

def report_qa05(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)

    q="SELECT g2a.stageNo AS stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage, g2x.driverName AS prevlead, \
        db.diffFirst AS diff, dx.diffFirst AS diff2, da.time AS time \
        FROM g2 AS g2a JOIN g2 AS g2b  JOIN g2 AS g2x \
        JOIN df_stage AS dx JOIN df_stage AS db JOIN df_stage AS da  \
        WHERE g2a.won=1 AND g2b.second=1 AND g2x.lostlead=1 AND g2b.gainedlead=1 \
        AND g2a.stage=g2b.stage AND g2x.stageNo=dx.stage {stager} \
        AND g2a.stage=g2x.stage AND g2x.carNo!=g2b.carNo AND g2x.carNo!=g2a.carNo \
        AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
        AND dx.carNo=g2x.carNo AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage AND g2x.stageNo=dx.stage  \
        ".format(stager=stager)
    
    def _reporter(row):
        row['nthstagewin']=getStageWinCount(row['stageNo'],post=',',pronoun='their')
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        row['prevlead']=fullAttrib(row['prevlead'])
        txt='In stage {stage}, {lead} took the stage{nthstagewin} in a time of {time}, {diff} ahead of {second}.\n\n{second} was second on the stage and took overall lead of the rally from {prevlead}.'.format(**row)
        return txt

    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)

def report_qa08(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)

    q="SELECT g2a.stageNo AS stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage, \
        g2x.driverName AS prevlead, \
        g2y.driverName AS newlead, db.diffFirst as diff, dy.diffFirst AS newdiff, dx.diffFirst AS prevdiff, \
        dy.pos AS newpos, dx.pos AS prevpos, da.time AS time, dy.time AS timel \
        FROM g2 AS g2a JOIN g2 AS g2b  JOIN g2 AS g2x JOIN g2 AS g2y \
        JOIN df_stage AS dx JOIN df_stage as db JOIN df_stage AS dy  JOIN df_stage AS da \
        WHERE g2a.won=1 AND g2b.second=1 AND g2x.lostlead=1 AND g2a.stage=g2b.stage AND g2x.stageNo=dx.stage \
        AND g2a.stage=g2x.stage {stager} \
        AND g2x.carNo!=g2b.carNo AND g2x.carNo!=g2a.carNo \
        AND g2a.stage=g2y.stage AND g2y.carNo!=g2b.carNo AND g2y.carNo!=g2a.carNo \
        AND g2y.carNo!=g2x.carNo AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
        AND g2y.gainedlead=1 AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage  \
        AND dx.carNo=g2x.carNo AND g2x.stageNo=dx.stage AND dy.carNo=g2y.carNo AND g2y.stageNo=dy.stage  \
        ".format(stager=stager)
    
    def _reporter(row):
        try:
            row['newpos']=p.ordinal(int(row['newpos']))
        except: row['newpos']='unplaced'
        try:
            row['prevpos']=p.ordinal(int(row['prevpos']))
        except: row['prevpos']='unplaced'
        row['nthstagewin']=getStageWinCount(row['stageNo'],pre='',pronoun='their')
        row['leadx']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        row['prevlead']=fullAttrib(row['prevlead'])
        row['newlead']=fullAttrib(row['newlead'])
        txt='In stage {stage}, {leadx} took {nthstagewin} by {diff} from {second}, in a time of {time}.\n\nWith his {newpos} place on stage, {newlead} ({newdiff} behind the stagewinner, with a stage time of {timel}), took overall lead of the rally from {prevlead}, {prevpos} on stage and {prevdiff} behind stage winner {lead}.'.format(**row)
        return txt
    
    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)

def report_qa09(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)

    q="SELECT g2a.stageNo AS stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage, g2x.driverName as prevlead, \
        db.diffFirst AS diff, dx.diffFirst AS prevdiff, \
        dx.pos AS prevpos , da.time AS time, dx.time AS timel \
        FROM g2 AS g2a JOIN g2 AS g2b JOIN g2 AS g2x \
        JOIN df_stage AS dx JOIN df_stage as db JOIN df_stage as da \
        WHERE g2a.won=1 AND g2b.second=1 AND g2x.retainedlead=1 AND g2a.stage=g2b.stage AND g2x.stageNo=dx.stage \
        AND g2a.stage=g2x.stage {stager} \
        AND g2x.carNo!=g2b.carNo AND g2x.carNo!=g2a.carNo \
        AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage  \
        AND dx.carNo=g2x.carNo AND g2x.stageNo=dx.stage \
        AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
        ".format(stager=stager)
    
    def _reporter(row):
        try:
            row['newpos']=p.number_to_words(p.ordinal(int(row['newpos'])))
        except: row['newpos']='unplaced'
        try:
            row['prevpos']=p.number_to_words(p.ordinal(int(row['prevpos'])))
        except: row['prevpos']='unplaced'
        row['nthstagewin']=getStageWinCount(row['stageNo'],pre=' and with ',post=',',pronoun='their')
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        row['prevlead']=fullAttrib(row['prevlead'])
        txt='On stage {stage},{nthstagewin} {lead} took the stage by {diff} from {second}, in a time of {time}.\n\n{prevlead}, {prevpos} on stage with a time of {timel}, and {prevdiff} behind the stagewinner, retained overall lead of the rally.'.format(**row)
        return txt
        
    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)

def report_qa10(stage=None):
    stager='' if stage is None else ' AND g2a.stageNo={}'.format(stage)

    q="SELECT g2a.stageNo AS stageNo, g2a.driverName AS lead, g2b.driverName AS second, g2a.stage, \
        db.diffFirst AS diff, da.time AS time \
        FROM g2 AS g2a JOIN g2 AS g2b  \
        JOIN df_stage AS db JOIN df_stage AS da \
        WHERE g2a.won=1 and g2b.second=1 AND g2b.retainedlead=1 \
        AND g2a.stage=g2b.stage {stager} \
        AND db.carNo=g2b.carNo AND g2b.stageNo=db.stage  \
        AND da.carNo=g2a.carNo AND g2a.stageNo=da.stage \
        ".format(stager=stager)

    def _reporter(row):
        try:
            row['newpos']=p.ordinal(int(row['newpos']))
        except: row['newpos']='unplaced'
        try:
            row['prevpos']=p.ordinal(int(row['prevpos']))
        except: row['prevpos']='unplaced'
        row['nthstagewin']=getStageWinCount(row['stageNo'],pre='giving them ',pronoun='their')
        row['lead']=fullAttrib(row['lead'])
        row['second']=fullAttrib(row['second'])
        txt='In stage {stage}, {lead} took the stage, {nthstagewin}, in a time of {time}.\n\nIn second place on stage, {diff} behind, {second} retained overall lead of the rally.'.format(**row)
        return txt
    
    df=sqldf(q, {'g2':wrcX.core.data.g2,'df_stage':wrcX.core.data.df_stage })
    return df.apply(_reporter,axis=1)