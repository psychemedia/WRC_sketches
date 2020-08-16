from pandas import merge, notnull

import random
import re

import wrcX.core.data as d
from wrcX.core.utils import strfdelta

import inflect
p = inflect.engine()

def report_driverStageSectors(stagenum,carNo,ddf_sectors=None):
    if ddf_sectors is None:
        ddf_sectors=d.df_splitSectorTimes_all[stagenum-int(wrcX.core.first_stage)]

    xp=ddf_sectors[ddf_sectors['carNo']==str(int(carNo))]

    cols=[col.split('_')[-1] for col in xp.columns if col.startswith('overall_sector_')]
    txt=''
    start_txt='{driverName} was '
    notcomplete=False
    for q in reversed(cols):
        if q==str(len(cols)) and notnull(xp.iloc[0]['sector_{}'.format(q)]): 
            txt=txt+p.number_to_words(p.ordinal(int(xp.iloc[0]['overall_time_stageTime'])))
            txt=txt+' overall on the stage, with an overall time of {}'.format(strfdelta(xp.iloc[0]['time_stageTime']))
            if len(cols)>1:
                txt2='{} and a final sector time of {} ({} fastest sector time)'
                txt=txt2.format(txt,strfdelta(xp.iloc[0]['sector_{}'.format(q)]),
                                p.number_to_words(p.ordinal(int(xp.iloc[0]['overall_sector_{}'.format(q)]))))
            txt=txt+'.'
        elif q=='1' and notnull(xp.iloc[0]['sector_{}'.format(q)]):
            txt2='{} fastest at the {} split, with a split time of {}; {}'
            txt=txt2.format(p.number_to_words(p.ordinal(int(xp.iloc[0]['overall_sector_{}'.format(q)]))),
                           p.number_to_words(p.ordinal(int(q))),
                           strfdelta(xp.iloc[0]['time_split_{}'.format(q)]),
                           txt)
        elif notnull(xp.iloc[0]['sector_{}'.format(q)]):
            txt2='{} at the {} split, (split time {}, {} fastest sector time of {}); {}'
            txt=txt2.format(p.number_to_words(p.ordinal(int(xp.iloc[0]['overall_time_split_{}'.format(q)]))),
                           p.number_to_words(p.ordinal(int(q))), strfdelta(xp.iloc[0]['time_split_{}'.format(q)]),
                           p.number_to_words(p.ordinal(int(xp.iloc[0]['overall_sector_{}'.format(q)]))),
                           strfdelta(xp.iloc[0]['sector_{}'.format(q)]),txt)
        else:
            notcomplete=True
            start_txt='{driverName} did not complete'

    if notcomplete and txt!='':
        start_txt=start_txt+' the stage. He was '
    else:
        start_txt=start_txt+' any part of the stage.'
    txt=start_txt.format(**xp.iloc[0].to_dict())+txt

    txt=re.sub('[,;]$', '.', txt.strip())
    return txt