from pandas import melt
import random

def symReport1(ddf_sectors,mapping=None):
    def symtest1(delta,mapping=None):
        if mapping is None: mapping=[40,20,10,5]
        i=0
        sym=chr(ord('A')+len(mapping))
        for threshold in mapping:
            if delta>threshold:
                sym=chr(ord('A')+i)
                break
            else: i=i+1
        return sym
    
    def _symReport1(ddf_sectors):
        tcols=[col for col in ddf_sectors.columns if col.startswith('d_')]

        dxs=ddf_sectors.loc[:,['carNo','driverName']+tcols]
        dxs=melt(dxs, id_vars=['driverName'], var_name='control', value_vars=tcols, value_name='sectordelta')
        dxs=dxs[dxs['sectordelta'].notnull()]
        dxs['sectordelta_s']=dxs['sectordelta'].apply(lambda x: x.total_seconds())
        dxs['spos']=dxs.groupby('control')['sectordelta_s'].rank().astype(int)
        return dxs

    dxs=_symReport1(ddf_sectors)
    dxs['sym1']=dxs.sort_values('control')['sectordelta_s'].apply(symtest1,mapping=mapping)
    patterns=dxs[['driverName','sym1']].groupby('driverName')['sym1'].apply(sum)
    return patterns


def report_poorStart(patterns):
    def _report_poorStart(name):
        var1=['Despite','Although getting']
        txt= \
    '''{} a poor time in the first part of the stage, {} made up time over the rest of the stage and finished strongly.'''.format(var1[random.randint(0,len(var1)-1)],name)
        return txt
    
    pattern=r'^[AB][BCD]+E+$'
    txt=patterns[patterns.str.contains(pattern)].reset_index()['driverName'].apply(_report_poorStart)
    return txt.tolist()

def report_poorEnd(patterns):
    def _report_poorEnd(name):
        var1=['Despite being','Although']
        txt='{} competitive at the start of the stage, {} lost ground in the final part of the stage.'.format(var1[random.randint(0,len(var1)-1)],name)
        return txt
    
    pattern=r'^[DE]+[ABC]$'
    txt=patterns[patterns.str.contains(pattern)].reset_index()['driverName'].apply(_report_poorEnd)
    return txt.tolist()