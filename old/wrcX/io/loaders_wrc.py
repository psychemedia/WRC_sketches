from pandas import DataFrame, concat, to_numeric, isnull, NaT, merge, Series

import wrcX.core
from wrcX.core import orderedRallyCategories
from wrcX.io.loaders import *
from wrcX.core.time import *

URLstub='http://www.wrc.com/live-ticker/daten'

driverRE_HTML=r'^\s*<img .* alt="([A-Z]*).*/>\s*(.*)$'
tagcontentRE_HTML= r'^<.*>(.*)</[a-zA-Z]+>$'
tagcontent2RE_HTML=r'^(<.*>)?(.+?)(</[a-zA-Z]+>)?$'
controlRE = r'^[A-Z]*([0-9]*)[A-Z]*$'
stagenumRE=r'^TC([0-9]+)[A-Z]*$'

def rankings(df,key='pos'):
    df=df.sort_values(['stage',key])
    df['overall_{}'.format(key)]=df.groupby('stage')[key].rank(method='min')
    df['class_{}'.format(key)]=df.groupby(['stage','groupClass'])[key].rank(method='min')
    df['comp_{}'.format(key)]=df.groupby(['stage','groupClass','eligibility'])[key].rank(method='min')
    return df

def _getTeamCar(df,key='team_Car'):
    return df[key].str.split('\n', 1).str

def _driverNameNationality(df,key='driverName'):
    tmp=df[key].str.extract(driverRE_HTML, expand=True)
    df[key]=tmp[1].str.strip()
    df[key+'_nationality']=tmp[0].str.strip()
    return df[key],df[key+'_nationality']

def _driverCodriverNameNationality(df,key='driver_Codriver'):
    dff=df[key].str.split('\n',expand=True)
    df['driverName']=dff[0]+'\n'+dff[1]
    df['coDriverName']=dff[2]+'\n'+dff[3]
    driver,driverNat=_driverNameNationality(df,key='driverName')
    coDriver,coDriverNat=_driverNameNationality(df,key='coDriverName')
    return driver,driverNat,coDriver,coDriverNat
    

#OVERALL
def _getOverallBase(year,rallyid,stages,final=False,urlrebase=None):
    stages=[stages] if not isinstance(stages,list) else stages
    urlrebase=urlrebase if urlrebase is not None else int(wrcX.core.first_stage==0)#int(stages[0]==0)
    df_overall=DataFrame()
    for stage in stages:
        #http://www.wrc.com/live-ticker/daten/2017/216/stage.216.17.overall.all.html
        #http://www.wrc.com/live-ticker/daten/2017/216/finalresult.overall.216.all.html
        url='{stub}/{year}/{rallyid}/stage.{rallyid}.{stage}.overall.all.html'.format(stub=URLstub,
                                                                                      year=year, rallyid=rallyid,
                                                                                      stage=stage+urlrebase)
        results=loader(url)[0]
        if not validateLoadedDF(results): break
        
        cols=['pos', 'carNo', 'driverName', 'coDriverName', 'team',
              'eligibility', 'groupClass', 'time', 'diffPrev', 'diffFirst']
        results.columns=cols
        results.fillna(0,inplace=True)
        results['pos']=results['pos'].astype(float)
        cols.remove('pos')
        for j in cols:
            results[j]=results[j].astype(str)
        for r in ['driverName','coDriverName']:
            results[r],results[r+'_nationality']=_driverNameNationality(results,r)

        results['stage']=stage
        df_overall=concat([df_overall,results])
    return df_overall.reset_index(drop=True)

def getOverall(year,rallyid,stages,urlrebase=None):
    df_overall=_getOverallBase(year,rallyid,stages,urlrebase=urlrebase)
    if not validateLoadedDF(df_overall): return DataFrame()
    
    for col in ['time','diffPrev','diffFirst']:
        df_overall['td_'+col]=df_overall.apply(lambda x: regularTimeString(x[col]),axis=1)


    df_overall=rankings(df_overall)
    return df_overall

#----

def _getStageResultsBase(year,rallyid,stages,urlrebase=None):
    ''' Get stage results and overall results at end of stage '''
    stages=[stages] if not isinstance(stages,list) else stages
    urlrebase=urlrebase if urlrebase is not None else int(wrcX.core.first_stage==0)#int(stages[0]==0)
    df_stage=DataFrame()
    df_overallpart=DataFrame()
    #Mexico had an SS0 that messed indexing up
    for stage in stages:
        url='{stub}/{year}/{rallyid}/stage.{rallyid}.{stage}.all.html'.format(stub=URLstub,
                year=year,rallyid=rallyid,stage=stage+urlrebase)

        results=loader(url)

        cols=[0,1]
        #No Data yet.
        if not validateLoadedDF(results[0]): break       
        
        results[0].columns=['pos', 'carNo','driverName','time','diffPrev','diffFirst']
        results[1].columns=['pos', 'carNo','driverName','time','diffPrev','diffFirst']
        for i in cols:
            results[i].fillna(0,inplace=True)
            results[i]['pos']=results[i]['pos'].astype(float)
            for j in ['carNo']:
                results[i][j]=results[i][j].astype(str)

            results[i]['driverName'],results[i]['driverName_nationality']=_driverNameNationality(results[i])

            results[i]['stage']=stage
        df_stage=concat([df_stage,results[0]])
        df_overallpart=concat([df_overallpart,results[1]])
    return df_stage.reset_index(drop=True), df_overallpart.reset_index(drop=True)

def getStageResults( year,rallyid,stages,urlrebase=None):
    r = _getStageResultsBase(year,rallyid,stages,urlrebase)
    for df in r:
        if validateLoadedDF(df): 
            for col in ['time','diffPrev','diffFirst']:
                df['td_'+col]=df.apply(lambda x: regularTimeString(x[col]),axis=1)
    return r[0],r[1] 

#------

def _getSplitTimesBase(year,rallyid,stage,urlrebase=None):
    urlrebase=urlrebase if urlrebase is not None else int(wrcX.core.first_stage==0)
    url='{stub}/{year}/{rallyid}/split.{rallyid}.{stage}.html'.format(stub=URLstub, year=year,
                                                                      rallyid=rallyid, stage=stage+urlrebase)
    df_splits=loader(url,na_values=[0,'0'])[0]
    if not validateLoadedDF(df_splits): return DataFrame()
    
    #df_splits.fillna(0,inplace=True)

    df_splits.columns=['start','carNo',
                       'driverName','team',
                       'eligibility']+['split_{}'.format(i) for i in range(1,len(df_splits.columns)-5)]+['stageTime']
    #df_splits.fillna(0,inplace=True)
    for j in df_splits.columns:
        if j in ['carNo']:
            df_splits[j]=df_splits[j].astype(str)
        else:
            df_splits['start']=to_numeric(df_splits['start'],downcast='integer')
    df_splits['stage']=stage
    return df_splits

#rebase is a url hack  - really should pick this up from a class variable set at start
def _getSplitTimes(year,rallyid,stage,urlrebase=None):
    df_splitTimes=_getSplitTimesBase(year,rallyid,stage,urlrebase)
    if not validateLoadedDF(df_splitTimes): return DataFrame()
    
    df_splitTimes['driverName'],df_splitTimes['driverName_nationality']=_driverNameNationality(df_splitTimes)

    for col in ['stageTime']:
        df_splitTimes['time_'+col]=df_splitTimes.apply(lambda x: regularTimeString(x[col]),axis=1)

    cols=[c for c in df_splitTimes.columns if c.startswith('split_')]
    for col in cols:
        df_splitTimes['td_'+col]=df_splitTimes.apply(lambda x: regularTimeString(x[col]),axis=1)
        df_splitTimes['time_'+col]=df_splitTimes['td_'+col]
        #This requires that there is a time set in row 0...
        basetime=df_splitTimes.loc[0,'time_'+col]
        if not isnull(df_splitTimes.loc[0,'time_'+col]):
            df_splitTimes.loc[0,'td_'+col]=regularTimeString(0)
            df_splitTimes.loc[0,'time_'+col]=regularTimeString(0)
            df_splitTimes['time_'+col]=basetime+df_splitTimes['time_'+col]

    return df_splitTimes

def getSplitTimes( year,rallyid,stages,urlrebase=None):
    if not isinstance(stages,list):
        stages=[stages]
    r=[]
    for stage in stages:
        r.append(_getSplitTimes(year,rallyid,stage,urlrebase))
    if len(stages)==1:
        return r[0]
    return r 
#----

#http://www.wrc.com/live-ticker/daten/2017/216/entry.216.html
def _getEntryListBase(year,rallyid):
    url='{stub}/{year}/{rallyid}/entry.{rallyid}.html'.format(stub=URLstub, rallyid=rallyid, year=year)
    df_entry=loader(url)[0]
    if not validateLoadedDF(df_entry): return DataFrame()
    
    cols=['carNo','driver_Codriver','team_Car','eligibility','groupClass','FIApriority']
    df_entry.columns=cols
    df_entry.fillna(0,inplace=True)
    for j in cols:
        df_entry[j]=df_entry[j].astype(str)
    #Make null competition entries None
    df_entry.loc[df_entry['eligibility']=='0','eligibility']=None
    return df_entry

def getEntryList(year,rallyid):
    df_entry=_getEntryListBase(year,rallyid)
    if not validateLoadedDF(df_entry): return DataFrame()

    df_entry['team'], df_entry['car'] = _getTeamCar(df_entry)

    df_entry['driverName'],df_entry['driverName_nationality'], \
              df_entry['coDriverName'],df_entry['coDriverName_nationality'] = _driverCodriverNameNationality(df_entry)

    df_entry['eligibility']=df_entry['eligibility'].astype('category')
    df_entry['eligibility'].cat.set_categories(orderedRallyCategories, ordered=True, inplace=True)

    wrcX.core.carClass=df_entry[['carNo','groupClass','eligibility']]
    return df_entry



#------
def _getSplitSectorTimes(year,rallyid,stage, urlrebase=None):
    df_splitTimes=getSplitTimes(year,rallyid,stage,urlrebase)
    if not validateLoadedDF(df_splitTimes): return DataFrame()

    ddf_sectors=df_splitTimes[['start','carNo','driverName','stage']+ \
                      [c for c in df_splitTimes.columns if c.startswith('time_split')]+['time_stageTime'] ]
    # Identify the columns associated with split times and overall stage time, in order
    # (Could make the ordering more robust by sorting columns headings based on the values in the first row?)
    tcols= [c for c in df_splitTimes.columns if c.startswith('time_split')]+['time_stageTime']

    # generate sector times by subtracting one col value from the previous going along each driver row
    sectorTimes=ddf_sectors[tcols].diff(axis=1)

    #Set the value of the first sector time (this is the same as the first split time)
    #?equivalent = ddf_sectors[tcols[0]]
    # .ix[:,0] says: all the rows (:), first (0th indexed) column
    sectorTimes[sectorTimes.columns[0]]=ddf_sectors[tcols].ix[:,0]
    #if a timesplit is NaT we need to set corresponding sector_ to NaT
    sectorTimes[ddf_sectors[tcols].isnull()] = NaT
    #For sector times, propagate NaT to the right - if we miss a split time, we can't calculate sector time
    #Note info may be lost if eg we lose a middle split time but then get final split and end of stage time
    #for col in range(1,len(sectorTimes.columns)):
    #    sectorTimes.loc[sectorTimes[sectorTimes.columns[col-1]].isnull(), sectorTimes.columns[col]] = pd.NaT 
    #H/T Alan Hall for suggesting use of cumsum to do this
    #if sectorTimes.dropna(how='all').empty: return ddf_sectors
    if len(sectorTimes.columns)>1:
            sectorTimes[sectorTimes.cumsum(axis=1,skipna=False).isnull()]=NaT

    sectorTimes.columns=['sector_{}'.format(i) for i in range(1,len(tcols)+1)]

    #NOTE - there should not be any negative or zero times set as sectorTimes
    sectorTimes[sectorTimes <= 0]=None
    #Merge the wide sector times back into the wide secto dataframe
    ddf_sectors=merge(ddf_sectors, sectorTimes, right_index=True, left_index=True)
    
    if wrcX.core.carClass is None:
        getEntryList( year,rallyid)
    ddf_sectors=merge(ddf_sectors,wrcX.core.carClass,on='carNo')
 
    #Now find deltas from ultimate sector time
    cols=[c for c in ddf_sectors.columns if c.startswith('sector_')]
    for c in cols:
        #The ultimate sector time is the minimum time in the sector - None/null is ignored
        # TO DO 
        #Should we be doing this relative to something else - eg within group ultimate ref?
        ultimateSectorTime = ddf_sectors[ddf_sectors['groupClass']=='RC1'][c].min()
        # Sector delta is the difference between sector time and ultimate sector time
        ddf_sectors['d_'+c]=ddf_sectors[c]-ultimateSectorTime

    for col in [c for c in ddf_sectors.columns if c.startswith('time_')]:
        ddf_sectors=rankings(ddf_sectors,col)
    for col in [c for c in ddf_sectors.columns if c.startswith('sector_')]:
        ddf_sectors=rankings(ddf_sectors,col)

    return ddf_sectors

def getSplitSectorTimes(year,rallyid,stages,urlrebase=None):
    if not isinstance(stages,list):
        stages=[stages]
    r=[]
    for stage in stages:
        r.append(_getSplitSectorTimes(year,rallyid,stage,urlrebase))
    if len(stages)==1:
        return r[0]
    return r 
#---

def _getRetirementsBase(year,rallyid):
    #http://www.wrc.com/live-ticker/daten/2017/216/retirements.216.all.html
    url='{stub}/{year}/{rallyid}/retirements.{rallyid}.all.html'.format(stub=URLstub,year=year,rallyid=rallyid)
    df_retirements=loader(url)[0]
    if not validateLoadedDF(df_retirements): return DataFrame()
    
    df_retirements.columns=['carNo','driverName','coDriverName','team_Car','eligibility','groupClass','control','reason']
    df_retirements['carNo']=df_retirements['carNo'].astype(str)
    return df_retirements

def getRetirements(year,rallyid):
    df_retirements=_getRetirementsBase(year,rallyid)
    if not validateLoadedDF(df_retirements): return DataFrame()

    df_retirements['team'],df_retirements['car']=_getTeamCar(df_retirements)

    for r in ['driverName','coDriverName']:
        df_retirements[r],df_retirements[r+'_nationality']=_driverNameNationality(df_retirements,r)

    df_retirements['eligibility']=df_retirements['eligibility'].astype('category')
    df_retirements['eligibility'].cat.set_categories(orderedRallyCategories,ordered=True,inplace=True)

    df_retirements['stagenumber']=df_retirements['control'].str.extract(controlRE, expand=True).astype(int)

    return df_retirements

### ---
def _getPenaltiesBase(year,rallyid):
    url='{stub}/{year}/{rallyid}/penalties.{rallyid}.all.html'.format(stub=URLstub, year=year, rallyid=rallyid)
    penalties=loader(url)[0]
    if not validateLoadedDF(penalties): return DataFrame()
    
    penalties.columns=['carNo', 'driverName', 'coDriverName', 'team_Car', 
                       'eligibility', 'groupClass', 'control', 'reason', 'penalty']
    penalties['carNo']=penalties['carNo'].astype(str)
    return penalties


def getPenalties(year, rallyid):
    df_penalties=_getPenaltiesBase(year,rallyid)
    if not validateLoadedDF(df_penalties): return DataFrame()

    for r in ['driverName','coDriverName']:
        df_penalties[r],df_penalties[r+'_nationality']=_driverNameNationality(df_penalties,r)

    df_penalties['team'],df_penalties['car']=_getTeamCar(df_penalties)

    df_penalties['tmp']=df_penalties.apply(lambda x: [y for y in zip(x['reason'].split('\n'),
                                                             x['penalty'].split('\n'),
                                                             x['control'].split('\n'))], axis=1)

    df_tmp = df_penalties.set_index('carNo')['tmp'].apply(Series) # break into separate columns
    df_tmp = df_tmp.stack().reset_index('carNo')
    df_tmp.rename(columns={0: 'penreasoncontrol'}, inplace=True)

    df_penalties=merge(df_penalties,df_tmp,on='carNo')
    df_penalties[['reason2','penalty2','control2']]=df_penalties['penreasoncontrol'].apply(Series)
    df_penalties['td_penalty']=df_penalties.apply(lambda x: regularTimeString(x['penalty2']),axis=1)
    df_penalties['eligibility']=df_penalties['eligibility'].astype('category')
    df_penalties['eligibility'].cat.set_categories(orderedRallyCategories,ordered=True,inplace=True)

    df_penalties['stagenumber']=df_penalties['control2'].str.extract(controlRE, expand=True).astype(int)
    return df_penalties
    
    
#---

def _getBaseItinerary(year,rallyid):
    url='{stub}/{year}/{rallyid}/itinerary.{rallyid}.all.html'.format(stub=URLstub, year=year,rallyid=rallyid)
    df_itinerary=loader(url)[0]
    if not validateLoadedDF(df_itinerary): return DataFrame()
    
    df_itinerary.columns=['stage','stagename','distance (km)','firstCar','status']
    return df_itinerary

def getItinerary(year,rallyid):
    df_itinerary = _getBaseItinerary(year,rallyid)
    if not validateLoadedDF(df_itinerary): return DataFrame()

    df_itinerary['stagename']=df_itinerary['stagename'].str.extract(tagcontent2RE_HTML,expand=True)[1]
    df_itinerary['stage']=df_itinerary['stage'].str.extract(tagcontent2RE_HTML,expand=True)[1]
    df_itinerary['stageNo']=df_itinerary['stage'].str.replace('SS','')
    df_itinerary['basestage']=df_itinerary['stage'].where(df_itinerary['stage'].str.startswith('SS'),
                                                          'SS'+df_itinerary['stage'].str.extract(stagenumRE,
                                                                                                 expand=True)[0])

    #Add the day number
    df_itinerary['day']=df_itinerary['stage'].str.startswith('Day')
    df_itinerary['day']=df_itinerary['day'].fillna(False).cumsum()

    #Add the section
    df_itinerary['section']=df_itinerary['stage'].str.contains(r'TC[0-9]+B')
    df_itinerary['section']=1+df_itinerary['section'].fillna(False).cumsum()

    return df_itinerary