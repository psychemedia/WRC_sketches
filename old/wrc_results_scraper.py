# WRC_sketches

URL_BASE='http://www.wrc.com'


from pandas import DataFrame, concat, to_numeric, isnull, NaT, merge, Series, wide_to_long, read_html

import pandas.io.html as ih
import re
import sqlite3

### PATCHES
#via http://stackoverflow.com/a/28173933/454773
def stringify_children(self,node):
    from lxml.etree import tostring
    from itertools import chain
    parts = ([node.text] +
            list(chain(*([tostring(c, with_tail=False), c.tail] for c in node.getchildren()))) +
            [node.tail])
    txt='\n'.join(str(v.decode()) if isinstance(v, bytes) else str(v) for v in filter(None, parts))
    txt=txt.replace('<br/>','\n')
    txt=re.sub('\n\n+','\n',txt)
    return txt

def nostrip(self, rows):
        """Parse the raw data into a list of lists.
        Parameters
        ----------
        rows : iterable of node-like
            A list of row elements.
        text_getter : callable
            A callable that gets the text from an individual node. This must be
            defined by subclasses.
        column_finder : callable
            A callable that takes a row node as input and returns a list of the
            column node in that row. This must be defined by subclasses.
        Returns
        -------
        data : list of list of strings
        """
        #TH: omit the whitespace stripper from the original function
        data = [[self._text_getter(col) for col in
                 self._parse_td(row)] for row in rows]
        return data

ih._LxmlFrameParser._text_getter=stringify_children
ih._HtmlFrameParser._parse_raw_data=nostrip

def loader(url,**kwargs):
    df=read_html(url,encoding='utf-8',**kwargs)
    #except:
    #    df=[DataFrame()]
    #The original pandas HTML table reader strips whitespace
    #If we disable that as part of gaining access to cell HTML contents, we need to make up for it
    for ddf in df:
        ddf[ddf.select_dtypes(include=['O']).columns]=ddf.select_dtypes(include=['O']).apply(lambda x: x.str.strip())
    return df

def validateLoadedDF(df):
    if (len(df.columns)==1) | (not df.size):
        return False
    return True
    
from pandas import isnull, cut, to_timedelta

def partofday(df):
    return cut(df.dt.hour,
                  [-1,5, 12, 17,19, 24],
                  labels=['Overnight','Morning', 'Afternoon', 'Evening','Night'])

def regularTimeString(strtime):
    if isnull(strtime) or strtime=='' or strtime=='nan':
        return to_timedelta(strtime)

    #Go defensive, just in case we're passed eg 0 as an int
    strtime=str(strtime)
    strtime=strtime.strip('+')

    modifier=''
    if strtime.startswith('-'):
        modifier='-'
        strtime=strtime.strip('-')
    timeComponents=strtime.split(':')
    ss=timeComponents[-1]
    mm=timeComponents[-2] if len(timeComponents)>1 else 0
    hh=timeComponents[-3] if len(timeComponents)>2 else 0
    timestr='{}{}:{}:{}'.format(modifier,hh,mm,ss)
    return to_timedelta(timestr)
    
#orderedRallyCategories=["M","WRC TROPHY","WRC2","WRC3","RGT",'']

driverRE_HTML=r'^\s*<img .* alt="([A-Z]*).*/>\s*(.*)$'
tagcontentRE_HTML= r'^<.*>(.*)</[a-zA-Z]+>$'
tagcontent2RE_HTML=r'^(<.*>)?(.+?)(</[a-zA-Z]+>)?$'
controlRE = r'^[A-Z]*([0-9]*)[A-Z]*$'
stagenumRE=r'^TC([0-9]+)[A-Z]*$'
standing_pointsRE=r'^([0-9\+]*).*\((.*)\).*$'
imgAltRE_HTML= r'^<img.* alt="([^"]*)".*$'


def unicodify(string):
    soup = BeautifulSoup(string,"html5lib")
    return soup.text


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
    
from bs4 import BeautifulSoup 
import requests

def getRalliesList(url='http://www.wrc.com/en/wrc/results/rally-results/page/3049----.html'):
    html=requests.get(url)
    soup=BeautifulSoup(html.content, "html5lib")
    #BeautifulSoup has a routine - find_all() - that will find all the HTML tags of a particular sort
    #Links are represented in HTML pages in the form <a href="http//example.com/page.html">link text</a>
    #Grab all the <a> (anchor) tags...
    souptable=soup.findAll("ul")[12].findAll('a')
    rallies={}
    for a in souptable:
        print(a['href'],a.find('img')['title'])#(a['title'],a['href'])
        rallies[a['href'].replace('/en/wrc/results/','').split('/')[0]] = {'path':a['href'],
                                                                           'name':a.find('img')['title']}
    return rallies
    

def getRallyDetails(rallies, rally):
    #url='http://www.wrc.com/en/wrc/results/monte-carlo/stage-times/page/318-226---.html'
    url='{base}{path}'.format(base=URL_BASE,path=rallies[rally]['path'])

    html=requests.get(url)
    soup=BeautifulSoup(html.content, "html5lib")
    #BeautifulSoup has a routine - find_all() - that will find all the HTML tags of a particular sort
    #Links are represented in HTML pages in the form <a href="http//example.com/page.html">link text</a>
    #Grab all the <a> (anchor) tags...
    souptable=soup.find("div",{'class':'content_full frontpage-livecenter'}).findAll('a')
    #souptable
    lookup={}
    for a in souptable:
        lookup[''.join(a.text.strip().split())] = a['href']
    return lookup
    
def getStageURLs(rallies,rally, optionpath):
    url='{base}{path}'.format(base=URL_BASE,path=optionpath)
    html=requests.get(url)
    soup=BeautifulSoup(html.content, "html5lib")
    souptable=soup.find("select",{'name':'rally_id'}).findAll('option')
    options={}
    for o in souptable:
        options[o.text.replace('\n','').strip()] = o['value']
    return options
    
def getChampionshipURLs(url='http://www.wrc.com/en/wrc/results/championship-standings/page/4176----.html'):
    html=requests.get(url)
    soup=BeautifulSoup(html.content, "html5lib")
    souptable=soup.find("div",{'class':'content_full frontpage-livecenter'}).findAll('a')
    championships={}
    for a in souptable:
        championships[a.text.strip()] = {'path':a['href']}
    return championships
    
def getStartinglistURLs(rallies,rally, optionpath):
    lookupstarts = getStageURLs(rallies,rally, optionpath)
    lookupstarts = {k.replace('Start List for ',''):lookupstarts[k] for k in lookupstarts}
    return lookupstarts   


def getOverallURLs(rallies,rally, optionpath):
    overall = getStageURLs(rallies,rally, optionpath)
    return overall
    

def getSplitTimesURLs(rallies,rally, optionpath):
    overall = getStageURLs(rallies,rally, optionpath)
    return overall
    
def getStageTimesURLs(rallies,rally, optionpath):
    overall = getStageURLs(rallies,rally, optionpath)
    return overall
    
def getStagewinnersURLs(rallies,rally, optionpath):
    overall = getStageURLs(rallies,rally, optionpath)
    return overall
    
def _getBaseItinerary(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_itinerary=loader(url)[0]
    if not validateLoadedDF(df_itinerary): return DataFrame()
    
    df_itinerary.columns=['stage','stagename','distance (km)','firstCar','status']
    return df_itinerary

def getItinerary(path):
    df_itinerary = _getBaseItinerary(path)
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
    
def _getChampionshipStandingsBase(path, typ='Drivers'):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_standings=loader(url)[0]
    if not validateLoadedDF(df_standings): return DataFrame()
    cols = ['pos','name']
    #rallynames is a list of the rally names
    rallynames = df_standings.iloc[0,2:-1].str.extract(imgAltRE_HTML, expand=True)[0].apply(lambda x: unicodify(x)).tolist()
    rallycols = ['Round_{}'.format(i) for i in range(1,1+len(rallynames))]
    cols = cols + rallycols
    cols=cols+['Total']
    df_standings.columns=cols
    df_standings['pos']=to_numeric(df_standings['pos'].str.strip('.'), errors='coerce')
    df_standings['Total'] = to_numeric(df_standings['Total'], errors='coerce')
    
    if typ!='Teams':
        df_standings['name'],df_standings['tmp']=_driverNameNationality(df_standings,'name')
        df_standings = df_standings.drop(columns = ['tmp'])
    df_standings = df_standings.drop([0])
    return df_standings, rallynames

def _ppx(x):
    points=None
    bonus=None
    if isinstance(x,list):
        if len(x)==2:
            points  = x[0]
            bonus = x[1]
        elif len(x)==1 and x[0]!='':
            points=x[0]
    return points, bonus

def getChampionshipStandings(path, typ='Drivers'):
    df_standings, rallynames=_getChampionshipStandingsBase(path, typ=typ)
    if not validateLoadedDF(df_standings): return DataFrame()
    #return df_standings
    for c in [col for col in df_standings if col.startswith('Round')]:
        #Get the points
        tmp=df_standings[c].str.replace('\n','').str.extract(standing_pointsRE, expand=True)
        if typ != 'Teams':
            tmp[1] = to_numeric(tmp[1].str.strip('.'), errors='coerce')
            df_standings['pos{}'.format(c)] = tmp[1]
            tmp2=tmp[0].str.split('+').apply(lambda x:_ppx(x)).apply(Series)
            for c2 in [0,1]:
                tmp[c2] = to_numeric(tmp2[c2], errors='coerce')
            df_standings['points{}'.format(c)]=tmp[0]
            df_standings['bonus{}'.format(c)]=tmp[1]
        else:
            tmp2 = tmp[1].str.split('+').apply(lambda x:_ppx(x)).apply(Series)
            for c2 in [0,1]:
                tmp2[c2] = to_numeric(tmp2[c2], errors='coerce')
            df_standings['D1pos{}'.format(c)] = tmp2[0]
            df_standings['D2pos{}'.format(c)] = tmp2[1]
            tmp2=tmp[0].str.split('+').apply(lambda x:_ppx(x)).apply(Series)
            for c2 in [0,1]:
                tmp[c2] = to_numeric(tmp2[c2], errors='coerce')
            df_standings['D1points{}'.format(c)]=tmp2[0]
            df_standings['D2points{}'.format(c)]=tmp2[1]

    if typ != 'Teams':
        df_standings =  wide_to_long(df_standings, ["Round_","bonusRound_", "posRound_","pointsRound_"],
                                    i=["pos",'name'], j="roundnum").reset_index()
    else:
        df_standings =  wide_to_long(df_standings, ["Round_","D1posRound_", "D2posRound_",
                                                    "D1pointsRound_", "D2pointsRound_"],
                                    i=["pos",'name'], j="roundnum").reset_index()


    df_standings['rallyname'] = df_standings['roundnum'].apply(lambda x: rallynames[int(x)-1] )
    df_standings.columns = [ c. strip('_') for c in df_standings.columns]
    return df_standings
    


def _getPenaltiesBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_penalties=loader(url)[0]
    if not validateLoadedDF(df_penalties): return DataFrame()
    
    df_penalties.columns=['carNo', 'driverName', 'coDriverName', 'team_Car', 
                       'eligibility', 'groupClass', 'control', 'reason', 'penalty']
    df_penalties['carNo']=df_penalties['carNo'].astype(str)
    return df_penalties


def getPenalties(path):
    df_penalties=_getPenaltiesBase(path)
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
    df_penalties['eligibility']=df_penalties['eligibility']

    df_penalties['stagenumber']=df_penalties['control2'].str.extract(controlRE, expand=True).astype(int)
    df_penalties.drop(['tmp', 'penreasoncontrol'], axis=1, inplace=True)
    return df_penalties
    
    
def _getRetirementsBase(path):
    #http://www.wrc.com/live-ticker/daten/2017/216/retirements.216.all.html
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_retirements=loader(url)[0]
    if not validateLoadedDF(df_retirements): return DataFrame()
    
    df_retirements.columns=['carNo','driverName','coDriverName','team_Car','eligibility','groupClass','control','reason']
    df_retirements['carNo']=df_retirements['carNo'].astype(str)
    return df_retirements

def getRetirements(path):
    df_retirements=_getRetirementsBase(path)
    if not validateLoadedDF(df_retirements): return DataFrame()

    df_retirements['team'],df_retirements['car']=_getTeamCar(df_retirements)

    for r in ['driverName','coDriverName']:
        df_retirements[r],df_retirements[r+'_nationality']=_driverNameNationality(df_retirements,r)

    df_retirements['eligibility']=df_retirements['eligibility']

    df_retirements['stagenumber']=df_retirements['control'].str.extract(controlRE, expand=True).astype(int)

    return df_retirements
    
def _getEntryListBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
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

def getEntryList(path):
    df_entry=_getEntryListBase(path)
    if not validateLoadedDF(df_entry): return DataFrame()

    df_entry['team'], df_entry['car'] = _getTeamCar(df_entry)

    df_entry['driverName'],df_entry['driverName_nationality'], \
              df_entry['coDriverName'],df_entry['coDriverName_nationality'] = _driverCodriverNameNationality(df_entry)

    return df_entry
    
    
def _getStartListBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_starts=loader(url)[0]
    if not validateLoadedDF(df_starts): return DataFrame()
    
    cols=['carNo','driver_Codriver','team_Car','eligibility','startTime']
    df_starts.columns=cols
    df_starts.fillna(0,inplace=True)
    for j in cols:
        df_starts[j]=df_starts[j].astype(str)
    #Make null competition entries None
    df_starts.loc[df_starts['eligibility']=='0','eligibility']=None
    return df_starts

def getStartList(path):
    df_starts=_getStartListBase(path)
    if not validateLoadedDF(df_starts): return DataFrame()
    df_starts['team'], df_starts['car'] = _getTeamCar(df_starts)

    df_starts['driverName'],df_starts['driverName_nationality'], \
              df_starts['coDriverName'],df_starts['coDriverName_nationality'] = _driverCodriverNameNationality(df_starts)

    return df_starts
    

def _getStagewinnersBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_stagewinners=loader(url)[0]
    if not validateLoadedDF(df_stagewinners): return DataFrame()
    cols=['stage','stagename','driverName','team','winnerTime']#'carNo','driver_Codriver','team_Car','eligibility','groupClass','FIApriority']
    df_stagewinners.columns=cols
    df_stagewinners['driverName'] = df_stagewinners['driverName'].str.replace('\n','')
    df_stagewinners['driverName'],df_stagewinners['driverName_nationality']=_driverNameNationality(df_stagewinners,'driverName')
    return df_stagewinners
    
def getStagewinners(path):
    df_stagewinners=_getStagewinnersBase(path)
    if not validateLoadedDF(df_stagewinners): return DataFrame()
    
    return df_stagewinners
    
#Overall
def _getOverallBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_overall=loader(url)[0]
    if not validateLoadedDF(df_overall): return DataFrame()
    
    cols=['pos', 'carNo', 'driverName', 'coDriverName', 'team',
          'eligibility', 'groupClass', 'time', 'diffPrev', 'diffFirst']
    df_overall.columns=cols
    df_overall.fillna(0,inplace=True)
    df_overall['pos']=df_overall['pos'].astype(float)
    cols.remove('pos')
    for j in cols:
        df_overall[j]=df_overall[j].astype(str)
    for r in ['driverName','coDriverName']:
        df_overall[r],df_overall[r+'_nationality']=_driverNameNationality(df_overall,r)

    return df_overall#.reset_index(drop=True)

def getOverall(path):
    df_overall=_getOverallBase(path)
    if not validateLoadedDF(df_overall): return DataFrame()
    
    for col in ['time','diffPrev','diffFirst']:
        df_overall['td_'+col]=df_overall.apply(lambda x: regularTimeString(x[col]),axis=1)

    return df_overall
    
def _getOverallFinalBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_overall=loader(url)[0]
    if not validateLoadedDF(df_overall): return DataFrame()
    cols=['pos','carNo', 'driverName', 'eligibility', 'stageTime', 'penalties', 'totaltime','diffPrev','diffFirst']
    df_overall.columns=cols
    df_overall.fillna(0,inplace=True)
    df_overall['pos']=df_overall['pos'].astype(float)
    cols.remove('pos')
    for j in cols:
        df_overall[j]=df_overall[j].astype(str)
    df_overall['driverName'],df_overall['driverName_nationality']=_driverNameNationality(df_overall,'driverName')

    return df_overall#.reset_index(drop=True)

def getOverallFinal(path):
    df_overall=_getOverallFinalBase(path)
    if not validateLoadedDF(df_overall): return DataFrame()
    
    for col in ['stageTime','totaltime','diffPrev','diffFirst']:
        df_overall['td_'+col]=df_overall.apply(lambda x: regularTimeString(x[col]),axis=1)

    return df_overall
    
#SplitTimes
def _getSplitTimesBase(path):
    url='{base}{path}'.format(base=URL_BASE, path=path)
    df_splits=loader(url)[0]
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
    return df_splits

def getSplitTimes(path):
    df_splitTimes=_getSplitTimesBase(path)
    if not validateLoadedDF(df_splitTimes): return DataFrame()
    
    df_splitTimes['driverName'],df_splitTimes['driverName_nationality']=_driverNameNationality(df_splitTimes)

    for col in ['stageTime']:
        df_splitTimes['time'+col]=df_splitTimes.apply(lambda x: regularTimeString(x[col]),axis=1)
    df_splitTimes_base=df_splitTimes[:]
    cols=[c for c in df_splitTimes.columns if c.startswith('split_')]
    df_splitTimes_splits=df_splitTimes_base[['carNo','driverName']+cols][:]
    for col in cols:
        df_splitTimes_splits['td'+col]=df_splitTimes_base.apply(lambda x: regularTimeString(x[col]),axis=1)
        df_splitTimes_splits['time'+col]=df_splitTimes_splits['td'+col]
        #This requires that there is a time set in row 0...
        basetime=df_splitTimes_splits.loc[0,'time'+col]
        if not isnull(df_splitTimes_splits.loc[0,'time'+col]):
            df_splitTimes_splits.loc[0,'td'+col]=regularTimeString(0)
            df_splitTimes_splits.loc[0,'time'+col]=regularTimeString(0)
            df_splitTimes_splits['time'+col]=basetime+df_splitTimes_splits['time'+col]
    df_splitTimes_splits = wide_to_long(df_splitTimes_splits, ["split_","tdsplit_", "timesplit_"],
                                        i=["carNo",'driverName'], j="Split").reset_index()
    df_splitTimes_base = df_splitTimes_base[['start', 'carNo', 'driverName', 'team', 'eligibility', 'stageTime', 'driverName_nationality', 'timestageTime']]
    return df_splitTimes_base,df_splitTimes_splits
    
#StageTimes stagetimes
def _getStageTimesBase(path):
    ''' Get stage results and overall results at end of stage '''
    
    url='{base}{path}'.format(base=URL_BASE, path=path)
    results=loader(url)
    if not validateLoadedDF(results[0]): return DataFrame()

    cols=[0,1]     

    results[0].columns=['pos', 'carNo','driverName','time','diffPrev','diffFirst']
    results[1].columns=['pos', 'carNo','driverName','time','diffPrev','diffFirst']
    for i in cols:
        results[i].fillna(0,inplace=True)
        results[i]['pos']=results[i]['pos'].astype(float)
        for j in ['carNo']:
            results[i][j]=results[i][j].astype(str)

        results[i]['driverName'],results[i]['driverName_nationality']=_driverNameNationality(results[i])

    return results[0], results[1]#results[0].reset_index(drop=True), df_overallpart.reset_index(drop=True)

def getStageTimes( path):
    r = _getStageTimesBase(path)
    for df in r:
        if validateLoadedDF(df): 
            for col in ['time','diffPrev','diffFirst']:
                df['td_'+col]=df.apply(lambda x: regularTimeString(x[col]),axis=1)
    return r[0],r[1] 
    
def _saveTable(conn, df, year, rally, table):
    if df.empty: return
    df['Year']=year
    df['Rally']=rally
    df.to_sql(table.lower(), conn, index=False, if_exists='append')

def saveItinerary(conn,lookup, year, rally, table='Itinerary'):
    df=getItinerary(lookup[table])
    _saveTable(conn,df, year, rally, table)
    
def savePenalties(conn,lookup, year, rally,table='Penalties'):
    df=getPenalties(lookup[table])
    _saveTable(conn,df, year, rally, table)
    
def saveRetirements(conn,lookup, year, rally, table='Retirements'):
    df=getRetirements(lookup[table])
    _saveTable(conn, df, year, rally, table)
    
def saveEntrylist(conn,lookup, year, rally, table='Startlists'):
    df=getEntryList(lookup[table])
    _saveTable(conn, df, year, rally, 'entrylist')
    
def saveStartlist(conn,lookup, year, rally, section, table='Startlists'):
    df=getEntryList(lookup[table])
    if df.empty: return
    df['section'] = section
    _saveTable(conn, df, year, rally, table)

def saveStageWinners(conn,lookup, year, rally, table='Stagewinners'):
    df=getStagewinners(lookup[table])
    _saveTable(conn, df, year, rally, table)

def saveOverall(conn, year, rally, overall, stage, table='overallstage'):
    df=getOverall(overall[stage])
    if df.empty: return
    df['stage'] = stage
    _saveTable(conn, df, year, rally, table)

def saveOverallFinal(conn, year, rally, overall, stage, table='overallfinal'):
    df=getOverallFinal(overall[stage])
    _saveTable(conn, df, year, rally, table)

def saveSplitTimes(conn,lookup, year, rally, splittimes, stage, table=('splittimesbase','splittimessplits')):
    df_splitTimes_base,df_splitTimes_splits = getSplitTimes(splittimes[stage])
    if not df_splitTimes_base.empty:
        df_splitTimes_base['stage'] = stage
    if not df_splitTimes_splits.empty:
        df_splitTimes_splits['stage'] = stage
    _saveTable(conn, df_splitTimes_base, year, rally, table[0])
    _saveTable(conn, df_splitTimes_splits, year, rally, table[1])
    
def saveStageTimes(conn,lookup, year, rally, stagetimes, stage, table=('stagetimes', 'overalltimes')):
    stagetime,overalltime = getStageTimes( stagetimes[stage])
    if not stagetime.empty: 
        stagetime['stage'] = stage
    if not overalltime.empty:
        overalltime['stage'] = stage
    _saveTable(conn, stagetime, year, rally, table[0])
    _saveTable(conn, overalltime, year, rally, table[1])

def saveChampionshipStandings(conn, year, rally, championship, typ='Drivers', table=None):
    df_standings = getChampionshipStandings(championship[typ]['path'], typ )
    
    if table is None: table='standings_{}'.format(typ.lower().replace('-',''))
    _saveTable(conn, df_standings, year, rally, table)
    


def _rallyscrape(rallies, conn):
    for rally in rallies:
        print('Grabbing data for {}'.format(rally))
        lookup = getRallyDetails(rallies, rally)
        lookupstarts = getStartinglistURLs(rallies,rally, lookup['Startlists'] )
        overall = getOverallURLs(rallies,rally, lookup['Overall'] )
        splittimes = getOverallURLs(rallies,rally, lookup['SplitTimes'] )
        stagetimes = getOverallURLs(rallies,rally, lookup['StageTimes'] )
        stagewinners = getOverallURLs(rallies,rally, lookup['Stagewinners'] )
        championship = getChampionshipURLs()
             
        saveChampionshipStandings(conn, year, rally, championship, 'Drivers')
        saveChampionshipStandings(conn, year, rally, championship, 'Co-Drivers')
        saveChampionshipStandings(conn, year, rally, championship, 'Teams')
        
        saveItinerary(conn,lookup, year, rally)
        savePenalties(conn,lookup, year, rally)
        saveRetirements(conn,lookup, year, rally)
        saveEntrylist(conn,lookup, year, rally)
        for section in lookupstarts:
            if section != 'Entry List':
                saveStartlist(conn,lookup, year, rally, section)
        saveStageWinners(conn,lookup, year, rally)
        for stage in overall:
            if stage=='Final Results':
                saveOverallFinal(conn, year, rally, overall, stage)
            else:
                saveOverall(conn, year, rally, overall, stage,)
        for stage in splittimes:
            print(stage)
            saveSplitTimes(conn,lookup, year, rally, splittimes, stage)
        for stage in stagetimes:
            try:
                saveStageTimes(conn,lookup, year, rally, stagetimes, stage)
            except:
                print('bailed on stagetimes {} {}'.format(rally, stage))

def rallyscrape(rally,dbname=None ):
    if dbname is None: dbname='rally.db'
    
    conn = sqlite3.connect(dbname)
    _rallyscrape(rally, conn)
    
def fullScrape(year='2017', url = 'http://www.wrc.com/en/wrc/results/rally-results/page/3049----.html',
               dbname='fulldb.db'):
    conn = sqlite3.connect(dbname)
    rallies = getRalliesList(url)
    _rallyscrape(rallies, conn)