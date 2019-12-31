---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.0rc1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# eWRC Scraper

`ewrc-results.com` is a comprehensive rally results service.

Let's see if we can build a data downloader and generate some reports from their data...

Simple js API that filters on WRC...: https://github.com/nathanjliu/WRC-API

(For Tendring, there were also results at https://www.rallies.info/res.php?e=296 .)

```python
import pandas as pd
import re
from dakar_utils import getTime
```

```python
url= 'https://www.ewrc-results.com/final/54762-corbeau-seats-rally-tendring-clacton-2019/'
```

![](images/eWRC-final_results.png)

```python
pd.read_html(url)[0].head()
```

How do we get the nested HTML out?

*It would be nice if this issue around converters was addressed? https://github.com/pandas-dev/pandas/issues/14608 *



```python
import requests

html = requests.get(url).text
```

```python
#https://stackoverflow.com/a/31772009/454773
import lxml.html as LH
from bs4 import BeautifulSoup

soup = BeautifulSoup(html, 'lxml') # Parse the HTML as a string
    
tables = soup.find_all('table')
#tables = LH.fromstring(html).xpath('//table')
df_rally_overall = pd.read_html('<html><body>{}</body></html>'.format(tables[0]))[0]
#df_rally_overall['badge'] = [img.find('img')['src'] for img in tables[0].findAll("td", {"class": "final-results-icon"}) ]
df_rally_overall.dropna(how='all', axis=1, inplace=True)
df_rally_overall.head()
```

```python
#df_rally_overall.columns=['Pos','CarNum','driverNav','ModelReg','Class', 'Time','GapDiff', 'Speedkm', 'badge']
df_rally_overall.columns=['Pos','CarNum','driverNav','ModelReg','Class', 'Time','GapDiff', 'Speedkm']
```

```python
#Get the entry ID - use this as the unique key
#in column 3, <a title='Entry info and stats'>
df_rally_overall['entryId']=[a['href'] for a in tables[0].findAll("a", {"title": "Entry info and stats"}) ]
df_rally_overall.set_index('entryId', inplace=True)
df_rally_overall.head()
```

```python
df_rally_overall[['Driver','CoDriver']] = df_rally_overall['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')
df_rally_overall.head()
```

```python
df_rally_overall['Historic']= df_rally_overall['Class'].str.contains('Historic')
df_rally_overall['Class']= df_rally_overall['Class'].str.replace('Historic','')
df_rally_overall['Class'].unique()
```

```python
df_rally_overall['Pos'] = df_rally_overall['Pos'].astype(str).str.extract(r'(.*)\.')
df_rally_overall['Pos'] = df_rally_overall['Pos'].astype(int)
df_rally_overall.dtypes
```

```python
df_rally_overall[['Model','Registration']]=df_rally_overall['ModelReg'].str.extract(r'(?P<Model>.*) \((?P<Registration>.*)\)')
df_rally_overall.head()
```

```python
df_rally_overall.tail()
```

```python
df_rally_overall["Class Rank"] = df_rally_overall.groupby("Class")["Pos"].rank(method='min')

df_rally_overall.head()
```

```python
df_rally_overall.tail()
```

```python
#Car registration not a reliable match
_entries = pd.read_csv('corbeau19_entries_archive.csv')
_entries['Driver Reverse'] = _entries['Driver'].str.replace(r'(.+)\s+([^\s]+)$', r'\2 \1')
_entries.head()
```

```python
#All the driver names are in the _entries but with minto string match differences
set(df_rally_overall['Driver'])-set(_entries['Driver Reverse'])
```

```python
#TO DO - pull out retirements table
```

## Stage Results

Get results by stage.

The stage results pages returns two tables side by side (the stage result and the overall result) potentially along with retirements and penalties tables.

```python
#Stage results
url='https://www.ewrc-results.com/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
```

First, let's get the list of stages:

```python
def soupify(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml') # Parse the HTML as a string
    return soup

#If navigation remains constant, items are in third list
links=[]

soup = soupify(url)
for li in soup.find_all('ul')[2].find_all('li'):
    #if 'class' in li.attrs:
    #    print(li['class'])
    #A class is set for service but not other things
    if 'class' not in li.attrs:
        a = li.find('a')
        if 'href' in a.attrs:
            links.append(a['href'])

links
```

```python
details = soup.find('h4').text
details
```

```python
#parse package (r1chardj0n3s/parse).
#!pip3 install parse
```

```python
from parse import parse

pattern = 'SS{stage} {name} - {dist:f} km - {datetime}'
parse_result = parse(pattern, details)

stage_num = f"SS{parse_result['stage']}"
stage_name = parse_result['name']
stage_dist =  parse_result['dist']
stage_datetime = parse_result['datetime']

stage_num, stage_name, stage_dist, stage_datetime
```

The stages are linked relative to the website root / domain.

```python
base_url = 'https://www.ewrc-results.com'
```

Scrape the page into some beautiful soup...:

```python
soup = soupify('{}{}'.format(base_url, links[0]))
```

A little helper to scrape tables in dataframes...

```python
def dfify(table):
    df = pd.read_html('<html><body>{}</body></html>'.format(table))[0]
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    return df
```

Extract the tables:

```python
tables = soup.find_all('table')
stage_result = tables[0]
stage_overall = tables[1]
```

### Stage Result...

```python
df = dfify(stage_result)
df.columns=['Pos','CarNum','Desc','Class', 'Time','GapDiff', 'Speedkm']
df.head()
```

Some of the columns contain multiple items. We need to convert these to separate columns.

```python
#https://stackoverflow.com/a/39358924/454773
#Extract out gap to leader and difference to car ahead
#First, add dummy values to the first (NA) row, then hack a strategy for splitting

#TO DO - need to convert from time string...
df['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'}).head()

#TODO - timify these cols
```

```python
def diffgapsplitter(col):
    #Normalise
    col=col.fillna('+0+0')
    #Remove leading +
    col=col.str.strip('+')
    #Split...
    col = col.str.split('+',expand=True)
    #Rename columns
    col = col.rename(columns={0:'Gap', 1:'Diff'})
    #Convert to numerics
    col['Gap'] = col['Gap'].apply(getTime)#.astype(float)
    col['Diff'] = col['Diff'].apply(getTime)
    return col

```

```python
df[['Gap','Diff']] = diffgapsplitter(df['GapDiff'])
df.head()
```

I'm not sure what the two elements in the `Speedkm` column are. The first appears to be speed but I'm not sure about the second one?

We can split those columns out on a regular expression.

```python
df['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)').head()
```

```python
import unicodedata

def cleanString(s):
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace with single space
    s = ' '.join(s.split())
    
    return s
    
```

The `Desc` column is scraped as `Driver Name - Navigator NameCar Model`. We can either parse these out with a camelcase pattern matcher, or perhaps more easily just scrape the original HTML using the pattern we developed above.

```python
rows=[]
for d in stage_result.findAll("td", {"class": "stage-results-drivers"}):
    entryId = d.find('a')['href']
    #print(str(d)) #This gives us the raw HTML in the soup element
    driverNav = d.find('a').text.split('-')
    model=d.find('a').nextSibling.nextSibling
    rows.append( {'entryId':entryId,
                   'model':model,
                  'driver':cleanString(driverNav[0]),
                  'navigator':cleanString(driverNav[1])}) 
    
df[['driver','entryId','model','navigator']] = pd.DataFrame(rows)
df.head()
```

```python
print(df['CarNum'].tolist())
```

### Stage Overall...

```python
df_overall = dfify(stage_overall)

cols = ['PosChange', 'CarNum', 'Desc','Class', 'Time', 'GapDiff', 'Speedkm' ]
df_overall.columns = cols

df_overall[['Pos','Change']] = df_overall['PosChange'].str.extract(r'(?P<Pos>[\d]*)\.\s?(?P<Change>.*)?')

df_overall['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'})
df_overall[['Gap','Diff']] = diffgapsplitter(df_overall['GapDiff'])
df_overall[['Speed','Dist']] = df_overall['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)')

df_overall
```

### Penalties...

```python
cols = ['CarNum', 'driverNav', 'Model', 'Status']
extra_cols = ['Driver', 'CoDriver', 'Stage']
retirements = pd.DataFrame(columns=cols+extra_cols)
retired = soup.find('div',{'class':'retired-inc'})
if retired:
    retirements = dfify(retired.find('table'))
    retirements.columns = cols
    retirements[['Driver','CoDriver']] = retirements['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')
    retirements['Stage'] = stage_num
retirements
```

### Retirements...

```python
cols = ['CarNum', 'driverNav', 'Model', 'PenReason']
extra_cols = ['Driver', 'CoDriver', 'Stage']
penalties = pd.DataFrame(columns=cols+extra_cols)

penalty = soup.find('div',{'class':'penalty-inc'})
if penalty:
    penalties = dfify(penalty.find('table'))
    penalties.columns = cols
    penalties[['Driver','CoDriver']] = penalties['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')
    penalties[['Time','Reason']] = penalties['PenReason'].str.extract(r'(?P<Time>[^\s]*)\s+(?P<Reason>.*)')
    penalties['Stage'] = stage_num
    
penalties
```

## Entry List

The entry list provides the basis for a whole set of metadata.

```python
entrylist_url = 'https://www.ewrc-results.com/entries/54762-corbeau-seats-rally-tendring-clacton-2019/'
entrylist_url = "https://www.ewrc-results.com/entries/42870-rallye-automobile-de-monte-carlo-2018/"
soup = soupify(entrylist_url)
```

```python
entrylist_table = soup.find('div',{'class':'startlist'}).find('table')
df_entrylist = dfify(entrylist_table)
df_entrylist
```

```python
base_cols = ['CarNum', 'DriverName','CoDriverName','Team','Car','Class']
for i in range(len(df_entrylist.columns) - len(base_cols)):
    base_cols.append(f'Meta_{i}')
df_entrylist.columns = base_cols
df_entrylist['carNum'] = df_entrylist['CarNum'].str.extract(r'#(.*)')
df_entrylist.head()

#TO DO - flag, driverId
```

For WRC at least, we can also get the starting order for each leg.


We have starting orders for legs if there is an `<h2>` element containing `Starting order`:

```python
if soup.find('h2', text='Starting order'):
    print('Starting order available...')
```

## Itinerary

For WRC at least, the itinerary breaks out the legs, which is useful when we are looking at statistics that relate to starting order / road order (RO) as obtained from the entry list.

```python
itinerary_url = 'https://www.ewrc-results.com/timetable/42870-rallye-automobile-de-monte-carlo-2018/itinerary_url'
#itinerary_url = 'https://www.ewrc-results.com/timetable/54762-corbeau-seats-rally-tendring-clacton-2019/'
```

```python
soup = soupify(itinerary_url)
```

```python
event_dist = soup.find('td',text='Event total').parent.find_all('td')[-1].text
event_dist
```

```python
from numpy import nan

itinerary_df = dfify( soup.find('div', {'class':'timetable'}).find('table') )
itinerary_df.columns = ['Stage','Name', 'Distance', 'Date', 'Time']
itinerary_df['Leg'] = [nan if 'leg' not in str(x) else str(x).replace('. leg','') for x in itinerary_df['Stage']]
itinerary_df['Leg'] = itinerary_df['Leg'].fillna(method='ffill')
itinerary_df['Date'] = itinerary_df['Date'].fillna(method='ffill')

itinerary_leg_totals = itinerary_df[itinerary_df['Name'].str.contains("Leg total")][['Leg', 'Distance']].reset_index(drop=True)

full_itinerary_df = itinerary_df[~itinerary_df['Name'].str.contains(". leg")]
full_itinerary_df = full_itinerary_df[~full_itinerary_df['Date'].str.contains(" km")]
full_itinerary_df = full_itinerary_df.fillna(method='bfill', axis=1)

#Legs may not be identified but we may want to identify services
full_itinerary_df['Service'] = [ 'Service' in i for i in full_itinerary_df['Distance'] ]
full_itinerary_df['Service_Num'] = full_itinerary_df['Service'].cumsum()
full_itinerary_df.reset_index(drop=True, inplace=True)
itinerary_df = full_itinerary_df[~full_itinerary_df['Service']].reset_index(drop=True)

itinerary_df
```

We can also add in the day number  by counting separate dates.

```python
# Add in event day number

# TO DO
```

```python
full_itinerary_df
```

```python
itinerary_leg_totals
```

## All in One - Stage Times

The Stage Times page is a single page for pretty much all the timing data we need...

```python
url='https://www.ewrc-results.com/times/54762-corbeau-seats-rally-tendring-clacton-2019/'
soup = soupify(url)
```

```python
times = soup.find('div',{'class':'times'}).findChildren('div' , recursive=False)

#The rows are essentially grouped in twos after the header row
cols = [c.text for c in times[0].findAll('div')]
cols
```

```python
#https://stackoverflow.com/a/2231685/454773
groupsize=2
groups = [times[i:i+groupsize] for i in range(1, len(times), groupsize)] 
```

```python
_pos=71


NAME_SUBGROUP = 0
TIME_SUBGROUP = 1

groups[_pos][NAME_SUBGROUP], groups[_pos][TIME_SUBGROUP]
```

```python
driverNav = cleanString(groups[_pos][NAME_SUBGROUP].find('a').text)
driver,navigator = driverNav.split(' - ')
driver,navigator
```

```python
#Extract the car number
carNumMatch = lambda txt: re.search('#(?P<carNum>[\d]*)', cleanString(txt))
carNum = carNumMatch(groups[_pos][NAME_SUBGROUP]).group('carNum')
carNum
```

```python run_control={"marked": false}
#Extract the car model
carModelMatch = lambda txt:  re.search('</a>\s*(?P<carModel>.*)</div>', cleanString(txt))
carModel = carModelMatch(groups[_pos][NAME_SUBGROUP]).group('carModel')
carModel
```

```python
entryId = groups[_pos][NAME_SUBGROUP].find('a')['href']
entryId
```

```python
#retired
retired = '<span class="r8_bold_red">R</span>' in cleanString(groups[_pos][NAME_SUBGROUP])
retired
```

```python
position = groups[_pos][NAME_SUBGROUP].find('span').text
position
```

```python
#cartimes

for c in groups[_pos][TIME_SUBGROUP].findAll('div')[:-1]:
    #Last row is distinct
    print(cleanString(c))
```

```python
from parse import parse
pattern = '''<div class="times-one-time">{stagetime}<br/><span class="times-after">{overalltime}</span><br/>{pos}</div>'''

txt = groups[_pos][TIME_SUBGROUP].findAll('div')[4]
#Tidy up
txt = cleanString(txt)
parse(pattern, txt )

```

```python
#Parsing times
#Times of form: minutes:seconds.tenths
```

```python
t=[]
i=0

penaltypattern='class="r7_bold_red">{penalty}</span>'

for g in groups:
    i=i+1
    driverNav_el = g[NAME_SUBGROUP].find('a')
    driverNav = driverNav_el.text
    driver,navigator = driverNav.split(' - ')
    entryId = driverNav_el['href']
    retired = '<span class="r8_bold_red">R</span>' in str(g[NAME_SUBGROUP])
    carNum = carNumMatch(g[NAME_SUBGROUP]).group('carNum')
    carModel = carModelMatch(g[NAME_SUBGROUP]).group('carModel')
    classification = pd.to_numeric(g[NAME_SUBGROUP].find('span').text.replace('R','').strip('').strip('.'))
    
    stagetimes = []
    overalltimes = []
    penalties=[]
    positions = []
    
    for stages in g[TIME_SUBGROUP].findAll('div'):
        txt = cleanString(stages)
        stagetimes_data = parse(pattern, txt )
        if stagetimes_data:
            stagetimes.append(stagetimes_data['stagetime'])
            overalltimes.append(stagetimes_data['overalltime'])
            
            #Need to parse this
            #There may be penalties in the pos
            penalty = 0
            p = stagetimes_data['pos'].split()
            if p[-1].endswith('</span>'):
                penalty = parse(penaltypattern, p[-1] )
                if penalty:
                    #This really needs parsing into a time; currently of form eg 0:10
                    penalty = penalty['penalty']
                p = int(p[-2].split('.')[0])
            else:
                p = int(p[-1].strip('.'))
            positions.append(p)
            penalties.append(penalty)
                        
    t.append({'entryId':entryId,
              'driver':driver.strip(),
             'navigator':navigator.strip(),
              'carNum':carNum,
              'carModel':carModel,
              'retired':retired,
              'Pos': classification,
             'stagetimes':stagetimes,
             'overalltimes':overalltimes,
             'positions':positions, 'penalties':penalties})


df = pd.DataFrame(t).set_index(['entryId'])
df.head()
```

```python
df.tail()
```

For a comprehensive table, we need to pull in things like `Class` into this table.

```python
df_overall = pd.DataFrame(df['overalltimes'].tolist(), index= df.index)
df_overall.columns = range(1,df_overall.shape[1]+1)
df_overall.head()
```

```python
df_overall.tail()
```

```python
df_overall_pos = pd.DataFrame(df['positions'].tolist(), index= df.index)
df_overall_pos.columns = range(1,df_overall_pos.shape[1]+1)
df_overall_pos.head()
```

```python
df_stages = pd.DataFrame(df['stagetimes'].tolist(), index= df.index)
#df_stages.columns = cols
df_stages.columns = range(1,df_stages.shape[1]+1)
df_stages.head()
```

```python
xcols = df_overall.columns

for ss in xcols:
    df_overall[ss] = df_overall[ss].apply(getTime)
    df_stages[ss] = df_stages[ss].apply(getTime)
    
df_overall.head()
```

# Give the numbers a spin

Let's see what we need to do to get the data into a form we can plot it using pre-existing tools...

```python
import dakar_utils as dakar
from dakar_utils import moveColumn, sparkline2, sparklineStep, moreStyleDriverSplitReportBaseDataframe

```

<!-- #raw -->
import notebookimport
wo = __import__("WRC Overall")

<!-- #endraw -->

```python
import io

#Dask may require dataframe spec
#But what about multi-index dataframes?
def schema_df_create(df):
    ''' Generate a an empty data frame according to the schema of a pre-existing data frame. '''
    
    idx_name = df.index.name
    idx_type = df.index.dtype
    
    cnames = [idx_name]+[c for c in df.columns]
    zz = pd.read_csv(io.StringIO(""),
                     names=cnames,
                     dtype=dict(zip(cnames,[idx_type]+df.dtypes.to_list())),
                     index_col=[idx_name])
    #print(zz.info())
    return zz

def schema_df_drop(df):
    ''' Create an empty dataframe with a schema of an existing dataframe
        by dropping all data from the existing dataframe.
    '''
    return df.drop(df.index)

```

```python
#Can we improve performance?
#https://www.ellicium.com/python-multiprocessing-pool-process/
from multiprocessing import cpu_count

num_cores = cpu_count()

#https://towardsdatascience.com/how-i-learned-to-love-parallelized-applies-with-python-pandas-dask-and-numba-f06b0b367138
#!pip3 install dask
#!pip3 install cloudpickle
from dask import dataframe as dd

```

```python
#These are in wo as well - should move to dakar utils


#TO DO - the chart should be separated out from the cols generator
# The chart function should return only the chart

#This has been changed from wo so as not to change polarity of the times
def _gapToLeaderBar(Xtmpq, typ, stages=None, milliseconds=True, flip=True, items=None):
    if milliseconds:
        Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    _tmp='_tmp'
    k = '{}GapToLeader'.format(typ)
    Xtmpq[_tmp] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    flip = -1 if flip else 1
    Xtmpq[_tmp] = Xtmpq[_tmp].apply(lambda x: [flip * y for y in x])
    #Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    #Chart generation is the slow step, so only do it where we need it
    if items is None:
        #Xtmpq[k] = Xtmpq[_tmp].apply(sparkline2, typ='bar', dot=True)
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        Xtmpq[k]=dd.from_pandas(Xtmpq[_tmp],npartitions=num_partitions).map_partitions( lambda df : df.apply( lambda x : sparkline2(x, typ='bar', dot=True)), 
                                                                                       meta=pd.Series(dtype=object)).compute(scheduler='processes')

    else:
        #Ony generate charts for specified rows
        #Create a dummy col
        Xtmpq[k]='s'
        #Use loc for index vals, iloc for row number
        #If the items are passed as dataframe index, we need to convert to a list
        if isinstance(items,pd.core.frame.DataFrame):
            items = items.index.to_list()
        #Xtmpq.loc[items,k] = Xtmpq[_tmp].loc[items].apply(sparkline2, typ='bar', dot=True)
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        Xtmpq.loc[items,k]=dd.from_pandas(Xtmpq[_tmp].loc[items],npartitions=num_partitions).map_partitions( lambda df : df.apply( lambda x : sparkline2(x, typ='bar', dot=True)), 
                                                                                       meta=pd.Series(dtype=object)).compute(scheduler='processes')

    Xtmpq = Xtmpq.drop(_tmp, 1)
    return Xtmpq 

import time
def _positionStep(Xtmpq, typ, stages=None, items=None):
    Xtmpq.columns = ['SS_{}_{}_pos'.format(c, typ) for c in Xtmpq.columns]
    k = '{}Position'.format(typ)
    _tmp='tmp'
    Xtmpq[_tmp] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[_tmp] = Xtmpq[_tmp].apply(lambda x: [-y for y in x])
    if items is None:
        t0 = time.time()
        #Xtmpq[k] = Xtmpq[_tmp].apply(sparklineStep)
        t1 = time.time()
        #print("Time to process without Dask {}".format(t1-t0))
        num_partitions = num_cores if num_cores < len(Xtmpq[_tmp]) else len(Xtmpq[_tmp])
        t0 = time.time()
        Xtmpq[k]=dd.from_pandas(Xtmpq[_tmp],npartitions=num_partitions).map_partitions( lambda df : df.apply(sparklineStep), meta=pd.Series(dtype=object)).compute(scheduler='processes')
        t1 = time.time()
        #print("Time to process with Dask {}".format(t1-t0))
        #scheduler='single-threaded | threads | processes')
    else:
        Xtmpq[k]=''
        if isinstance(items,pd.core.frame.DataFrame):
            items = items.index.to_list() 
        #Xtmpq.loc[items, k]= Xtmpq[_tmp].loc[items].apply(sparklineStep)
        num_partitions =num_cores if num_cores<len(items) else len(items)
        Xtmpq.loc[items, k]=dd.from_pandas(Xtmpq[_tmp].loc[items],npartitions=num_partitions).map_partitions( lambda df : df.apply(sparklineStep), meta=pd.Series(dtype=object)).compute(scheduler='processes')
        
    Xtmpq = Xtmpq.drop(_tmp, 1)
    return Xtmpq 

```

```python
#ccols = [c for c in df_overall.columns if c.startswith('SS') ]
```

```python
def _rebaseTimes(times, bib=None, basetimes=None):
    ''' Rebase times relative to specified driver. '''
    #Should we rebase against entryId, so need to lool that up. In which case, leave index as entryId
    if bib is None and basetimes is None: return times
    #bibid = codes[codes['Code']==bib].index.tolist()[0]
    if bib is not None:
        return times - times.loc[bib]
    if times is not None:
        return times - basetimes
    return times
```

There are five cross-stage reports we can add in:

- `overallPosition`: step line chart showing evolution of overall position
- `overallGapToLeader`: bar chart showing overall gap to leader
- `stagePosition`: step chart showing stage positions
- `stageWinnerGap`: bar chart showing gap to stage winner
- `Gap`: bar chart showing gap relative to rebased entry

```python
df_overall.head()
```

```python
#Need to refactor the times in the tables to gaps

#The overall times need rebasing to the overall leader at each stage
leaderTimes = df_overall.min()#iloc[0]
df_overall[xcols] = df_overall[xcols].apply(_rebaseTimes, basetimes=leaderTimes, axis=1)

df_overall.head()
```

```python
df_stages.head()
```

```python
#We need to finesse the stage positions from the stage times... or get them from elsewhere
df_stages_pos = df_stages.rank(method='min')
df_stages_pos.columns = range(1,df_stages_pos.shape[1]+1)
df_stages_pos.head()
```

```python
df_stages_pos.tail()
```

```python
#A little test that ranking on the overall positions pase on times matches overall rankings in data
#_df_overall_pos = df_overall.rank(method='min', ascending=False)
#_df_overall_pos.equals(df_overall_pos)
```

```python
#The stage times need rebasing to the overall leader
#Gap to overall leader
leaderStagetimes = df_stages.iloc[0]

df_stages[xcols] = df_stages[xcols].apply(_rebaseTimes, basetimes=leaderStagetimes, axis=1)

df_stages.head()
```

```python
df_stages_winner = df_stages[xcols].apply(_rebaseTimes, basetimes=df_stages.min(), axis=1)
df_stages_winner.head()
```

```python
from IPython.display import HTML
```

```python
#Use a codes list to control display df
#Limit for testing...
codes = pd.DataFrame(df_stages.index.tolist()).rename(columns={0:'entryId'}).set_index('entryId').head()
codes
```

```python
tmp = pd.merge(codes, df_rally_overall[['Class']], left_index=True, right_index=True)
```

Generating the inline charts is really slow.... TO DO: check to see if there are any optimitsations...

```python
#overallGapToLeader: bar chart showing overall gap to leader

tmp = pd.merge(tmp,_gapToLeaderBar(-df_overall[xcols], 'overall', stages, False, False), left_index=True, right_index=True)
s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')

#Introduce a dot marker to highlight winner
display(HTML(s2))

```

```python
#overallPosition: step line chart showing evolution of overall position
#For retired cars, this will return an appropriately cut-off image

#We need to pass a position table in
xx=_positionStep(df_overall_pos[xcols], 'overall', stages)[['overallPosition']]
tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp[['overallPosition']],'')
display(HTML(s2))

```

```python
df_overall_pos.tail()
```

```python
# stageWinnerGap: bar chart showing gap to stage winner
#For retired cars, this will return an appropriately cut-off image
#The gapToLeaderBar needs to return the gap to the stage winner
tmp = pd.merge(tmp,_gapToLeaderBar(-df_stages_winner[xcols], 'stages', stages, False,False), left_index=True, right_index=True)
#In the preview the SS_N_stages bars are wrong because we have not rebased yet
tmp.rename(columns={'stagesGapToLeader':'stageWinnerGap'},inplace=True)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp[['stageWinnerGap']],'')
display(HTML(s2))
```

```python
# stagePosition: step chart showing stage positions
xx=_positionStep(df_stages_pos[xcols], 'stages', stages)['stagesPosition']
tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp[['stagesPosition']],'')
display(HTML(s2))
```

```python
#Need final pos column

#Where are penalties handled
```

```python
#We always need to rebase to ensure that timing cols are correct
rebase = '/entryinfo/54762-corbeau-seats-rally-tendring-clacton-2019/2162347/'
```

```python
#rebase
cols = [c for c in tmp.columns if c.startswith('SS')]
tmp[cols] = tmp[cols].apply(_rebaseTimes, bib=rebase, axis=0)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
display(HTML(s2))
```

```python
## gapBar looks simple? From wo:
def gapBar(df):
    ''' Bar chart showing rebased gap at each stage. '''
    col='Gap'
    df[col] = df[[c for c in df.columns if c.startswith('SS_') and c.endswith('_overall')]].values.tolist()
    df[col] = df[col].apply(lambda x: [-y for y in x])
    df[col] = df[col].apply(sparkline2, typ='bar', dot=False)
    return df
```

```python
# Gap: bar chart showing gap relative to rebased entry
# This is just taken from the overall in the table
tmp = gapBar(tmp)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
display(HTML(s2))
```

```python
moveColumn(tmp, 'stageWinnerGap', right_of='overallPosition')
moveColumn(tmp, 'stagesPosition', right_of='overallPosition')
moveColumn(tmp, 'Gap', right_of='overallPosition')

moveColumn(tmp, 'overallPosition', pos=0)
moveColumn(tmp, 'overallGapToLeader', right_of='overallPosition')
s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
display(HTML(s2))
```

```python
tmp = pd.merge(tmp, df_rally_overall[['Pos']], left_index=True, right_index=True)
moveColumn(tmp, 'Pos', right_of='overallGapToLeader')
moveColumn(tmp, 'Class', pos=0)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
display(HTML(s2))
```

```python
tmp = pd.merge(tmp, df_rally_overall[['Class Rank']], left_index=True, right_index=True)
moveColumn(tmp, 'Class Rank', right_of='Class')

s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
display(HTML(s2))
```

```python
codes = pd.DataFrame(df_rally_overall[df_rally_overall['Class']=='C'].index.tolist()).rename(columns={0:'entryId'}).set_index('entryId')
codes

```

```python
wREBASE=codes.iloc[10].name
wREBASE
```

```python
df.columns
```

```python
#Reporter

def rally_report(codes, rebase):
    #rebase is the index value
    #tmp = pd.merge(codes, df_rally_overall[['Class']], how='left', left_index=True, right_index=True)
    tmp = pd.merge(codes, df[['carNum']], how='left', left_index=True, right_index=True)
    #tmp = pd.merge(tmp, df_entrylist[['Class','carNum']], how='left', on='carNum')
    #https://stackoverflow.com/a/11982843/454773
    tmp = tmp.reset_index().merge(df_entrylist[['Class','carNum']], how="left", on='carNum').set_index('entryId')
    print(tmp[-1:].index)
    #If we want the charts relative to overall,
    # we need to assemble them at least on cars ranked above lowest ranked car in codes
    
    #But we could optimise by getting rid of lower ranked cars
    #eg we can get the row index for a given index value as:
    #tmp.index.get_loc('/entryinfo/54762-corbeau-seats-rally-tendring-clacton-2019/2226942/')
    lastcar = tmp[-1:].index[0]
    print(lastcar)
    overall_idx = df_overall.index.get_loc( lastcar )
    #Then slice to 1 past this for lowest ranked car in selection so we don't rebase irrelevant/lower cars
    
    #Also perhaps provide an option to generate charts just relative to cars identified in codes?
    
    #overallGapToLeader: bar chart showing overall gap to leader
    tmp = pd.merge(tmp,_gapToLeaderBar(-df_overall[xcols][:(overall_idx+1)], 'overall', stages, False, False, codes), left_index=True, right_index=True)
    print(tmp[-1:].index)
    #overallPosition: step line chart showing evolution of overall position
    
    #We need to pass a position table in
    xx=_positionStep(df_overall_pos[xcols][:(overall_idx+1)], 'overall', stages, codes)[['overallPosition']]
    tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

    # stageWinnerGap: bar chart showing gap to stage winner
    #The gapToLeaderBar needs to return the gap to the stage winner
    tmp = pd.merge(tmp,_gapToLeaderBar(-df_stages_winner[xcols][:(overall_idx+1)], 'stages', stages, False,False, codes), left_index=True, right_index=True)
    #In the preview the SS_N_stages bars are wrong because we have not rebased yet
    tmp.rename(columns={'stagesGapToLeader':'stageWinnerGap'},inplace=True)

    # stagePosition: step chart showing stage positions
    xx=_positionStep(df_stages_pos[xcols][:(overall_idx+1)], 'stages', stages, codes)['stagesPosition']
    tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

    #Rebase
    cols = [c for c in tmp.columns if c.startswith('SS')]
    tmp[cols] = tmp[cols].apply(_rebaseTimes, bib=rebase, axis=0)

    # Gap: bar chart showing gap relative to rebased entry
    # This is just taken from the overall in the table
    #The gap should be ignored for the rebased driver?
    tmp = gapBar(tmp)
    print('v',tmp[-1:].index)

    moveColumn(tmp, 'stageWinnerGap', right_of='overallPosition')
    moveColumn(tmp, 'stagesPosition', right_of='overallPosition')
    moveColumn(tmp, 'Gap', right_of='overallPosition')

    moveColumn(tmp, 'overallPosition', pos=0)
    moveColumn(tmp, 'overallGapToLeader', right_of='overallPosition')
    print('w',tmp[-1:].index)
    tmp = pd.merge(tmp, df_rally_overall[['Pos']], how='left', left_index=True, right_index=True)
    moveColumn(tmp, 'Pos', right_of='overallGapToLeader')
    moveColumn(tmp, 'Class', pos=0)
    print('x',tmp[-1:].index)
    #tmp = pd.merge(tmp, df_rally_overall[['CarNum','Class Rank']], how='left', left_index=True, right_index=True)
    tmp = pd.merge(tmp, df_rally_overall[['Class Rank']], how='left', left_index=True, right_index=True)
    moveColumn(tmp, 'Class Rank', right_of='Class')
    moveColumn(tmp, 'carNum', pos=0)
    #disambiguate carnum
    tmp['carNum'] = '#'+tmp['carNum'].astype(str)
    tmp = tmp.rename(columns={'carNum': 'CarNum'})
    print('y',tmp[-1:].index)
    
    #is this the slow bit?
    print('styling...')
    s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
    print('...done')
    return tmp, s2
    


```

```python
tmp, s2 = rally_report(codes, wREBASE)
display(HTML(s2))
```

```python
dakar.getTablePNG(s2, fnstub='overall_{}_'.format(wREBASE.replace('/','_')),scale_factor=2)
```

```python
from IPython.display import Image
Image(_)
```

```python
#TOP 20 overall
codes = pd.DataFrame(df_rally_overall[:20].index.tolist()).rename(columns={0:'entryId'}).set_index('entryId')
wREBASE=codes.iloc[7].name
print(wREBASE)
codes

```

```python
tmp, s2 = rally_report(codes, wREBASE)
_ = dakar.getTablePNG(s2, fnstub='overall_{}_'.format(wREBASE.replace('/','_')),scale_factor=2)
Image(_)
```

```python
_
```

## Widgets Example


Full results should cope with retirements because they affect stage rankings?

```javascript
IPython.OutputArea.auto_scroll_threshold = 9999;

```

```python
import ipywidgets as widgets
from ipywidgets import interact

classes = widgets.Dropdown(
    #Omit car 0
    options=['All']+df_entrylist[df_entrylist['CarNum']!='#0']['Class'].unique().tolist(),
    value='All', description='Class:', disabled=False )

def carsInClass(qclass):
    #Can't we also pass a dict of key/vals to the widget?
    #Omit car 0
    if qclass=='All':
        return df_entrylist[df_entrylist['CarNum']!='#0']['carNum'].to_list()
    return df_entrylist[df_entrylist['CarNum']!='#0' & df_entrylist['Class']==qclass]['carNum'].to_list()

carNum = widgets.Dropdown(
    options=carsInClass(classes.value),
    description='Car:', disabled=False)

def update_drivers(*args):
    carlist = carsInClass(classes.value)
    carNum.options = carlist
    
classes.observe(update_drivers, 'value')

def rally_report2(cl, carNum):
    #rebase = df_rally_overall[df_rally_overall['CarNum']==carNum].index[0]
    #carNums = df_rally_overall[df_rally_overall['CarNum'].isin(carsInClass(cl))].index.tolist()
    rebase = df[df['carNum']==carNum].index[0]

    carNums = df[df['carNum'].isin(carsInClass(cl))].index.tolist()

    codes = pd.DataFrame(carNums).rename(columns={0:'entryId'}).set_index('entryId')
    print(codes[-1:])
    tmp, s2 = rally_report(codes, rebase)

    #display(HTML(s2))
    rally_logo='<img width="100%" src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/CSRTC-Logo-Banner-2019-01-1920x600-e1527255159629.jpg"/>'
    #rallydj_logo='<img style="float: left; src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/rallydj.png"/>'
    #datasrc_logo='<img style="background-color:black;float: right;" src="/Users/tonyhirst/Documents/GitHub/WRC_sketches/doodles/images/ewrcresults800.png"/>'
    #bottom_logos='<div>'+rallydj_logo+datasrc_logo+'</div>'
    footer='<div style="margin-top:50px;margin-bottom:20px">Results and timing data sourced from <em>ewrc-results.com</em>. Chart generated by <em>rallydatajunkie.com</em>.</div>'
    #footer1=bottom_logos
    inclass='' if cl=='All' else ' (Class {})'.format(cl)
    title='<div><h1>Overall Results'+inclass+'</h1><p>Times rebased relative to car {}.</p></div>'.format(carNum)
    
    html='<div style="font-family:sans-serif;margin-top:10px;margin-bottom:10px"><div style="margin-top:10px;margin-bottom:50px;">'+rally_logo+'</div>'
    html = html+'<div style="margin-left:20px;margin-right:20px;">'+title+s2+'</div>'+footer+'</div>'
    
    print('grabbing screenshot...')
    _ = dakar.getTablePNG(html, fnstub='overall_{}_'.format(rebase.replace('/','_')),scale_factor=2)
    print('...done')
    display(Image(_))
    print(_)
    
interact(rally_report2, cl=classes, carNum=carNum);
```

```python
#last is 2236623
```

```python
df_entrylist.head()
```

```python
df.head()
```

```python

```
