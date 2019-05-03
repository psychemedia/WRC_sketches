---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.0'
      jupytext_version: 0.8.6
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
df_rally_overall['badge'] = [img.find('img')['src'] for img in tables[0].findAll("td", {"class": "final-results-icon"}) ]
df_rally_overall.dropna(how='all', axis=1, inplace=True)
df_rally_overall.head()
```

```python
df_rally_overall.columns=['Pos','CarNum','driverNav','ModelReg','Class', 'Time','GapDiff', 'Speedkm', 'badge']
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
df_rally_overall['Pos'] = df_rally_overall['Pos'].str.extract(r'(.*)\.')
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

The stage results pages returns two tables side by side (the stage result and the overall result) and a third retirements table.

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

The stages are linked relative to the website root / domain.

```python
base_url = 'https://www.ewrc-results.com'
```

Scrape the page into some bueatiful soup...:

```python
soup = soupify('{}{}'.format(base_url, links[0]))
```

Extract the tables:

```python
tables = soup.find_all('table')
stage_result = tables[0]
stage_overall = tables[1]
stage_retirements = tables[2]
```

A little helper to scrape tables in dataframes...

```python
def dfify(table):
    df = pd.read_html('<html><body>{}</body></html>'.format(table))[0]
    return df
```

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
df['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<unknown>.*)').head()
```

```python
import unicodedata

def cleanString(s):
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace with single space
    s = ' '.join(s.split())
    
    return s
    
```

The `Desc` column is scraped as `Driver Name - Navigator NameCar Model`. We can either parse these out with a camelcase pattern matcher, or perhaps more easily just scrape the original HTML using the pattern we developed abobve to scrape glad image URIs.

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

## Entry List

The entry list provides the basis for a whole set of metadata.

```python
entrylist_url = 'https://www.ewrc-results.com/entries/54762-corbeau-seats-rally-tendring-clacton-2019/'
soup = soupify(entrylist_url)
```

```python
entrylist_table = soup.find('div',{'class':'startlist'}).find('table')
df_entrylist = dfify(entrylist_table)
df_entrylist.columns = ['CarNum', 'DriverName','CoDriverName','Team','Car','Class','Meta']
df_entrylist['carNum'] = df_entrylist['CarNum'].str.extract(r'#(.*)')
df_entrylist.head()

#TO DO - flag, driverId
```

## All in One - Stage Times

The stage Times pags is a single page for pretty much all the timing data we need...

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

```python
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
#parse package (r1chardj0n3s/parse).
#!pip3 install parse
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

```python
import notebookimport
wo = __import__("WRC Overall")

```

```python
#These are in wo as well - should move to dakar utils


#TO DO - the chart should be separated out from the cols generator
# The chart function should return only the chart


#This has been changed from wo so as not to change polarity of the times
def _gapToLeaderBar(Xtmpq, typ, stages=None, milliseconds=True, flip=True):
    if milliseconds:
        Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    k = '{}GapToLeader'.format(typ)
    Xtmpq[k] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    flip = -1 if flip else 1
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [flip * y for y in x])
    Xtmpq[k] = Xtmpq[k].apply(sparkline2, typ='bar', dot=True)
    return Xtmpq 

def _positionStep(Xtmpq, typ, stages=None):
    Xtmpq.columns = ['SS_{}_{}_pos'.format(c, typ) for c in Xtmpq.columns]
    k = '{}Position'.format(typ)
    Xtmpq[k] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
    Xtmpq[k] = Xtmpq[k].apply(sparklineStep)
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

```python
#overallGapToLeader: bar chart showing overall gap to leader

tmp = pd.merge(tmp,_gapToLeaderBar(-df_overall[xcols], 'overall', stages, False, False), left_index=True, right_index=True)
s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')

#Introduce a dot marker to highlight winner
display(HTML(s2))

```

```python
#overallPosition: step line chart showing evolution of overall position

#We need to pass a position table in
xx=_positionStep(df_overall_pos[xcols], 'overall', stages)[['overallPosition']]
tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

s2 = moreStyleDriverSplitReportBaseDataframe(tmp[['overallPosition']],'')
display(HTML(s2))

```

```python
# stageWinnerGap: bar chart showing gap to stage winner
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
# Gap: bar chart showing gap relative to rebased entry
# This is just taken from the overall in the table
tmp = wo.gapBar(tmp)

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
len(df_rally_overall[:85]), df_overall.index.get_loc( '/entryinfo/54762-corbeau-seats-rally-tendring-clacton-2019/2227145/' )
#'/entryinfo/54762-corbeau-seats-rally-tendring-clacton-2019/2227145/'
```

```python
#Reporter

def rally_report(codes, rebase):
    
    tmp = pd.merge(codes, df_rally_overall[['Class']], left_index=True, right_index=True)
    
    #If we want the charts relative to overall,
    # we need to assemble them at least on cars ranked above lowest ranked car in codes
    
    #But we could optimise by getting rid of lower ranked cars
    #eg we can get the row index for a given index value as:
    #tmp.index.get_loc('/entryinfo/54762-corbeau-seats-rally-tendring-clacton-2019/2226942/')
    lastcar = tmp[-1:].index[0]
    overall_idx = df_overall.index.get_loc( lastcar )
    _df_overall = df_overall[xcols][:(overall_idx+1)]
    #Then slice to 1 past this for lowest ranked car in selection so we donlt rebase irrelevant/lower cars
    
    #Also perhaps provide an option to generate charts just relative to cars identified in codes?
    
    #overallGapToLeader: bar chart showing overall gap to leader
    tmp = pd.merge(tmp,_gapToLeaderBar(-_df_overall, 'overall', stages, False, False), left_index=True, right_index=True)

    #overallPosition: step line chart showing evolution of overall position

    #We need to pass a position table in
    xx=_positionStep(_df_overall, 'overall', stages)[['overallPosition']]
    tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

    
    # stageWinnerGap: bar chart showing gap to stage winner
    #The gapToLeaderBar needs to return the gap to the stage winner
    tmp = pd.merge(tmp,_gapToLeaderBar(-df_stages_winner[xcols], 'stages', stages, False,False), left_index=True, right_index=True)
    #In the preview the SS_N_stages bars are wrong because we have not rebased yet
    tmp.rename(columns={'stagesGapToLeader':'stageWinnerGap'},inplace=True)

    # stagePosition: step chart showing stage positions
    xx=_positionStep(df_stages_pos[xcols], 'stages', stages)['stagesPosition']
    tmp = pd.merge(tmp, xx, left_index=True, right_index=True)

    #Rebase
    cols = [c for c in tmp.columns if c.startswith('SS')]
    tmp[cols] = tmp[cols].apply(_rebaseTimes, bib=rebase, axis=0)

    # Gap: bar chart showing gap relative to rebased entry
    # This is just taken from the overall in the table
    tmp = wo.gapBar(tmp)

    
    
    moveColumn(tmp, 'stageWinnerGap', right_of='overallPosition')
    moveColumn(tmp, 'stagesPosition', right_of='overallPosition')
    moveColumn(tmp, 'Gap', right_of='overallPosition')

    moveColumn(tmp, 'overallPosition', pos=0)
    moveColumn(tmp, 'overallGapToLeader', right_of='overallPosition')
    
    tmp = pd.merge(tmp, df_rally_overall[['Pos']], left_index=True, right_index=True)
    moveColumn(tmp, 'Pos', right_of='overallGapToLeader')
    moveColumn(tmp, 'Class', pos=0)

    tmp = pd.merge(tmp, df_rally_overall[['CarNum','Class Rank']], left_index=True, right_index=True)
    moveColumn(tmp, 'Class Rank', right_of='Class')
    moveColumn(tmp, 'CarNum', pos=0)
    
    s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')
    
    return tmp, s2
    

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

```javascript
IPython.OutputArea.auto_scroll_threshold = 9999;

```

```python
import ipywidgets as widgets
from ipywidgets import interact

classes = widgets.Dropdown(
    options=['All']+df_rally_overall['Class'].unique().tolist(),
    value='All', description='Class:', disabled=False )

def carsInClass(qclass):
    #Can't we also pass a dict of key/vals to the widget?
    if qclass=='All':
        return df_rally_overall['CarNum'].to_list()
    return df_rally_overall[df_rally_overall['Class']==qclass]['CarNum'].to_list()

carNum = widgets.Dropdown(
    options=carsInClass(classes.value),
    description='Car:', disabled=False)

def update_drivers(*args):
    carlist = carsInClass(classes.value)
    carNum.options = carlist
    
classes.observe(update_drivers, 'value')

def rally_report2(cl, carNum):
    rebase = df_rally_overall[df_rally_overall['CarNum']==carNum].index[0]
    carNums = df_rally_overall[df_rally_overall['CarNum'].isin(carsInClass(cl))].index.tolist()
    codes = pd.DataFrame(carNums).rename(columns={0:'entryId'}).set_index('entryId')
    tmp, s2 = rally_report(codes, rebase)
    
    #display(HTML(s2))
    _ = dakar.getTablePNG(s2, fnstub='overall_{}_'.format(rebase.replace('/','_')),scale_factor=2)
    display(Image(_))
    print(_)
    
interact(rally_report2, cl=classes, carNum=carNum);
```

```python

```
