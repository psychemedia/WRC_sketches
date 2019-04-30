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
```

```python
url= 'https://www.ewrc-results.com/final/46492-corbeau-seats-rally-tendring-clacton-2018/'
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
df = pd.read_html('<html><body>{}</body></html>'.format(tables[0]))[0]
df['badge'] = [img.find('img')['src'] for img in tables[0].findAll("td", {"class": "final-results-icon"}) ]
df.head()
```

## Stage Results

Get results by stage.

The stage results pages returns two tables side by side (the stage result and the overall result) and a third retirements table.

```python
#Stage results
url='https://www.ewrc-results.com/results/46492-corbeau-seats-rally-tendring-clacton-2018/'
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
#First, add dummy values to the first (NA) row, then hack a stragety for splitting
df['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'}).astype(float).head()

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
    col = col.astype(float)
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

## All in One - Stage Times

The stage Times pags is a single page for pretty much all the timing data we need...

```python
url='https://www.ewrc-results.com/times/46492-corbeau-seats-rally-tendring-clacton-2018/'
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
pos=71


NAME_SUBGROUP = 0
TIME_SUBGROUP = 1

groups[pos][NAME_SUBGROUP], groups[pos][TIME_SUBGROUP]
```

```python
import unicodedata

def cleanString(s):
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace with single space
    s = ' '.join(s.split())
    
    return s
    
```

```python
driverNav = cleanString(groups[pos][NAME_SUBGROUP].find('a').text)
driver,navigator = driverNav.split(' - ')
driver,navigator
```

```python
import re


#Extract the car number
carNumMatch = lambda txt: re.search('#(?P<carNum>[\d]*)', cleanString(txt))
carNum = carNumMatch(groups[pos][NAME_SUBGROUP]).group('carNum')
carNum
```

```python
#Extract the car model
carModelMatch = lambda txt:  re.search('</a>\s*(?P<carModel>.*)</div>', cleanString(txt))
carModel = carModelMatch(groups[pos][NAME_SUBGROUP]).group('carModel')
carModel
```

```python
entryId = groups[pos][NAME_SUBGROUP].find('a')['href']
entryId
```

```python
#retired
retired = '<span class="r8_bold_red">R</span>' in cleanString(groups[pos][NAME_SUBGROUP])
retired
```

```python
#cartimes

for c in groups[pos][TIME_SUBGROUP].findAll('div')[:-1]:
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

txt = groups[pos][TIME_SUBGROUP].findAll('div')[4]
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

    stagetimes = []
    overalltimes = []
    penalties=[]
    pos = []
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
            pos.append(p)
            penalties.append(penalty)
                        
    t.append({'entryId':entryId,
              'driver':driver.strip(),
             'navigator':navigator.strip(),
              'carNum':carNum,
              'carModel':carModel,
              'retired':retired,
             'stagetimes':stagetimes,
             'overalltimes':overalltimes,
             'pos':pos, 'penalties':penalties})


df = pd.DataFrame(t).set_index(['entryId'])
df.head()
```

```python
df.tail()
```

```python
df_overall = pd.DataFrame(df['overalltimes'].tolist(), index= df.index)
#df_overall.columns = cols
df_overall.head()
```

```python
df_overall.tail()
```

```python
df_overall_pos = pd.DataFrame(df['pos'].tolist(), index= df.index)
df_overall_pos.head()
```

```python
df_stages = pd.DataFrame(df['stagetimes'].tolist(), index= df.index)
#df_stages.columns = cols
df_stages.head()
```

```python
from dakar_utils import getTime
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
def _gapToLeaderBar(Xtmpq, typ, stages=None, milliseconds=True):
    if milliseconds:
        Xtmpq = Xtmpq/1000
    if typ=='stage':
        Xtmpq.columns = ['SS_{}'.format(c) for c in Xtmpq.columns]
    else:
        Xtmpq.columns = ['SS_{}_{}'.format(c, typ) for c in Xtmpq.columns]
    k = '{}GapToLeader'.format(typ)
    Xtmpq[k] = Xtmpq[[c for c in Xtmpq.columns ]].values.tolist()
    Xtmpq[k] = Xtmpq[k].apply(lambda x: [-y for y in x])
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

```python
leaderTimes
```

There are five cross-stage reports we can add in:
    - `overallPosition`: step line chart showing evolution of overall position
    - `overallGapToLeader`: bar chart showing overall gap to leader
    - `Gap`: bar chart showing gap relative to rebased entry
    - `stagePosition`: step chart showing stage positions
    - `stageWinnerGap`: bar chart showing gap to stage winner

```python
#Need to refactor the times in the tables to gaps

#The overall times need rebasing to the overall leader at the end of the stage, that is, pos==1 on df
leaderTimes = df_overall.min()
df_overall[xcols] = -df_overall[xcols].apply(_rebaseTimes, basetimes=leaderTimes, axis=1)

df_overall.head()
```

```python
df_stages.head()
```

```python
#We need to finesse the stage positions from the stage times... or get them from elsewhere
df_stages_pos = df_stages.rank(method='min')
df_stages_pos.head()
```

```python
#A little test that ranking on the overall positions pase on times matches overall rankings in data
#_df_overall_pos = df_overall.rank(method='min', ascending=False)
#_df_overall_pos.equals(df_overall_pos)
```

```python
#The stage times need rebasing to the stage winner, which we need to get elsewhere or generate
#Gap to stage winner
stagewinnertimes = df_stages.min()

df_stages[ccols] = df_stages[xcols].apply(_rebaseTimes, basetimes=stagewinnertimes, axis=1)

df_stages.head()
```

```python
rebase = '/entryinfo/46492-corbeau-seats-rally-tendring-clacton-2018/1719852/'
```

```python
from IPython.display import HTML
```

```python
#overallGapToLeader: bar chart showing overall gap to leader


#wrc = pd.merge(codes, positionStep(sr.dbGetStageRank(conn, rally, rc, 'overall', stages), 'overall', stages)[['overallPosition']], left_index=True, right_index=True)

#test with a limit
tmp = _gapToLeaderBar(-df_overall[xcols], 'overall', stages, False)[:10]#[['overallPosition']]
s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')

#Introduce a dot marker to highlight winner
display(HTML(s2))

```

```python
#overallPosition: step line chart showing evolution of overall position

#wrc = pd.merge(codes, positionStep(sr.dbGetStageRank(conn, rally, rc, 'overall', stages), 'overall', stages)[['overallPosition']], left_index=True, right_index=True)

#This is WRONG atm - what col is is on?

#We need to pass a position table in
xx=_positionStep(df_overall_pos[xcols], 'overall', stages)[['overallPosition']]

tmp = pd.merge(tmp, xx, left_index=True, right_index=True)
s2 = moreStyleDriverSplitReportBaseDataframe(tmp,'')

#Introduce a dot marker to highlight winner
display(HTML(s2))

```

```python
cols = [c for c in tmp.columns if c.startswith('SS')]
s2 = moreStyleDriverSplitReportBaseDataframe(-tmp[cols].apply(_rebaseTimes, bib=rebase, axis=0),'')

#Introduce a dot marker to highlight winner
display(HTML(s2))
```

```python
# Gap: bar chart showing gap relative to rebased entry

```

```python
# stagePosition: step chart showing stage positions

```

```python
# stageWinnerGap: bar chart showing gap to stage winner
```
