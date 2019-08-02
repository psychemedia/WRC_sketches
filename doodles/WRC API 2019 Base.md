---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.1'
      jupytext_version: 1.2.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Package for WRC Results Scrape

This notebook contains most of the ingredients for a package that can be used to scrape results data from current and archived WRC Live Timing pages.

At the moment, the script will only happily downloaded the complete set of results for a particular rally. The code needs updating to allow:

- loading data for a completed stage into a database that already contains results for other completed stages;
- incremental loads of data to upsert data into the database from an ongoing stage;

```python
#https://www.wrc.com/service/sasCacheApi.php?route=rallies%2F40%2Fitinerary
```

```python
YEAR = 2019
```

```python
url_root='http://www.wrc.com/service/sasCacheApi.php?route={stub}'

#What's the events/{ID}/{stub} ID number?
#url_base='http://www.wrc.com/service/sasCacheApi.php?route=events/79/{stub}'
url_base='http://www.wrc.com/service/sasCacheApi.php?route=events/{SASEVENTID}/{{stub}}'
#we need to grab SASEVENTID from getEventMetadata()
```

```python
#Call a resource by ID
wrcapi='https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byId?id=%22{}%22' #requires resource ID
```

```python
stubs = { 'itinerary': 'rallies/{rallyId}/itinerary',
          'startlists': 'rallies/{rallyId}/entries',
         'penalties': 'rallies/{rallyId}/penalties',
         'retirements': 'rallies/{rallyId}/retirements',
         'stagewinners':'rallies/{rallyId}/stagewinners',
         'overall':'stages/{stageId}/results?rallyId={rallyId}',
         'split_times':'stages/{stageId}/splittimes?rallyId={rallyId}',
         'stage_times_stage':'stages/{stageId}/stagetimes?rallyId={rallyId}',
         'stage_times_overall':'stages/{stageId}/results?rallyId={rallyId}',
         'seasons':'seasons',
         'seasonDetails':'seasons/{seasonId}',
         # TO DO - for 2019, the following may need to be prefixed with seasons/ ?
         'championship':'seasons/4/championships/{championshipId}',
         'championship_results':'seasons/4/championships/{championshipId}/results',
        }
```

```python
#pip3 install sqlite-utils

import requests
import re
import json
from bs4 import BeautifulSoup 

import sqlite3
from sqlite_utils import Database

import pandas as pd
from pandas.io.json import json_normalize

#!pip3 install isodate
import isodate
```

## Database Schema

The following tables are literal mappings from flattened JSON datafiles published by the WRC.

The data model (primary and foreign key relationships) is derived by observation.

```python
#SQL in wrcResults.sql
setup_q='''
CREATE TABLE "itinerary_event" (
  "eventId" INTEGER,
  "itineraryId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "priority" INTEGER
);
CREATE TABLE "itinerary_legs" (
  "itineraryId" INTEGER,
  "itineraryLegId" INTEGER PRIMARY KEY,
  "legDate" TEXT,
  "name" TEXT,
  "order" INTEGER,
  "startListId" INTEGER,
  "status" TEXT,
  FOREIGN KEY ("itineraryId") REFERENCES "itinerary_event" ("itineraryId")
);
CREATE TABLE "itinerary_sections" (
  "itineraryLegId" INTEGER,
  "itinerarySectionId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "order" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "itinerary_stages" (
  "code" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "name" TEXT,
  "number" INTEGER,
  "stageId" INTEGER PRIMARY KEY,
  "stageType" TEXT,
  "status" TEXT,
  "timingPrecision" TEXT,
  "itineraryLegId" INTEGER,
  "itinerarySections.itinerarySectionId" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "itinerary_controls" (
  "code" TEXT,
  "controlId" INTEGER PRIMARY KEY,
  "controlPenalties" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "firstCarDueDateTime" TEXT,
  "firstCarDueDateTimeLocal" TEXT,
  "location" TEXT,
  "stageId" INTEGER,
  "status" TEXT,
  "targetDuration" TEXT,
  "targetDurationMs" INTEGER,
  "timingPrecision" TEXT,
  "type" TEXT,
  "itineraryLegId" INTEGER,
  "itinerarySections.itinerarySectionId" INTEGER,
  "roundingPolicy" TEXT,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "startlists" (
  "codriver.abbvName" TEXT,
  "codriver.code" TEXT,
  "codriver.country.countryId" INTEGER,
  "codriver.country.iso2" TEXT,
  "codriver.country.iso3" TEXT,
  "codriver.country.name" TEXT,
  "codriver.countryId" INTEGER,
  "codriver.firstName" TEXT,
  "codriver.fullName" TEXT,
  "codriver.lastName" TEXT,
  "codriver.personId" INTEGER,
  "codriverId" INTEGER,
  "driver.abbvName" TEXT,
  "driver.code" TEXT,
  "driver.country.countryId" INTEGER,
  "driver.country.iso2" TEXT,
  "driver.country.iso3" TEXT,
  "driver.country.name" TEXT,
  "driver.countryId" INTEGER,
  "driver.firstName" TEXT,
  "driver.fullName" TEXT,
  "driver.lastName" TEXT,
  "driver.personId" INTEGER,
  "driverId" INTEGER,
  "eligibility" TEXT,
  "entrant.entrantId" INTEGER,
  "entrant.logoFilename" TEXT,
  "entrant.name" TEXT,
  "entrantId" INTEGER,
  "entryId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "group.name" TEXT,
  "groupId" INTEGER,
  "group.groupId" INTEGER,
  "identifier" TEXT,
  "manufacturer.logoFilename" TEXT,
  "manufacturer.manufacturerId" INTEGER,
  "manufacturer.name" TEXT,
  "manufacturerId" INTEGER,
  "priority" TEXT,
  "status" TEXT,
  "tag" TEXT,
  "tag.name" TEXT,
  "tag.tagId" INTEGER,
  "tagId" INTEGER,
  "tyreManufacturer" TEXT,
  "vehicleModel" TEXT,
  "entryListOrder" INTEGER,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "roster" (
  "fiasn" INTEGER,
  "code" TEXT,
  "sas-entryid" INTEGER PRIMARY KEY,
  "roster_num" INTEGER,
  FOREIGN KEY ("sas-entryid") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "startlist_classes" (
  "eventClassId" INTEGER,
  "eventId" INTEGER,
  "name" TEXT,
  "entryId" INTEGER,
  PRIMARY KEY ("eventClassId","entryId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "penalties" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "penaltyDuration" TEXT,
  "penaltyDurationMs" INTEGER,
  "penaltyId" INTEGER PRIMARY KEY,
  "reason" TEXT,
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "retirements" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "reason" TEXT,
  "retirementDateTime" TEXT,
  "retirementDateTimeLocal" TEXT,
  "retirementId" INTEGER PRIMARY KEY,
  "status" TEXT,
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stagewinners" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "stageId" INTEGER,
  "stageName" TEXT,
  PRIMARY KEY ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId")
);
CREATE TABLE "stage_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "split_times" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "splitDateTime" TEXT,
  "splitDateTimeLocal" TEXT,
  "splitPointId" INTEGER,
  "splitPointTimeId" INTEGER PRIMARY KEY,
  "stageTimeDuration" TEXT,
  "stageTimeDurationMs" REAL,
  "startDateTime" TEXT,
  "startDateTimeLocal" TEXT,
  "stageId" INTEGER,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_stage" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "position" INTEGER,
  "source" TEXT,
  "stageId" INTEGER,
  "stageTimeId" INTEGER PRIMARY KEY,
  "status" TEXT,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "championship_lookup" (
  "championshipId" INTEGER PRIMARY KEY,
  "fieldFiveDescription" TEXT,
  "fieldFourDescription" TEXT,
  "fieldOneDescription" TEXT,
  "fieldThreeDescription" TEXT,
  "fieldTwoDescription" TEXT,
  "name" TEXT,
  "seasonId" INTEGER,
  "type" TEXT,
  "_codeClass" TEXT,
  "_codeTyp" TEXT
);
CREATE TABLE "championship_results" (
  "championshipEntryId" INTEGER,
  "championshipId" INTEGER,
  "dropped" INTEGER,
  "eventId" INTEGER,
  "pointsBreakdown" TEXT,
  "position" INTEGER,
  "publishedStatus" TEXT,
  "status" TEXT,
  "totalPoints" INTEGER,
  PRIMARY KEY ("championshipEntryId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_entries_codrivers" (
  "championshipEntryId" INTEGER PRIMARY KEY,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "CountryISO2" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_entries_manufacturers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" INTEGER,
  "Name" TEXT,
  "LogoFileName" TEXT,
  "Manufacturer" TEXT,
  "manufacturerId" INTEGER,
  "personId" TEXT,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_rounds" (
  "championshipId" INTEGER,
  "eventId" INTEGER,
  "order" INTEGER,
  PRIMARY KEY ("championshipId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_events" (
  "categories" TEXT,
  "clerkOfTheCourse" TEXT,
  "country.countryId" INTEGER,
  "country.iso2" TEXT,
  "country.iso3" TEXT,
  "country.name" TEXT,
  "countryId" INTEGER,
  "eventId" INTEGER PRIMARY KEY,
  "finishDate" TEXT,
  "location" TEXT,
  "mode" TEXT,
  "name" TEXT,
  "organiserUrl" TEXT,
  "slug" TEXT,
  "startDate" TEXT,
  "stewards" TEXT,
  "surfaces" TEXT,
  "templateFilename" TEXT,
  "timeZoneId" TEXT,
  "timeZoneName" TEXT,
  "timeZoneOffset" INTEGER,
  "trackingEventId" INTEGER ,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_entries_drivers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "CountryISO2" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "event_metadata" (
  "_id" TEXT,
  "availability" TEXT,
  "date-finish" TEXT,
  "date-start" TEXT,
  "gallery" TEXT,
  "hasdata" TEXT,
  "hasfootage" TEXT,
  "hasvideos" TEXT,
  "id" TEXT,
  "info-based" TEXT,
  "info-categories" TEXT,
  "info-date" TEXT,
  "info-flag" TEXT,
  "info-surface" TEXT,
  "info-website" TEXT,
  "kmlfile" TEXT,
  "logo" TEXT,
  "name" TEXT,
  "org-website" TEXT,
  "poi-Klo im Wald" TEXT,
  "poilistid" TEXT,
  "position" TEXT,
  "rosterid" TEXT,
  "sas-eventid" TEXT,
  "sas-itineraryid" TEXT,
  "sas-rallyid" TEXT,
  "sas-trackingid" TEXT,
  "sitid" TEXT,
  "testid" TEXT,
  "thumbnail" TEXT,
  "time-zone" TEXT,
  "tzoffset" TEXT,
  "year" INTEGER
);


'''


#conn = sqlite3.connect('wrc18_test1keys.db')
#c = conn.cursor()
#c.executescript(setup_q)
```

```python
#Maybe bring in additional tables 
#eg via https://blog.ouseful.info/2016/11/14/what-nationality-did-you-say-you-were-again/
#bring in a table to give nationalites from country codes
#Or maybe make that a pip package?
```

```python
setup_views_q = '''
'''
```

```python
#meta={'rallyId':None, 'stages':[], 'championshipId':None }
```

```
#This is a literal approach and is DEPRECATED

def getRally_URLs(results_main_url=None):
    if results_main_url is None:
        results_main_url='http://www.wrc.com/en/wrc/results/wales/stage-times/page/416-238---.html#'

    html=requests.get(results_main_url)
    soup=BeautifulSoup(html.content, "html5lib")
    #BeautifulSoup has a routine - find_all() - that will find all the HTML tags of a particular sort
    #Links are represented in HTML pages in the form <a href="http//example.com/page.html">link text</a>
    #Grab all the <a> (anchor) tags...
    souplist=soup.findAll("li",{'class':'flag'})

    items={}
    for s in souplist:
        href=s.find('a')['href']
        if href:
            title=s.find('img')['title']
            title = 'Monaco' if title == 'Monte Carlo' else title
            items[title]=href
    return items

def listRallies(display=True, **kwargs):
    rallyURLs = getRally_URLs(**kwargs)
    if display:
        print( ', '.join(rallyURLs.keys()) )
    else:
        return getRallyIDs
```

```python
def _getEventMetadata():
    ''' Get event metadata as JSON data feed from WRC API. '''
    url='https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22Event%22&maxdepth=1'
    eventmeta = requests.get(url).json()
    
    return eventmeta

def getEventMetadata():
    ''' Get a list of events from WRC as a flat pandas dataframe.
        Itinerary / event data is only available for rallies starting in stated year. '''
    eventMetadata = json_normalize(_getEventMetadata(),
                                   record_path='_meta',
                                   meta='_id'  ).drop_duplicates().pivot('_id', 'n','v').reset_index()

    eventMetadata['date-finish']=pd.to_datetime(eventMetadata['date-finish'])
    eventMetadata['date-start']=pd.to_datetime(eventMetadata['date-start'])
    eventMetadata['year'] = eventMetadata['date-start'].dt.year
    
    #hack for now - really need to check this against table cols
    #eventMetadata.drop(columns=['hasvieos'], inplace=True)
    return eventMetadata


getEventMetadata().head()
```

```python
def _getRallyIDs2(year=YEAR):
    em=getEventMetadata()
    em = em[em['year']==year][['name','sas-rallyid', 'sas-eventid', 'kmlfile', 'date-start']].reset_index(drop=True).dropna()
    em['stub']=em['kmlfile'].apply(lambda x: x.split('_')[0])
    return em

def getRallyIDs2(year=YEAR):
    em = _getRallyIDs2(year=year)
    return em[['stub','sas-rallyid']].set_index('stub').to_dict()['sas-rallyid']

def getEventID(year=YEAR):
    em = _getRallyIDs2(year=year)
    return em[['stub','sas-eventid']].set_index('stub').to_dict()['sas-eventid']


def listRallies2(year=YEAR):
    return getRallyIDs2(year)
```

```python
getEventID(2019)

```

```python
name='france'
```

```python
url_base='http://www.wrc.com/service/sasCacheApi.php?route=events/{SASEVENTID}/{{stub}}'.format(SASEVENTID=getEventID(2019)[name])
url_base
```

```python
listRallies2()
```

```
#This appproach makes literal calls to an original HTML page and is DEPRECATED
def _getRallyID(rallyURL):
    html=requests.get(rallyURL)
    m = re.search("var rallyId = '(.+?)'", html.text)
    if m:
        return m.group(1)
    return None
            
def getRallyIDs(rally=None,results_main_url=None):
    rallyids={}

    items = getRally_URLs(results_main_url)
    
    #if we know the rally, just get that one.. 
    if rally in items:
        items = {rally:items[rally]}

    for item in items:
        rallyids[item] = _getRallyID(items[item])

    return rallyids 
```

```python
'''
def set_rallyId(rally, year, rallyIDs=None):
    meta={'rallyId':None, 'stages':[], 'championshipId':None }
    if rallyIDs is None:
        rallyIDs = getRallyIDs()
    if rally in rallyIDs:
        meta['rallyId']=rallyIDs[rally]
        meta['rally_name'] = rally
    return meta
'''

def set_rallyId2(rally, year, rallyIDs=None):
    meta={'rallyId':None, 'stages':[], 'championshipId':None }
    if rallyIDs is None:
        rallyIDs = getRallyIDs2()
    if rally in rallyIDs:
        meta['rallyId']=rallyIDs[rally]
        meta['rally_name'] = rally
    return meta
```

```python
year = 2019
name = 'france'
dbname='france19.db'

meta = set_rallyId2(name, year)
meta
```

```python
listRallies2()
```

```python
#rallyIDs = getRallyIDs2()
#rallyIDs
```

```python
def nvToDict(nvdict, key='n',val='v', retdict=None):
    if retdict is None:
        retdict={nvdict[key]:nvdict[val]}
    else:
        retdict[nvdict[key]]=nvdict[val]
    return retdict
#assert nvToDict({'n': "id",'v': "adac-rallye-deutschland"}) == {'id': 'adac-rallye-deutschland'}
```

```python
 #getEventMetadata()['rosterid'].iloc[0]
```

```python
getEventMetadata().head(2)
```

```python
#roster_id='bab64d15-4691-4561-a6bf-7284f3bd85f9'
import requests
#roster_json = requests.get( '{}&maxdepth=2'.format(wrcapi.format(roster_id),) ).json()
#roster_json   

#TO CHECK - is the sas-entryid the entryid we use elsewhere?

#This comes from event metadata
def _getRoster(roster_id):
    roster_json = requests.get(wrcapi.format(roster_id) ).json()
    roster=json_normalize(roster_json)
    
    aa=json_normalize(roster_json, record_path='_dchildren')
    zz=json_normalize(roster_json['_dchildren'],record_path=['_meta'], meta='_id').pivot('_id', 'n','v').reset_index()
    zz=pd.merge(zz,aa[['_id','name','type']], on='_id')[['fiasn','filename','sas-entryid','name']]
    zz.columns = ['fiasn','code','sas-entryid','roster_num']
    #defensive?
    zz = zz.dropna(subset=['sas-entryid'])
    return zz

def getRoster(meta):
    em = getEventMetadata()
    roster_id= em[em['sas-rallyid']==meta['rallyId']]['rosterid'].iloc[0]
    return _getRoster(roster_id)
```

```python
def getItinerary(meta):
    ''' Get event itinerary. Also updates the stages metadata. '''
    itinerary_json=requests.get( url_base.format(stub=stubs['itinerary'].format(**meta) ) ).json()
    itinerary_event = json_normalize(itinerary_json).drop('itineraryLegs', axis=1)
    
    #meta='eventId' for eventId
    itinerary_legs = json_normalize(itinerary_json, 
                                    record_path='itineraryLegs').drop('itinerarySections', axis=1)
    #meta='eventId' for eventId
    itinerary_sections = json_normalize(itinerary_json,
                                        ['itineraryLegs', 'itinerarySections']).drop(['stages','controls'],axis=1)

    itinerary_stages=json_normalize(itinerary_json['itineraryLegs'],
                                    ['itinerarySections','stages'],
                                   meta=['itineraryLegId',['itinerarySections','itinerarySectionId']])
    meta['stages']=itinerary_stages['stageId'].tolist()
    #Should do this a pandas idiomatic way
    #meta['_stages']=zip(itinerary_stages['stageId'].tolist(),
     #                   itinerary_stages['code'].tolist(),
     #                   itinerary_stages['status'].tolist())
    meta['_stages'] = itinerary_stages[['stageId','code','status']].set_index('code').to_dict(orient='index')
    itinerary_controls=json_normalize(itinerary_json['itineraryLegs'], 
                                  ['itinerarySections','controls'] ,
                                     meta=['itineraryLegId',['itinerarySections','itinerarySectionId']])
    itinerary_controls['stageId'] = itinerary_controls['stageId'].fillna(-1).astype(int)
    
    return itinerary_event, itinerary_legs, itinerary_sections, itinerary_stages, itinerary_controls
```

```python
itinerary_json=requests.get( url_base.format(stub=stubs['itinerary'].format(**meta) ) ).json()
itinerary_json
```

```python
'''
{'rallyId': '30',
 'stages': [],
 'championshipId': None,
 'rally_name': 'montecarlo'}
 '''

#https://www.wrc.com/service/sasCacheApi.php?route=events%2F78%2Frallies%2F94%2Fitinerary
meta
```

```python
getItinerary(meta)
```

```python
#a,b,c,d,e = getItinerary(meta)
```

```python
def _get_single_json_table(meta, stub):
    _json = requests.get( url_base.format(stub=stubs[stub].format(**meta) ) ).json()
    return json_normalize(_json)

def _get_single_json_table_root(meta, stub):
    _json = requests.get( url_root.format(stub=stubs[stub].format(**meta) ) ).json()
    return json_normalize(_json)
```

```python
#meta =  set_rallyId(name, year)

#startlists_json=requests.get( url_base.format(stub=stubs['startlists'].format(**meta) ) ).json()
#ff=[]
#for f in startlists_json:
#    if f['manufacturer']['logoFilename'] is None:
#        f['manufacturer']['logoFilename']=''
#    if f['entrant']['logoFilename'] is None:
#        f['entrant']['logoFilename']='' 
#    ff.append(f)
#ff
```

```python
#startlists = json_normalize(ff).drop('eventClasses', axis=1)
```

```python
def get_startlists(meta):
    startlists_json=requests.get( url_base.format(stub=stubs['startlists'].format(**meta) ) ).json()
    ff=[]
    for f in startlists_json:
        if f['manufacturer']['logoFilename'] is None:
            f['manufacturer']['logoFilename']=''
        if f['entrant']['logoFilename'] is None:
            f['entrant']['logoFilename']='' 
        ff.append(f)
    startlists = json_normalize(ff).drop('eventClasses', axis=1)
    startlist_classes = json_normalize(ff,['eventClasses'], 'entryId' )
    #startlists = json_normalize(startlists_json).drop('eventClasses', axis=1)
    #startlist_classes = json_normalize(startlists_json,['eventClasses'], 'entryId' )
    
    return startlists, startlist_classes 
```

```python
def get_penalties(meta):
    ''' Get the list of penalties for a specified event. '''
    penalties = _get_single_json_table(meta, 'penalties')
    return penalties
```

```python
def get_retirements(meta):
    ''' Get the list of retirements for a specified event. '''
    retirements = _get_single_json_table(meta, 'retirements')
    return retirements
```

```python
def get_stagewinners(meta):
    ''' Get the stage winners table for a specified event. '''
    stagewinners = _get_single_json_table(meta, 'stagewinners')
    return stagewinners
```

```python
def _single_stage(meta2, stub, stageId):
    ''' For a single stageId, get the requested resource. '''
    meta2['stageId']=stageId
    _json=requests.get( url_base.format(stub=stubs[stub].format(**meta2) ) ).json()
    _df = json_normalize(_json)
    _df['stageId'] = stageId
    return _df

def _stage_iterator(meta, stub, stage=None):
    ''' Iterate through a list of stageId values and get requested resource. '''
    meta2={'rallyId':meta['rallyId']}
    df = pd.DataFrame()
    #If stage is None get data for all stages
    if stage is not None:
        stages=[]
        #If we have a single stage (specified in form SS4) get it
        if isinstance(stage,str) and stage in meta['_stages']:
            stages.append(meta['_stages'][stage]['stageId'])
        #If we have a list of stages (in form ['SS4','SS5']) get them all
        elif isinstance(stage, list):
            for _stage in stage:
                if isinstance(_stage,str) and _stage in meta['_stages']:
                    stages.append(meta['_stages'][_stage]['stageId'])
                elif _stage in meta['stages']:
                    stages.append(_stage)
    else:
        stages = meta['stages']
        
    #Get data for required stages
    for stageId in stages:
        #meta2['stageId']=stageId
        #_json=requests.get( url_base.format(stub=stubs[stub].format(**meta2) ) ).json()
        #_df = json_normalize(_json)
        #_df['stageId'] = stageId
        _df = _single_stage(meta2, stub, stageId)
        df = pd.concat([df, _df], sort=False)
    return df.reset_index(drop=True)
```

```
def _stage_iterator(meta, stub):
    ''' Iterate through a list of stageId values and get requested resource. '''
    meta2={'rallyId':meta['rallyId']}
    df = pd.DataFrame()
    for stageId in meta['stages']:
        meta2['stageId']=stageId
        _json=requests.get( url_base.format(stub=stubs[stub].format(**meta2) ) ).json()
        _df = json_normalize(_json)
        _df['stageId'] = stageId
        df = pd.concat([df, _df], sort=False)
    return df
```

```python
def get_overall(meta, stage=None):
    ''' Get the overall results table for all stages on an event or a specified stage. '''
    stage_overall = _stage_iterator(meta, 'overall', stage)
    return stage_overall
```

```python
#get_overall(meta)
```

```python
def get_splitTimes(meta, stage=None):
    ''' Get split times table for all stages on an event or a specified stage. '''
    split_times = _stage_iterator(meta, 'split_times', stage)
    return split_times
```

```python
#get_splitTimes(meta)
```

```python
def get_stage_times_stage(meta, stage=None):
    ''' Get stage times table for all stages on an event or a specified stage. '''
    stage_times_stage = _stage_iterator(meta, 'stage_times_stage', stage)
    return stage_times_stage
```

```python
def get_stage_times_overall(meta,stage=None):
    ''' Get overall stage times table for all stages on an event or a specified stage. '''
    stage_times_overall = _stage_iterator(meta, 'stage_times_overall', stage)
    return stage_times_overall
```

```python
#There must be a JSON/API way of getting this rather than having to fish for it
def _get_championship_codesOLD(url=None):
    if url is None:
        url = 'http://www.wrc.com/en/wrc/results/championship-standings/page/4176----.html'
    html2=requests.get(url).text
    m = re.search("var championshipClasses = (.*?);", html2, re.DOTALL)
    mm=m.group(1).replace('\n','').replace("'",'"')
    #Hack for null table
    for v in ['jwrcDriver','jwrcCoDriver','wrcDriver','wrcCoDriver','wrcManufacturers','wrc2ProDriver',
              'wrc2ProCoDriver','wrc2ProManufacturers','wrc2Driver','wrc2CoDriver']:
        mm = mm.replace(v,'[]')
    d=json.loads(mm)
    #https://stackoverflow.com/a/35758583/454773
    championshipClasses={k.replace(' ', ''): v for k, v in d.items()}
    return championshipClasses
```

```python
#         'championship':'seasons/4/championships/{championshipId}',
#         'championship_results':'seasons/4/championships/{championshipId}/results',
_get_championship_codesOLD(season)
```

```python
# TO DO
def get_championship_rounds():
    pass
    #https://www.wrc.com/service/sasCacheApi.php?route=seasons%2F4%2Fchampionships%2F24
```

```python
def get_seasons():
    ''' Get season info. '''
    return requests.get(url_root.format(stub=stubs['seasons'] )).json()
#https://www.wrc.com/service/sasCacheApi.php?route=seasons/
get_seasons()
```

```python
def getSeasonDetails(seasonId):
    return requests.get(url_root.format(stub=stubs['seasonDetails'].format(seasonId=seasonId) )).json()
getSeasonDetails(4)
```

```python
# TO DO set seasonId properly
def championship_tables(champ_class=None, champ_typ=None, seasonId=4):
    ''' Get all championship tables in a particular championship and / or class. '''
    #if championship is None then get all
    championship_lookup = pd.DataFrame()
    championship_entries_all = {}
    championship_rounds = pd.DataFrame()
    championship_events = pd.DataFrame()
    championship_results = pd.DataFrame()
    
    championships = getSeasonDetails(seasonId)['championships']
    
    for championship in championships:
        champ_num = championship['championshipId']
        #TO DO - are we setting the champType correctly?
        # championship['type'] returns as Person or Manufacturer
        champType = championship['name'].split()[-1]#championship['type']
        if champType not in championship_entries_all:
            championship_entries_all[champType] = pd.DataFrame()
            
        meta2={'championshipId': champ_num}
        championship_url = url_root.format(stub=stubs['championship'].format(**meta2) )
        championship_json=requests.get( championship_url ).json()
        if championship_json:
            _championship_lookup = json_normalize(championship_json).drop(['championshipEntries','championshipRounds'], axis=1)
            _championship_lookup['_codeClass'] = championship['name']
            _championship_lookup['_codeTyp'] = championship['type']
            championship_lookup = pd.concat([championship_lookup,_championship_lookup],sort=True)
    
            championships={}
            championship_dict = _championship_lookup.to_dict()
            championships[champ_num] = {c:championship_dict[c][0] for c in championship_dict}
            renamer={c.replace('Description',''):championships[champ_num][c] for c in championships[champ_num] if c.startswith('field')}            
            _championship_entries = json_normalize(championship_json,['championshipEntries'] )
            _championship_entries = _championship_entries.rename(columns=renamer)
            _championship_entries = _championship_entries[[c for c in _championship_entries.columns if c!='']]
            #pd.concat sort=False to retain current behaviour
            
            championship_entries_all[champType] = pd.concat([championship_entries_all[champType],_championship_entries],sort=False)

            _championship_rounds = json_normalize(championship_json,['championshipRounds'] ).drop('event', axis=1)
            championship_rounds = pd.concat([championship_rounds,_championship_rounds],sort=False).drop_duplicates()

            _events_json = json_normalize(championship_json,['championshipRounds' ])['event']
            _championship_events = json_normalize(_events_json)
            #Below also available as eg https://www.wrc.com/service/sasCacheApi.php?route=seasons/4/championships/24
            championship_events = pd.concat([championship_events,_championship_events],sort=False).drop_duplicates()

            _championship_results =  _get_single_json_table_root(meta2, 'championship_results')
            championship_results = pd.concat([championship_results, _championship_results],sort=False)
    
    for k in championship_entries_all:
        championship_entries_all[k].reset_index(drop=True)
        if k in ['Driver', 'Co-Driver']:
            championship_entries_all[k] = championship_entries_all[k].rename(columns={'TyreManufacturer':'ManufacturerTyre'})
    
    return championship_lookup.reset_index(drop=True), \
            championship_results.reset_index(drop=True), \
            championship_entries_all, \
            championship_rounds.reset_index(drop=True), \
            championship_events.reset_index(drop=True)

#championship_tables()
```

```
def championship_tablesOLD(champ_class=None, champ_typ=None):
    ''' Get all championship tables in a particular championship and / or class. '''
    #if championship is None then get all
    championship_lookup = pd.DataFrame()
    championship_entries_all = {}
    championship_rounds = pd.DataFrame()
    championship_events = pd.DataFrame()
    championship_results = pd.DataFrame()
    
    championship_codes = _get_championship_codes()
    _class_codes = championship_codes.keys() if champ_class is None else [champ_class]
    for champClass in _class_codes:
        _champ_typ = championship_codes[champClass].keys() if champ_typ is None else [champ_typ]
        for champType in _champ_typ:
            if champType not in championship_entries_all:
                championship_entries_all[champType] = pd.DataFrame()
            
            champ_num = championship_codes[champClass][champType]
            meta2={'championshipId': champ_num}
            
            championship_url = url_base.format(stub=stubs['championship'].format(**meta2) )
            print(championship_url)
            championship_json=requests.get( championship_url ).json()
            if championship_json:
                _championship_lookup = json_normalize(championship_json).drop(['championshipEntries','championshipRounds'], axis=1)
                _championship_lookup['_codeClass'] = champClass
                _championship_lookup['_codeTyp'] = champType
                championship_lookup = pd.concat([championship_lookup,_championship_lookup],sort=True)

                championships={}
                championship_dict = _championship_lookup.to_dict()
                championships[champ_num] = {c:championship_dict[c][0] for c in championship_dict}
                renamer={c.replace('Description',''):championships[champ_num][c] for c in championships[champ_num] if c.startswith('field')}            
                _championship_entries = json_normalize(championship_json,['championshipEntries'] )
                _championship_entries = _championship_entries.rename(columns=renamer)
                _championship_entries = _championship_entries[[c for c in _championship_entries.columns if c!='']]
                #pd.concat sort=False to retain current behaviour
                championship_entries_all[champType] = pd.concat([championship_entries_all[champType],_championship_entries],sort=False)

                _championship_rounds = json_normalize(championship_json,['championshipRounds'] ).drop('event', axis=1)
                championship_rounds = pd.concat([championship_rounds,_championship_rounds],sort=False).drop_duplicates()

                _events_json = json_normalize(championship_json,['championshipRounds' ])['event']
                _championship_events = json_normalize(_events_json)
                #TO DO: Season id -> https://www.wrc.com/service/sasCacheApi.php?route=seasons/
                # TO DO: list of championships: eg https://www.wrc.com/service/sasCacheApi.php?route=seasons/4
                #Below also available as eg https://www.wrc.com/service/sasCacheApi.php?route=seasons/4/championships/24
                championship_events = pd.concat([championship_events,_championship_events],sort=False).drop_duplicates()

                _championship_results = _get_single_json_table(meta2, 'championship_results')
                championship_results = pd.concat([championship_results, _championship_results],sort=False)
    
    for k in championship_entries_all:
        championship_entries_all[k].reset_index(drop=True)
        if k in ['Driver', 'Co-Driver']:
            championship_entries_all[k] = championship_entries_all[k].rename(columns={'TyreManufacturer':'ManufacturerTyre'})
    
    return championship_lookup.reset_index(drop=True), \
            championship_results.reset_index(drop=True), \
            championship_entries_all, \
            championship_rounds.reset_index(drop=True), \
            championship_events.reset_index(drop=True)

```

## Usage

```python
#listRallies2()
```

```python
def cleardbtable(conn, table):
    ''' Clear the table whilst retaining the table definition '''
    c = conn.cursor()
    c.execute('DELETE FROM "{}"'.format(table))
    
def dbfy(conn, df, table, if_exists='upsert', index=False, clear=False, **kwargs):
    ''' Save a dataframe as a SQLite table.
        Clearing or replacing a table will first empty the table of entries but retain the structure. '''
    if if_exists=='replace':
        clear=True
        if_exists='append'
    if clear: cleardbtable(conn, table)
        
    #Get columns  
    q="PRAGMA table_info({})".format(table)
    cols = pd.read_sql(q,conn)['name'].tolist()
    for c in df.columns:
        if c not in cols:
            print('Hmmm... column name `{}` appears in data but not {} table def?'.format(c,table ))
            df.drop(columns=[c], inplace=True)
            
    if if_exists=='upsert':
        DB[table].upsert_all(df.to_dict(orient='records'))
    else:
        df.to_sql(table,conn,if_exists=if_exists,index=index)
```

```python
def save_rally(meta, conn, stage=None):
    ''' Save all tables associated with a particular rally. '''
    
    if stage is None:
        print('Getting base info...')
        roster = getRoster(meta)
        dbfy(conn, roster, 'roster', if_exists='replace')

        itinerary_event, itinerary_legs, itinerary_sections, \
        itinerary_stages, itinerary_controls = getItinerary(meta)

        dbfy(conn, itinerary_event, 'itinerary_event', if_exists='replace')
        dbfy(conn, itinerary_legs, 'itinerary_legs', if_exists='replace')
        dbfy(conn, itinerary_sections, 'itinerary_sections', if_exists='replace')
        dbfy(conn, itinerary_stages, 'itinerary_stages', if_exists='replace')
        dbfy(conn, itinerary_controls, 'itinerary_controls', if_exists='replace')

        startlists, startlist_classes = get_startlists(meta)
        dbfy(conn, startlists, 'startlists', if_exists='replace')
        dbfy(conn, startlist_classes, 'startlist_classes', if_exists='replace')

    #These need to be upserted
    print('Getting penalties...')
    penalties = get_penalties(meta)
    dbfy(conn, penalties, 'penalties')

    print('Getting retirements...')
    retirements = get_retirements(meta)
    dbfy(conn, retirements, 'retirements')

    print('Getting stagewinners...')
    stagewinners = get_stagewinners(meta)
    dbfy(conn, stagewinners, 'stagewinners')

    print('Getting stage_overall...')
    stage_overall = get_overall(meta, stage)
    dbfy(conn, stage_overall, 'stage_overall')

    print('Getting split_times...')
    split_times = get_splitTimes(meta, stage)
    dbfy(conn, split_times, 'split_times')
    
    print('Getting stage_times_stage...')
    stage_times_stage = get_stage_times_stage(meta, stage)
    dbfy(conn, stage_times_stage, 'stage_times_stage')
    
    print('Getting stage_times_overall...')
    stage_times_overall = get_stage_times_overall(meta, stage)
    dbfy(conn, stage_times_overall, 'stage_times_overall')

    
def save_championship(meta, conn):
    ''' Save all championship tables for a particular year. '''
    championship_lookup, championship_results, _championship_entries_all, \
        championship_rounds, championship_events = championship_tables()
        
    championship_entries_drivers = _championship_entries_all['Drivers']
    championship_entries_codrivers = _championship_entries_all['Co-Drivers']
    championship_entries_manufacturers = _championship_entries_all['Manufacturers']
    #championship_entries_nations = _championship_entries_all['Nations']
    
    dbfy(conn, championship_lookup, 'championship_lookup', if_exists='replace')
    dbfy(conn, championship_results, 'championship_results', if_exists='replace')
    dbfy(conn, championship_entries_drivers, 'championship_entries_drivers',if_exists='replace')
    dbfy(conn, championship_entries_codrivers, 'championship_entries_codrivers', if_exists='replace')
    dbfy(conn, championship_entries_manufacturers, 'championship_entries_manufacturers', if_exists='replace')
    dbfy(conn, championship_rounds, 'championship_rounds', if_exists='replace')
    dbfy(conn, championship_events, 'championship_events', if_exists='replace')

def get_one(rally, stage, dbname='wrc19_test1.db', year=YEAR):
    conn = sqlite3.connect(dbname)
    meta =  set_rallyId2(rally, year)
    getItinerary(meta) #to update meta
    print(meta)
    save_rally(meta, conn, stage)
    
def get_all(rally, dbname='wrc19_test1.db', year=YEAR):
    
    conn = sqlite3.connect(dbname)
    
    meta =  set_rallyId2(rally, year)
    
    save_rally(meta, conn)
    save_championship(meta, conn)
    
def get_championship(rally, dbname='wrc19_test1.db', year=YEAR):
    
    conn = sqlite3.connect(dbname)
    
    meta =  set_rallyId2(rally, year)

    save_championship(meta, conn)

```

```python
championship_lookup, championship_results, _championship_entries_all, \
        championship_rounds, championship_events = championship_tables()
```

```python
_championship_entries_all['Manufacturers']
```

```python
#listRallies()
listRallies2()
```

```python
#dbname='wrc18.db'
#year = 2019
#name = 'sweden'
#dbname='sweden.db'
```

```python
meta =  set_rallyId2(name, year)
meta
```

```python
import os

#For some reason, these don't seem to get set / picked up correctly from a notebook?
os.environ["WRC_RESULTS_NAME"] = name
os.environ["WRC_RESULTS_DBNAME"] = dbname
os.environ["WRC_RESULTS_YEAR"] = str(year)
```

```
_meta=set_rallyId2(name, year)
_,_,_,_s,_ = getItinerary(_meta)
_meta
```

```python
#championship_tables()
```

```python
#set_rallyId2('uk', 2018)
```

```python
#rr=get_retirements(meta)
#rr.head()
```

```python
# TO DO - ability to top up just the stage we need
```

```python
#set_rallyId("Finland",2018)
#set_rallyId2("australia",2018)
```

```python
getEventMetadata().columns
```

```python
#full run
#dbname = 'wrc18.db'
```

```python
#from sqlite_utils import Database
#DB = Database(sqlite3.connect(dbname))
```

```python
!ls
```

```python
dbname
```

```python
#new db
!mv $dbname old-$dbname
!rm $dbname
conn = sqlite3.connect(dbname, timeout=10)
c = conn.cursor()

DB = Database(sqlite3.connect(dbname))

c.executescript(setup_q)
c.executescript(setup_views_q)
q="SELECT name FROM sqlite_master WHERE type = 'table';"

#The upsert breaks with the - and space chars in column names
dbfy(conn, getEventMetadata(), 'event_metadata', if_exists='replace')

url_base='http://www.wrc.com/service/sasCacheApi.php?route=events/{SASEVENTID}/{{stub}}'
url_base = url_base.format(SASEVENTID=getEventID(year)[name])


pd.read_sql(q,conn)
```

```python
dbname
```

```python
getEventMetadata()
```

```python
#DEBUG
#for example: OperationalError: table event_metadata has no column named hasfootage
#
```

```python
#itinerary_event
```

```python
#itinerary_event
```

```python
get_championship(name, dbname=dbname, year=year )
```

```python
#set_rallyId( name,year )
get_all(name, dbname=dbname, year=year )
```

```python
#full run:
for name in listRallies2():
    print('trying {}'.format(name))
    get_all(name, dbname=dbname, year=year )
```

```python
#PK issues?
#upsert issues? Pandas doesn't support upsert

#Use Simon Willison's sqlite utils, which has upsert.
get_one(name, 'SS4', dbname=dbname, year=year)
```

```python
meta =  set_rallyId2(name, year)
meta
```

```python
conn = sqlite3.connect(dbname)

q="SELECT name FROM sqlite_master WHERE type = 'table';"
pd.read_sql(q,conn)
```

```python
q="PRAGMA table_info(event_metadata)"
pd.read_sql(q,conn)['name'].tolist()
```

```python
q="SELECT * FROM event_metadata LIMIT 1;"
pd.read_sql(q,conn).columns
```

```python
q="SELECT * FROM championship_events LIMIT 1;"
pd.read_sql(q,conn)
```

```python
q="SELECT * FROM championship_rounds LIMIT 1;"
pd.read_sql(q,conn)
```

```python
#!rm wrc18_test1.db
```

```python
#!pip3 install isodate
import isodate
pd.to_timedelta(isodate.parse_duration('PT1H5M26S')), isodate.parse_duration('PT0.1S')
#ISO 8601 https://stackoverflow.com/questions/51168022/what-does-pt-prefix-stand-for-in-duration

# TO DO - some function that timifies particular columns

def _totime(s):
    if s and isinstance(s,str):
        if s.startswith('PT'):
            return pd.to_timedelta(isodate.parse_duration(s))
        else:
            #Should regex to check this?
            return pd.to_timedelta(s)
            
def timeify(df):
    time_cols = ['diffFirst', 'diffPrev']
    for c in [c for c in df.columns if c in time_cols]:
        df[c] = df[c].apply(_totime)
    return df
```

```python
#Data2Text - Stage Result notebook has time wrangling
```

```python
#Why does this get cast to time but stage_times_overall doesn't?
q="SELECT * FROM stage_times_stage LIMIT 10 ;"
timeify(pd.read_sql(q,conn)).dtypes

#The diffFirst etc are not time objects - they;re strings; cast to timedelta? DOes SQLIte do timedelta?
#The cast is easy in Python: pd.to_timedelta('00:04:45.5000000')
```

```python
## Are stage_times_overall and stage_overall the same?


#Need to parse these time things correctly...
#Maybe parse 'PT10M40.4S'  to  '00:10:40.4000000',  'PT1H9M30.8S' to '00:01:09:30.800000'
#0.1S ->00:00:00.100000, 1M0.1s->00:10:00.100000, 1H1M0.1S ->01:10:00.100000
q="SELECT * FROM stage_times_overall ORDER BY totalTimeMs LIMIT 10 ;"
pd.read_sql(q,conn)
```

```python
q="SELECT * FROM stage_overall ORDER BY totalTimeMs LIMIT 10;"
pd.read_sql(q,conn)
```

```python
q="SELECT * FROM stagewinners;"
pd.read_sql(q,conn)

```

```python
!ls -al *.db
```

```python

```

```python
import sqlite3
import pandas as pd
conn = sqlite3.connect(dbname)
c = conn.cursor()
#c.executescript(setup_q)
#c.executescript(setup_views_q)
q="SELECT name FROM sqlite_master WHERE type = 'table';"
pd.read_sql(q,conn)
```

```python
q="SELECT * FROM startlists LIMIT 1;"
pd.read_sql(q,conn).to_dict()
```

```python

```

```python

```

```python

```
