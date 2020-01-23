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

## Dakar Rally Scraper

This notebook provides a case study in retrieving, cleaning, organising and processing data obtained from  a third party website, specifically, timing and results data from the 2019 Dakar Rally.

Another way of thinking of it is as a series of marks created during an exploratory data analysis performance.

Or palaver.

Whatever.

Shall we begin?

```python
#Use requests cache so we can keep an archive of the results HTML
import requests
import pandas as pd

from numpy import nan as NaN
```

```python
import requests_cache
requests_cache.install_cache('dakar_cache_2020', backend='sqlite')
```

Timing data is provided in two forms:

- time at waypoint / split;
- gap to leader at that waypoint;

Ranking data for the stage and overall at end of stage is also available.

Timing and ranking data is available for:

- car
- moto (motorbike)
- quad
- sxs
- truck


```python
YEAR = 2020
STAGE = 1
VTYPE = 'car'
```

## Stage Info

Retrieve some basic information about a stage.

```python
def get_stage_stats(stage):
    stage_stats_url='https://gaps.dakar.com/2020/dakar/index_info.php?l=ukie&s={stage}&vh=a'

    html = requests.get(stage_stats_url.format(stage=STAGE)).content
        
    stage_stats_df=pd.read_html(html)[0]

    return stage_stats_df#.rename(columns=stage_stats_df.iloc[0]).drop(stage_stats_df.index[0])

get_stage_stats(STAGE)
```

## Timing Data

Typical of many rallies, the live timing pages return several sorts of data:

- times and gaps for each stage;
- stage and overall results for each stage.

```python
URL_PATTERN='https://gaps.dakar.com/2020/dakar/?s={stage}&c=aso&l=ukie&vi={tab}&sv={timerank}&vh={vtype}&sws=99'
#sws - selected waypoint?
```

```python
#Vehicle types
VTYPE_ ={ 'car':'a','moto':'m','quad':'q','sxs':'s','truck':'c'}

#Screen / tab selection
TAB_ = {'timing':0,'news':1,'ranking':2}

#Options for timing data
TIMING_ = {'gap':0,'time':1}

#Options for ranking data
RANKING_ = {'stage':0, 'general':1}
```

## Previewing the data

Let's see what the data looks like...

*Uncomment and run the following to preview / inspect the data that's avaliable.*

```python
#pd.read_html(URL_PATTERN.format(stage=STAGE,tab=TAB_['timing'],vtype=VTYPE_['car'],timerank='time'))
```

```python
#pd.read_html(URL_PATTERN.format(stage=STAGE,tab=TAB_['ranking'],vtype=VTYPE_['car'],timerank='stage'))
```

By inspection, we note that:

- several tables are returned by each page;
- there are common identifier columns (`['Pos','Bib','Crew','Brand']`);
- there are irrelevant columns (`['View details','Select']`);
- the raw timing data columns (timig data at each waypoint) include information about split rank and how it compares to the rank at the previous split / waypoint; this needs to be cleaned before we can convert timestrings to timedeltas.

```python
#TIMING = RANKING = 0 #deprecate these
TIME = RANK = 0
CREWTEAM = 1
BRANDS = 2
COUNTRIES = 3
```

## Retrieving the Data Tables

We can define helper functions to pull back tables associated with the timing or ranking pages.

```python
#Retrieve a page

#Should we also return the stage so that tables stand alone as items with complete information?
def _data(stage,vtype='car',tab='timing', timerank='time', showstage=False):
    ''' Retrieve timing or ranking HTML page and scrape HTML tables. '''
    timerank = RANKING_[timerank] if tab=='ranking' else TIMING_[timerank]
        
    url = URL_PATTERN.format(stage=stage,tab=TAB_[tab],vtype=VTYPE_[vtype],timerank=timerank)

    html = requests.get(url).content
    _tmp = pd.read_html(html, na_values=['-'])
    if showstage:
        #This is a hack - elsewhere we use TIME, RANK etc in case there are more tables?
        _tmp[0].insert(0, 'Stage', stage)
    return _tmp


def _fetch_timing_data(stage, vtype='car', timerank='time', showstage=False):
    ''' Return data tables from timing page. '''
    _tmp = _data(stage,vtype=vtype, tab='timing', timerank=timerank, showstage=showstage)    
    _tmp[TIME].drop(columns=['View details','Select'], inplace=True)
    return _tmp

def _fetch_ranking_data(stage, vtype='car', timerank='stage', showstage=False):
    ''' Return data tables from ranking page. '''
    rank_cols = ['Pos','Bib','Crew','Brand','Time','Gap','Penalty']
    _tmp = _data(stage,vtype=vtype, tab='ranking', timerank=timerank, showstage=showstage)
    _tmp[RANK].drop(columns=['View details','Select'], inplace=True)
    #if timerank=='general':
    #    _tmp[RANK].rename(columns={'Pos':'Overall Position'}, inplace=True)
    return _tmp


```

<!-- #raw -->
#cache grab - grab all the HTML pages into a SQLite cache without expiry

#The news tab returns news items as a list rather than in a table
def _get_news(stage,vtype='car',):
    _tmp = _data(stage,vtype=vtype, tab='news')
#    return _tmp

for stage in [1,2,3,4,5,6,7, 8, 9, 10]:
    for v in VTYPE_:
        _get_news(stage,vtype=v)
        get_stage_stats(stage)
        for timing in TIMING_:
            _fetch_timing_data(stage,vtype=v, timerank=timing)
        for ranking in RANKING_:  
            _fetch_ranking_data(stage,vtype=v, timerank=ranking)

<!-- #endraw -->

## Ranking Data

Process the ranking data...

So what have we got to work with?

```python
rdata = _fetch_ranking_data(STAGE, vtype=VTYPE, timerank='general', showstage=True)

rdata[RANK].head()
```

```python
rdata[RANK].dtypes
```

<!-- #region -->
The basic retrieval returns a table with timing data as strings, and the `Bib` identifier as an integer.

The `Bib` identifer, We could also regard it as a string so that we aren't tempted to treat it as a number, inwhich case we should also ensure that any extraneous whitespace is stripped if the `Bib` was already a string:

```python
rdata[RANK]['Bib'] = rdata[RANK]['Bib'].astype(str).str.strip()
```
<!-- #endregion -->

```python
rdata[RANK]['Bib'] = rdata[RANK]['Bib'].astype(int)
```

## Convert time to timedelta

Several of the datasets return times, as strings, in the form: `HH:MM:SS`.

We can convert these times to timedeltas.

Timing related columns are all columns except those in `['Pos','Bib','Crew','Brand']`.

We can also prefix timing columns in the timing data screens so we can recreate the order they should appear in:

```python
#Prefix each split designator with a split count
timingcols=['dss','wp1','wp2','ass']
{ x:'{}_{}'.format('{0:02d}'.format(i), x) for i, x in enumerate(timingcols, 0) }
```

One of the things we need to handle are timing columns where the timing data may be mixed with other sorts of data in the raw data table.

Routines for cleaner the data are included in the timing handler function but they were actually "backfilled" into the function after creating them (originally) later on in the notebook.

```python
from pandas.api.types import is_string_dtype

#At first sight, this looks quite complicated, but a lot of it is backfilled 
# to take into account some of the cleaning we need to do for the full (messy) timing data
def _get_timing(df, typ=TIME, kind='simple'):
    ''' Convert times to time deltas and
        prefix waypoint / timing columns with a two digit counter. '''
    #kind: simple, full, raw
    #Some of the exclusion column names are backfilled into this function
    # from columns introduced later in the notebook
    # What we're trying to do is identify columns that aren't timing related
    timingcols = [c for c in df[typ].columns if c not in ['Pos','Overall Position','Bib','Crew','Brand',
                                                          'Refuel', 'Road Position', 'Stage'] ]
    
    #Clean up the data in a timing column, then cast to timedelta
    for col in timingcols:
        #A column of NAs may be incorrectly typed
        #print(df[typ].columns)
        df[typ][col] = df[typ][col].fillna('').str.strip()
        #In the simple approach, we just grab the timing data and dump the mess
        if kind!='full':
            df[typ][col] = df[typ][col].str.extract(r'(\d{1,2}:\d{1,2}:\d{1,2})')
        else:
            #The full on extractor - try to parse out all the data
            #  that has been munged into a timing column
            if col==timingcols[-1]:
                #There's an end effect:
                #  the last column in the timing dataset doesn't have position embedded in it
                # In this case, just pull out the position gained/maintained/lost flag
                df[typ][[col,col+'_gain']] = df[typ][col].str.extract(r'(\d{1,2}:\d{1,2}:\d{1,2})(.*)', expand=True)
            else:
                #In the main body of the table, position gain as well as waypoint rank position are available
                df[typ][[col,col+'_gain',col+'_pos']] = df[typ][col].str.extract(r'(\d{1,2}:\d{1,2}:\d{1,2})(.*)\((\d*)\)', expand=True)
                #Ideally, the pos cols would be of int type, but int doesn't support NA
                df[typ][col+'_pos'] = df[typ][col+'_pos'].astype(float)
        
        #Cast the time string to a timedelta
        if kind=='full' or kind=='raw':
            df[typ]['{}_raw'.format(col)] = df[typ][col][:]
        df[typ][col] = pd.to_timedelta( df[typ][col] )


    #In timing screen, rename cols with a leading two digit index
    #This allows us to report splits in order
    #We only want to do this for the timing data columns, not the rank timing columns...
    #Should really do this based on time type?
    timingcols = [c for c in timingcols if c not in ['Time','Gap','Penalty'] and not c.endswith(('_raw','_pos','_gain'))]
    timingcols_map = { x:'{}_{}'.format('{0:02d}'.format(i), x) for i, x in enumerate(timingcols, 0) }
    df[typ].rename(columns=timingcols_map, inplace=True)
    
    #TO_DO - need to number label the ..._raw, _pos and ..._gain cols
    #ThIS is overly complex becuase it handles ranking and timing frames
    #Better to split out to separate ones?
    for suffix in ['_raw','_pos','_gain']:
        cols=[c for c in df[typ].columns if (not c.startswith(('Time','Gap','Penalty'))) and c.endswith(suffix)]
        cols_map = { x:'{}_{}'.format(x,'{0:02d}'.format(i)) for i, x in enumerate(cols, 0) }
        df[typ].rename(columns=cols_map, inplace=True)
#TO_DO - elsewhere: trap for start with 0/1 and not end _gain etc
                    
    return df

```

<!-- #region -->
## Ranking Data Redux

Normalise the times as timedeltas.

For timestrings of the form `HH:MM:SS`, this is as simple as passing the timestring column to the *pandas* `.to_timedelta()` function:

```python
pd.to_timedelta( df[TIMESTRING_COLUMN] )
```

We just need to ensure we pass it the correct columns...
<!-- #endregion -->

```python
def get_ranking_data(stage,vtype='car', timerank='stage', kind='simple'):
    ''' Retrieve rank timing data and return it in a form we can work directly with. '''
    
    #kind: simple, raw
    df = _fetch_ranking_data(stage,vtype=vtype, timerank=timerank)
    df[RANK]['Bib'] = df[RANK]['Bib'].astype(int)
    return _get_timing(df, typ=RANK, kind=kind)
```

```python
get_ranking_data(STAGE, VTYPE, kind='raw')[RANK].head()
```

```python
STAGE, VTYPE
```

```python
#What changed?
get_ranking_data(STAGE, VTYPE, timerank='general', kind='raw')[RANK].head()
```

```python
ranking_data = get_ranking_data(STAGE, VTYPE)

ranking_data[RANK].head()
```

```python
ranking_data[RANK].dtypes
```

The `Crew` data is a bit of a mishmash. If we were to normalise this table, we'd have to split that data out...

For now, let's leave it...

...because sometimes, it can be handy to be able to pull out a chunk of unnormalised data as a simple string.

```python
ranking_data[RANK].dtypes
```

## Timing Data

The timing data needs some processing:

```python
data = _fetch_timing_data(STAGE, VTYPE)

data[TIME][60:70]
```

A full inspection of the time data shows that some additional metadata corresponding to whether in-stage refuelling is allowed may also be recorded in the `Bib` column (for example, `403 ⛽`).

We can extract this information into a separate dataframe / table.

```python
def get_refuel_status(df):
    ''' Parse the refuel status out of timing data Bib column.
        Return extended dataframe with a clean Bib column and a new Refuel column. '''
    
    #The .str.extract() function allows us to match separate groups using a regex
    #  and return the corresponding group data as distinct columns
    #Force the Bin type to a str if it isn't created as such so we can regex it...
    df[['Bib','_tmp']] = df['Bib'].astype(str).str.extract(r'(\d*)([^\d]*)', expand=True)
    
    #Set the Refuel status as a Boolean
    df.insert(2, 'Refuel', df['_tmp'])
    df.drop('_tmp', axis=1, inplace=True)
    df['Refuel'] = df['Refuel']!=''
    
    #Set the Bib value as an int
    df['Bib'] = df['Bib'].astype(int)
    
    return df
```

```python
data[TIME] = get_refuel_status(data[TIME])
data[TIME][60:70]
```

We also notice that the raw timing data includes information about split rank and how it compares to the rank at the previous split / waypoint, with the raw data taking the form `08:44:00= (11)`. Which is to say, `HH:MM:DDx (NN?)` where `x` is a comparator showing whether the rank at that waypoint improved (▲), remained the same as (=), or worsened (▼) compared to the previous waypoint.

Note that the final `ass` column does not include the rank.

We can use a regular expression to separate the data out, with each regex group being expanded into a separate column:

```python
data[TIME]['dss'].str.extract(r'(\d{2}:\d{2}:\d{2})(.*)\((\d*)\)', expand=True).head()
```

We can backfill an expression of that form into the timing data handler function above...

Now we wrap several steps together into a function that gets us a clean set of timing data, with columns of an appropriate type:

```python
def get_timing_data(stage,vtype='car', timerank='time', kind='simple'):
    ''' Get timing data in a form ready to use. '''
    df = _fetch_timing_data(stage,vtype=vtype, timerank=timerank)
    
    df[TIME] = get_refuel_status(df[TIME])
    return _get_timing(df, typ=TIME, kind=kind)
    
```

```python
get_timing_data(STAGE, VTYPE, kind='simple')[TIME].head()
```

```python
data = get_timing_data(STAGE, VTYPE, kind='full')
data[TIME].head()
```

```python
data[TIME].dtypes
```

## Parse Metadata

Some of the scraped tables are used to provide selection lists, but we might be able to use them as metadata tables.

For example, here's a pretty complete set, although mangled together, set of competititor names, nationalities, and team names:

```python
data[ CREWTEAM ].head()
```

It'll probably be convenient to have the unique `Bib` values available as an index:

```python
data[ CREWTEAM ] = data[ CREWTEAM ][['Bib', 'Names']].set_index('Bib')
data[ CREWTEAM ].head()
```

The `Names` may have several `Name (Country)` values, followed by a team name. The original HTML uses `<span>` tags to separate out values but the *pandas* `.read_html()` function flattens cell contents.
    
Let's have a go at pulling out the team names, which appear at the end of the string. If we can split each name, and the team name, into separate columns, and then metl those columns into separate rows, grouped by `Bib` number, we should be able to grab the last row, corrsponding to the team, in each group:

```python
#Perhaps split on brackets?
# At least one team has brackets in the name at the end of the name
# So let's make that case, at least, a "not bracket" by setting a ) at the end to a :]:
# so we don't (mistakenly) split on it as if it were a country-associated bracket.
teams = data[ CREWTEAM ]['Names'].str.replace(r'\)$',':]:').str.split(')').apply(pd.Series).reset_index().melt(id_vars='Bib', var_name='Num').dropna()

#Find last item in each group, which is to say: the team
teamnames = teams.groupby('Bib').last()
#Defudge any brackets at the end back
teamnames = teamnames['value'].str.replace(':]:',')')
teamnames.head()
```

Now let's go after the competitors. These are all *but* the last row in each group:

```python
#Remove last row in group i.e. the team
personnel = teams.groupby('Bib').apply(lambda x: x.iloc[:-1]).set_index('Bib').reset_index()

personnel[['Name','Country']] = personnel['value'].str.split('(').apply(pd.Series)

#Strip whitespace
for c in ['Name','Country']:
    personnel[c] = personnel[c].str.strip()
    
personnel[['Bib','Num','Name','Country']].head()
```

For convenience, we might want to reshape this long form back to a wide form, with a single string containing all the competitor names associated with a particular `Bib` identifier:

```python
#Create a single name string for each vehicle
#For each Bib number, group the rows associated with that number
# and aggregate the names in those rows into a single, comma separated, joined string
# indexed by the corresponding Bib number
personnel.groupby('Bib')['Name'].agg(lambda col: ', '.join(col)).tail()
```

```python
data[ BRANDS ].head()
```

```python
data[ COUNTRIES ].head()
```

```python
data[ COUNTRIES ][['Country','CountryCode']] = data[ COUNTRIES ]['Names'].str.extract(r'(.*) \((.*)\)',expand=True)
data[ COUNTRIES ].head()
```

```python
def get_annotated_timing_data(stage,vtype='car', timerank='time', kind='simple'):
    ''' Return a timing dataset that's ready to use. '''
    
    df = get_timing_data(stage, vtype, timerank, kind)
    
    col00 = [c for c in df[TIME].columns if c.startswith('00_')][0]
    df[TIME].insert(2,'Road Position', df[TIME].sort_values(col00,ascending=True)[col00].rank())
    return df
```

```python
get_annotated_timing_data(STAGE,vtype=VTYPE, timerank='time', kind='full')[TIME].head()
```

```python
t_data = get_annotated_timing_data(STAGE,vtype=VTYPE, timerank='time')[TIME]
t_data.head()
```

```python
not_timing_cols = ['Pos','Road Position','Refuel','Bib','Crew','Brand']

driver_data = t_data[ not_timing_cols ]
driver_data.head()
```

The number of waypoints differs across stages. If we cast the wide format waypoint data into a long form, we can more conveniently merge waypoint timing data from separate stages into the same dataframe.

```python
pd.melt(t_data.head(),
        id_vars=not_timing_cols,
        var_name='Waypoint', value_name='Time').head()
```

```python
def _timing_long(df, nodss=True):
    ''' Cast timing data to long data frame. '''
    
    df = pd.melt(df,
                 id_vars=[c for c in df.columns if not any(_c in c for _c in ['dss','wp','ass'])],
                 var_name='Waypoint', value_name='Time')

    if nodss:
        return df[~df['Waypoint'].str.startswith('00')]
    return df

def _typ_long(df, typ='_pos_', nodss=False):
    ''' Cast wide data to long data frame. '''
    
    df = pd.melt(df[['Bib']+[c for c in df.columns if typ in c]],
                 id_vars=['Bib'],
                 var_name='Waypoint', value_name=typ)
    df['WaypointOrder'] = df['Waypoint'].str.slice(-2).astype(int)
    if nodss:
        return df[~df['Waypoint'].str.startswith('00')]
    return df
```

```python
t_data_long = _timing_long(t_data)
t_data_long.head()
```

```python
t_data2 = get_annotated_timing_data(STAGE,vtype=VTYPE, timerank='time', kind='full')[TIME]
display(_typ_long(t_data2, '_gain_').head())
#_pos_, _raw_, _gain_       
#Clear down
t_data2 = None
```

```python
def get_long_annotated_timing_data(stage,vtype='car', timerank='time', kind='simple'):
    ''' Get annotated timing dataframe and convert it to long format. '''
    
    #TO DO: But for the db, we want the raw long, not the time long
    _tmp = get_annotated_timing_data(stage, vtype, timerank, kind=kind)

    #I don't think this works for anything other than kind=simple
    #TO DO: do we need to cope with the kind='full' stuff differently?
    _tmp[TIME] = _timing_long(_tmp[TIME])
    if kind=='simple':
        #Should really be testing if starts with an int
        _tmp[TIME]=_tmp[TIME][_tmp[TIME]['Waypoint'].str.startswith(('0','1'))]
    
    #Find the total seconds for each split / waypoint duration
    _tmp[TIME]['TimeInS'] = _tmp[TIME]['Time'].dt.total_seconds()
    
    if timerank=='gap':
        _tmp[TIME].rename(columns={'Time':'Gap', 'TimeInS':'GapInS'}, inplace=True)
    
    
    return _tmp
```

```python
#get_long_annotated_timing_data(3,vtype='quad', timerank='time')[TIME]
```

```python
get_long_annotated_timing_data(STAGE, VTYPE)[TIME].head()
```

```python
get_long_annotated_timing_data(STAGE, VTYPE, 'gap')[TIME].head()
```

## Saving the Data to a Database

The data can be saved to a database directly in an unnormalised form, or we can tidy it up a bit and save it in a higher normal form.

The table structure is far from best practice - it's pragmatic and in first instance intended simply to be useful...

```python
#%pip install git+https://github.com/simonw/sqlite-utils.git
```

```python
#%pip uninstall -y sqlite-utils
#%pip install --upgrade sqlite-utils
from sqlite_utils import Database
```

```python
def cleardbtable(conn, table):
    ''' Clear the table whilst retaining the table definition '''
    c = conn.cursor()
    c.execute('DELETE FROM "{}"'.format(table))
    
def dbfy(conn, df, table, if_exists='append', index=False, clear=False, **kwargs):
    ''' Save a dataframe as a SQLite table.
        Clearing or replacing a table will first empty the table of entries but retain the structure. '''
    if if_exists=='replace':
        clear=True
        if_exists='append'
    if clear: cleardbtable(conn, table)
    df.to_sql(table,conn,if_exists=if_exists,index=index)
```

```python
#dbname='dakar_test_sql.sqlite'
#!rm $dbname
```

```python
import sqlite3

dbname='dakar_sql_2020.sqlite'
!rm $dbname

conn = sqlite3.connect(dbname)

c = conn.cursor()

setup_sql= 'dakar.sql'
with open(setup_sql,'r') as f:
    txt = f.read()
    c.executescript(txt)
    
db = Database(conn)
```

```python
q="SELECT name FROM sqlite_master WHERE type = 'table';"
pd.read_sql(q, conn)
```

```python
tmp = teamnames.reset_index()
tmp['Year'] = YEAR
tmp.rename(columns={'value':'Team'}, inplace=True)
dbfy(conn, tmp, "teams")
```

```python
q="SELECT * FROM teams LIMIT 3;"
pd.read_sql(q, conn)
```

```python
tmp = personnel[['Bib','Num','Name','Country']]
tmp['Year'] = YEAR
dbfy(conn, tmp, "crew")
```

```python
q="SELECT * FROM crew LIMIT 3;"
pd.read_sql(q, conn)
```

```python
q="SELECT * FROM stagestats LIMIT 3;"
pd.read_sql(q, conn)
```

```python
tmp = get_stage_stats(STAGE).set_index('Special').T.reset_index()
tmp.rename(columns={'Leader at latest WP':'LeaderLatestWP', 'index':'Vehicle', "Latest WP":'LatestWP',
                    'At start':'AtStart', 'Nb at latest WP':'NumLatestWP' }, inplace=True)
tmp[['BibLatestWP', 'NameLatestWP']] = tmp['LeaderLatestWP'].str.extract(r'([^ ]*) (.*)',expand=True)
tmp['BibLatestWP'] = tmp['BibLatestWP'].astype(int)
tmp
```

```python
#If the stage hasn't run we get live stats?
get_stage_stats(3).set_index('Special').T
```

```python
t_vehicles = db["vehicles"]
t_ranking = db["ranking"]
t_waypoints = db["waypoints"]
t_stagemeta = db["stagemeta"]
t_stagestats = db["stagestats"]

# 'Year', 'Bib' is a sensible common key for several things?

for stage in [1,2,3,4,5,6,7, 8, 9, 10]:
    for v in VTYPE_:
        #_get_news(stage,vtype=v)
        #get_stage_stats(stage)
        print(stage,v)
        
        tmp = get_stage_stats(STAGE).set_index('Special').T
        tmp.drop(['Number of participants'], axis=1, inplace=True)
        tmp.rename(columns={'Leader at latest WP':'LeaderLatestWP', 'index':'Vehicle',  "Latest WP":'LatestWP',
                    'At start':'AtStart', 'Nb at latest WP':'NumLatestWP' }, inplace=True)
        tmp[['BibLatestWP', 'NameLatestWP']] = tmp['LeaderLatestWP'].str.extract(r'([^ ]*) (.*)',expand=True)
        tmp['BibLatestWP'] = tmp['BibLatestWP'].astype(int)
        tmp['Stage'] = stage

        try:
            t_stagestats.upsert_all(tmp.to_dict(orient='records'))
        except:
            t_stagestats.insert_all(tmp.to_dict(orient='records'))
        
        #TO DO create new functions to get data for the db?
        # TO DO: Gap and Time as raw not time, use _raw - perhaps another melt and merge?
        # TO DO: bring in _pos for Waypoint_Rank  - perhaps another melt and merge?
        #use kind=full
        tmp = get_long_annotated_timing_data(stage,vtype=v, timerank='time')[TIME]
        tmp['Year'] = YEAR
        tmp['Stage'] = stage
        
        tmp.rename(columns={'Road Position':'RoadPos'}, inplace=True)
        try:
            t_stagemeta.upsert_all(tmp[['Year', 'Stage', 'Bib','RoadPos','Refuel']].to_dict(orient='records'))
        except:
            t_stagemeta.insert_all(tmp[['Year', 'Stage', 'Bib','RoadPos','Refuel']].to_dict(orient='records'))
            
        tmp2 = get_long_annotated_timing_data(stage,vtype=v, timerank='gap')[TIME]
        tmp = pd.merge(tmp, tmp2[['Bib', 'Gap', 'GapInS']], on='Bib')

        tmp['WaypointOrder'] = tmp['Waypoint'].str.slice(0,2).astype(int)
        tmp3=get_annotated_timing_data(stage,vtype=v, timerank='time', kind='full')[TIME]

        tmp = pd.merge(tmp, _typ_long(tmp3, '_pos_')[['Bib','WaypointOrder', '_pos_']], on=['Bib','WaypointOrder'])
        tmp.rename(columns={'_pos_':'WaypointRank'}, inplace=True)

        #TO DO: use Gap and Time as raw
        #print(tmp.columns, tmp.dtypes)
        _tcols = ["Time","Gap"]
        for t in _tcols:
            tmp['{}InS'.format(t)] = pd.to_timedelta( tmp[t] ).dt.total_seconds()
        tmp.drop(['RoadPos', 'Refuel', 'Brand', 'Crew'], axis=1, inplace=True)
        tmp.drop(['Time','Gap'], axis=1, inplace=True)
        
        tmp = pd.merge(tmp, _typ_long(tmp3, '_raw_')[['Bib','WaypointOrder', '_raw_']], on=['Bib','WaypointOrder'])
        tmp.rename(columns={'_raw_':'Time_raw'}, inplace=True)

        tmp = pd.merge(tmp, _typ_long(get_annotated_timing_data(stage,vtype=v, timerank='gap', kind='full')[TIME], '_raw_')[['Bib','WaypointOrder', '_raw_']],
                       on=['Bib','WaypointOrder'])
        tmp.rename(columns={'_raw_':'Gap_raw'}, inplace=True)
        try:
            t_waypoints.upsert_all(tmp.to_dict(orient='records'))
        except:
            t_waypoints.insert_all(tmp.to_dict(orient='records'))
            
        for ranking in RANKING_:  
            tmp = get_ranking_data(stage,vtype=v, timerank=ranking, kind='raw')[RANK]
            tmp['Year'] = YEAR
            
            tmp['Stage'] = stage
            tmp['Type'] = ranking
            _tcols = ["Time","Gap","Penalty"]
            for t in _tcols:
                tmp['{}InS'.format(t)] = pd.to_timedelta( tmp[t] ).dt.total_seconds()
            tmp.drop(_tcols, axis=1, inplace=True)
            
            tmp['VehicleType'] = v
            try:
                t_ranking.upsert_all(tmp.to_dict(orient='records'))
            except:
                t_ranking.insert_all(tmp.to_dict(orient='records'))

            tmp = tmp[['Year','Bib','VehicleType', 'Brand']]
            try:
                t_vehicles.upsert_all(tmp.to_dict(orient='records'))
            except:
                t_vehicles.insert_all(tmp.to_dict(orient='records'))
                       

```

```python
tmp
```

```python
q="SELECT * FROM stagestats;"
pd.read_sql(q, conn)
```

```python
tmp

```

```python
q="SELECT * FROM waypoints where bib=509 LIMIT 20;"
pd.read_sql(q, conn)
```

```python
q="SELECT DISTINCT VehicleType FROM vehicles;"
pd.read_sql(q, conn)
```

```python
q="SELECT * FROM ranking LIMIT 3;"
pd.read_sql(q, conn)
```

```python
q="SELECT * FROM ranking WHERE Type='general' AND Stage=2 AND Pos=4 LIMIT 5;"
pd.read_sql(q, conn)
```

```python
q="SELECT DISTINCT Brand FROM vehicles WHERE VehicleType='sxs' ;"
pd.read_sql(q, conn)
```

```python
q="SELECT w.* FROM waypoints w JOIN vehicles v ON w.Bib=v.Bib WHERE Stage=2 AND VehicleType='car' AND Pos<=10"
tmpq = pd.read_sql(q, conn)
tmpq.pivot(index='Bib',columns='Waypoint',values='TimeInS')
```

```python
#Example - driver overall ranks by stage in in top10 overall at end of stage
q="SELECT * FROM ranking WHERE VehicleType='car' AND Type='general' AND Pos<=10"
tmpq = pd.read_sql(q, conn)
tmpq.pivot(index='Bib',columns='Stage',values='Pos')
```

```python
tmpq
```

```python
#Driver top 5 finishes by stage
q="SELECT r.*, c.Name FROM ranking r LEFT JOIN crew c ON c.Bib=r.Bib WHERE Num=0 AND VehicleType='car' AND Type='stage' AND Pos<=5"

tmpq = pd.read_sql(q, conn)
tmpq.pivot(index='Stage',columns='Pos',values='Name')
```

# Reset Base Dataframes

That is, reset for the particular data config we defined at the top of the notebook.

```python

```

## Find the time between each waypoint

That is, the time taken to get from one waypoint to the next. If we think of waypoints as splits, this is eesentially a `timeInSplit` value. If we know this information, we can work out how much time each competitor made, or lost, relative to every other competitor in the same class, going between each waypoint.

This means we may be able to work out which parts of the stage a particular competitor was pushing on, or had difficulties on.

```python
def _get_time_between_waypoints(timing_data_long):
    ''' Find time taken to go from one waypoint to the next for each vehicle. '''
    
    #The timeInSplit is the time between waypoints.
    #So find the diff between each consecutive waypoint time for each Crew
    timing_data_long['timeInSplit'] = timing_data_long[['Crew','Time']].groupby('Crew').diff()

    #Because we're using a diff(), the first row is set to NaN - there's nothing to diff to
    #So use the time at the first split as the time from the start to the first waypoint.
    timing_data_long.loc[timing_data_long.groupby('Crew')['timeInSplit'].head(1).index, 'timeInSplit'] = timing_data_long.loc[timing_data_long.groupby('Crew')['timeInSplit'].head(1).index,'Time']

    #To finesse diff calculations on NaT, set diff with day!=0 to NaT
    #This catches things where we get spurious times calculated as diff times against NaTs
    timing_data_long.loc[timing_data_long['Time'].isna(),'timeInSplit'] = pd.NaT
    timing_data_long.loc[timing_data_long['timeInSplit'].dt.days!=0,'timeInSplit'] = pd.NaT

    #If there's been a reset, we can fill across
    timing_data_long[['Time','timeInSplit']] = timing_data_long[['Time','timeInSplit']].fillna(method='ffill',axis=1)

    #Find the total seconds for each split / waypoint duration
    timing_data_long['splitS'] = timing_data_long['timeInSplit'].dt.total_seconds()
    
    return timing_data_long



def get_timing_data_long_timeInSplit(stage, vtype='car', timerank='time'):
    ''' For a stage, get the data in long form, including timeInSplit times. '''
    
    timing_data_long = get_long_annotated_timing_data(stage, vtype, timerank)[TIME]
    timing_data_long = _get_time_between_waypoints(timing_data_long)
    return timing_data_long



#Preview some data
timing_data_long_insplit = get_timing_data_long_timeInSplit(STAGE, VTYPE)
#timing_data_long_insplit[timing_data_long_insplit['Brand']=='PEUGEOT'].head()
timing_data_long_insplit.head()

```

```python
timing_data_long = get_long_annotated_timing_data(STAGE, VTYPE)[TIME]
timing_data_long
#timing_data_long[timing_data_long['Brand']=='PEUGEOT'].head()
timing_data_long.head()
```

## Rebase relative to driver

Rebasing means finding deltas relative to a specified driver. It lets us see the deltas a particular driver has to each other driver.

```python
def rebaseTimes(times, bib=None, col=None):
    if bib is None or col is None: return times
    return times[col] - times[times['Bib']==bib][col].iloc[0]

def rebaseWaypointTimes(times, bib=None, col='splitS'):
    ''' Rebase times relative to a particular competitor. '''
    
    if bib is None: return times
    bib = int(bib)
    rebase = times[times['Bib']==bib][['Waypoint',col]].set_index('Waypoint').to_dict(orient='dict')[col]
    times['rebased']=times[col]-times['Waypoint'].map(rebase)
    return times

```

## Rebase Overall Waypoint Times and Time In Split Times

That is, rebase the overall time in stage at each waypoint relative to a specified driver.

```python
REBASER = 306
#loeb 306, AL-ATTIYAH 301, peterhansel 304
#aravind prabhakar 48 coronel 347
```

```python
timing_data_long_min = rebaseWaypointTimes( timing_data_long , REBASER, 'TimeInS')
```

```python
-timing_data_long_min.reset_index().pivot('Bib','Waypoint','rebased').head()
```

```python
rebaseWaypointTimes(timing_data_long_insplit,REBASER).head()
```

```python
def pivotRebasedSplits(rebasedSplits):
    ''' For each driver row, find the split. '''
    
    #If there are no splits...
    if rebasedSplits.empty:
        return pd.DataFrame(columns=['Bib']).set_index('Bib')
    
    rbp=-rebasedSplits.pivot('Bib','Waypoint','rebased')
    rbp.columns=['D{}'.format(c) for c in rbp.columns]
    #The columns seem to be sorted? Need to sort in actual split order
    rbp.sort_values(rbp.columns[-1],ascending =True)
    return rbp

```

```python
tmp = pivotRebasedSplits(rebaseWaypointTimes(timing_data_long_insplit,REBASER))
tmp.head()
```

```python
#top10 = driver_data[(driver_data['Pos']>=45) & (driver_data['Pos']<=65)]
top10 = driver_data[(driver_data['Pos']<=20)]
top10.set_index('Bib', inplace=True)
top10
```

## Display Stage Tables

A lot of this work is copied directly from my WRC stage tables notebooks, so there is less explanation here about what the pieces are and how they work together.

```python
#UPDATE THIS FROM WRC NOTEBOOKS TO HANDLE EXCEPTIONS, DATATYPES ETC
from IPython.core.display import HTML
import seaborn as sns

from numpy import NaN
from math import nan

def bg_color(s):
    ''' Set background colour sensitive to time gained or lost.
    '''
    attrs=[]
    for _s in s:        
        if _s < 0:
            attr = 'background-color: green; color: white'
        elif _s > 0: 
            attr = 'background-color: red; color: white'
        else:
            attr = ''
        attrs.append(attr)
    return attrs

#https://pandas.pydata.org/pandas-docs/stable/style.html
def color_negative(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    if isinstance(val, str) or pd.isnull(val): return ''
    
    
    val = val.total_seconds() if isinstance(val,pd._libs.tslibs.timedeltas.Timedelta) else val
    
    if val and (isinstance(val,int) or isinstance(val,float)):
        color = 'green' if val < 0 else 'red' if val > 0  else 'black'
    else:
        color='white'
    return 'color: %s' % color

def cleanDriverSplitReportBaseDataframe(rb2, ss):
    ''' Tidy up the driver split report dataframe, replacing 0 values with NaNs that can be hidden.
        Check column names and data types. '''
    
    #TO DO: set proper colnames
    if rb2.empty: return rb2
    
    rb2=rb2.replace(0,NaN)
    #rb2=rb2.fillna('') #This casts columns containing NA to object type which means we can't use nan processing
    
    #rb2['Road Position']=rb2['Road Position'].astype(float)
    return rb2

def __styleDriverSplitReportBaseDataframe(rb2, ss):
    ''' Test if basic dataframe styling.
        DEPRECATED. '''
    
    cm=sns.light_palette((210, 90, 60), input="husl",as_cmap=True)

    s=(rb2.fillna('').style
        .applymap(color_negative, subset=[c for c in rb2.columns if c not in ['Pos','Road Position','Crew','Brand'] ])
        .background_gradient(cmap=cm, subset=['Road Position'])
      )
    #data.style.applymap(highlight_cols, subset=pd.IndexSlice[:, ['B', 'C']])

    s.set_caption("{}: running times and deltas between each checkpoint.".format(ss))
    return s
```

```python
rb2c = pivotRebasedSplits(rebaseWaypointTimes(timing_data_long_insplit,REBASER))
rb2c = cleanDriverSplitReportBaseDataframe(rb2c, STAGE)

#rb2cTop10 = rb2c[rb2c.index.isin(top10['Bib'])]
rb2cTop10 = pd.merge(top10, rb2c, how='left', left_index=True,right_index=True)
#Need processing on the below - also change column order
newColOrder = rb2cTop10.columns[1:].tolist()+[rb2cTop10.columns[0]]
rb2cTop10=rb2cTop10[newColOrder]
rb2cTop10 = pd.merge(rb2cTop10, -timing_data_long_min.reset_index().pivot('Bib','Waypoint','rebased'), how='left', left_index=True,right_index=True)


#Cast s to timedelta

#for c in [c for c in rb2cTop10.columns if c.startswith('0')]:
#    rb2cTop10[c]=rb2cTop10[c].apply(lambda x: pd.to_timedelta('{}00:00:{}'.format('-' if x<0 else '', '0' if pd.isnull(x) else abs(x))))

#Rename last column
rb2cTop10.rename(columns={rb2cTop10.columns[-1]:'Stage Overall'}, inplace=True)


#Drop refuel column
rb2cTop10.drop('Refuel', axis=1, inplace=True)

rb2cTop10.head(3)
```

```python
s = __styleDriverSplitReportBaseDataframe(rb2cTop10, 'Stage {}'.format(STAGE))
html=s.render()
display(HTML(html))
```

```python
#Changes from WRC
#applymap(color_negative, - change column identification
#final apply - add 'Stage Overall' to list
#.background_gradient Add Pos
def moreStyleDriverSplitReportBaseDataframe(rb2,ss, caption=None):
    ''' Style the driver split report dataframe. '''
    
    if rb2.empty: return ''
        
    def _subsetter(cols, items):
        ''' Generate a subset of valid columns from a list. '''
        return [c for c in cols if c in items]
    
    
    #https://community.modeanalytics.com/gallery/python_dataframe_styling/
    # Set CSS properties for th elements in dataframe
    th_props = [
      ('font-size', '11px'),
      ('text-align', 'center'),
      ('font-weight', 'bold'),
      ('color', '#6d6d6d'),
      ('background-color', '#f7f7f9')
      ]

    # Set CSS properties for td elements in dataframe
    td_props = [
      ('font-size', '11px'),
      ]

    # Set table styles
    styles = [
      dict(selector="th", props=th_props),
      dict(selector="td", props=td_props)
      ]
    
    #Define colour palettes
    #cmg = sns.light_palette("green", as_cmap=True)
    #The blue palette helps us scale the Road Position column
    # This may help us to help identify any obvious road position effect when sorting stage times by stage rank
    cm=sns.light_palette((210, 90, 60), input="husl",as_cmap=True)
    s2=(rb2.style
        .background_gradient(cmap=cm, subset=_subsetter(rb2.columns, ['Road Position', 'Pos','Overall Position', 'Previous Overall Position']))
        .applymap(color_negative,
                  subset=[c for c in rb2.columns if rb2[c].dtype==float and (not c.startswith('D') and c not in ['Overall Position','Overall Gap','Road Position', 'Pos'])])
        .highlight_min(subset=_subsetter(rb2.columns, ['Overall Position','Previous Overall Position']), color='lightgrey')
        .highlight_max(subset=_subsetter(rb2.columns, ['Overall Time', 'Overall Gap']), color='lightgrey')
        .highlight_max(subset=_subsetter(rb2.columns, ['Previous']), color='lightgrey')
        .apply(bg_color,subset=_subsetter(rb2.columns, ['{} Overall'.format(ss), 'Overall Time','Overall Gap', 'Previous', 'Stage Overall']))
        .bar(subset=[c for c in rb2.columns if str(c).startswith('D')], align='zero', color=[ '#5fba7d','#d65f5f'])
        .set_table_styles(styles)
        #.format({'total_amt_usd_pct_diff': "{:.2%}"})
       )
    
    if caption is not None:
        s2.set_caption(caption)

    #nan issue: https://github.com/pandas-dev/pandas/issues/21527
    return s2.render().replace('nan','')
```

```python
## Can we style the type in the red/green thing by using timedelta and formatter
# eg https://docs.python.org/3.4/library/datetime.html?highlight=weekday#datetime.date.__format__
#and https://stackoverflow.com/a/46370761/454773

#Maybe also add a sparkline? Need to set a common y axis on all charts?
```

```python
s2 = moreStyleDriverSplitReportBaseDataframe(rb2cTop10, STAGE)
display(HTML(s2))
```

# Sparklines

Sparklines are small, "in-cell" charts that can be used to summarise trend behaviour across multiple columns in a single dtaa table row.

If we visualise the time gap relative to each other driver across checkpoints we can gain a better idea of how the overall stage gap is evolving.

A basic sparkline simply indicates trends in terms of gradient change, although we can annotate it to show whether the final value represents a positive or negative gain overall:

```python
#https://github.com/iiSeymour/sparkline-nb/blob/master/sparkline-nb.ipynb
import matplotlib.pyplot as plt

from io import BytesIO
import urllib
import base64

def fig2inlinehtml(fig):
    figfile = BytesIO()
    fig.savefig(figfile, format='png')
    figfile.seek(0) 
    figdata_png = base64.b64encode(figfile.getvalue())
    #imgstr = '<img src="data:image/png;base64,{}" />'.format(figdata_png)
    imgstr = '<img src="data:image/png;base64,{}" />'.format(urllib.parse.quote(figdata_png))
    return imgstr


def sparkline(data, figsize=(4, 0.5), **kwags):
    """
    Returns a HTML image tag containing a base64 encoded sparkline style plot
    """
    data = list(data)
    
    fig, ax = plt.subplots(1, 1, figsize=figsize, **kwags)
    ax.plot(data)
    for k,v in ax.spines.items():
        v.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])    

    dot = 'r.' if data[len(data) - 1] <0 else 'g.'
       
    plt.plot(len(data) - 1, data[len(data) - 1], dot)

    ax.fill_between(range(len(data)), data, len(data)*[min(data)], alpha=0.1)

    return fig2inlinehtml(fig)
```

Sparkline charts get messed up by NAs. If there are lots of NAs in a column, dump the column.

```python
#Get rid of NA cols - maybe do this on a theshold?
rb2cTop10.dropna(how='all',axis=1,inplace=True)
```

```python
#The sparkline expects a list of values, so lets create a a temporary / dummy cell
# that has a list of values from columns in the same row that we want to plot across
#For example, let's grab the gap times at each waypoint:
#  these are in columns prefixed with a digit, (for convenience, just grap on leading 0;
# if there are more than 10 waypoints, we'll need to address this...)
#Ee should really also add the Stage Overall time in too, as that's essentially the last waypoint
rb2cTop10['test']= rb2cTop10[[c for c in rb2cTop10.columns if c.startswith(('0','1','Stage Overall'))]].values.tolist()
#Swap the sign of the values
rb2cTop10['test'] = rb2cTop10['test'].apply(lambda x: [-y for y in x])
rb2cTop10.head()
```

```python
rb2cTop10[:3]['test'].map(sparkline)
```

A more informative display gain show whether the gapt time at each split / waypoint is positive or negative by using different colour fills above and below a gap of zero seconds. We can also interpolate values to remove gaps appearing in the line when the sign of the value at one way point is different to the sign of the gap at the previous waypoint.

Because the sign of the gap is indicated, we don't need to identify it with the sign colour marker at the end of the line.

```python
import scipy

def sparkline2(data, figsize=(2, 0.5), colband=(('red','green'),('red','green')),
               dot=False, typ='line', **kwargs):
    """
    Returns a HTML image tag containing a base64 encoded sparkline style plot
    """
    #data = [0 if pd.isnull(d) else d for d in data]
    
    fig, ax = plt.subplots(1, 1, figsize=figsize, **kwargs)
    
    if typ=='bar':
        color=[ colband[0][0] if c<0 else colband[0][1] for c in data  ]
        ax.bar(range(len(data)),data, color=color, width=0.8)
    else:
        #Default is line plot
        ax.plot(data, linewidth=0.0)
    
        d = scipy.zeros(len(data))

        #If we don't interpolate, we get a gap in the sections/waypoints
        #  where times change sign compared to the previous section/waypoint
        ax.fill_between(range(len(data)), data, where=data<d, interpolate=True, color=colband[0][0])
        ax.fill_between(range(len(data)), data, where=data>d, interpolate=True,  color=colband[0][1])

        if dot:
            dot = colband[1][0] if data[len(data) - 1] <0 else colband[1][1]
            plt.plot(len(data) - 1, data[len(data) - 1], dot)

    for k,v in ax.spines.items():
        v.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])

    
    return fig2inlinehtml(fig)
```

```python
rb2cTop10.head(3)['test'].apply(sparkline2, typ='bar');
```

```python
display(HTML(rb2cTop10.head(3).style.render()))
```

```python
%%capture
brandcol = rb2cTop10.columns.get_loc("Brand")
rb2cTop10.insert(brandcol+1, 'Stage Gap', rb2cTop10['test'].apply(sparkline2,typ='bar'))
rb2cTop10.drop('test', axis=1, inplace=True)
```

```python
display(HTML(rb2cTop10.head(3).style.render()))
```

## Additional Sparklines

A couple more sparkline charts:

- rank across splits;
- gap to leader.

```python
# get gap to leader at each split
tmp = get_timing_data(STAGE,vtype=VTYPE, timerank='gap', kind='full')[TIME].set_index('Bib')
tmp.head()
```

```python
cols = [c for c in tmp.columns if c.startswith(('0','1'))]
tmp[cols]  = tmp[cols].apply(lambda x: x.dt.total_seconds())
tmp['test']= tmp[[c for c in tmp.columns if (c.startswith(('0','1')) and 'dss' not in c)]].values.tolist()
tmp['test2']= tmp[[c for c in tmp.columns if '_pos' in c]+['Pos']].values.tolist()
#Want better rank higher up
tmp['test2'] = tmp['test2'].apply(lambda x: [-y for y in x])
tmp.head()
```

```python
tmp.head(3)['test'].map(sparkline2);
```

```python
def sparklineStep(data, figsize=(2, 0.5), **kwags):
    #data = [0 if pd.isnull(d) else d for d in data]
    
    fig, ax = plt.subplots(1, 1, figsize=figsize, **kwags)
    
    plt.axhspan(-1, -3, facecolor='lightgrey', alpha=0.5)
    #ax.plot(range(len(data)), [-3]*len(data), linestyle=':', color='lightgrey')
    #ax.plot(range(len(data)), [-1]*len(data), linestyle=':', color='lightgrey')
    ax.plot(range(len(data)), [-10]*len(data), linestyle=':', color='lightgrey')
    ax.step(range(len(data)), data, where='mid')

    ax.set_ylim(top=-0.9)
        
    for k,v in ax.spines.items():
        v.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    
    return fig2inlinehtml(fig)
```

```python
rb3c = get_timing_data(STAGE,vtype=VTYPE, timerank='gap', kind='full')[TIME].set_index('Bib')
```

```python
def _get_col_loc(df, col=None, pos=None, left_of=None, right_of=None):
    ''' Return column position number. '''
    if col in df.columns:
        return df.columns.get_loc(col)
    elif pos and pos <len(df.columns):
        return pos
    else:
        pos = 0
    if left_of in df.columns:
        pos = df.columns.get_loc(left_of)
    elif right_of in df.columns:
        pos = min(df.columns.get_loc(right_of)+1,len(df.columns)-1)
    return pos

def moveColumn(df, col, pos=None, left_of=None, right_of=None):
    ''' Move dataframe column adjacent to a specified column. '''
    pos = _get_col_loc(df, None, pos, left_of, right_of)
    
    data = df[col].tolist()
    df.drop(col, axis=1, inplace=True)
    df.insert(pos, col, data)

def insertColumn(df, col, data, pos=None, left_of=None, right_of=None):
    ''' Insert data in dataframe column at specified location. '''
    pos =  _get_col_loc(df, col, pos, left_of, right_of)
    print(pos)
    df.insert(pos, col, data)
```

```python
rb4cTop10.insert?

```

```python
%%capture
rb3cTop10 = pd.merge(rb2cTop10[[]], rb3c, how='left', left_index=True,right_index=True)

cols = [c for c in rb3cTop10.columns if c.startswith(('0','1'))]
#rb2cTop10[cols]  = rb2cTop10[cols].apply(lambda x: x.dt.total_seconds())

rb3cTop10['test2']= rb3cTop10[[c for c in rb3cTop10.columns if ('_pos' in c and 'dss' not in c)]+['Pos']].values.tolist()
#Want better rank higher up
rb3cTop10['test2'] = rb3cTop10['test2'].apply(lambda x: [-y if not pd.isnull(y) else float('NaN') for y in x ])
rb3cTop10['Waypoint Rank'] = rb3cTop10['test2'].apply(sparklineStep,figsize=(0.5, 0.5))

#rb3cTop10['test']= rb3cTop10[[c for c in rb3cTop10.columns if (c.startswith(('0','1')) and 'dss' not in c)]].values.tolist()
#rb3cTop10['test'] = rb3cTop10['test'].apply(lambda x: [-y if not pd.isnull(y) else float('NaN') for y in x ])
#rb3cTop10['Gap to Leader'] =  rb3cTop10['test'].apply(sparkline2, 
#                                                      figsize=(0.5, 0.5), 
#                                                      dot=True, 
#                                                      colband=(('pink','lightgreen'),('r.','g.')))

#Get rid of NA cols - maybe do this on a threshold?
rb3cTop10.dropna(how='all',axis=1,inplace=True)

rb3cTop10['test']= rb3cTop10[[c for c in rb3cTop10.columns if (c.startswith(('0','1')) and 'dss' not in c)]].values.tolist()
rb3cTop10['test'] = rb3cTop10['test'].apply(lambda x: [-y if not pd.isnull(y) else float('NaN') for y in x ])
rb3cTop10['Gap to Leader'] =  rb3cTop10['test'].apply(sparkline2, 
                                                      figsize=(0.5, 0.5), 
                                                      dot=True,
                                                      colband=(('pink','lightgreen'),('r.','g.')))

#rb3cTop10.drop('test', axis=1, inplace=True)
#rb3cTop10.drop('test2', axis=1, inplace=True)

#Use a copy for nw, while testing
rb4cTop10 = rb2cTop10.copy()
insertColumn(rb4cTop10, 'Gap to Leader', rb3cTop10['Gap to Leader'], right_of='Pos')
insertColumn(rb4cTop10, 'Waypoint Rank', rb3cTop10['Waypoint Rank'], right_of='Brand')

#rb4cTop10 = pd.merge(rb2cTop10, rb3cTop10[['Gap to Leader','Waypoint Rank']], left_index=True,right_index=True)

```

```python
rb4cTop10.head(2)
```

## Add in Overall Position

Bring in overall position data from end of stage and end of previous stage

```python
#Overall Position, Previous
tmp = get_ranking_data(STAGE, VTYPE,timerank='general')[RANK].head()
tmp
#Rename pos as Overall Position?
```

```python
def getCurrPrevOverallRank(stage, vtype='car', rebase=None):
    curr = get_ranking_data(stage, vtype,timerank='general')[RANK]
    curr.rename(columns={'Time':'Overall Time', 'Pos':'Overall Position'}, inplace=True)
    curr['Overall Gap'] = curr['Gap'].dt.total_seconds()
    if stage>1:
        prev = get_ranking_data(stage-1, vtype,timerank='general')[RANK]
        prev.rename(columns={'Overall Position':'Previous Overall Position',
                             'Time':'Previous Time',
                             'Gap':'Previous Gap'}, inplace=True)
        prev['Previous'] = prev['Previous Gap'].dt.total_seconds()
    else:
        prev=pd.DataFrame({'Bib':curr['Bib'],'Previous Overall Position':NaN,
                           'Previous Time':pd.Timedelta(''),
                           'Previous Gap':pd.Timedelta(''), 
                           'Previous':pd.Timedelta('')})
        
    #if rebase we need to rebase prev['Previous'] and curr['Overall Gap']
    if rebase:
        #in rebaser, note the ploarity of gap
        prev['Previous'] = -rebaseTimes(prev, bib=rebase, col='Previous Gap')
        prev['Previous'] = prev['Previous'].dt.total_seconds()
        curr['Overall Gap'] = -rebaseTimes(curr, bib=rebase, col='Overall Gap')
    df = pd.merge(curr[['Bib','Overall Position', 'Overall Gap']],
                    prev[['Bib','Previous Overall Position','Previous Time', 'Previous']],
                    on='Bib')
    
    return df.set_index('Bib')
```

```python
getCurrPrevOverallRank(1, rebase=REBASER).head().dtypes
```

```python
getCurrPrevOverallRank(STAGE, VTYPE, rebase=REBASER).head()
```

```python
rb4cTop10 = pd.merge(rb4cTop10,
                     getCurrPrevOverallRank(STAGE, VTYPE, rebase=REBASER)[['Overall Position',
                                                                            'Previous Overall Position',
                                                                           'Overall Gap', 'Previous']],
                     left_index=True, right_index=True)
moveColumn(rb4cTop10, 'Previous', left_of='Crew')
moveColumn(rb4cTop10, 'Previous Overall Position', left_of='Previous')
moveColumn(rb4cTop10, 'Overall Position', right_of='Pos')
moveColumn(rb4cTop10, 'Overall Gap', left_of='Pos')
rb4cTop10.head(3)
```

```python
rb4cTop10.columns
```

## Table Image Grabber

One way of capturing the table output is to render the HTML page in a browser and then grab a screenshot of the contents of the browser window.

The `selenium` web testing framework can help us achieve this.

The code below is taken from my WRC notebooks, so there is less commentary about what the pieces are and how they fit together than there might otherwise be...


Changes to outputter - comment out set_window_size to allow browser to get full table, full table grabber

```python
import os
import time
from selenium import webdriver

#Via https://stackoverflow.com/a/52572919/454773
def setup_screenshot(driver,path):
    # Ref: https://stackoverflow.com/a/52572919/
    original_size = driver.get_window_size()
    required_width = driver.execute_script('return document.body.parentNode.scrollWidth')
    required_height = driver.execute_script('return document.body.parentNode.scrollHeight')
    driver.set_window_size(required_width, required_height)
    # driver.save_screenshot(path)  # has scrollbar
    driver.find_element_by_tag_name('body').screenshot(path)  # avoids scrollbar
    driver.set_window_size(original_size['width'], original_size['height'])
    

def getTableImage(url, fn='dummy_table', basepath='.', path='.', delay=5, height=420, width=800):
    ''' Render HTML file in browser and grab a screenshot. '''
    browser = webdriver.Chrome()
    #browser.set_window_size(width, height)
    browser.get(url)
    #Give the map tiles some time to load
    time.sleep(delay)
    imgpath='{}/{}.png'.format(path,fn)
    imgfn = '{}/{}'.format(basepath, imgpath)
    imgfile = '{}/{}'.format(os.getcwd(),imgfn)
    
    setup_screenshot(browser,imgfile)
    browser.quit()
    os.remove(imgfile.replace('.png','.html'))
    #print(imgfn)
    return imgpath


def getTablePNG(tablehtml,basepath='.', path='testpng', fnstub='testhtml'):
    ''' Save HTML table as file. '''
    if not os.path.exists(path):
        os.makedirs('{}/{}'.format(basepath, path))
    fn='{cwd}/{basepath}/{path}/{fn}.html'.format(cwd=os.getcwd(), basepath=basepath, path=path,fn=fnstub)
    tmpurl='file://{fn}'.format(fn=fn)
    with open(fn, 'w') as out:
        out.write(tablehtml)
    return getTableImage(tmpurl, fnstub, basepath, path)
```

We get some nonsense in charts if there are missing values.

```python
#TO DO - how about if we colour the Overall pos
# eg increasing red for further ahead, increasing green for further behind?
#or colour depending on whether overall went up a rank, down, or stayed same?
```

```python
s2 = moreStyleDriverSplitReportBaseDataframe(rb4cTop10, STAGE)
display(HTML(s2))
```

```python
getTablePNG(s2)
```

```python
!pwd
```

```python

```

## Grab data over several stages

```python
##sketch - data grab

#Create a dataframe of stage times over several stages
rallydata = pd.DataFrame()

for stage in [1,2,3,4,5, 6]:
    _data = _get_timing( _get_data(stage), TIME)[TIME]
    _timing_data_long = _timing_long(_data)
    _timing_data_long.insert(0,'stage', stage)
    rallydata = pd.concat([rallydata, _timing_data_long], sort=False)
```

```python

```

```python

```
