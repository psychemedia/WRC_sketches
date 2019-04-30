
## Dakar Rally Scraper

This notebook provides a case study in retrieving, cleaning, organising and processing data obtained from  a third party website, specifically, timing and results data from the 2019 Dakar Rally.

Another way of thinking of it is as a series of marks created during an exploratory data analysis performance.

Or palaver.

Whatever.

Shall we begin?

    In [634]: #Use requests cache so we can keep an archive of the results HTML
              import requests
              import pandas as pd
              
              from numpy import nan as NaN

    In [23]: import requests_cache
             requests_cache.install_cache('dakar_cache_2019', backend='sqlite')

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


    In [24]: YEAR = 2019
             STAGE = 3
             VTYPE = 'car'


## Stage Info

Retrieve some basic information about a stage.

    In [25]: def get_stage_stats(stage):
                 stage_stats_url='https://gaps.dakar.com/2019/dakar/index_info.php?l=ukie&s={stage}&vh=a'
             
                 html = requests.get(stage_stats_url.format(stage=STAGE)).content
                     
                 stage_stats_df=pd.read_html(html)[0]
             
                 return stage_stats_df.rename(columns=stage_stats_df.iloc[0]).drop(stage_stats_df.index[0])
             
             get_stage_stats(STAGE)
    
    Out[25]: 

{width="wide"}
|    |                Special |             Moto |                 Quad |             Car |              SxS |        Truck |
|----|------------------------|------------------|----------------------|-----------------|------------------|--------------|
|  1 |                  Start |            06:15 |                07:16 |           08:15 |            09:22 |        09:02 |
|  2 |                Liaison |            798km |                798km |           798km |            798km |        798km |
|  3 |                Special |            331km |                331km |           331km |            331km |        331km |
|  4 | Number of participants |              NaN |                  NaN |             NaN |              NaN |          NaN |
|  5 |               At start |              134 |                   25 |              95 |               30 |           38 |
|  6 |                   Left |              133 |                   24 |              94 |               30 |           30 |
|  7 |                Arrived |              124 |                   23 |              80 |               26 |           24 |
|  8 |              Latest WP |              ass |                  ass |             ass |              ass |          ass |
|  9 |    Leader at latest WP | 018 DE SOULTRAIT | 241 GONZALEZ FERIOLI | 304 PETERHANSEL | 358 FARRES GUELL | 518 KARGINOV |
| 10 |        Nb at latest WP |              124 |                   23 |              80 |               26 |           24 |




## Timing Data

Typical of many rallies, the live timing pages return several sorts of data:

- times and gaps for each stage;
- stage and overall results for each stage.

    In [26]: URL_PATTERN='https://gaps.dakar.com/2019/dakar/?s={stage}&c=aso&l=ukie&vi={tab}&sv={timerank}&vh={vtype}&sws=99'
             #sws - selected waypoint?

    In [27]: #Vehicle types
             VTYPE_ ={ 'car':'a','moto':'m','quad':'q','sxs':'s','truck':'c'}
             
             #Screen / tab selection
             TAB_ = {'timing':0,'news':1,'ranking':2}
             
             #Options for timing data
             TIMING_ = {'gap':0,'time':1}
             
             #Options for ranking data
             RANKING_ = {'stage':0, 'general':1}


## Previewing the data

Let's see what the data looks like...

*Uncomment and run the following to preview / inspect the data that's avaliable.*

    In [130]: #pd.read_html(URL_PATTERN.format(stage=STAGE,tab=TAB_['timing'],vtype=VTYPE_['car'],timerank='time'))

    In [131]: #pd.read_html(URL_PATTERN.format(stage=STAGE,tab=TAB_['ranking'],vtype=VTYPE_['car'],timerank='stage'))

By inspection, we note that:

- several tables are returned by each page;
- there are common identifier columns (`['Pos','Bib','Crew','Brand']`);
- there are irrelevant columns (`['View details','Select']`);
- the raw timing data columns (timig data at each waypoint) include information about split rank and how it compares to the rank at the previous split / waypoint; this needs to be cleaned before we can convert timestrings to timedeltas.

    In [28]: #TIMING = RANKING = 0 #deprecate these
             TIME = RANK = 0
             CREWTEAM = 1
             BRANDS = 2
             COUNTRIES = 3


## Retrieving the Data Tables

We can define helper functions to pull back tables associated with the timing or ranking pages.

    In [601]: #Retrieve a page
              
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
              
              
              def _fetch_timing_data(stage,vtype='car', timerank='time', showstage=False):
                  ''' Return data tables from timing page. '''
                  _tmp = _data(stage,vtype=vtype, tab='timing', timerank=timerank, showstage=showstage)
                  _tmp[TIME].drop(columns=['View details','Select'], inplace=True)
                  return _tmp
              
              def _fetch_ranking_data(stage,vtype='car', timerank='stage', showstage=False):
                  ''' Return data tables from ranking page. '''
                  rank_cols = ['Pos','Bib','Crew','Brand','Time','Gap','Penalty']
                  _tmp = _data(stage,vtype=vtype, tab='ranking', timerank=timerank, showstage=showstage)
                  _tmp[RANK].drop(columns=['View details','Select'], inplace=True)
                  if timerank=='general':
                      _tmp[RANK].rename(columns={'Pos':'Overall Position'}, inplace=True)
                  return _tmp
              


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



## Ranking Data

Process the ranking data...

So what have we got to work with?

    In [602]: rdata = _fetch_ranking_data(STAGE, vtype=VTYPE, timerank='general', showstage=True)
              
              rdata[RANK].head()
    
    Out[602]: 

{width="wide"}
|   | Stage | Overall Position | Bib |                                           Crew |  Brand |     Time |     Gap |  Penalty |
|---|-------|------------------|-----|------------------------------------------------|--------|----------|---------|----------|
| 0 |     3 |                1 | 301 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 08:34:08 | 0:00:00 | 00:00:00 |
| 1 |     3 |                2 | 314 |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 08:40:56 | 0:06:48 | 00:00:00 |
| 2 |     3 |                3 | 304 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 08:41:11 | 0:07:03 | 00:00:00 |
| 3 |     3 |                4 | 307 |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 08:46:10 | 0:12:02 | 00:00:00 |
| 4 |     3 |                5 | 303 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 08:47:53 | 0:13:45 | 00:00:00 |



    In [31]: rdata[RANK].dtypes
    
    Out[31]: Pos         int64
             Bib         int64
             Crew       object
             Brand      object
             Time       object
             Gap        object
             Penalty    object
             dtype: object


The basic retrieval returns a table with timing data as strings, and the `Bib` identifier as an integer.

The `Bib` identifer, We could also regard it as a string so that we aren't tempted to treat it as a number, inwhich case we should also ensure that any extraneous whitespace is stripped if the `Bib` was already a string:

```python
rdata[RANK]['Bib'] = rdata[RANK]['Bib'].astype(str).str.strip()
```

    In [32]: rdata[RANK]['Bib'] = rdata[RANK]['Bib'].astype(int)


## Convert time to timedelta

Several of the datasets return times, as strings, in the form: `HH:MM:SS`.

We can convert these times to timedeltas.

Timing related columns are all columns except those in `['Pos','Bib','Crew','Brand']`.

We can also prefix timing columns in the timing data screens so we can recreate the order they should appear in:

    In [33]: #Prefix each split designator with a split count
             timingcols=['dss','wp1','wp2','ass']
             { x:'{}_{}'.format('{0:02d}'.format(i), x) for i, x in enumerate(timingcols, 0) }
    
    Out[33]: {'dss': '00_dss', 'wp1': '01_wp1', 'wp2': '02_wp2', 'ass': '03_ass'}


One of the things we need to handle are timing columns where the timing data may be mixed with other sorts of data in the raw data table.

Routines for cleaner the data are included in the timing handler function but they were actually "backfilled" into the function after creating them (originally) later on in the notebook.

    In [620]: from pandas.api.types import is_string_dtype
              
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



## Ranking Data Redux

Normalise the times as timedeltas.

For timestrings of the form `HH:MM:SS`, this is as simple as passing the timestring column to the *pandas* `.to_timedelta()` function:

```python
pd.to_timedelta( df[TIMESTRING_COLUMN] )
```

We just need to ensure we pass it the correct columns...

    In [607]: def get_ranking_data(stage,vtype='car', timerank='stage', kind='simple'):
                  ''' Retrieve rank timing data and return it in a form we can work directly with. '''
                  
                  #kind: simple, raw
                  df = _fetch_ranking_data(stage,vtype=vtype, timerank=timerank)
                  df[RANK]['Bib'] = df[RANK]['Bib'].astype(int)
                  return _get_timing(df, typ=RANK, kind=kind)

    In [608]: get_ranking_data(STAGE, VTYPE, kind='raw')[RANK].head()
    
    Out[608]: 

{width="wide"}
|   | Pos | Bib |                                           Crew |  Brand |     Time |      Gap | Penalty | Time_raw |  Gap_raw | Penalty_raw |
|---|-----|-----|------------------------------------------------|--------|----------|----------|---------|----------|----------|-------------|
| 0 |   1 | 304 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 03:54:31 |      NaT |  0 days | 03:54:31 |      NaN |    00:00:00 |
| 1 |   2 | 301 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 03:57:57 | 00:03:26 |  0 days | 03:57:57 | 00:03:26 |    00:00:00 |
| 2 |   3 | 303 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 04:06:18 | 00:11:47 |  0 days | 04:06:18 | 00:11:47 |    00:00:00 |
| 3 |   4 | 314 |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 04:07:21 | 00:12:50 |  0 days | 04:07:21 | 00:12:50 |    00:00:00 |
| 4 |   5 | 307 |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 04:12:43 | 00:18:12 |  0 days | 04:12:43 | 00:18:12 |    00:00:00 |



    In [407]: STAGE, VTYPE
    
    Out[407]: (3, 'car')


    In [621]: #What changed?
              get_ranking_data(STAGE, VTYPE,timerank='general', kind='raw')[RANK].head()
    
    Out[621]: 

{width="wide"}
|   | Overall Position | Bib |                                           Crew |  Brand |     Time |      Gap | Penalty | Time_raw | Gap_raw | Penalty_raw |
|---|------------------|-----|------------------------------------------------|--------|----------|----------|---------|----------|---------|-------------|
| 0 |                1 | 301 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 08:34:08 | 00:00:00 |  0 days | 08:34:08 | 0:00:00 |    00:00:00 |
| 1 |                2 | 314 |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 08:40:56 | 00:06:48 |  0 days | 08:40:56 | 0:06:48 |    00:00:00 |
| 2 |                3 | 304 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 08:41:11 | 00:07:03 |  0 days | 08:41:11 | 0:07:03 |    00:00:00 |
| 3 |                4 | 307 |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 08:46:10 | 00:12:02 |  0 days | 08:46:10 | 0:12:02 |    00:00:00 |
| 4 |                5 | 303 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 08:47:53 | 00:13:45 |  0 days | 08:47:53 | 0:13:45 |    00:00:00 |



    In [418]: ranking_data = get_ranking_data(STAGE, VTYPE)
              
              ranking_data[RANK].head()
    
    Out[418]: 

{width="wide"}
|   | Pos | Bib |                                           Crew |  Brand |     Time |      Gap | Penalty |
|---|-----|-----|------------------------------------------------|--------|----------|----------|---------|
| 0 |   1 | 304 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 03:54:31 |      NaT |  0 days |
| 1 |   2 | 301 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 03:57:57 | 00:03:26 |  0 days |
| 2 |   3 | 303 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 04:06:18 | 00:11:47 |  0 days |
| 3 |   4 | 314 |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 04:07:21 | 00:12:50 |  0 days |
| 4 |   5 | 307 |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 04:12:43 | 00:18:12 |  0 days |



    In [273]: ranking_data[RANK].dtypes
    
    Out[273]: Pos                  int64
              Bib                  int64
              Crew                object
              Brand               object
              Time       timedelta64[ns]
              Gap        timedelta64[ns]
              Penalty    timedelta64[ns]
              dtype: object


The `Crew` data is a bit of a mishmash. If we were to normalise this table, we'd have to split that data out...

For now, let's leave it...

...because sometimes, it can be handy to be able to pull out a chunk of unnormalised data as a simple string.

    In [37]: ranking_data[RANK].dtypes
    
    Out[37]: Pos                  int64
             Bib                  int64
             Crew                object
             Brand               object
             Time       timedelta64[ns]
             Gap        timedelta64[ns]
             Penalty    timedelta64[ns]
             dtype: object



## Timing Data

The timing data needs some processing:

    In [274]: data = _fetch_timing_data(STAGE, VTYPE)
              
              data[TIME][60:70]
    
    Out[274]: 

{width="wide"}
|    |  Pos |   Bib |                                              Crew |            Brand |            dss |             wp1 |             wp2 |             wp3 | wp4 |         wp5/rav |             wp6 |             wp7 |             wp8 |        ass |
|----|------|-------|---------------------------------------------------|------------------|----------------|-----------------|-----------------|-----------------|-----|-----------------|-----------------|-----------------|-----------------|------------|
| 60 | 61.0 |   409 |            S. RILEY T. HANKS SMS MINING AUSTRALIA | HOLDEN AUSTRALIA | 10:01:00= (56) | 000:24:59▲ (46) | 000:39:51▼ (51) | 002:26:51▲ (45) | NaN | 004:17:53= (45) | 004:29:43▲ (43) | 006:04:26▼ (55) | 007:11:22▼ (56) | 007:39:41▼ |
| 61 | 62.0 |   355 |              P. BOUTRON M. BARBET SODICARS RACING |            BUGGY | 09:54:30= (50) | 000:23:02▲ (33) | 000:36:21▼ (35) | 003:57:13▼ (77) | NaN | 005:47:30▲ (64) | 005:59:00= (64) | 006:55:02= (64) | 007:28:20▲ (60) | 007:47:18▼ |
| 62 | 63.0 |   365 | M. SALAZAR VELASQUEZ M. SALAZAR SIERRA PRO RAI... |       VOLKSWAGEN | 10:07:30= (67) | 000:27:16▲ (58) | 000:42:25▲ (57) | 002:52:40▼ (63) | NaN | 005:26:28▲ (62) | 005:38:58▲ (61) | 006:30:29▲ (60) | 007:14:46▲ (58) | 007:47:56▼ |
| 63 | 64.0 | 354 ⛽ |   OE. GANDARA R. CORVALAN OMAR GANDARA DAKAR TEAM |        PROTOTIPO | 10:13:30= (76) | 000:29:35▲ (71) | 000:46:19= (71) | 002:30:47▲ (50) | NaN | 004:54:01▼ (55) | 005:10:15▲ (54) | 006:40:46▼ (61) | 007:34:30▼ (62) | 008:07:21▼ |
| 64 | 65.0 |   300 |             C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |             MINI |  08:36:00= (8) |  000:17:25▲ (2) | 003:44:13▼ (85) | 004:47:50▲ (80) | NaN | 006:38:08▲ (72) | 006:49:24▲ (69) | 007:24:16▲ (66) | 008:29:02▼ (67) | 008:14:49▲ |
| 65 | 66.0 | 371 ⛽ |   C. LIPAROTI R. ROMERO FONT C.A.T. RACING YAMAHA |           YAMAHA | 10:01:30= (57) | 000:29:15▼ (70) | 000:45:36▲ (68) | 003:44:38▼ (76) | NaN | 006:19:00▲ (69) | 006:34:06▲ (67) | 007:28:08= (67) | 008:10:48▲ (66) | 008:22:52= |
| 66 | 67.0 |   329 |              J. SCHRODER D. SCHRODER SOUTH RACING |           NISSAN | 09:42:00= (36) | 000:23:11▲ (34) | 000:37:20▼ (39) | 002:41:52▼ (59) | NaN | 006:26:02▼ (70) | 006:37:46▲ (68) | 007:22:01▲ (65) | 007:58:22▲ (63) | 008:23:22▼ |
| 67 | 68.0 |   302 | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |           TOYOTA |  08:24:00= (4) | 006:12:20▼ (87) |  000:29:41▲ (8) |  001:20:58▲ (4) | NaN | 006:43:52▼ (73) | 006:53:46▲ (71) | 007:32:52▲ (68) | 008:04:14▲ (64) | 008:25:16▼ |
| 68 | 69.0 |   356 |                  M. PISANO V. SARREAUD SRT RACING |            BUGGY | 09:38:30= (32) | 000:22:28▲ (27) | 000:35:02▲ (26) | 003:14:16▼ (69) | NaN | 005:42:14▲ (63) | 005:53:01▲ (62) | 007:35:21▼ (69) | 008:08:18▲ (65) | 008:31:14▼ |
| 69 | 70.0 |   317 |      E. VAN LOON H. SCHOLTALBERS OVERDRIVE TOYOTA |           TOYOTA | 08:52:00= (15) | 005:29:40▼ (86) | 000:30:36▲ (12) | 005:57:19▼ (82) | NaN | 007:23:37▲ (78) | 007:44:40▲ (77) | 008:22:25▲ (74) | 008:52:48▲ (69) | 009:14:51▼ |



A full inspection of the time data shows that some additional metadata corresponding to whether in-stage refuelling is allowed may also be recorded in the `Bib` column (for example, `403 ⛽`).

We can extract this information into a separate dataframe / table.

    In [39]: def get_refuel_status(df):
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

    In [275]: data[TIME] = get_refuel_status(data[TIME])
              data[TIME][60:70]
    
    Out[275]: 

{width="wide"}
|    |  Pos | Bib | Refuel |                                              Crew |            Brand |            dss |             wp1 |             wp2 |             wp3 | wp4 |         wp5/rav |             wp6 |             wp7 |             wp8 |        ass |
|----|------|-----|--------|---------------------------------------------------|------------------|----------------|-----------------|-----------------|-----------------|-----|-----------------|-----------------|-----------------|-----------------|------------|
| 60 | 61.0 | 409 |  False |            S. RILEY T. HANKS SMS MINING AUSTRALIA | HOLDEN AUSTRALIA | 10:01:00= (56) | 000:24:59▲ (46) | 000:39:51▼ (51) | 002:26:51▲ (45) | NaN | 004:17:53= (45) | 004:29:43▲ (43) | 006:04:26▼ (55) | 007:11:22▼ (56) | 007:39:41▼ |
| 61 | 62.0 | 355 |  False |              P. BOUTRON M. BARBET SODICARS RACING |            BUGGY | 09:54:30= (50) | 000:23:02▲ (33) | 000:36:21▼ (35) | 003:57:13▼ (77) | NaN | 005:47:30▲ (64) | 005:59:00= (64) | 006:55:02= (64) | 007:28:20▲ (60) | 007:47:18▼ |
| 62 | 63.0 | 365 |  False | M. SALAZAR VELASQUEZ M. SALAZAR SIERRA PRO RAI... |       VOLKSWAGEN | 10:07:30= (67) | 000:27:16▲ (58) | 000:42:25▲ (57) | 002:52:40▼ (63) | NaN | 005:26:28▲ (62) | 005:38:58▲ (61) | 006:30:29▲ (60) | 007:14:46▲ (58) | 007:47:56▼ |
| 63 | 64.0 | 354 |   True |   OE. GANDARA R. CORVALAN OMAR GANDARA DAKAR TEAM |        PROTOTIPO | 10:13:30= (76) | 000:29:35▲ (71) | 000:46:19= (71) | 002:30:47▲ (50) | NaN | 004:54:01▼ (55) | 005:10:15▲ (54) | 006:40:46▼ (61) | 007:34:30▼ (62) | 008:07:21▼ |
| 64 | 65.0 | 300 |  False |             C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |             MINI |  08:36:00= (8) |  000:17:25▲ (2) | 003:44:13▼ (85) | 004:47:50▲ (80) | NaN | 006:38:08▲ (72) | 006:49:24▲ (69) | 007:24:16▲ (66) | 008:29:02▼ (67) | 008:14:49▲ |
| 65 | 66.0 | 371 |   True |   C. LIPAROTI R. ROMERO FONT C.A.T. RACING YAMAHA |           YAMAHA | 10:01:30= (57) | 000:29:15▼ (70) | 000:45:36▲ (68) | 003:44:38▼ (76) | NaN | 006:19:00▲ (69) | 006:34:06▲ (67) | 007:28:08= (67) | 008:10:48▲ (66) | 008:22:52= |
| 66 | 67.0 | 329 |  False |              J. SCHRODER D. SCHRODER SOUTH RACING |           NISSAN | 09:42:00= (36) | 000:23:11▲ (34) | 000:37:20▼ (39) | 002:41:52▼ (59) | NaN | 006:26:02▼ (70) | 006:37:46▲ (68) | 007:22:01▲ (65) | 007:58:22▲ (63) | 008:23:22▼ |
| 67 | 68.0 | 302 |  False | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |           TOYOTA |  08:24:00= (4) | 006:12:20▼ (87) |  000:29:41▲ (8) |  001:20:58▲ (4) | NaN | 006:43:52▼ (73) | 006:53:46▲ (71) | 007:32:52▲ (68) | 008:04:14▲ (64) | 008:25:16▼ |
| 68 | 69.0 | 356 |  False |                  M. PISANO V. SARREAUD SRT RACING |            BUGGY | 09:38:30= (32) | 000:22:28▲ (27) | 000:35:02▲ (26) | 003:14:16▼ (69) | NaN | 005:42:14▲ (63) | 005:53:01▲ (62) | 007:35:21▼ (69) | 008:08:18▲ (65) | 008:31:14▼ |
| 69 | 70.0 | 317 |  False |      E. VAN LOON H. SCHOLTALBERS OVERDRIVE TOYOTA |           TOYOTA | 08:52:00= (15) | 005:29:40▼ (86) | 000:30:36▲ (12) | 005:57:19▼ (82) | NaN | 007:23:37▲ (78) | 007:44:40▲ (77) | 008:22:25▲ (74) | 008:52:48▲ (69) | 009:14:51▼ |



We also notice that the raw timing data includes information about split rank and how it compares to the rank at the previous split / waypoint, with the raw data taking the form `08:44:00= (11)`. Which is to say, `HH:MM:DDx (NN?)` where `x` is a comparator showing whether the rank at that waypoint improved (▲), remained the same as (=), or worsened (▼) compared to the previous waypoint.

Note that the final `ass` column does not include the rank.

We can use a regular expression to separate the data out, with each regex group being expanded into a separate column:

    In [41]: data[TIME]['dss'].str.extract(r'(\d{2}:\d{2}:\d{2})(.*)\((\d*)\)', expand=True).head()
    
    Out[41]: 

{width="narrow"}
|   |        0 | 1 |  2 |
|---|----------|---|----|
| 0 | 08:56:00 | = | 17 |
| 1 | 08:44:00 | = | 11 |
| 2 | 08:48:00 | = | 13 |
| 3 | 08:27:00 | = |  5 |
| 4 | 08:18:00 | = |  2 |



We can backfill an expression of that form into the timing data handler function above...

Now we wrap several steps together into a function that gets us a clean set of timing data, with columns of an appropriate type:

    In [42]: def get_timing_data(stage,vtype='car', timerank='time', kind='simple'):
                 ''' Get timing data in a form ready to use. '''
                 df = _fetch_timing_data(stage,vtype=vtype, timerank=timerank)
             
                 df[TIME] = get_refuel_status(df[TIME])
                 return _get_timing(df, typ=TIME, kind=kind)
                 

    In [278]: get_timing_data(STAGE, VTYPE, kind='simple')[TIME].head()
    
    Out[278]: 

{width="wide"}
|   | Pos | Bib | Refuel |                                           Crew |  Brand |   00_dss |   01_wp1 |   02_wp2 |   03_wp3 | 04_wp4 | 05_wp5/rav |   06_wp6 |   07_wp7 |   08_wp8 |   09_ass |
|---|-----|-----|--------|------------------------------------------------|--------|----------|----------|----------|----------|--------|------------|----------|----------|----------|----------|
| 0 | 1.0 | 304 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 08:56:00 | 00:17:58 | 00:27:54 | 01:17:27 |    NaT |   02:26:09 | 02:34:27 | 03:06:29 | 03:34:32 | 03:54:31 |
| 1 | 2.0 | 301 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 08:44:00 | 00:17:22 | 00:27:54 | 01:18:40 |    NaT |   02:26:47 | 02:35:14 | 03:09:08 | 03:36:55 | 03:57:57 |
| 2 | 3.0 | 303 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 08:48:00 | 00:18:35 | 00:29:36 | 01:23:59 |    NaT |   02:34:41 | 02:43:25 | 03:18:06 | 03:46:37 | 04:06:18 |
| 3 | 4.0 | 314 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 08:27:00 | 00:18:23 | 00:29:16 | 01:22:59 |    NaT |   02:35:37 | 02:43:47 | 03:20:00 | 03:47:53 | 04:07:21 |
| 4 | 5.0 | 307 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 08:18:00 | 00:18:52 | 00:29:48 | 01:25:01 |    NaT |   02:39:35 | 02:48:42 | 03:23:19 | 03:52:24 | 04:12:43 |



    In [276]: data = get_timing_data(STAGE, VTYPE, kind='full')
              data[TIME].head()
    
    Out[276]: 

{width="wide"}
|   | Pos | Bib | Refuel |                                           Crew |  Brand |   00_dss |   01_wp1 |   02_wp2 |   03_wp3 | 04_wp4 | ... | wp6_pos_06 | wp6_raw_06 | wp7_gain_07 | wp7_pos_07 | wp7_raw_07 | wp8_gain_08 | wp8_pos_08 | wp8_raw_08 | ass_gain_09 | ass_raw_09 |
|---|-----|-----|--------|------------------------------------------------|--------|----------|----------|----------|----------|--------|-----|------------|------------|-------------|------------|------------|-------------|------------|------------|-------------|------------|
| 0 | 1.0 | 304 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 08:56:00 | 00:17:58 | 00:27:54 | 01:17:27 |    NaT | ... |        1.0 |   02:34:27 |           = |        1.0 |   03:06:29 |           = |        1.0 |   03:34:32 |           = |   03:54:31 |
| 1 | 2.0 | 301 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 08:44:00 | 00:17:22 | 00:27:54 | 01:18:40 |    NaT | ... |        2.0 |   02:35:14 |           = |        2.0 |   03:09:08 |           = |        2.0 |   03:36:55 |           = |   03:57:57 |
| 2 | 3.0 | 303 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 08:48:00 | 00:18:35 | 00:29:36 | 01:23:59 |    NaT | ... |        3.0 |   02:43:25 |           = |        3.0 |   03:18:06 |           = |        3.0 |   03:46:37 |           = |   04:06:18 |
| 3 | 4.0 | 314 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 08:27:00 | 00:18:23 | 00:29:16 | 01:22:59 |    NaT | ... |        4.0 |   02:43:47 |           = |        4.0 |   03:20:00 |           = |        4.0 |   03:47:53 |           = |   04:07:21 |
| 4 | 5.0 | 307 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 08:18:00 | 00:18:52 | 00:29:48 | 01:25:01 |    NaT | ... |        5.0 |   02:48:42 |           = |        5.0 |   03:23:19 |           = |        5.0 |   03:52:24 |           = |   04:12:43 |



    In [44]: data[TIME].dtypes
    
    Out[44]: Pos                     float64
             Bib                       int64
             Refuel                     bool
             Crew                     object
             Brand                    object
             00_dss          timedelta64[ns]
             01_wp1          timedelta64[ns]
             02_wp2          timedelta64[ns]
             03_wp3          timedelta64[ns]
             04_wp4          timedelta64[ns]
             05_wp5/rav      timedelta64[ns]
             06_wp6          timedelta64[ns]
             07_wp7          timedelta64[ns]
             08_wp8          timedelta64[ns]
             09_ass          timedelta64[ns]
             dss_gain                 object
             dss_pos                 float64
             wp1_gain                 object
             wp1_pos                 float64
             wp2_gain                 object
             wp2_pos                 float64
             wp3_gain                 object
             wp3_pos                 float64
             wp4_gain                 object
             wp4_pos                 float64
             wp5/rav_gain             object
             wp5/rav_pos             float64
             wp6_gain                 object
             wp6_pos                 float64
             wp7_gain                 object
             wp7_pos                 float64
             wp8_gain                 object
             wp8_pos                 float64
             ass_gain                 object
             dtype: object



## Parse Metadata

Some of the scraped tables are used to provide selection lists, but we might be able to use them as metadata tables.

For example, here's a pretty complete set, although mangled together, set of competititor names, nationalities, and team names:

    In [45]: data[ CREWTEAM ].head()
    
    Out[45]: 

{width="wide"}
|   |       Highlight |      Filter | Bib |                                             Names |
|---|-----------------|-------------|-----|---------------------------------------------------|
| 0 | Highglight crew | Filter crew |   1 |     M. WALKNER (Austria)RED BULL KTM FACTORY TEAM |
| 1 | Highglight crew | Filter crew |   2 | P. GONCALVES (Portugal)MONSTER ENERGY HONDA TE... |
| 2 | Highglight crew | Filter crew |   3 |     T. PRICE (Australia)RED BULL KTM FACTORY TEAM |
| 3 | Highglight crew | Filter crew |   4 | A. VAN BEVEREN (France)YAMALUBE YAMAHA OFFICIA... |
| 4 | Highglight crew | Filter crew |   5 | J. BARREDA BORT (Spain)MONSTER ENERGY HONDA TE... |



It'll probably be convenient to have the unique `Bib` values available as an index:

    In [46]: data[ CREWTEAM ] = data[ CREWTEAM ][['Bib', 'Names']].set_index('Bib')
             data[ CREWTEAM ].head()
    
    Out[46]: 

{width="narrow"}
|     |                                             Names |
|-----|---------------------------------------------------|
| Bib |                                                   |
|   1 |     M. WALKNER (Austria)RED BULL KTM FACTORY TEAM |
|   2 | P. GONCALVES (Portugal)MONSTER ENERGY HONDA TE... |
|   3 |     T. PRICE (Australia)RED BULL KTM FACTORY TEAM |
|   4 | A. VAN BEVEREN (France)YAMALUBE YAMAHA OFFICIA... |
|   5 | J. BARREDA BORT (Spain)MONSTER ENERGY HONDA TE... |



The `Names` may have several `Name (Country)` values, followed by a team name. The original HTML uses `<span>` tags to separate out values but the *pandas* `.read_html()` function flattens cell contents.
    
Let's have a go at pulling out the team names, which appear at the end of the string. If we can split each name, and the team name, into separate columns, and then metl those columns into separate rows, grouped by `Bib` number, we should be able to grab the last row, corrsponding to the team, in each group:

    In [47]: #Perhaps split on brackets?
             # At least one team has brackets in the name at the end of the name
             # So let's make that case, at least, a "not bracket" by setting a ) at the end to a :]:
             # so we don't (mistakenly) split on it as if it were a country-associated bracket.
             teams = data[ CREWTEAM ]['Names'].str.replace(r'\)$',':]:').str.split(')').apply(pd.Series).reset_index().melt(id_vars='Bib', var_name='Num').dropna()
             
             #Find last item in each group, which is to say: the team
             teamnames = teams.groupby('Bib').last()
             #Defudge any brackets at the end back
             teamnames = teamnames['value'].str.replace(':]:',')')
             teamnames.head()
    
    Out[47]: Bib
             1              RED BULL KTM FACTORY TEAM
             2         MONSTER ENERGY HONDA TEAM 2019
             3              RED BULL KTM FACTORY TEAM
             4    YAMALUBE YAMAHA OFFICIAL RALLY TEAM
             5         MONSTER ENERGY HONDA TEAM 2019
             Name: value, dtype: object


Now let's go after the competitors. These are all *but* the last row in each group:

    In [48]: #Remove last row in group i.e. the team
             personnel = teams.groupby('Bib').apply(lambda x: x.iloc[:-1]).set_index('Bib').reset_index()
             
             personnel[['Name','Country']] = personnel['value'].str.split('(').apply(pd.Series)
             
             #Strip whitespace
             for c in ['Name','Country']:
                 personnel[c] = personnel[c].str.strip()
                 
             personnel[['Bib','Num','Name','Country']].head()
    
    Out[48]: 

{width="narrow"}
|   | Bib | Num |            Name |   Country |
|---|-----|-----|-----------------|-----------|
| 0 |   1 |   0 |      M. WALKNER |   Austria |
| 1 |   2 |   0 |    P. GONCALVES |  Portugal |
| 2 |   3 |   0 |        T. PRICE | Australia |
| 3 |   4 |   0 |  A. VAN BEVEREN |    France |
| 4 |   5 |   0 | J. BARREDA BORT |     Spain |



For convenience, we might want to reshape this long form back to a wide form, with a single string containing all the competitor names associated with a particular `Bib` identifier:

    In [49]: #Create a single name string for each vehicle
             #For each Bib number, group the rows associated with that number
             # and aggregate the names in those rows into a single, comma separated, joined string
             # indexed by the corresponding Bib number
             personnel.groupby('Bib')['Name'].agg(lambda col: ', '.join(col)).tail()
    
    Out[49]: Bib
             538                  CG. RICKLER DEL MARE, DR. BURAN
             539               F. SKROBANEK, P. LESAK, R. BACULIK
             540    J. GINESTA, F. ESTER FERNANDEZ, M. DARDAILLON
             541            A. BENBEKHTI, S. BENBEKHTI, R. OSMANI
             542               S. BESNARD, F. DERONCE, S. LALICHE
             Name: Name, dtype: object


    In [50]: data[ BRANDS ].head()
    
    Out[50]: 

{width="narrow"}
|   |       Filter |    Names |
|---|--------------|----------|
| 0 | Filter brand |     BETA |
| 1 | Filter brand |      BMW |
| 2 | Filter brand | BORGWARD |
| 3 | Filter brand |   BOSUER |
| 4 | Filter brand |      BRP |



    In [51]: data[ COUNTRIES ].head()
    
    Out[51]: 

{width="narrow"}
|   |         Filter |                      Names |
|---|----------------|----------------------------|
| 0 | Filter country |              Andorra (AND) |
| 1 | Filter country | United Arab Emirates (ARE) |
| 2 | Filter country |            Argentina (ARG) |
| 3 | Filter country |            Australia (AUS) |
| 4 | Filter country |              Austria (AUT) |



    In [52]: data[ COUNTRIES ][['Country','CountryCode']] = data[ COUNTRIES ]['Names'].str.extract(r'(.*) \((.*)\)',expand=True)
             data[ COUNTRIES ].head()
    
    Out[52]: 

{width="wide"}
|   |         Filter |                      Names |              Country | CountryCode |
|---|----------------|----------------------------|----------------------|-------------|
| 0 | Filter country |              Andorra (AND) |              Andorra |         AND |
| 1 | Filter country | United Arab Emirates (ARE) | United Arab Emirates |         ARE |
| 2 | Filter country |            Argentina (ARG) |            Argentina |         ARG |
| 3 | Filter country |            Australia (AUS) |            Australia |         AUS |
| 4 | Filter country |              Austria (AUT) |              Austria |         AUT |



    In [207]: def get_annotated_timing_data(stage,vtype='car', timerank='time', kind='simple'):
                  ''' Return a timing dataset that's ready to use. '''
                  
                  df = get_timing_data(stage, vtype, timerank, kind)
                  col00 = [c for c in df[TIME].columns if c.startswith('00_')][0]
                  df[TIME].insert(2,'Road Position', df[TIME].sort_values(col00,ascending=True)[col00].rank())
                  return df

    In [268]: get_annotated_timing_data(STAGE,vtype=VTYPE, timerank='time', kind='full')[TIME].head()
    
    Out[268]: 

{width="wide"}
|   | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand |   00_dss |   01_wp1 |   02_wp2 |   03_wp3 | ... | wp6_pos_06 | wp6_raw_06 | wp7_gain_07 | wp7_pos_07 | wp7_raw_07 | wp8_gain_08 | wp8_pos_08 | wp8_raw_08 | ass_gain_09 | ass_raw_09 |
|---|-----|-----|---------------|--------|------------------------------------------------|--------|----------|----------|----------|----------|-----|------------|------------|-------------|------------|------------|-------------|------------|------------|-------------|------------|
| 0 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 08:56:00 | 00:17:58 | 00:27:54 | 01:17:27 | ... |        1.0 |   02:34:27 |           = |        1.0 |   03:06:29 |           = |        1.0 |   03:34:32 |           = |   03:54:31 |
| 1 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 08:44:00 | 00:17:22 | 00:27:54 | 01:18:40 | ... |        2.0 |   02:35:14 |           = |        2.0 |   03:09:08 |           = |        2.0 |   03:36:55 |           = |   03:57:57 |
| 2 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 08:48:00 | 00:18:35 | 00:29:36 | 01:23:59 | ... |        3.0 |   02:43:25 |           = |        3.0 |   03:18:06 |           = |        3.0 |   03:46:37 |           = |   04:06:18 |
| 3 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 08:27:00 | 00:18:23 | 00:29:16 | 01:22:59 | ... |        4.0 |   02:43:47 |           = |        4.0 |   03:20:00 |           = |        4.0 |   03:47:53 |           = |   04:07:21 |
| 4 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 08:18:00 | 00:18:52 | 00:29:48 | 01:25:01 | ... |        5.0 |   02:48:42 |           = |        5.0 |   03:23:19 |           = |        5.0 |   03:52:24 |           = |   04:12:43 |



    In [54]: t_data = get_annotated_timing_data(STAGE,vtype=VTYPE, timerank='time')[TIME]
             t_data.head()
    
    Out[54]: 

{width="wide"}
|   | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand |   00_dss |   01_wp1 |   02_wp2 |   03_wp3 | 04_wp4 | 05_wp5/rav |   06_wp6 |   07_wp7 |   08_wp8 |   09_ass |
|---|-----|-----|---------------|--------|------------------------------------------------|--------|----------|----------|----------|----------|--------|------------|----------|----------|----------|----------|
| 0 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI | 08:56:00 | 00:17:58 | 00:27:54 | 01:17:27 |    NaT |   02:26:09 | 02:34:27 | 03:06:29 | 03:34:32 | 03:54:31 |
| 1 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 08:44:00 | 00:17:22 | 00:27:54 | 01:18:40 |    NaT |   02:26:47 | 02:35:14 | 03:09:08 | 03:36:55 | 03:57:57 |
| 2 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 08:48:00 | 00:18:35 | 00:29:36 | 01:23:59 |    NaT |   02:34:41 | 02:43:25 | 03:18:06 | 03:46:37 | 04:06:18 |
| 3 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI | 08:27:00 | 00:18:23 | 00:29:16 | 01:22:59 |    NaT |   02:35:37 | 02:43:47 | 03:20:00 | 03:47:53 | 04:07:21 |
| 4 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI | 08:18:00 | 00:18:52 | 00:29:48 | 01:25:01 |    NaT |   02:39:35 | 02:48:42 | 03:23:19 | 03:52:24 | 04:12:43 |



    In [55]: not_timing_cols = ['Pos','Road Position','Refuel','Bib','Crew','Brand']
             
             driver_data = t_data[ not_timing_cols ]
             driver_data.head()
    
    Out[55]: 

{width="wide"}
|   | Pos | Road Position | Refuel | Bib |                                           Crew |  Brand |
|---|-----|---------------|--------|-----|------------------------------------------------|--------|
| 0 | 1.0 |          17.0 |  False | 304 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |
| 1 | 2.0 |          11.0 |  False | 301 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |
| 2 | 3.0 |          13.0 |  False | 303 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |
| 3 | 4.0 |           5.0 |  False | 314 |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |
| 4 | 5.0 |           2.0 |  False | 307 |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |



The number of waypoints differs across stages. If we cast the wide format waypoint data into a long form, we can more conveniently merge waypoint timing data from separate stages into the same dataframe.

    In [56]: pd.melt(t_data.head(),
                     id_vars=not_timing_cols,
                     var_name='Waypoint', value_name='Time').head()
    
    Out[56]: 

{width="wide"}
|   | Pos | Road Position | Refuel | Bib |                                           Crew |  Brand | Section |     Time |
|---|-----|---------------|--------|-----|------------------------------------------------|--------|---------|----------|
| 0 | 1.0 |          17.0 |  False | 304 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |  00_dss | 08:56:00 |
| 1 | 2.0 |          11.0 |  False | 301 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |  00_dss | 08:44:00 |
| 2 | 3.0 |          13.0 |  False | 303 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |  00_dss | 08:48:00 |
| 3 | 4.0 |           5.0 |  False | 314 |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |  00_dss | 08:27:00 |
| 4 | 5.0 |           2.0 |  False | 307 |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |  00_dss | 08:18:00 |



    In [305]: def _timing_long(df, nodss=True):
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

    In [301]: t_data_long = _timing_long(t_data)
              t_data_long.head()
    
    Out[301]: 

{width="wide"}
|    | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand | Waypoint |     Time |
|----|-----|-----|---------------|--------|------------------------------------------------|--------|----------|----------|
| 95 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |   01_wp1 | 00:17:58 |
| 96 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |   01_wp1 | 00:17:22 |
| 97 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |   01_wp1 | 00:18:35 |
| 98 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |   01_wp1 | 00:18:23 |
| 99 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |   01_wp1 | 00:18:52 |



    In [301]: t_data2 = get_annotated_timing_data(STAGE,vtype=VTYPE, timerank='time', kind='full')[TIME]
              display(_typ_long(t_data2, '_gain_').head())
              #_pos_, _raw_, _gain_       
              #Clear down
              t_data2 = None

    In [230]: def get_long_annotated_timing_data(stage,vtype='car', timerank='time', kind='simple'):
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

    In [232]: #get_long_annotated_timing_data(3,vtype='quad', timerank='time')[TIME]

    In [999]: get_long_annotated_timing_data(STAGE, VTYPE)[TIME].head()

    In [168]: get_long_annotated_timing_data(STAGE, VTYPE, 'gap')[TIME].head()
    
    Out[168]: 

{width="wide"}
|    | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand | Section |      Gap | GapInS |
|----|-----|-----|---------------|--------|------------------------------------------------|--------|---------|----------|--------|
| 95 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |  01_wp1 | 00:00:36 |   36.0 |
| 96 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |  01_wp1 | 00:00:00 |    0.0 |
| 97 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |  01_wp1 | 00:01:13 |   73.0 |
| 98 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |  01_wp1 | 00:01:01 |   61.0 |
| 99 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |  01_wp1 | 00:01:30 |   90.0 |




## Saving the Data to a Database

The data can be saved to a database directly in an unnormalised form, or we can tidy it up a bit and save it in a higher normal form.

The table structure is far from best practice - it's pragmatic and in first instance intended simply to be useful...

    In [77]: #!pip3 install sqlite-utils
             from sqlite_utils import Database

    In [379]: def cleardbtable(conn, table):
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

    In [463]: #dbname='dakar_test_sql.sqlite'
              #!rm $dbname

    In [483]: import sqlite3
              
              dbname='dakar_sql.sqlite'
              !rm $dbname
              
              conn = sqlite3.connect(dbname)
              
              c = conn.cursor()
              
              setup_sql= 'dakar.sql'
              with open(setup_sql,'r') as f:
                  txt = f.read()
                  c.executescript(txt)
                  
              db = Database(conn)

    In [484]: q="SELECT name FROM sqlite_master WHERE type = 'table';"
              pd.read_sql(q, conn)
    
    Out[484]: 

{width="narrow"}
|   |       name |
|---|------------|
| 0 |      teams |
| 1 |       crew |
| 2 |   vehicles |
| 3 | stagestats |
| 4 |    ranking |
| 5 |  stagemeta |
| 6 |  waypoints |



    In [485]: tmp = teamnames.reset_index()
              tmp['Year'] = YEAR
              tmp.rename(columns={'value':'Team'}, inplace=True)
              dbfy(conn, tmp, "teams")

    In [486]: q="SELECT * FROM teams LIMIT 3;"
              pd.read_sql(q, conn)
    
    Out[486]: 

{width="narrow"}
|   | Year | Bib |                           Team |
|---|------|-----|--------------------------------|
| 0 | 2019 |   1 |      RED BULL KTM FACTORY TEAM |
| 1 | 2019 |   2 | MONSTER ENERGY HONDA TEAM 2019 |
| 2 | 2019 |   3 |      RED BULL KTM FACTORY TEAM |



    In [487]: tmp = personnel[['Bib','Num','Name','Country']]
              tmp['Year'] = YEAR
              dbfy(conn, tmp, "crew")

    In [488]: q="SELECT * FROM crew LIMIT 3;"
              pd.read_sql(q, conn)
    
    Out[488]: 

{width="narrow"}
|   | Year | Bib | Num |         Name |   Country |
|---|------|-----|-----|--------------|-----------|
| 0 | 2019 |   1 |   0 |   M. WALKNER |   Austria |
| 1 | 2019 |   2 |   0 | P. GONCALVES |  Portugal |
| 2 | 2019 |   3 |   0 |     T. PRICE | Australia |



    In [489]: tmp = get_stage_stats(STAGE).set_index('Special').T.reset_index()
              tmp.rename(columns={'Leader at latest WP':'LeaderLatestWP', 'index':'Vehicle', "Latest WP":'LatestWP',
                                  'At start':'AtStart', 'Nb at latest WP':'NumLatestWP' }, inplace=True)
              tmp[['BibLatestWP', 'NameLatestWP']] = tmp['LeaderLatestWP'].str.extract(r'([^ ]*) (.*)',expand=True)
              tmp['BibLatestWP'] = tmp['BibLatestWP'].astype(int)
              tmp
    
    Out[489]: 

{width="wide"}
| Special | Vehicle | Start | Liaison | Special | Number of participants | AtStart | Left | Arrived | LatestWP |       LeaderLatestWP | NumLatestWP | BibLatestWP |     NameLatestWP |
|---------|---------|-------|---------|---------|------------------------|---------|------|---------|----------|----------------------|-------------|-------------|------------------|
|       0 |    Moto | 06:15 |   798km |   331km |                    NaN |     134 |  133 |     124 |      ass |     018 DE SOULTRAIT |         124 |          18 |     DE SOULTRAIT |
|       1 |    Quad | 07:16 |   798km |   331km |                    NaN |      25 |   24 |      23 |      ass | 241 GONZALEZ FERIOLI |          23 |         241 | GONZALEZ FERIOLI |
|       2 |     Car | 08:15 |   798km |   331km |                    NaN |      95 |   94 |      80 |      ass |      304 PETERHANSEL |          80 |         304 |      PETERHANSEL |
|       3 |     SxS | 09:22 |   798km |   331km |                    NaN |      30 |   30 |      26 |      ass |     358 FARRES GUELL |          26 |         358 |     FARRES GUELL |
|       4 |   Truck | 09:02 |   798km |   331km |                    NaN |      38 |   30 |      24 |      ass |         518 KARGINOV |          24 |         518 |         KARGINOV |



    In [490]: t_vehicles = db["vehicles"]
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
                      t_stagestats.upsert_all(tmp.to_dict(orient='records'))
                      
                      
                      #TO DO create new functions to get data for the db?
                      # TO DO: Gap and Time as raw not time, use _raw - perhaps another melt and merge?
                      # TO DO: bring in _pos for Waypoint_Rank  - perhaps another melt and merge?
                      #use kind=full
                      tmp = get_long_annotated_timing_data(stage,vtype=v, timerank='time')[TIME]
                      tmp['Year'] = YEAR
                      tmp['Stage'] = stage
                      
                      tmp.rename(columns={'Road Position':'RoadPos'}, inplace=True)
                      t_stagemeta.upsert_all(tmp[['Year', 'Stage', 'Bib','RoadPos','Refuel']].to_dict(orient='records'))
                      
                      
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
                      t_waypoints.upsert_all(tmp.to_dict(orient='records'))
              
                          
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
                          t_ranking.upsert_all(tmp.to_dict(orient='records'))
              
                          tmp = tmp[['Year','Bib','VehicleType', 'Brand']]
                          t_vehicles.upsert_all(tmp.to_dict(orient='records'))
                                     

    
    Out[490]: 1 car
              1 moto
              1 quad
              1 sxs
              1 truck
              2 car
              2 moto
              2 quad
              2 sxs
              2 truck
              3 car
              3 moto
              3 quad
              3 sxs
              3 truck
              4 car
              4 moto
              4 quad
              4 sxs
              4 truck
              5 car
              5 moto
              5 quad
              5 sxs
              5 truck
              6 car
              6 moto
              6 quad
              6 sxs
              6 truck
              7 car
              7 moto
              7 quad
              7 sxs
              7 truck
              8 car
              8 moto
              8 quad
              8 sxs
              8 truck
              9 car
              9 moto
              9 quad
              9 sxs
              9 truck
              10 car
              10 moto
              10 quad
              10 sxs
              10 truck



    In [491]: q="SELECT * FROM waypoints where bib=509 LIMIT 20;"
              pd.read_sql(q, conn)
    
    Out[491]: 

{width="wide"}
|    | Year | Stage | Bib | Pos |   Waypoint | WaypointOrder | WaypointRank | Time_raw | TimeInS |  Gap_raw | GapInS |
|----|------|-------|-----|-----|------------|---------------|--------------|----------|---------|----------|--------|
|  0 | 2019 |     1 | 509 |   2 |     01_wp1 |             1 |          5.0 | 00:18:29 |  1109.0 | 00:02:15 |     18 |
|  1 | 2019 |     1 | 509 |   2 |     02_wp2 |             2 |          4.0 | 00:35:25 |  2125.0 | 00:02:10 |     18 |
|  2 | 2019 |     2 | 509 |  10 |     01_wp1 |             1 |          7.0 | 00:42:44 |  2564.0 | 00:04:12 |   1817 |
|  3 | 2019 |     2 | 509 |  10 |     02_wp2 |             2 |          8.0 | 01:04:08 |  3848.0 | 00:05:11 |   1817 |
|  4 | 2019 |     2 | 509 |  10 |     03_wp3 |             3 |          7.0 | 01:17:45 |  4665.0 | 00:05:24 |   1817 |
|  5 | 2019 |     2 | 509 |  10 |     04_wp4 |             4 |          7.0 | 01:50:43 |  6643.0 | 00:05:36 |   1817 |
|  6 | 2019 |     2 | 509 |  10 |     05_wp5 |             5 |          6.0 | 02:21:57 |  8517.0 | 00:06:19 |   1817 |
|  7 | 2019 |     2 | 509 |  10 |     06_wp6 |             6 |          6.0 | 02:42:19 |  9739.0 | 00:07:13 |   1817 |
|  8 | 2019 |     2 | 509 |  10 |     07_wp7 |             7 |          7.0 | 02:54:00 | 10440.0 | 00:07:16 |   1817 |
|  9 | 2019 |     2 | 509 |  10 |     08_wp8 |             8 |          7.0 | 03:23:03 | 12183.0 | 00:07:11 |   1817 |
| 10 | 2019 |     3 | 509 |   8 |     01_wp1 |             1 |          8.0 | 00:21:38 |  1298.0 | 00:01:38 |   3585 |
| 11 | 2019 |     3 | 509 |   8 |     02_wp2 |             2 |          8.0 | 00:34:33 |  2073.0 | 00:03:28 |   3585 |
| 12 | 2019 |     3 | 509 |   8 |     03_wp3 |             3 |          9.0 | 02:03:33 |  7413.0 | 00:35:28 |   3585 |
| 13 | 2019 |     3 | 509 |   8 |     04_wp4 |             4 |          NaN |     None |     NaN |     None |   3585 |
| 14 | 2019 |     3 | 509 |   8 | 05_wp5/rav |             5 |         10.0 | 03:35:22 | 12922.0 | 00:46:16 |   3585 |
| 15 | 2019 |     3 | 509 |   8 |     06_wp6 |             6 |         10.0 | 03:50:16 | 13816.0 | 00:51:56 |   3585 |
| 16 | 2019 |     3 | 509 |   8 |     07_wp7 |             7 |          9.0 | 04:31:26 | 16286.0 | 00:54:34 |   3585 |
| 17 | 2019 |     3 | 509 |   8 |     08_wp8 |             8 |          9.0 | 05:04:18 | 18258.0 | 00:57:53 |   3585 |
| 18 | 2019 |     4 | 509 |  13 |     01_wp1 |             1 |          6.0 | 00:31:37 |  1897.0 | 00:03:42 |   2970 |
| 19 | 2019 |     4 | 509 |  13 |     02_wp2 |             2 |          5.0 | 01:04:43 |  3883.0 | 00:06:47 |   2970 |



    In [492]: q="SELECT DISTINCT VehicleType FROM vehicles;"
              pd.read_sql(q, conn)
    
    Out[492]: 

{width="narrow"}
|   | VehicleType |
|---|-------------|
| 0 |         car |
| 1 |        moto |
| 2 |        quad |
| 3 |       truck |
| 4 |         sxs |



    In [493]: q="SELECT * FROM ranking LIMIT 3;"
              pd.read_sql(q, conn)
    
    Out[493]: 

{width="wide"}
|   | Year | Stage |  Type | Pos | Bib | VehicleType |                                           Crew |  Brand | Time_raw | TimeInS |  Gap_raw | GapInS | Penalty_raw | PenaltyInS |
|---|------|-------|-------|-----|-----|-------------|------------------------------------------------|--------|----------|---------|----------|--------|-------------|------------|
| 0 | 2019 |     1 | stage |   1 | 301 |         car | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA | 01:01:41 |  3701.0 |     None |    NaN |    00:00:00 |          0 |
| 1 | 2019 |     1 | stage |   2 | 300 |         car |          C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |   MINI | 01:03:40 |  3820.0 | 00:01:59 |  119.0 |    00:00:00 |          0 |
| 2 | 2019 |     1 | stage |   3 | 303 |         car |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI | 01:03:41 |  3821.0 | 00:02:00 |  120.0 |    00:00:00 |          0 |



    In [494]: q="SELECT * FROM ranking WHERE Type='general' AND Stage=2 AND Pos=4 LIMIT 5;"
              pd.read_sql(q, conn)
    
    Out[494]: 

{width="wide"}
|   | Year | Stage |    Type | Pos | Bib | VehicleType |                                              Crew |     Brand | Time_raw | TimeInS | Gap_raw | GapInS | Penalty_raw | PenaltyInS |
|---|------|-------|---------|-----|-----|-------------|---------------------------------------------------|-----------|----------|---------|---------|--------|-------------|------------|
| 0 | 2019 |     2 | general |   4 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |      MINI | 04:33:35 | 16415.0 | 0:00:50 |   50.0 |    00:00:00 |          0 |
| 1 | 2019 |     2 | general |   4 |   6 |        moto | P. QUINTANILLA ROCKSTAR ENERGY HUSQVARNA FACTO... | HUSQVARNA | 04:26:31 | 15991.0 | 0:03:17 |  197.0 |    00:00:00 |          0 |
| 2 | 2019 |     2 | general |   4 | 253 |        quad |                T. KUBIENA MOTO RACING GROUP (MRG) |      IBOS | 06:10:09 | 22209.0 | 0:30:44 | 1844.0 |    00:00:00 |          0 |
| 3 | 2019 |     2 | general |   4 | 358 |         sxs | G. FARRES GUELL D. OLIVERAS CARRERAS MONSTER E... |  CAN - AM | 05:27:54 | 19674.0 | 0:06:35 |  395.0 |    00:00:00 |          0 |
| 4 | 2019 |     2 | general |   4 | 514 |       truck | D. SOTNIKOV D. NIKITIN I. MUSTAFIN KAMAZ - MASTER |     KAMAZ | 05:05:57 | 18357.0 | 0:09:01 |  541.0 |    00:00:00 |          0 |



    In [495]: q="SELECT DISTINCT Brand FROM vehicles WHERE VehicleType='sxs' ;"
              pd.read_sql(q, conn)
    
    Out[495]: 

{width="narrow"}
|   |    Brand |
|---|----------|
| 0 |  POLARIS |
| 1 |   CAN-AM |
| 2 |   YAMAHA |
| 3 |      BRP |
| 4 | CAN - AM |



    In [1063]: q="SELECT w.* FROM waypoints w JOIN vehicles v ON w.Bib=v.Bib WHERE Stage=2 AND VehicleType='car' AND Pos<=10"
               tmpq = pd.read_sql(q, conn)
               tmpq.pivot(index='Bib',columns='Waypoint',values='TimeInS')
    
    Out[1063]: 

{width="wide"}
| Waypoint | 01_wp1 | 02_wp2 | 03_wp3 | 04_wp4 | 05_wp5 | 06_wp6 | 07_wp7 |  08_wp8 |
|----------|--------|--------|--------|--------|--------|--------|--------|---------|
|      Bib |        |        |        |        |        |        |        |         |
|      300 | 2142.0 | 3533.0 | 4216.0 | 5903.0 | 7617.0 | 8745.0 | 9361.0 | 10904.0 |
|      302 | 2144.0 | 3274.0 | 3999.0 | 5716.0 | 7441.0 | 8565.0 | 9187.0 | 10754.0 |
|      306 | 2146.0 | 3289.0 | 3984.0 | 5701.0 | 7406.0 | 8500.0 | 9093.0 | 10623.0 |
|      307 | 2203.0 | 3321.0 | 4040.0 | 5750.0 | 7474.0 | 8593.0 | 9192.0 | 10742.0 |
|      308 | 2201.0 | 3317.0 | 4032.0 | 5992.0 | 7631.0 | 8742.0 | 9362.0 | 10891.0 |
|      309 | 2183.0 | 3324.0 | 4052.0 | 5809.0 | 7481.0 | 8575.0 | 9167.0 | 10698.0 |
|      311 | 2197.0 | 3385.0 | 4116.0 | 5876.0 | 7607.0 | 8729.0 | 9339.0 | 10936.0 |
|      312 | 2242.0 | 3396.0 | 4130.0 | 5872.0 | 7545.0 | 8659.0 | 9268.0 | 10861.0 |
|      314 | 2211.0 | 3352.0 | 4064.0 | 5776.0 | 7446.0 | 8581.0 | 9205.0 | 10790.0 |
|      322 | 2202.0 | 3346.0 | 4063.0 | 5768.0 | 7559.0 | 8707.0 | 9342.0 | 10924.0 |



    In [1076]: #Example - driver overall ranks by stage in in top10 overall at end of stage
               q="SELECT * FROM ranking WHERE VehicleType='car' AND Type='general' AND Pos<=10"
               tmpq = pd.read_sql(q, conn)
               tmpq.pivot(index='Bib',columns='Stage',values='Pos')
    
    Out[1076]: 

{width="wide"}
| Stage |    1 |    2 |    3 |    4 |    5 |    6 |    7 |    8 |    9 |   10 |
|-------|------|------|------|------|------|------|------|------|------|------|
|   Bib |      |      |      |      |      |      |      |      |      |      |
|   300 |  2.0 |  6.0 |  NaN |  NaN |  NaN |  NaN | 10.0 |  9.0 |  NaN |  NaN |
|   301 |  1.0 |  8.0 |  1.0 |  1.0 |  1.0 |  1.0 |  1.0 |  1.0 |  1.0 |  1.0 |
|   302 |  6.0 |  1.0 |  NaN |  NaN |  NaN |  NaN |  NaN | 10.0 |  8.0 |  9.0 |
|   303 |  3.0 |  NaN |  5.0 |  4.0 |  4.0 |  6.0 |  6.0 |  6.0 |  4.0 |  4.0 |
|   304 |  7.0 |  NaN |  3.0 |  2.0 |  2.0 |  3.0 |  2.0 |  4.0 |  NaN |  NaN |
|   305 |  NaN |  NaN |  NaN |  NaN |  8.0 |  7.0 |  7.0 |  7.0 |  6.0 |  6.0 |
|   306 |  NaN |  5.0 |  8.0 |  6.0 |  5.0 |  2.0 |  4.0 |  3.0 |  3.0 |  3.0 |
|   307 |  NaN |  3.0 |  4.0 |  3.0 |  3.0 |  4.0 |  3.0 |  2.0 |  2.0 |  2.0 |
|   308 | 10.0 |  9.0 |  6.0 |  7.0 |  6.0 |  5.0 |  5.0 |  5.0 |  5.0 |  5.0 |
|   309 |  8.0 |  2.0 |  7.0 |  NaN |  NaN |  9.0 |  9.0 |  NaN |  NaN |  NaN |
|   311 |  4.0 | 10.0 |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN |
|   312 |  9.0 |  7.0 |  NaN | 10.0 |  7.0 |  NaN |  NaN |  NaN |  NaN |  NaN |
|   314 |  5.0 |  4.0 |  2.0 |  5.0 | 10.0 |  8.0 |  8.0 |  8.0 |  7.0 |  7.0 |
|   319 |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN | 10.0 | 10.0 |
|   321 |  NaN |  NaN |  NaN |  9.0 |  9.0 |  NaN |  NaN |  NaN |  9.0 |  8.0 |
|   327 |  NaN |  NaN |  9.0 |  8.0 |  NaN |  NaN |  NaN |  NaN |  NaN |  NaN |
|   330 |  NaN |  NaN | 10.0 |  NaN |  NaN | 10.0 |  NaN |  NaN |  NaN |  NaN |



    In [1088]: tmpq
    
    Out[1088]: 

{width="wide"}
|     | Year | Stage |  Type | Pos | Bib | VehicleType |                                              Crew |   Brand | Time_raw | TimeInS |  Gap_raw | GapInS | Penalty_raw | PenaltyInS |             Name |
|-----|------|-------|-------|-----|-----|-------------|---------------------------------------------------|---------|----------|---------|----------|--------|-------------|------------|------------------|
|   0 | 2019 |     1 | stage |   1 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 01:01:41 |  3701.0 |     None |    NaN |    00:00:00 |          0 |        M. BAUMEL |
|   1 | 2019 |     1 | stage |   1 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 01:01:41 |  3701.0 |     None |    NaN |    00:00:00 |          0 |    N. AL-ATTIYAH |
|   2 | 2019 |     1 | stage |   2 | 300 |         car |             C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |    MINI | 01:03:40 |  3820.0 | 00:01:59 |  119.0 |    00:00:00 |          0 |         C. SAINZ |
|   3 | 2019 |     1 | stage |   2 | 300 |         car |             C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |    MINI | 01:03:40 |  3820.0 | 00:01:59 |  119.0 |    00:00:00 |          0 |          L. CRUZ |
|   4 | 2019 |     1 | stage |   3 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 01:03:41 |  3821.0 | 00:02:00 |  120.0 |    00:00:00 |          0 |    J. PRZYGONSKI |
|   5 | 2019 |     1 | stage |   3 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 01:03:41 |  3821.0 | 00:02:00 |  120.0 |    00:00:00 |          0 |       T. COLSOUL |
|   6 | 2019 |     1 | stage |   4 | 311 |         car |             V. VASILYEV K. ZHILTSOV G-ENERGY TEAM |  TOYOTA | 01:03:59 |  3839.0 | 00:02:18 |  138.0 |    00:00:00 |          0 |      K. ZHILTSOV |
|   7 | 2019 |     1 | stage |   4 | 311 |         car |             V. VASILYEV K. ZHILTSOV G-ENERGY TEAM |  TOYOTA | 01:03:59 |  3839.0 | 00:02:18 |  138.0 |    00:00:00 |          0 |      V. VASILYEV |
|   8 | 2019 |     1 | stage |   5 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |    MINI | 01:04:09 |  3849.0 | 00:02:28 |  148.0 |    00:00:00 |          0 |    T. GOTTSCHALK |
|   9 | 2019 |     1 | stage |   5 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |    MINI | 01:04:09 |  3849.0 | 00:02:28 |  148.0 |    00:00:00 |          0 |      Y. AL RAJHI |
|  10 | 2019 |     2 | stage |   1 | 306 |         car |                         S. LOEB D. ELENA PH-SPORT | PEUGEOT | 03:26:53 | 12413.0 |     None |    NaN |    00:00:00 |          0 |         D. ELENA |
|  11 | 2019 |     2 | stage |   1 | 306 |         car |                         S. LOEB D. ELENA PH-SPORT | PEUGEOT | 03:26:53 | 12413.0 |     None |    NaN |    00:00:00 |          0 |          S. LOEB |
|  12 | 2019 |     2 | stage |   2 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 03:27:01 | 12421.0 | 00:00:08 |    8.0 |    00:00:00 |          0 |    A. HARO BRAVO |
|  13 | 2019 |     2 | stage |   2 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 03:27:01 | 12421.0 | 00:00:08 |    8.0 |    00:00:00 |          0 |          N. ROMA |
|  14 | 2019 |     2 | stage |   3 | 309 |         car |   B. TEN BRINKE X. PANSERI TOYOTA GAZOO RACING SA |  TOYOTA | 03:28:13 | 12493.0 | 00:01:20 |   80.0 |    00:00:00 |          0 |    B. TEN BRINKE |
|  15 | 2019 |     2 | stage |   3 | 309 |         car |   B. TEN BRINKE X. PANSERI TOYOTA GAZOO RACING SA |  TOYOTA | 03:28:13 | 12493.0 | 00:01:20 |   80.0 |    00:00:00 |          0 |       X. PANSERI |
|  16 | 2019 |     2 | stage |   4 | 302 |         car | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |  TOYOTA | 03:28:24 | 12504.0 | 00:01:31 |   91.0 |    00:00:00 |          0 | D. VON ZITZEWITZ |
|  17 | 2019 |     2 | stage |   4 | 302 |         car | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |  TOYOTA | 03:28:24 | 12504.0 | 00:01:31 |   91.0 |    00:00:00 |          0 |   G. DE VILLIERS |
|  18 | 2019 |     2 | stage |   5 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |    MINI | 03:29:26 | 12566.0 | 00:02:33 |  153.0 |    00:00:00 |          0 |    T. GOTTSCHALK |
|  19 | 2019 |     2 | stage |   5 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |    MINI | 03:29:26 | 12566.0 | 00:02:33 |  153.0 |    00:00:00 |          0 |      Y. AL RAJHI |
|  20 | 2019 |     3 | stage |   1 | 304 |         car |    S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |    MINI | 03:54:31 | 14071.0 |     None |    NaN |    00:00:00 |          0 |       D. CASTERA |
|  21 | 2019 |     3 | stage |   1 | 304 |         car |    S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |    MINI | 03:54:31 | 14071.0 |     None |    NaN |    00:00:00 |          0 |   S. PETERHANSEL |
|  22 | 2019 |     3 | stage |   2 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 03:57:57 | 14277.0 | 00:03:26 |  206.0 |    00:00:00 |          0 |        M. BAUMEL |
|  23 | 2019 |     3 | stage |   2 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 03:57:57 | 14277.0 | 00:03:26 |  206.0 |    00:00:00 |          0 |    N. AL-ATTIYAH |
|  24 | 2019 |     3 | stage |   3 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 04:06:18 | 14778.0 | 00:11:47 |  707.0 |    00:00:00 |          0 |    J. PRZYGONSKI |
|  25 | 2019 |     3 | stage |   3 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 04:06:18 | 14778.0 | 00:11:47 |  707.0 |    00:00:00 |          0 |       T. COLSOUL |
|  26 | 2019 |     3 | stage |   4 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |    MINI | 04:07:21 | 14841.0 | 00:12:50 |  770.0 |    00:00:00 |          0 |    T. GOTTSCHALK |
|  27 | 2019 |     3 | stage |   4 | 314 |         car |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |    MINI | 04:07:21 | 14841.0 | 00:12:50 |  770.0 |    00:00:00 |          0 |      Y. AL RAJHI |
|  28 | 2019 |     3 | stage |   5 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 04:12:43 | 15163.0 | 00:18:12 | 1092.0 |    00:00:00 |          0 |    A. HARO BRAVO |
|  29 | 2019 |     3 | stage |   5 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 04:12:43 | 15163.0 | 00:18:12 | 1092.0 |    00:00:00 |          0 |          N. ROMA |
| ... |  ... |   ... |   ... | ... | ... |         ... |                                               ... |     ... |      ... |     ... |      ... |    ... |         ... |        ... |              ... |
|  70 | 2019 |     8 | stage |   1 | 306 |         car |                         S. LOEB D. ELENA PH-SPORT | PEUGEOT | 03:54:53 | 14093.0 |     None |    NaN |    00:00:00 |          0 |         D. ELENA |
|  71 | 2019 |     8 | stage |   1 | 306 |         car |                         S. LOEB D. ELENA PH-SPORT | PEUGEOT | 03:54:53 | 14093.0 |     None |    NaN |    00:00:00 |          0 |          S. LOEB |
|  72 | 2019 |     8 | stage |   2 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 04:02:20 | 14540.0 | 00:07:27 |  447.0 |    00:00:00 |          0 |        M. BAUMEL |
|  73 | 2019 |     8 | stage |   2 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 04:02:20 | 14540.0 | 00:07:27 |  447.0 |    00:00:00 |          0 |    N. AL-ATTIYAH |
|  74 | 2019 |     8 | stage |   3 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 04:10:08 | 15008.0 | 00:15:15 |  915.0 |    00:00:00 |          0 |    J. PRZYGONSKI |
|  75 | 2019 |     8 | stage |   3 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 04:10:08 | 15008.0 | 00:15:15 |  915.0 |    00:00:00 |          0 |       T. COLSOUL |
|  76 | 2019 |     8 | stage |   4 | 302 |         car | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |  TOYOTA | 04:10:48 | 15048.0 | 00:15:55 |  955.0 |    00:00:00 |          0 | D. VON ZITZEWITZ |
|  77 | 2019 |     8 | stage |   4 | 302 |         car | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |  TOYOTA | 04:10:48 | 15048.0 | 00:15:55 |  955.0 |    00:00:00 |          0 |   G. DE VILLIERS |
|  78 | 2019 |     8 | stage |   5 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 04:10:50 | 15050.0 | 00:15:57 |  957.0 |    00:00:00 |          0 |    A. HARO BRAVO |
|  79 | 2019 |     8 | stage |   5 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 04:10:50 | 15050.0 | 00:15:57 |  957.0 |    00:00:00 |          0 |          N. ROMA |
|  80 | 2019 |     9 | stage |   1 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 03:53:22 | 14002.0 |     None |    NaN |    00:00:00 |          0 |        M. BAUMEL |
|  81 | 2019 |     9 | stage |   1 | 301 |         car |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |  TOYOTA | 03:53:22 | 14002.0 |     None |    NaN |    00:00:00 |          0 |    N. AL-ATTIYAH |
|  82 | 2019 |     9 | stage |   2 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 03:58:20 | 14300.0 | 00:04:58 |  298.0 |    00:00:00 |          0 |    A. HARO BRAVO |
|  83 | 2019 |     9 | stage |   2 | 307 |         car |                 N. ROMA A. HARO BRAVO X-RAID TEAM |    MINI | 03:58:20 | 14300.0 | 00:04:58 |  298.0 |    00:00:00 |          0 |          N. ROMA |
|  84 | 2019 |     9 | stage |   3 | 302 |         car | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |  TOYOTA | 04:00:37 | 14437.0 | 00:07:15 |  435.0 |    00:00:00 |          0 | D. VON ZITZEWITZ |
|  85 | 2019 |     9 | stage |   3 | 302 |         car | G. DE VILLIERS D. VON ZITZEWITZ TOYOTA GAZOO R... |  TOYOTA | 04:00:37 | 14437.0 | 00:07:15 |  435.0 |    00:00:00 |          0 |   G. DE VILLIERS |
|  86 | 2019 |     9 | stage |   4 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 04:07:23 | 14843.0 | 00:14:01 |  841.0 |    00:00:00 |          0 |    J. PRZYGONSKI |
|  87 | 2019 |     9 | stage |   4 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 04:07:23 | 14843.0 | 00:14:01 |  841.0 |    00:00:00 |          0 |       T. COLSOUL |
|  88 | 2019 |     9 | stage |   5 | 330 |         car | B. VANAGAS S. ROZWADOWSKI GENERAL FINANCING TE... |  TOYOTA | 04:09:07 | 14947.0 | 00:15:45 |  945.0 |    00:00:00 |          0 |       B. VANAGAS |
|  89 | 2019 |     9 | stage |   5 | 330 |         car | B. VANAGAS S. ROZWADOWSKI GENERAL FINANCING TE... |  TOYOTA | 04:09:07 | 14947.0 | 00:15:45 |  945.0 |    00:00:00 |          0 |   S. ROZWADOWSKI |
|  90 | 2019 |    10 | stage |   1 | 300 |         car |             C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |    MINI | 01:20:01 |  4801.0 |     None |    NaN |    00:00:00 |          0 |         C. SAINZ |
|  91 | 2019 |    10 | stage |   1 | 300 |         car |             C. SAINZ L. CRUZ X-RAID MINI JCW TEAM |    MINI | 01:20:01 |  4801.0 |     None |    NaN |    00:00:00 |          0 |          L. CRUZ |
|  92 | 2019 |    10 | stage |   2 | 306 |         car |                         S. LOEB D. ELENA PH-SPORT | PEUGEOT | 01:20:43 |  4843.0 | 00:00:42 |   42.0 |    00:00:00 |          0 |         D. ELENA |
|  93 | 2019 |    10 | stage |   2 | 306 |         car |                         S. LOEB D. ELENA PH-SPORT | PEUGEOT | 01:20:43 |  4843.0 | 00:00:42 |   42.0 |    00:00:00 |          0 |          S. LOEB |
|  94 | 2019 |    10 | stage |   3 | 308 |         car |       C. DESPRES JP. COTTRET X-RAID MINI JCW TEAM |    MINI | 01:22:32 |  4952.0 | 00:02:31 |  151.0 |    00:00:00 |          0 |       C. DESPRES |
|  95 | 2019 |    10 | stage |   3 | 308 |         car |       C. DESPRES JP. COTTRET X-RAID MINI JCW TEAM |    MINI | 01:22:32 |  4952.0 | 00:02:31 |  151.0 |    00:00:00 |          0 |      JP. COTTRET |
|  96 | 2019 |    10 | stage |   4 | 330 |         car | B. VANAGAS S. ROZWADOWSKI GENERAL FINANCING TE... |  TOYOTA | 01:23:39 |  5019.0 | 00:03:38 |  218.0 |    00:00:00 |          0 |       B. VANAGAS |
|  97 | 2019 |    10 | stage |   4 | 330 |         car | B. VANAGAS S. ROZWADOWSKI GENERAL FINANCING TE... |  TOYOTA | 01:23:39 |  5019.0 | 00:03:38 |  218.0 |    00:00:00 |          0 |   S. ROZWADOWSKI |
|  98 | 2019 |    10 | stage |   5 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 01:23:42 |  5022.0 | 00:03:41 |  221.0 |    00:00:00 |          0 |    J. PRZYGONSKI |
|  99 | 2019 |    10 | stage |   5 | 303 |         car |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |    MINI | 01:23:42 |  5022.0 | 00:03:41 |  221.0 |    00:00:00 |          0 |       T. COLSOUL |



    In [1095]: #Driver top 5 finishes by stage
               q="SELECT r.*, c.Name FROM ranking r LEFT JOIN crew c ON c.Bib=r.Bib WHERE Num=0 AND VehicleType='car' AND Type='stage' AND Pos<=5"
               
               tmpq = pd.read_sql(q, conn)
               tmpq.pivot(index='Stage',columns='Pos',values='Name')
    
    Out[1095]: 

{width="wide"}
|   Pos |              1 |              2 |              3 |              4 |             5 |
|-------|----------------|----------------|----------------|----------------|---------------|
| Stage |                |                |                |                |               |
|     1 |  N. AL-ATTIYAH |       C. SAINZ |  J. PRZYGONSKI |    V. VASILYEV |   Y. AL RAJHI |
|     2 |        S. LOEB |        N. ROMA |  B. TEN BRINKE | G. DE VILLIERS |   Y. AL RAJHI |
|     3 | S. PETERHANSEL |  N. AL-ATTIYAH |  J. PRZYGONSKI |    Y. AL RAJHI |       N. ROMA |
|     4 |  N. AL-ATTIYAH | S. PETERHANSEL |  J. PRZYGONSKI |        N. ROMA |       S. LOEB |
|     5 |        S. LOEB |  N. AL-ATTIYAH |        N. ROMA | S. PETERHANSEL | J. PRZYGONSKI |
|     6 |        S. LOEB |  N. AL-ATTIYAH |       C. SAINZ |     C. DESPRES |       N. ROMA |
|     7 | S. PETERHANSEL |        N. ROMA |       C. SAINZ |  N. AL-ATTIYAH |    C. DESPRES |
|     8 |        S. LOEB |  N. AL-ATTIYAH |  J. PRZYGONSKI | G. DE VILLIERS |       N. ROMA |
|     9 |  N. AL-ATTIYAH |        N. ROMA | G. DE VILLIERS |  J. PRZYGONSKI |    B. VANAGAS |
|    10 |       C. SAINZ |        S. LOEB |     C. DESPRES |     B. VANAGAS | J. PRZYGONSKI |




# Reset Base Dataframes

That is, reset for the particular data config we defined at the top of the notebook.




## Find the time between each waypoint

That is, the time taken to get from one waypoint to the next. If we think of waypoints as splits, this is eesentially a `timeInSplit` value. If we know this information, we can work out how much time each competitor made, or lost, relative to every other competitor in the same class, going between each waypoint.

This means we may be able to work out which parts of the stage a particular competitor was pushing on, or had difficulties on.

    In [496]: def _get_time_between_waypoints(timing_data_long):
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

    
    Out[496]: 

{width="wide"}
|    | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand | Waypoint |     Time | TimeInS | timeInSplit | splitS |
|----|-----|-----|---------------|--------|------------------------------------------------|--------|----------|----------|---------|-------------|--------|
| 95 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |   01_wp1 | 00:17:58 |  1078.0 |    00:17:58 | 1078.0 |
| 96 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |   01_wp1 | 00:17:22 |  1042.0 |    00:17:22 | 1042.0 |
| 97 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |   01_wp1 | 00:18:35 |  1115.0 |    00:18:35 | 1115.0 |
| 98 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |   01_wp1 | 00:18:23 |  1103.0 |    00:18:23 | 1103.0 |
| 99 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |   01_wp1 | 00:18:52 |  1132.0 |    00:18:52 | 1132.0 |



    In [497]: timing_data_long = get_long_annotated_timing_data(STAGE, VTYPE)[TIME]
              timing_data_long
              #timing_data_long[timing_data_long['Brand']=='PEUGEOT'].head()
              timing_data_long.head()
    
    Out[497]: 

{width="wide"}
|    | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand | Waypoint |     Time | TimeInS |
|----|-----|-----|---------------|--------|------------------------------------------------|--------|----------|----------|---------|
| 95 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |   01_wp1 | 00:17:58 |  1078.0 |
| 96 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |   01_wp1 | 00:17:22 |  1042.0 |
| 97 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |   01_wp1 | 00:18:35 |  1115.0 |
| 98 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |   01_wp1 | 00:18:23 |  1103.0 |
| 99 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |   01_wp1 | 00:18:52 |  1132.0 |




## Rebase relative to driver

Rebasing means finding deltas relative to a specified driver. It lets us see the deltas a particular driver has to each other driver.

    In [931]: def rebaseTimes(times, bib=None, col=None):
                  if bib is None or col is None: return times
                  return times[col] - times[times['Bib']==bib][col].iloc[0]
              
              def rebaseWaypointTimes(times, bib=None, col='splitS'):
                  ''' Rebase times relative to a particular competitor. '''
                  
                  if bib is None: return times
                  bib = int(bib)
                  rebase = times[times['Bib']==bib][['Waypoint',col]].set_index('Waypoint').to_dict(orient='dict')[col]
                  times['rebased']=times[col]-times['Waypoint'].map(rebase)
                  return times



## Rebase Overall Waypoint Times and Time In Split Times

That is, rebase the overall time in stage at each waypoint relative to a specified driver.

    In [855]: REBASER = 306
              #loeb 306, AL-ATTIYAH 301, peterhansel 304
              #aravind prabhakar 48 coronel 347

    In [856]: timing_data_long_min = rebaseWaypointTimes( timing_data_long , REBASER, 'TimeInS')

    In [857]: -timing_data_long_min.reset_index().pivot('Bib','Waypoint','rebased').head()
    
    Out[857]: 

{width="wide"}
| Waypoint |   01_wp1 |   02_wp2 |   03_wp3 | 04_wp4 | 05_wp5/rav |   06_wp6 |   07_wp7 |   08_wp8 |   09_ass |
|----------|----------|----------|----------|--------|------------|----------|----------|----------|----------|
|      Bib |          |          |          |        |            |          |          |          |          |
|      300 |     35.0 | -11758.0 | -12136.0 |    NaN |   -12795.0 | -12928.0 | -12961.0 | -15114.0 | -13043.0 |
|      301 |     38.0 |     21.0 |    414.0 |    NaN |     2286.0 |   2322.0 |   2347.0 |   2413.0 |   2369.0 |
|      302 | -21260.0 |    -86.0 |    276.0 |    NaN |   -13139.0 | -13190.0 | -13477.0 | -13626.0 | -13670.0 |
|      303 |    -35.0 |    -81.0 |     95.0 |    NaN |     1812.0 |   1831.0 |   1809.0 |   1831.0 |   1868.0 |
|      304 |      2.0 |     21.0 |    487.0 |    NaN |     2324.0 |   2369.0 |   2506.0 |   2556.0 |   2575.0 |



    In [858]: rebaseWaypointTimes(timing_data_long_insplit,REBASER).head()
    
    Out[858]: 

{width="wide"}
|    | Pos | Bib | Road Position | Refuel |                                           Crew |  Brand | Waypoint |     Time | TimeInS | timeInSplit | splitS | rebased |
|----|-----|-----|---------------|--------|------------------------------------------------|--------|----------|----------|---------|-------------|--------|---------|
| 95 | 1.0 | 304 |          17.0 |  False | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |   01_wp1 | 00:17:58 |  1078.0 |    00:17:58 | 1078.0 |    -2.0 |
| 96 | 2.0 | 301 |          11.0 |  False | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |   01_wp1 | 00:17:22 |  1042.0 |    00:17:22 | 1042.0 |   -38.0 |
| 97 | 3.0 | 303 |          13.0 |  False |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |   01_wp1 | 00:18:35 |  1115.0 |    00:18:35 | 1115.0 |    35.0 |
| 98 | 4.0 | 314 |           5.0 |  False |          Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |   MINI |   01_wp1 | 00:18:23 |  1103.0 |    00:18:23 | 1103.0 |    23.0 |
| 99 | 5.0 | 307 |           2.0 |  False |              N. ROMA A. HARO BRAVO X-RAID TEAM |   MINI |   01_wp1 | 00:18:52 |  1132.0 |    00:18:52 | 1132.0 |    52.0 |



    In [859]: def pivotRebasedSplits(rebasedSplits):
                  ''' For each driver row, find the split. '''
                  
                  #If there are no splits...
                  if rebasedSplits.empty:
                      return pd.DataFrame(columns=['Bib']).set_index('Bib')
                  
                  rbp=-rebasedSplits.pivot('Bib','Waypoint','rebased')
                  rbp.columns=['D{}'.format(c) for c in rbp.columns]
                  #The columns seem to be sorted? Need to sort in actual split order
                  rbp.sort_values(rbp.columns[-1],ascending =True)
                  return rbp


    In [860]: tmp = pivotRebasedSplits(rebaseWaypointTimes(timing_data_long_insplit,REBASER))
              tmp.head()
    
    Out[860]: 

{width="wide"}
|     |  D01_wp1 |  D02_wp2 | D03_wp3 | D04_wp4 | D05_wp5/rav | D06_wp6 | D07_wp7 | D08_wp8 |  D09_ass |
|-----|----------|----------|---------|---------|-------------|---------|---------|---------|----------|
| Bib |          |          |         |         |             |         |         |         |          |
| 300 |     35.0 | -11793.0 |  -378.0 |     NaN |    -12795.0 |  -133.0 |   -33.0 | -2153.0 | -28471.0 |
| 301 |     38.0 |    -17.0 |   393.0 |     NaN |      2286.0 |    36.0 |    25.0 |    66.0 |    -44.0 |
| 302 | -21260.0 |  -1166.0 |   362.0 |     NaN |    -13139.0 |   -51.0 |  -287.0 |  -149.0 |    -44.0 |
| 303 |    -35.0 |    -46.0 |   176.0 |     NaN |      1812.0 |    19.0 |   -22.0 |    22.0 |     37.0 |
| 304 |      2.0 |     19.0 |   466.0 |     NaN |      2324.0 |    45.0 |   137.0 |    50.0 |     19.0 |



    In [861]: #top10 = driver_data[(driver_data['Pos']>=45) & (driver_data['Pos']<=65)]
              top10 = driver_data[(driver_data['Pos']<=20)]
              top10.set_index('Bib', inplace=True)
              top10
    
    Out[861]: 

{width="wide"}
|     |  Pos | Road Position | Refuel |                                              Crew |                 Brand |
|-----|------|---------------|--------|---------------------------------------------------|-----------------------|
| Bib |      |               |        |                                                   |                       |
| 304 |  1.0 |          17.0 |  False |    S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |                  MINI |
| 301 |  2.0 |          11.0 |  False |    N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA |                TOYOTA |
| 303 |  3.0 |          13.0 |  False |        J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |                  MINI |
| 314 |  4.0 |           5.0 |  False |             Y. AL RAJHI T. GOTTSCHALK X-RAID TEAM |                  MINI |
| 307 |  5.0 |           2.0 |  False |                 N. ROMA A. HARO BRAVO X-RAID TEAM |                  MINI |
| 308 |  6.0 |           6.0 |  False |       C. DESPRES JP. COTTRET X-RAID MINI JCW TEAM |                  MINI |
| 309 |  7.0 |           3.0 |  False |   B. TEN BRINKE X. PANSERI TOYOTA GAZOO RACING SA |                TOYOTA |
| 327 |  8.0 |          14.0 |  False |             A. DOMZALA M. MARTON OVERDRIVE TOYOTA |                TOYOTA |
| 305 |  9.0 |          12.0 |  False |                    M. PROKOP J. TOMANEK MP-SPORTS |                  FORD |
| 330 | 10.0 |          16.0 |  False | B. VANAGAS S. ROZWADOWSKI GENERAL FINANCING TE... |                TOYOTA |
| 306 | 11.0 |           1.0 |  False |                         S. LOEB D. ELENA PH-SPORT |               PEUGEOT |
| 320 | 12.0 |          20.0 |  False |                M. SERRADORI F. LURQUIN SRT RACING |                 BUGGY |
| 337 | 13.0 |          23.0 |  False |                   V. ZALA S. JURGELENAS AGRORODEO |                TOYOTA |
| 321 | 14.0 |          18.0 |  False |              B. GARAFULIC F. PALMEIRO X-RAID TEAM |                  MINI |
| 319 | 15.0 |          19.0 |  False |              R. CHABOT G. PILLOT OVERDRIVE TOYOTA |                TOYOTA |
| 326 | 16.0 |          26.0 |  False | P. GACHE S. PREVOT GEELY AUTO SHELL LUBRICANT ... |                 BUGGY |
| 347 | 17.0 |          31.0 |   True |            T. CORONEL T. CORONEL MAXXIS DAKARTEAM | JEFFERIES DAKAR RALLY |
| 374 | 18.0 |          21.0 |  False |                 D. KROTOV D. TSYRO MSK RALLY TEAM |                  MINI |
| 363 | 19.0 |          75.0 |  False |           R. VAUTHIER P. LARROQUE MD RALLYE SPORT |               OPTIMUS |
| 312 | 20.0 |           7.0 |  False |                      H. HUNT W. ROSEGAAR PH-SPORT |               PEUGEOT |




## Display Stage Tables

A lot of this work is copied directly from my WRC stage tables notebooks, so there is less explanation here about what the pieces are and how they work together.

    In [862]: #UPDATE THIS FROM WRC NOTEBOOKS TO HANDLE EXCEPTIONS, DATATYPES ETC
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

    In [863]: rb2c = pivotRebasedSplits(rebaseWaypointTimes(timing_data_long_insplit,REBASER))
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
    
    Out[863]: 

{width="wide"}
|     | Road Position |                                           Crew |  Brand | D01_wp1 | D02_wp2 | D03_wp3 | D04_wp4 | D05_wp5/rav | D06_wp6 | D07_wp7 | ... | Pos | 01_wp1 | 02_wp2 | 03_wp3 | 04_wp4 | 05_wp5/rav | 06_wp6 | 07_wp7 | 08_wp8 | Stage Overall |
|-----|---------------|------------------------------------------------|--------|---------|---------|---------|---------|-------------|---------|---------|-----|-----|--------|--------|--------|--------|------------|--------|--------|--------|---------------|
| Bib |               |                                                |        |         |         |         |         |             |         |         |     |     |        |        |        |        |            |        |        |        |               |
| 304 |          17.0 | S. PETERHANSEL D. CASTERA X-RAID MINI JCW TEAM |   MINI |     2.0 |    19.0 |   466.0 |     NaN |      2324.0 |    45.0 |   137.0 | ... | 1.0 |    2.0 |   21.0 |  487.0 |    NaN |     2324.0 | 2369.0 | 2506.0 | 2556.0 |        2575.0 |
| 301 |          11.0 | N. AL-ATTIYAH M. BAUMEL TOYOTA GAZOO RACING SA | TOYOTA |    38.0 |   -17.0 |   393.0 |     NaN |      2286.0 |    36.0 |    25.0 | ... | 2.0 |   38.0 |   21.0 |  414.0 |    NaN |     2286.0 | 2322.0 | 2347.0 | 2413.0 |        2369.0 |
| 303 |          13.0 |     J. PRZYGONSKI T. COLSOUL ORLEN X-RAID TEAM |   MINI |   -35.0 |   -46.0 |   176.0 |     NaN |      1812.0 |    19.0 |   -22.0 | ... | 3.0 |  -35.0 |  -81.0 |   95.0 |    NaN |     1812.0 | 1831.0 | 1809.0 | 1831.0 |        1868.0 |



    In [999]: s = __styleDriverSplitReportBaseDataframe(rb2cTop10, 'Stage {}'.format(STAGE))
              html=s.render()
              display(HTML(html))

    In [1020]: #Changes from WRC
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

    In [866]: ## Can we style the type in the red/green thing by using timedelta and formatter
              # eg https://docs.python.org/3.4/library/datetime.html?highlight=weekday#datetime.date.__format__
              #and https://stackoverflow.com/a/46370761/454773
              
              #Maybe also add a sparkline? Need to set a common y axis on all charts?

    In [867]: s2 = moreStyleDriverSplitReportBaseDataframe(rb2cTop10, STAGE)
              display(HTML(s2))
