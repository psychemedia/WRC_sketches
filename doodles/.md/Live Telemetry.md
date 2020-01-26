---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Grabbing Live Telemetry Data

The actual telemetry is unavailable, other than `kms` (??) and GPS co-ordinates.

The times feed regularly updates and we should probably capture the most recent (so upsert).

The data feed is a time series, so each record needs its own entry. We can then generate a time series for each driver around their data samples.

There seem to be a couple of live feeds we can pull from:

```python
url='https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getData?timeout=500'

import requests

livetimes = requests.get(url)
livetimes.json()['_entries'][12]
```

```python hide_input=false
from pandas.io.json import json_normalize
df = json_normalize(livetimes.json(), '_entries', meta='timestamp')
df.head()
```

```python
df.head(2).to_dict(orient='records')
```

```python
from sqlite_utils import Database

db = Database("testlive-wrc-mc.db")
```

```python
test1 = db["test1"]
test1.upsert_all(df.to_dict(orient='records'),pk=['name','utx'])
```

```python
import os
import json

def dir_loader(dbtable, path, suffix='txt'):
    for fn in [f for f in os.listdir(path) if f.split('.')[-1]==suffix]:
        with open(os.path.join(path,fn)) as f:
            _df = json_normalize( json.load(f) )
            dbtable.upsert_all(df.to_dict(orient='records'),
                               pk=['name','utx'])
```

```python
dir_loader(db["test1"],'times/raw')
```

```python
import pandas as pd
pd.read_sql('select * from test1 limit 3',db.conn)
```

```python
!ls -al testlive-wrc-mc-livegrab.db
```

```python
!ls *xml
```

```python
import kml2geojson
!mkdir -p gj_out
kml2geojson.main.convert('montecarlo_2020.xml','gj_out')

```

```python
!ls gj_out
```

```python

```

Things like
https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getTimes?sitsid=%2283%22&stageid=%221122%22&stagename=%22SS12%22&timeout=5000
give start times but we can't tell which car is which?
The `startnumber` is perhaps one of the driver identifying values?


```
{
sitsid: "83",
stageid: "1122",
stagename: "SS12",
status: "",
_entries: [
{
startnumber: "1",
position: 4,
starttime: "16:44",
racetime: "10:11.6",
totaltime: "02:41:35.8",
totalpos: "2",
status: "COMPLETED",
_items: [
{
timestamp: 185009
},
{
timestamp: 338700
},
{
timestamp: 528800
}
]
},
{
startnumber: "10",
position: 0,
starttime: "16:41",
racetime: "",
totaltime: "",
totalpos: "",
status: "RUNNING",
_items: [
{
timestamp: 183400
}
]
},
```

Not sure whether anything in https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getData?timeout=5000 matches across to getTimes?

```
{
hash: "636932102363236630",
timestamp: 1557599034200,
_entries: [
{
name: "001",
lon: -73.07338,
lat: -36.77735,
speed: 0,
heading: 234,
utx: 1557599022300,
driverid: "",
track: "",
status: "Competing",
gear: 0,
throttle: 0,
brk: 0,
rpm: 0,
accx: 0,
accy: 0,
kms: 0.1,
altitude: 0
},
{
name: "003",
lon: -73.07195,
lat: -36.77664,
speed: 0,
heading: 254,
utx: 1557598210500,
driverid: "",
track: "",
status: "Competing",
gear: 0,
throttle: 0,
brk: 0,
rpm: 0,
accx: 0,
accy: 0,
kms: 0.3,
altitude: 0
},
...
]
```
The `name` is perhaps an identifying number.

```python
url='https://www.wrc.com/live-ticker/live_popup_text.html?absolute=true'
r_html = requests.get(url)
```

```python
r_html.text
```

```python
import pandas as pd

url='https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getTimes?sitsid=%2283%22&stageid=%221122%22&stagename=%22SS12%22&timeout=5000'
url='https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getTimes?sitsid="83"&stageid="1122"&stagename="SS12"&timeout=5000'
livetimes = requests.get(url)
df_livetimes = pd.DataFrame(livetimes.json())
df_livetimes.head()
```

```python
from pandas.io.json import json_normalize
json_normalize(livetimes.json(),['_entries', '_items'])#['_items'].apply(pd.Series)
#startnumber
```

```python
json_normalize(livetimes.json(),['_entries'])#['_items'].apply(pd.Series)

```

```python
#zz = pd.concat([df_livetimes.drop(['_entries'], axis=1), df_livetimes['_entries'].apply(pd.Series)], axis=1)
zz=df_livetimes['_entries'].apply(pd.Series)
zz.head()
```

```python
zz.iloc[0]['_items']
```

```python
#COMPLETED', 'RUNNING', '', 'DNS', 'TORUN'
zz['status'].unique()
```

```python
df_livetimes['status'].unique()
#RUNNING
```

```python
url='https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getTimes?sitsid="83"'
requests.get(url).json()
'''
{'sitsid': '83',
 'stageid': None,
 'stagename': None,
 'status': '',
 '_entries': []}
'''
#So we need to pass  in the sitsid, stageid and stagename to get the data back
```

```python

```
