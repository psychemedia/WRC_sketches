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

# Scraper for Rallies.info

The *rallies.info* website publishes a wide range of results for UK rallies.

This notebook describes a simple scraper for retrieving state time information from that site and transforming it into a range of derived data tables describing the state of the rally at each stage.

Whilst the publishes a range of tables including times and rankings, we should be able to derive positions / rankings from the just the stage times table.

Additional information about entries is also available from the entry list table.

```python
import pandas as pd
```

## Entry List

The entry list provides the name of the  car number, driver and co-driver, car type, engine size, class, club and entrant/sponsor name.

```python
entry_list_url = 'https://www.rallies.info/res.php?e=296&o=p&t=999&r=e&n=999999&i=300'
entry_list_url = 'https://www.rallies.info/res.php?e=305&o=p&t=999&r=e&n=999999&i=300'

pd.read_html(entry_list_url)[0][:-1]
```

Three Shires - multiple loops:

- first loop:  https://rallies.info/res.php?e=305&o=p&t=21&r=s&i=300&n=999999
- second loop: https://rallies.info/res.php?e=305&o=p&t=43&r=s&i=300&n=999999
- third loop:  https://rallies.info/res.php?e=305&o=p&t=65&r=s&i=300&n=999999

The `t=` parameter seems to be the one to set. Can we get more about this from an itinerary?

```python
stage_times_url = 'https://www.rallies.info/res.php?e=296'
stage_times_url = 'https://www.rallies.info/res.php?e=305'
```

```python
df = pd.read_html(stage_times_url)[0]
df
```

```python
df = df[~df['Car'].str.startswith('Car')]
df
```

```python
df.columns
```

```python
df[[str(i) for i in range(1,13)]+['Penalty','Total']].applymap(getTime2)
```

A table on each loop page provides information about the stage name / sponsor. (A better scraper is really required.)

```python
pd.read_html('https://rallies.info/res.php?e=305&o=p&t=65&r=s&i=300&n=999999')[2]
```

```python
#https://github.com/r1chardj0n3s/parse
from parse import parse
p = parse("Stage {:d}{} {:f} miles", "Stage 13SVP Motorsport 31.47 miles")
p[0], p[1], p[2]
```

```python
#Preferred time format
def formatTime(t):
    return float("%.3f" % t)

# Accept times in the form of hh:mm:ss.ss or mm:ss.ss
# Return the equivalent number of seconds and milliseconds
def getTime(ts, ms=False):
    ts=str(ts)
    t=ts.strip()
    if t=='': return pd.to_datetime('')
    if ts=='P': return None
    if 'LAP'.lower() in ts.lower():
        ts=str(1000*int(ts.split(' ')[0]))
    t=ts.split(':')
    if len(t)==3:
        tm=3600*int(t[0])+60*int(t[1])+float(t[2])
    elif len(t)==2:
        tm=60*int(t[0])+float(t[1])
    else:
        tm=float(pd.to_numeric(t[0], errors='coerce'))
    if ms:
        #We can't cast a NaN as an int
        return float(1000*formatTime(tm))
    return float(formatTime(tm))
```

```python
getTime(''), getTime('oops'), getTime('1.2'), getTime('51.2'), getTime('1:51.2'), getTime('1:1:51.2')
```

```python
getTime(''), getTime('oops'), getTime('1.2', True), \
getTime('51.2', True), getTime('1:51.2', True), getTime('1:1:51.2', True)

```

```python

```

```python
pd.to_datetime('1:12.1',format=r'(%H:)(%m:)(%S)', exact=False)
```

```python
pd.isnull( pd.to_datetime('12.1', errors='coerce') )
```

```python

        
def getTime2(ts, ms=False, astime=False): 
    def formatTime(t):
        return float("%.3f" % t)
    
    def retNull(astime=False):
        if astime:
            return pd.to_datetime('')
        else:
            return None #What about np.nan?
    
    ts=str(ts).strip()
    if not ts:
        return retNull(astime)
    
    t=ts.split(':')
    if len(t)==3:
        tt = timedelta(hours=int(t[0]), minutes=int(t[1]), seconds=float(t[2]))
    elif len(t)==2:
        tt=timedelta(minutes=int(t[0]), seconds=float(t[1]))
    else:
        tt = pd.to_numeric(t[0], errors='coerce')
        if pd.isnull(tt):
            return retNull(astime)
        tt=timedelta(seconds=tt)
    
    if astime:
        return tt
    elif ms:
        return 1000*formatTime(tt.total_seconds())

    return formatTime(tt.total_seconds())
```

```python
ms=False
at=False
getTime2('',ms,at), getTime2('oops',ms,at), getTime2('1.2', ms,at), \
getTime2('51.2',ms, at), getTime2('1:51.2', ms,at), getTime2('1:1:51.2',ms, at)

```

```python
pd.NaN
```

```python
pd.DataFrame({'r':[None, None, 1.2, 51.2, 111.2, 3711.2]})
```

```python
from datetime import timedelta
hour=1
minute="2"
second="12.4"
tt = timedelta(hours=hour, minutes=minute, seconds=second)
tt
```

```python
tt.total_seconds()
```

```python

```
