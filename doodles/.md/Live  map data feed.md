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

```python
#datagrab.py
import time
import requests
import json
 
import os

 
url='https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getData?timeout=5000'
d='times/raw'
 
if not os.path.exists(d):
    os.makedirs(d)

competing=True
while competing:
    r=requests.get(url)
    try:
        j=r.json()
    except: continue
    ts=j['timestamp']
    competing=False
    if '_entries' in j:
        for x in j['_entries']:
              if 'status' in  x and x['status']=='Competing': competing=True
    with open('{}/_{}.txt'.format(d,ts),'w') as outfile:
          json.dump(r.json(),outfile)
    time.sleep(2)
```

```python

```
