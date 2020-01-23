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
%load_ext autoreload
%autoreload 2
```

```python
import pandas as pd
import math
    
import dakar_utils as dakar
from dakar_utils import sparkline2, sparklineStep, \
                    pos1symbols, posgainLoss, \
                    moreStyleDriverSplitReportBaseDataframe
```

```python
import sqlite3
from sqlite_utils import Database

dbname = 'dakar_2020.db'

conn = sqlite3.connect(dbname)
db = Database(conn)

```

```python
from IPython.display import HTML
```

```python
def col_replacer(row, col, replace):
    return row[col].replace(row[replace],'').strip()
```

```python
def _rebaseTimes(times, bib=None):
    #If we don't have the bib number we really should raise a warning at least
    if bib is None or bib not in times.index: return times
    return times - times.loc[bib]
```

```python
REBASER=319
MAX = 20
YEAR=2020

setups = {'sunderland':{'v':'moto','b':3},
          'alo':{'v':'car','b':310},
          'sainz':{'v':'car','b':305},
          'attiyah':{'v':'car', 'b':300},
          'price':{'v':'moto','b':1},
          'peterh':{'v':'car','b':302},
          'trucks':{'v':'truck','b':511},
          'sanz':{'v':'moto','b':14},
          'coronel':{'v':'car', 'b':347},
         }

def get_setup(n):
    return setups[n]['v'],setups[n]['b']

VTYPE, REBASER = get_setup('coronel')
```

```python
STAGE=12
```

```python run_control={"marked": false}
q=f"SELECT Pos FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Bib={REBASER}"
_rebaser_pos = pd.read_sql(q, conn)['Pos'].iloc[0]
if MAX < _rebaser_pos:
    #Round up to nearest 5
    MAX=int(math.ceil(_rebaser_pos / 5.0) * 5)
MAX
```

```python
q=f"SELECT * FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Bib IN (SELECT Bib FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX})"
tmpq = pd.read_sql(q, conn).fillna(0)
tmpq = tmpq.pivot(index='Bib',columns='Stage',values='GapInS')
tmpq.columns = ['SS_{}_overall'.format(c) for c in tmpq.columns]

_gap_to_leader = tmpq[[c for c in tmpq.columns ]]
tmpq['GapToLeader'] = _gap_to_leader.values.tolist()
tmpq['GapToLeader'] = tmpq['GapToLeader'].apply(lambda x: [-y for y in x])
tmpq['GapToLeader'] = tmpq['GapToLeader'].apply(sparkline2, typ='bar')
tmpq
```

The following cell errors if the `Bib` is not in the selection.

```python
cols = [c for c in tmpq.columns if c.startswith('SS')]
tmpq[cols] = -tmpq[cols].apply(_rebaseTimes,bib=REBASER, axis=0)

tmpq['Gap']= tmpq[cols].values.tolist()
tmpq['Gap'] = tmpq['Gap'].apply(lambda x: [-y for y in x])
tmpq['Gap'] = tmpq['Gap'].apply(sparkline2, typ='bar')

tmpq.head()
```

```python
%%capture
q=f"SELECT * FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Bib IN (SELECT Bib FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX}) "
tmpq3 = pd.read_sql(q, conn)
tmpq3 = tmpq3.pivot(index='Bib',columns='Stage',values='Pos')
tmpq3['Overall']= tmpq3[[c for c in tmpq3.columns ]].values.tolist()
tmpq3['Overall'] = tmpq3['Overall'].apply(lambda x: [-y for y in x])
tmpq3['Overall'] = tmpq3['Overall'].apply(sparklineStep)
```

```python
tmpq3.head()
```

```python
q=f"SELECT * FROM ranking WHERE VehicleType='{VTYPE}' AND Type='stage' AND Bib IN (SELECT Bib FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX}) "
tmpq2 = pd.read_sql(q, conn).fillna(0)
tmpq2 = tmpq2.pivot(index='Bib',columns='Stage',values='GapInS')
tmpq2.columns = ['SS_{}'.format(c) for c in tmpq2.columns]
tmpq2 = -tmpq2.apply(_rebaseTimes,bib=REBASER, axis=0)
tmpq2.head()
```

```python
tmpq2.head()
```

```python
q=f"SELECT * FROM ranking WHERE VehicleType='{VTYPE}' AND Type='stage' AND Bib IN (SELECT Bib FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX}) "
tmpq5 = pd.read_sql(q, conn)

tmpq5 = tmpq5.pivot(index='Bib',columns='Stage',values='Pos')
tmpq5['Stage']= tmpq5[[c for c in tmpq5.columns ]].values.tolist()
tmp5_tmp = tmpq5['Stage'].apply(pos1symbols)
tmp5_tmp2 = tmpq5['Stage'].apply(posgainLoss)
posgainLoss
tmpq5['Stage'] = tmpq5['Stage'].apply(lambda x: [-y for y in x])
display(tmpq5['Stage'])
tmpq5['Stage'] = tmpq5['Stage'].apply(sparklineStep)
display(tmp5_tmp)
display(tmp5_tmp2)
```

```python
tmpq5.head()
```

```python
q=f"SELECT Bib, Pos, Crew, Brand FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX} ORDER BY Pos"
tmpq4 = pd.read_sql(q, conn).set_index('Bib')

tmpq4
```

```python
q=f"SELECT Bib, Team FROM teams WHERE Year={YEAR};"
TEAMS = pd.read_sql(q, conn).fillna(0).set_index('Bib')
tmpq4 = tmpq4.merge(TEAMS, left_index=True, right_index=True)
tmpq4['Crew'] = tmpq4.apply(col_replacer,col='Crew',replace='Team', axis=1)
tmpq4.columns = ['Pos','Crew','Team','Brand']
tmpq4
```

```python
tmpq4.head()
```

```python
tmpq3.dtypes
```

```python
results = pd.merge(tmpq4,tmpq3[['Overall']], left_index=True, right_index=True)
results = pd.merge(results,tmpq2, left_index=True, right_index=True)
results = pd.merge(results,tmpq5[['Stage']], left_index=True, right_index=True)
results = pd.merge(results,tmpq, left_index=True, right_index=True)
results.head()
#rb2cTop10.head(3)['test'].apply(sparkline2, typ='bar');
```

```python
dakar.moveColumn(results, 'GapToLeader', right_of='Stage')

```

```python
s2 = moreStyleDriverSplitReportBaseDataframe(results,'')
display(HTML(s2))
```

```python
if not MAX:
    TYP='All'
else:
    TYP=f'top_{MAX}'

img = dakar.getTablePNG(s2,fnstub=f'overall_SS{STAGE}_{VTYPE}_{REBASER}')
img
```

```python
from IPython.display import Image
Image(img)
```

Chart ranking across stage and overall for a particular driver

```python
import matplotlib.pyplot as plt

q=f'''
SELECT o.Bib, o.Stage SS, o.Pos Overall, s.Pos Stage FROM ranking o JOIN ranking s 
ON s.Bib=o.Bib AND s.Stage=o.Stage AND s.Type='stage' AND o.Type='general' AND s.Bib={REBASER}
WHERE o.Bib IN (SELECT Bib FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX})

'''

data = pd.read_sql(q, conn).set_index('Bib')
data
```

```python
fig, ax = plt.subplots(1, 1)

plt.axhspan(1, 3, facecolor='lightgrey', alpha=0.5)
ax.plot(data['SS'], [10]*len(data[['Stage','Overall']]), linestyle=':', color='lightgrey')
ax.step(data['SS'], data['Stage'], where='mid', linestyle=':', color='blue')
ax.step(data['SS'], data['Overall'], where='mid', color='orange', linestyle='--')

plt.gca().invert_yaxis()
```

```python
data.plot(x='SS',drawstyle="steps-mid",linestyle=':')
plt.gca().invert_yaxis()
```

```python
data
```

```python
import matplotlib.pyplot as plt
w=10
h=5
d=80

plt.figure(figsize=(w, h), dpi=d)
ax = plt.gca()

q=f"SELECT * FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Bib IN (SELECT Bib FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX} ORDER BY Pos LIMIT 5)"
tmpq = pd.read_sql(q, conn).fillna(0)
tmpq = tmpq.pivot(index='Bib',columns='Stage',values='GapInS')
tmpq.columns = ['SS_{}_overall'.format(c) for c in tmpq.columns]

_gap_to_leader = tmpq[[c for c in tmpq.columns ]]
_gap_to_leader.T.plot(ax=ax, title='Dakar 2020 — Cars — Top 5')

for i in range(5):
    plt.axvline(i, linestyle='--', color='lightgrey')

plt.ylabel('Gap to Leader (s)')

ax.invert_yaxis()
```

```python

```
