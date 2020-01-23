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

# Dakar Rules

An experiment in using a rule based system to generate rally results fact statements.


Here's some set-up for working with my scraped Dakar results data.

```python
STAGE = 3

MAX = 10

setups = {'sunderland':{'v':'moto','b':3},
          'alo':{'v':'car','b':310},
          'sainz':{'v':'car','b':305},
          'attiyah':{'v':'car', 'b':300},
          'price':{'v':'moto','b':1}
         }

def get_setup(n):
    return setups[n]['v'],setups[n]['b']

VTYPE, REBASER = get_setup('price')
```

And the database handler itself...

```python
import sqlite3
from sqlite_utils import Database

dbname = 'dakar_2020.db'

conn = sqlite3.connect(dbname)
db = Database(conn)
```

The rules engine I'm going to use is [`durable_rules`](https://github.com/jruizgit/rules).

```python
#https://github.com/jruizgit/rules/blob/master/docs/py/reference.md
#%pip install durable_rules
from durable.lang import *
```

We'll also be using `pandas`...

```python
import pandas as pd
```

Let's grab a simple set of example rankings from the database, as a `pandas` dataframe...

```python
q=f"SELECT * FROM ranking WHERE VehicleType='{VTYPE}' AND Type='general' AND Stage={STAGE} AND Pos<={MAX}"
tmpq = pd.read_sql(q, conn).fillna(0)
tmpq.head(3)
```

The `inflect` package makes it easy to generate numner words from numerics... and a whole host of other things...

```python
#https://github.com/jazzband/inflect
import inflect

p = inflect.engine()
```

The following function is a simple handler for generating nice time strings...

```python
#BAsed on: https://stackoverflow.com/a/24542445/454773
intervals = (
    ('weeks', 604800),  # 60 * 60 * 24 * 7
    ('days', 86400),    # 60 * 60 * 24
    ('hours', 3600),    # 60 * 60
    ('minutes', 60),
    ('seconds', 1),
    )
    
def display_time(t, granularity=3,
                 sep=',', andword='and',
                 units = 'seconds', intify=True):
    """Take a time in seconds and return a sensible
        natural language interpretation of it."""
    def nl_join(l):
        if len(l)>2:
            return ', '.join(f'{l[:-1]} {andword} {str(l[-1])}')
        elif len(l)==2:
            return f' {andword} '.join(l)
        return l[0]
    
    result = []

    if intify:
        t=int(t)

    #Need better handle for arbitrary time strings
    #Perhaps parse into a timedelta object
    # and then generate NL string from that?
    if units=='seconds':
        for name, count in intervals:
            value = t // count
            if value:
                t -= value * count
                if value == 1:
                    name = name.rstrip('s')
                result.append("{} {}".format(value, name))

        return nl_join(result[:granularity])
```

I suspect there's a way of doing things "occasionally" via the rules engine, but at times it may be easier to have rules that create statements "occasionally" as part of the rule code. This adds variety to generated text.

The following functions help with that, returning strings probabilistically.

```python
import random

def sometimes(t, p=0.5):
    """Sometimes return a string passed to the function."""
    if random.random()>=p:
        return t
    return ''

def occasionally(t):
    """Sometimes return a string passed to the function."""
    return sometimes(t, p=0.2)

def rarely(t):
    """Rarely return a string passed to the function."""
    return sometimes(t, p=0.05)

def pickone_equally(l, prefix='', suffix=''):
    """Return an item from a list,
       selected at random with equal probability."""
    t = random.choice(l)
    if t:
        return f'{prefix}{t}{suffix}'
    return suffix

def pickfirst_prob(l, p=0.5):
    """Select the first item in a list with the specified probability,
       else select an item, with equal probability, from the rest of the list."""
    if len(l)>1 and random.random() >= p:
        return random.choice(l[1:])
    return l[0]

```

Create a simple test ruleset for commenting on a simple results table.

Rather than printing out statements in each rule, as the demos show, lets instead append generated text elements to an ordered list, and then render that at the end.

(We could also return a tuple from a rule, eg `(POS, TXT)` that would allow us to re-order statements when generating the final text rendering.)

```python run_control={"marked": false}
from durable.lang import *

txts = []

with ruleset('test1'):
    
    #Display something about the crew in first place
    @when_all(m.Pos == 1)
    def whos_in_first(c):
        """Generate a sentence to report on the first placed vehicle."""
        #We can add additional state, accessiblr from other rules
        #In this case, record the Crew and Brand for the first placed crew
        c.s.first_crew = c.m.Crew
        c.s.first_brand = c.m.Brand
        
        #Python f-strings make it easy to generate text sentences that include data elements
        txts.append(f'{c.m.Crew} were in first in their {c.m.Brand} with a time of {c.m.Time_raw}.')
    
    #This just checks whether we get multiple rule fires...
    @when_all(m.Pos == 1)
    def whos_in_first2(c):
        txts.append('we got another first...')
        
    #We can be a bit more creative in the other results
    @when_all(m.Pos>1)
    def whos_where(c):
        """Generate a sentence to describe the position of each other placed vehicle."""
        
        #Use the inflect package to natural language textify position numbers...
        nth = p.number_to_words(p.ordinal(c.m.Pos))
        
        #Use various probabalistic text generators to make a comment for each other result
        first_opts = [c.s.first_crew, 'the stage winner']
        if c.m.Brand==c.s.first_brand:
            first_opts.append(f'the first placed {c.m.Brand}')
        t = pickone_equally([f'with a time of {c.m.Time_raw}',
                             f'{sometimes(f"{display_time(c.m.GapInS)} behind {pickone_equally(first_opts)}")}'],
                           prefix=', ')
        
        #And add even more variation possibilities into the returned generated sentence
        txts.append(f'{c.m.Crew} were in {nth}{sometimes(" position")}{sometimes(f" representing {c.m.Brand}")}{t}.')
    
```

The rules handler doesn't seem to like the `numpy` typed numerical objects that the `pandas` dataframe provides, but if we cast the dataframe values to JSON and then back to a Python `dict`, everything seems to work fine.

```python
type(tmpq.iloc[0].to_dict()['Pos'])
```

```python
tmpq[['Pos', 'Crew','Brand']].iloc[0].to_dict()
```

```python

post('test1',tmpq[['Pos', 'Crew','Brand']].iloc[0].to_dict())
txts
```

```python
import json
#This handles numpy types that ruleset json serialiser doesn't like
tmp = json.loads(tmpq.iloc[0].to_json())
```

If we post as an event, then only a single rule can be fired from it


```python
post('test1',tmp)
print(''.join(txts))
```

We can create a function that can be applied to each row of a `pandas` dataframe that will run the conents of the row through the ruleset:

```python
def rulesbyrow(row, ruleset):
    row = json.loads(json.dumps(row.to_dict()))
    post(ruleset,row)
```

Capture the text results generated from the ruleset into a list, and then display the results. 

```python
txts=[]
tmpq.apply(rulesbyrow, ruleset='test1', axis=1)

print('\n\n'.join(txts))
```

We can evaluate a whole set of events passed as list of events using the `post_batch(RULESET,EVENTS)` function. It's easy enough to convert a `pandas` dataframe into a list of palatable `dict`s... 

```python
def df_json(df):
    """Convert rows in a pandas dataframe to a JSON string.
       Cast the JSON string back to a list of dicts 
       that are palatable to the rules engine. 
    """
    return json.loads(df.to_json(orient='records'))
```

Unfortunately, the `post_batch()` route doesn't look like it necessarily commits the rows to the ruleset in the provided row order? (Has the `dict` lost its ordering?)

```python
txts=[]

post_batch('test1', df_json(tmpq))
print('\n\n'.join(txts))
```

We can also assert the rows as `facts` rather than running them through the ruleset as `events`.

```python
def factsbyrow(row, ruleset):
    row = json.loads(json.dumps(row.to_dict()))
    assert_fact(ruleset,row)
```

The fact is retained even it it matches a rule, so it gets a chance to match other rules too...

```python
txts=[]
tmpq.apply(factsbyrow, ruleset='test1', axis=1);
print('\n\n'.join(txts))
```

However, if we apply the same facts multiple times, I think we get an error and bork the ruleset...

```python
from durable.lang import *
with ruleset('flow4'):
    
    @when_all(m.action == 'start')
    def first(c):
        raise Exception('Unhandled Exception!')

    # when the exception property exists
    @when_all(+s.exception)
    def second(c):
        print(c.s.exception)
        c.s.exception = None
            
post('flow4', { 'action': 'start' })
post('flow4', { 'action': 'stop' })
post('flow4', { 'action': 'stops' })
```

```python
import pandas as pd
import numpy as np
df=pd.DataFrame({'intval':[1], 'strval':['a']})
df['intval'] = df['intval'].astype(np.int64)
df.dtypes
```

```python
type(df.iloc[0]['intval'])
```

```python
df.iloc[0].to_dict()
```

```python
from durable.lang import *
with ruleset('_npint_test'):
    
    @when_all(m.intval >0)
    def testint(c):
        print('works')
            

```

```python
post('_npint_test', df.iloc[0].to_dict())
```

```python
post('_npint_test', df[['intval']].iloc[0].to_dict())
```

```python
df.iloc[0].to_dict()
```

```python
df[['intval']].iloc[0].to_dict()
```

```python run_control={"marked": false}
from durable.lang import *

TEST = 'test819'

capture=[]
with ruleset(TEST):
    
    @when_all(m.test.matches('F[T]*'))
    def trailT(c):
        print('F[T]*',c.m.test)
        
    @when_all(m.test.matches('[TF]*[T]{3}'))
    def trail3T(c):
        print('[TF]*[T]{3}',c.m.test)
        
    @when_all(m.test.matches('.*[T]{4}'))
    def trail4T(c):
        print('[T]{4}', c.m.test)
        
    @when_all(m.test.matches('[TF]*T'))
    def shouldfail(c):
        print('[TF]*T', c.m.test)
        
    @when_all(m.test.matches('[TF]*T'))
    def finalt(c):
        print('[TF]*T',c.m.test)
        
        
    # when the exception property exists
    @when_all(m.test.matches('.*'))
    def catcher(c):
        print(f'missed {c.m.test}')

            

```

```python
from durable.engine import MessageNotHandledException

facts = [{'test':'FTFT'}, {'test':'FFFF'}, {'test':'FFFT'}, {'test':'FTTTT'}, 
         {'test2':[1,2,3,1]}]

for fact in facts:
    try:
        assert_fact(TEST, fact )
        retract_fact(TEST, fact )
    except MessageNotHandledException as error:
        pass
    
```

```python
"TTTT".count("T"), "TFTT".count("T")
```

```python
dir(capture[0])
```

```python
dir(capture[0].matches)
```

```python
capture.value
```

```python
capture[0].allItems
```

```python

```
