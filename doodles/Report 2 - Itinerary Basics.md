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

# Itinerary Basics

Basic report on stage itinerary - stage name, distance etc.

```python
import notebookimport
```

```python
import os
import sqlite3
import pandas as pd
```

```python
#!pip3 install tracery
#!pip3 install inflect
```

```python
if __name__=='__main__':
    #dbname='wrc18.db'
    YEAR=2019
    dbname='sweden19.db'
    conn = sqlite3.connect(dbname)
    rally='Sweden'
    rebase = 'LOE'
    rebase = ''
```

```python
#!/Users/ajh59/anaconda3/bin/pip install tracery
import tracery
from tracery.modifiers import base_english
from inflectenglish import inflect_english
from pytracery_logic import pytracery_logic

def pandas_row_mapper(row, rules, root,  modifiers=base_english):
    ''' Generate text from a single row of dataframe using a tracery grammar. '''
    row=row.to_dict()
    rules=rules.copy()

    for k in row:
        rules[k] = str(row[k])
        grammar = tracery.Grammar(rules)
        if modifiers is not None:
            if isinstance(modifiers,list):
                for modifier in modifiers:
                    grammar.add_modifiers(modifier)
            else:
                grammar.add_modifiers(modifiers)

    return grammar.flatten(root)

def traceryparse(rules, root, modifiers=base_english):
    ''' Create a tracery grammar from a rules set and set of modifiers and apply it to tracery template string. '''
    grammar = tracery.Grammar(rules)
    if modifiers is not None:
        if isinstance(modifiers,list):
            for modifier in modifiers:
                grammar.add_modifiers(modifier)
        else:
            grammar.add_modifiers(modifiers)
    
    return grammar.flatten(root)
    
```

## Rally Summary

```python
def dbGetRallySummary(rally, year=YEAR):
    ''' Create a dataframe containing summary details of a specified rally. '''
    q='''
    SELECT o.*, t.totalDistance FROM 
        (SELECT ce.`country.name`, ce.startDate, ce.finishDate, ce.name, ce.`country.iso3`, ce.surfaces,
            COUNT(*) numOfStages, SUM(itc.distance) AS compDistanceKm
            FROM itinerary_controls itc
            JOIN championship_events ce ON itc.eventId=ce.eventId
            WHERE ce.`country.name`="{rally}" AND type='StageStart' AND strftime('%Y', startDate)='{year}' ) AS o
        JOIN (SELECT ce.`country.name`, SUM(itc.distance) totalDistance FROM itinerary_controls itc
                JOIN championship_events ce ON itc.eventId=ce.eventId
                WHERE ce.`country.name`="{rally}") AS t ON o.`country.name` = t.`country.name`
    '''.format(rally=rally, year=year)
    
    rallydetails = pd.read_sql(q,conn)
    
    return rallydetails

```

```python
if __name__=='__main__':
    rs = dbGetRallySummary(rally)
    display(rs)
    display(rs.dtypes)
```

Tracery rules specify an emergent story template.

```python
rules = {'origin': "#name# (#startDate# to #finishDate#) #stages#. #dist#. #surface#.",
         'stages': "runs over #numOfStages# competitive special stages",
         'dist': "The distance covered on the special stages is #compDistanceKm.round#km, with an overall rally distance of #totalDistance.round#km",
         'surface':"The special stage surface type is predominantly #surfaces#",
        }
```

```python
if __name__=='__main__':
    rs['report'] = rs.apply(lambda row: pandas_row_mapper(row, rules, "#origin#",[base_english,inflect_english, pytracery_logic]), axis=1)
```

```python
if __name__=='__main__':
    rs[rs['country.name']==rally]['report'].iloc[0]
```

```python
basepath = 'report'
if not os.path.exists(basepath):
    os.makedirs(basepath)
```

```python
README='''# Rally Report - {}

*This report is unofficial and is not associated in any way with the Fédération Internationale de l’Automobile (FIA) or WRC Promoter GmbH.*


{}
'''.format(rally, rs[rs['country.name']==rally]['report'].iloc[0])

with open('{}/README.md'.format(basepath), 'w') as out_file:
    out_file.write(README)
```

```python
import inflect
p=inflect.engine()
```

```python
rs['startDate']=pd.to_datetime(rs['startDate'])
rs['startDate'].loc[0].strftime("%A %d,  %B %Y")
```

```python
p.number_to_words(p.ordinal((int(rs['startDate'].loc[0].strftime("%d")))))
```

## Itinerary Items

```python
q='''
    SELECT *
    FROM itinerary_controls itc
    JOIN championship_events ce ON itc.eventId=ce.eventId
    JOIN itinerary_sections isc ON itc.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
    JOIN itinerary_legs il ON isc.itineraryLegId=il.itineraryLegId
    WHERE ce.`country.name`="{rally}" AND strftime('%Y', startDate)='{year}' 
            AND firstCarDueDateTimeLocal NOT NULL ORDER BY firstCarDueDateTimeLocal 
    '''.format(rally=rally, year=YEAR)
xx =pd.read_sql(q,conn)
xx.columns
```

```python
xx.head()
```

```python
import datetime
t=datetime.time(abs(int(360/60)), 360 % 60)
dt = datetime.datetime.combine(datetime.date.today(), t)
dt.isoformat()
```

```python
t = datetime.datetime.combine(datetime.date.today(),datetime.time(0))
(t  + datetime.timedelta( minutes=-359)).isoformat()
```

```python
xx[:2][['timeZoneId','timeZoneOffset']]
```

```python
def dbGetTimeControls(rally, year=YEAR):
    ''' Get dataframe containing time control details for a specified rally. '''
    q='''
    SELECT il.name AS date, itc.*, ce.timeZoneOffset,
         isc.itinerarySectionId, isc.name AS section, isc.`order`
    FROM itinerary_controls itc
    JOIN championship_events ce ON itc.eventId=ce.eventId
    JOIN itinerary_sections isc ON itc.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
    JOIN itinerary_legs il ON isc.itineraryLegId=il.itineraryLegId
    WHERE ce.`country.name`="{rally}" AND strftime('%Y', startDate)='{year}'
            AND firstCarDueDateTimeLocal NOT NULL ORDER BY firstCarDueDateTimeLocal 
    '''.format(rally=rally, year=year)
    time_controls = pd.read_sql(q,conn)
    time_controls['firstCarDueDateTimeLocal']=pd.to_datetime(time_controls['firstCarDueDateTimeLocal'])
    return time_controls
```

```python
if __name__=='__main__':
    time_controls = dbGetTimeControls(rally)
    display(time_controls.head())
```

```python
#Check datetime type
time_controls['firstCarDueDateTime'] = pd.to_datetime(time_controls['firstCarDueDateTime'])
```

```python
import datetime

def newtime(row):
    t=datetime.timedelta( minutes=row['timeZoneOffset'])
    return row['firstCarDueDateTime']+ t

time_controls['mylocaltime'] = time_controls.apply(lambda row: newtime(row),axis=1)
```

```python
rules = {'origin': "#mylocaltime.pdtime(%H:%M:%S)# #code# #location# #distance.isNotNull(post=km).brackets# \[#targetDuration#\]",
        }
```

```python
dategroups = time_controls.groupby('date',sort=False)
for key in dategroups.groups.keys():
    print('---\n\n{}:\n'.format(key))
    grouped2=dategroups.get_group(key).groupby('section',sort=False)
    for key2 in grouped2.groups.keys():
        g2 = grouped2.get_group(key2)
        l=len(g2[g2['code'].str.startswith('SS')])
        print('{} - {} special {}\n'.format(key2,p.number_to_words(l),p.plural_noun('stage',l)))
        for r in grouped2.get_group(key2).apply(lambda row: pandas_row_mapper(row, rules, "#origin#",[base_english,inflect_english]), axis=1):
            print('\t{}'.format(r))
        print('\n')
```

```python
def initStageReports(rebase='overall'):
    ''' Generate the initial report for a particular section rebased to a particular position or driver.'''
    sectionREADME = '''### {section} Report
    '''

    SUMMARY ='\n'
    
    rules['stages'] = "#code# - #location# #distance.isNotNull(post=km).brackets#"

    dn = '' if not rebase or 'overall' in rebase else '_'+rebase
  
    sections = time_controls.groupby('section',sort=False)
    keyorder = [k for k in time_controls.sort_values('order')['section'].unique() if k in sections.groups.keys()]
    for key in keyorder:

        sectionfn = '{bp}/{key}_report{dn}.md'.format(bp=basepath,key=key, dn=dn)

        with open(sectionfn, 'w') as out_file:
            out_file.write('')

        sectionControls = sections.get_group(key)
        sstages = sectionControls[sectionControls['code'].str.startswith('SS')]
        l=len(sstages)
        
        if l:
            title = '# {}, {}\n\nThis section comprises {} special {}'.format(key,sectionControls['date'].iloc[0], p.number_to_words(l),p.plural_noun('stage',l))
            sstage=[]
            for r in sstages.apply(lambda row: pandas_row_mapper(row, rules, "#stages#",[base_english,inflect_english]), axis=1):
                sstage.append(r)
            sectionREADME = '{} ({})'.format(sectionREADME,', '.join(sstage))
        else: 
            title = '# {}, {}\n\nThere were no special stages in this section.'.format(key,sectionControls['date'].iloc[0])
        sectionREADME = '''{title}'''.format( title=title)

        sectionREADME = '''{s}\n\nThe full scheduled itinerary for the section was as follows:\n'''.format(s=sectionREADME,)

        controls = []
        for r in sectionControls.apply(lambda row: pandas_row_mapper(row, rules, "#origin#",[base_english,inflect_english]), axis=1):
            controls.append(r)
        sectionREADME = '{}\n\t- {}\n'.format(sectionREADME, '\n\t- '.join(controls))

        print(sectionREADME,'\n------\n')
        with open(sectionfn, 'a') as out_file:
            out_file.write(sectionREADME)

        #Add section
        SUMMARY = '{summary}\n* [{key}]({key}_report{dn}.md)\n'.format(summary=SUMMARY, key=key,dn=dn)

        sstageDict = sstages.set_index('code').to_dict(orient='index')
        #Add special stages
        for s in sstageDict: #[Section 2](Section 2_report.md)
            SUMMARY = '{summary}  - [{s} - {n}]({s}_report{dn}.md)\n'.format(summary=SUMMARY,
                                                                              s=s, 
                                                                              n=sstageDict[s]['location'],
                                                                              dn=dn)

    with open('{}/SUMMARY{}.md'.format(basepath,'_{}'.format(dn)), 'a') as out_file:
        out_file.write(SUMMARY)

initStageReports(rebase)
#initStageReports()
```

```python
sstages['date']
```

```python
sstages.set_index('code').to_dict(orient='index')
```

```python
print(SUMMARY)
```

```python
!head -n100 "report/Section 3_report.md"
```

```python
sectionControls['code']
```

```python
sectionControls#[sectionControls['code']]
```

```python
l
```

```python

```
