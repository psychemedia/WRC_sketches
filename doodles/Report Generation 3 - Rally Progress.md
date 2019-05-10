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

# Report Generation - Rally Progress

Generate a report for each section of a specified rally and each stage in each section.

The report text is appended to either pre-existing report files, or written to newly created report files.

The report generator will generate:

- section summary reports;
- stage reports.

It also arranges map items for embedding in the stage reports, although the map images are currently created elsewhere...

```python
import notebookimport
```

```python
#!pip3 install unidecode
```

```python
if __name__=='__main__':
    rebase='overallleader'
    #rebase='PAD'#'PAD'
    MAXINSPLITDELTA=20 #set xlim on the within split delta
```

```python
sr = __import__("Charts - Stage Results")
ssd = __import__("Charts - Split Sector Delta")
sp = __import__("Charts - Stage Progress")
ds = __import__("Report - Driver Splits")
```

```python
import tracery
from tracery.modifiers import base_english
from inflectenglish import inflect_english
from pytracery_logic import pytracery_logic
from IPython.display import HTML

def pandas_row_mapper(row, rules, root,  modifiers=base_english):
    ''' Function to parse single row of dataframe and apply a tracery grammar to it. '''
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
```

```python
#need to get db builder -
#eg http://localhost:8888/notebooks/Documents/code/github%20forks/WRC_sketches/doodles/WRC%20API%202018%20Base.ipynb
```

```python
import os
import sqlite3
import pandas as pd
import pytablewriter
import six

#dbname='wrc18.db'
#dbname='france18.db'
#conn = sqlite3.connect(dbname)

if __name__=='__main__':
    #dbname='wrc18.db'
    dbname='australia18.db'
    conn = sqlite3.connect(dbname)
    rally='Australia'
    rc='RC1'
    year=2018
    #ss='SS4'
```

```python
basepath = 'report'
imgdir = 'images'
imgdirfull = '{}/{}'.format(basepath, imgdir)

forceImages=False
```

```python
#rally='France'
```

```python
'''year=2018
rc='RC2'
ss='SS4'


typ='stage_times_stage' #stage_times_stage stage_times_overall
typ='stage_times_overall'''
```

```python
if __name__=='__main__':
    
    #There is an issue with enrichers - we need to check that the correct results are presented for enrichment
    #The combined results enricher is probably the safest to use...
    stageresults = sr.dbGetRallyStages(conn, rally).sort_values('number')

    #The getEnrichedStageRank query filters down to a class - default is RC1
    stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall')

    stagerank_stage = sr.getEnrichedStageRank(conn, rally, typ='stage')
```

This is a simple example of the sort of report string we want to report against.

# ```python
txt='In stage {stage}, {lead} took the stage, {nthstagewin}, in a time of {time}.\n\nIn second place on stage, {diff} behind, {second} retained overall lead of the rally.'.format(**row)
# ```

```python
stagerank_stage.iloc[10]
```

```
import inflect
p = inflect.engine()

def timedelta_to_text(td): 
    if not td: return
    c=pd.to_timedelta(td).components
    txt=[]
    if c.hours>0:
        txt_.append(p.number_to_words(c.hours)+p.plural('hours',c.hours))
    if c.minutes>0:
        txt.append('{} {}'.format(p.number_to_words(c.minutes),p.plural('minute',c.minutes)))
    if c.seconds>0:
        seconds=c.seconds
        if c.milliseconds>0:
            seconds=seconds+c.milliseconds/1000
        txt.append('{} {}'.format(p.number_to_words(seconds),p.plural('second',seconds)))
    return p.join(txt, final_sep="and")

def timedelta_to_numtext(td):
    if not td: return
    c=pd.to_timedelta(td).components
    txt=[]
    if c.hours>0:
        txt.append('{}{}'.format(c.hours,p.plural('hr',c.hours)))
    if c.minutes>0:
        txt.append('{}m'.format(c.minutes))
    if c.seconds>0:
        seconds=c.seconds
        if c.milliseconds>0:
            seconds=round(seconds+c.milliseconds/1000, 2)
        txt.append('{}s'.format(seconds))

    return p.join(txt,conj='')
```

```
_code='SS5'
typ=stagerank_stage
typ['elapsedDuration_txt'] =typ['elapsedDuration'].apply(timedelta_to_numtext)
typ['diffFirst_txt'] =typ['diffFirst'].apply(timedelta_to_numtext)
_srs=typ[(typ['code']==_code) & (typ['position']<=3)][['code','position','firstinarow','elapsedDuration',
                                                    'elapsedDuration_txt', 'stagewin','diffFirst_txt',
                                                                                         'stagewincount',
                                                                                         'drivercode']]
_srs.columns=[c.replace('.','') for c in _srs.columns]
_srs.to_dict(orient='record')
```

```
rules = {'onetwo_':'[branch:#_onetwo_#]#branch##condition#',
         '_onetwo_':'#stagewin._branch(True,stagewinner_,second)# ',
         'y':'[switch:#sval#]#switch##condition#',
         'yy':'#drivercode# [switch:#position.int_to_words.ordinal._prefix(stage)._switch#]#switch##condition#',
         'sval':'#position.int_to_words.ordinal._prefix(stage)._switch#',
         'stagefirst':'in first',
         'stagethird':'took third',
         'stagewinner_': "#drivercode# #won_# stage #code# #inwith_# a time of #elapsedDuration_txt#.",
         'won_':['won', 'took'],
         'inwith_':['in', 'with'],
        'stagesecond':'took second, #diffFirst_txt# behind.',
        'z':'#position._gte(1,stagewinner)#',
        'toptwo_':'[switch:#_toptwo_#]#switch##condition#','_toptwo_':'#position._gte(2,onetwo_)#',
        'null':''}

#dd.columns=[c.replace('.','') for c in dd.columns]
#dd['report'] = dd.apply(lambda row: pandas_row_mapper(row, rules, "#origin#"), axis=1)
#for k in row:
#    rules[k] = str(row[k])

#grammar = tracery.Grammar(rules)
#grammar.add_modifiers(inflect_english)
#grammar.flatten('stagewinner')
_srs.apply(lambda row: pandas_row_mapper(row, rules, "#y#",[base_english,inflect_english,pytracery_logic]), axis=1)
```

```
_srs
```

```
_srs.apply(lambda row: pandas_row_mapper(row, rules, "#yy#",[base_english,inflect_english,pytracery_logic]), axis=1)

```

```
typ=stagerank_overall
_sro=typ[(typ['code']==_code) & (typ['stagewin'])][['code','position','winsinarow',
                                                                                         'stagewin',
                                                                                         'stagewincount',
                                                                                         'retainedOverallLead',
                                                                                         'drivercode']]

_sro.columns=[c.replace('.','') for c in _sro.columns]
_sro.to_dict(orient='record')
```

```python
sections = stageresults.groupby('section')

#Generate list from group: stageresults.groupby('section')['code'].apply(list)

#Generate group then iterate through groups
for key in sections.groups.keys():
    print(key, sections.get_group(key)['code'].tolist())

```

```python
sectionREADME_base = '''\n### {section} Report
'''
```

## Section Reports

Can these be limited to e.g. just RC1?

TO DO - need something if the section is a single stage - not s split chart, just a spot chart?

```python
cols=['diffFirst', 'diffFirstMs', 'diffPrev', 'diffPrevMs', 'elapsedDuration',
       'elapsedDurationMs', 'entryId', 'position', 'source', 'stageId',
       'stageTimeId', 'status', 'class', 'code', 'distance', 'name', 'snum',
       'drivercode', 'entrant.name', 'classrank', 'gainedClassPos',
       'gainedClassLead', 'classPosDiff', 'lostClassLead', 'retainedClassLead',
       'gainedOverallPos', 'gainedOverallLead', 'overallPosDiff',
       'lostOverallLead', 'retainedOverallLead', 'stagewin', 'stagewincount',
       'firstinarow', 'gainedTime']
cols=['drivercode','entrant.name','elapsedDuration','position','classrank' ,'diffFirst', 'diffPrev']

#stagerank_stage[stagerank_stage['code']=='SS9'][cols].sort_values('position')
```

```python
q='''
    SELECT itc.code, itc.status
    FROM itinerary_controls itc
    JOIN championship_events ce ON itc.eventId=ce.eventId
    JOIN itinerary_sections isc ON itc.`itinerarySections.itinerarySectionId`=isc.itinerarySectionId
    JOIN itinerary_legs il ON isc.itineraryLegId=il.itineraryLegId
    WHERE ce.`country.name`="{rally}" AND itc.code LIKE "SS%" 
    '''.format(rally=rally)
stage_status=pd.read_sql(q,conn).set_index('code').to_dict(orient='index')
stage_status
```

```python
%%capture
if not os.path.exists(imgdirfull):
    os.makedirs(imgdirfull)


#Maybe have a dataframe styling / formatting / output notebook?
def _markdown_table_write(tmp_df, prefix='\n\n'):
    ''' Write out pandas dataframe as a markdown table. '''
    writer = pytablewriter.MarkdownTableWriter()
    writer.stream = six.StringIO()
    
    #Drop drivers without a position
    writer.from_dataframe(tmp_df)
    writer.write_table()
    return prefix+writer.stream.getvalue()


def get_overall_results_table(stagerank_overall,ss):
    ''' Retrieve overall rank data and return it as a markdown formatted table. '''
    
    cols=cols=['drivercode','entrant.name','totalTime','position','classrank' ,'diffFirst', 'diffPrev']
    tmp_df = stagerank_overall[stagerank_overall['code']==ss][cols].sort_values('position')

    tmp_df.columns=['Driver','Team','Elapsed Duration','Position','Class Rank' ,'diffFirst', 'diffPrev']  
    
    #Hack the markdown to get rid of the trailing zeroes in the millisecond representation
    return _markdown_table_write(tmp_df.dropna(subset=['Position'])).replace('000000','')


def get_stage_results_table(stagerank_stage,ss):
    ''' Retrieve stage rank data and return it as a markdown formatted table. '''
    
    cols=['drivercode','entrant.name','elapsedDuration','position','classrank' ,'diffFirst', 'diffPrev']
    tmp_df = stagerank_stage[stagerank_stage['code']==ss][cols].sort_values('position')
    tmp_df.columns=['Driver','Team','Elapsed Duration','Position','Class Rank' ,'diffFirst', 'diffPrev']  
    
    #Hack the markdown to get rid of the trailing zeroes in the millisecond representation
    return _markdown_table_write(tmp_df.dropna(subset=['Position'])).replace('000000','')
    #writer = pytablewriter.MarkdownTableWriter()
    #writer.stream = six.StringIO()
    
    ##Drop drivers without a position
    #writer.from_dataframe(tmp_df.dropna(subset=['Position']))
    #writer.write_table()
    #return '\n\n'+writer.stream.getvalue()

    
def sectionRankingChart(data, title, sectionREADME, key, sectionStages, fn_core, deltalabels=True, elaborate=True):
    ''' Generate section ranking chart, save as image file, and add embedded image link to sectionREADME text. '''
    data = sr.stageOverallEnrichers(data)
    ss = data[data['code'].isin(sectionStages)].copy()
    ss = sr.stageLegEnrichers(ss,sectionStages)
    if ss.empty or ss.dropna(subset=['diffFirst']).empty: return sectionREADME
    fn='{}/{}'.format(imgdirfull, fn_core)

    fig, ax = sr.plotStageProgressionChart( ss, linecolor='lightgrey' , deltalabels=deltalabels, elaborate=elaborate,
                                        progress=True, stageAnnotate=False, filename=fn, title = title); 
    if fig is not None:
        fn='{}/{}'.format(imgdir, fn_core)
        sectionREADME = '''\n\n{s}{key}\n\n![]({img})\n'''.format(s=sectionREADME, key=key,img=fn)
    return sectionREADME


def _sectionStagesReport(sections, stagerank_overall, stagerank_stage, basepath, rebase='overall'):
    ''' Generate stage sections reports. '''
    
    #TO DO - these aren't yet available as rebased reports, if that event makes sense?
    #The table could be rebased
    dn ='_'+rebase if rebase and 'overall' not in rebase else ''
        
    for key in sections.groups.keys():
        #path = '{}/{}'.format(basepath,key)
        #if not os.path.exists(path):
        #    os.makedirs(path)

        out_file_name = '{bp}/{key}_report{dn}.md'.format(bp=basepath, key=key, dn=dn)

        sectionStages = sections.get_group(key)['code'].tolist()
        print('Section stages: {}'.format(sectionStages))
        #Check section stages - if TORUN, then break
        
        sectionREADME = sectionREADME_base.format(section=key)
        
        #Stage rank chart
        fn_core='spchart_stage_{}.png'.format( key)
        title = '{} - Stage Ranking'.format(key)
        sectionREADME = sectionRankingChart(stagerank_stage, title, sectionREADME,
                                            key, sectionStages, fn_core, deltalabels=False, elaborate=False)

        #Overall rally ranking chart
        fn_core='spchart_overall_{}.png'.format( key)
        title = '{} - Overall Rally Ranking Evolution'.format(key)
        sectionREADME = sectionRankingChart(stagerank_overall, title, sectionREADME,
                                            key, sectionStages, fn_core, deltalabels=True, elaborate=True )

        #Display results as table
        # TO DO - Need to do something about stage vs rally overall result?
        for stage in sectionStages:
            if stage_status[stage]['status']=='ToRun': break
            #print(stage)
            stage_result_table = get_stage_results_table(stagerank_stage,stage)
            sectionREADME = '{}\n\n##Stage Result - {}\n\n{}\n\n'.format(sectionREADME, stage, stage_result_table)

            #TO DO - do we need to over the ability to rebase this table?
            overall_result_table = get_overall_results_table(stagerank_overall,stage)
            sectionREADME = '{}\n\n##Overall Result - {}\n\n{}\n\n'.format(sectionREADME, stage, overall_result_table)

            

        with open(out_file_name, 'a') as out_file:
            out_file.write(sectionREADME)

def sectionStagesReport(conn, rally, basepath, rebase='overall'):
    ''' Generate a report summarising stage activity within a section. '''
    stagerank_overall = sr.getEnrichedStageRank(conn, rally, typ='overall')
    stagerank_stage = sr.getEnrichedStageRank(conn, rally, typ='stage')
    stageresults = sr.dbGetRallyStages(conn, rally).sort_values('number')
    sections = stageresults.groupby('section')

    _sectionStagesReport(sections, stagerank_overall, stagerank_stage, basepath, rebase)

sectionStagesReport(conn, rally, basepath, rebase=rebase)#'overall')
```

### Stage progression chart

```python
%%capture

#This is a step in the creation of output files used to generate the rally report


#README.md is created in step 2 - Itinerary Basics
# Generate the stage progression chart for the extent of the rally to date
fn='{}/spchart_full.png'.format(imgdirfull)
fig, ax = sr.plotStageProgressionChart( stagerank_overall, linecolor='lightgrey' , deltalabels=True, 
                                        progress=True, stageAnnotate=False, filename=fn);


#what do we need to add to allow stageannotate to be set True?
#fn='{}/spchart_full2.png'.format(imgdirfull)
#fig, ax = sr.plotStageProgressionChart( stagerank_overall, 
#                                        progress=True, stageAnnotate=True, filename=fn);

#Need to find a way to check we don't add the link repeatedly to the file
#if 'spchart_full.png' not in open('{}/README.md'.format(basepath)).read()
README='''\n![]({})'''.format('{}/spchart_full.png'.format(imgdir))
with open('{}/README.md'.format(basepath), 'a') as out_file:
    out_file.write(README)
```

```python
!ls report
```

## Stage Reports

```python
#overallleader = stagerank_overall[stagerank_overall['position']==1].set_index('code')[['drivercode']].to_dict(orient='dict')['drivercode']
#overallleader
```

```python
if __name__=='__main__':
    sections.groups.keys()
```

```python
if __name__=='__main__':
    #Look for maps
    #Quick sketch to see what maps are available and what stages they refer to
    #The maps are pulled from http://localhost:8888/notebooks/Documents/GitHub/wrcplus/notebooks/Map%20KML%20etc.ipynb
    # TO DO: bring stage map capture creation tools into this workflow
    maps={}
    for i in os.listdir('report/maps'):
        maps[i] = i.split('.')[0].replace('0','').replace('-','-SS').replace('SSS','SS')
    smaps = {}
    for m in maps:
        for i in maps[m].split('-'):
            smaps[i] = m
    smaps
```

```python
#stagecodes
```

## Chart Interrogator

The chart interrogator is a first pass at trying to create obersvational prompts to help a user read a chart and gain insight from it.

The intention is to develop interrogator lines of inquiry that are useful as a support for active reading of a chart, whilst also acting as a stepping stone towards automating particular lines of analysis.

```python
chartinterrogator={}
chartinterrogator['stageprogresschart']= \
'''
The stage progress chart shows times rebased relative to the specified driver and highlights:

- the overall gap relative to other drivers at the start of the stage;
- the time gained / lost relative to other drivers on the stage;
- the overall gap relative to other drivers at the end of the stage.
'''

chartinterrogator['splitdeltaoverallchart']= \
'''
Relative to the specified driver, show the overall stage time delta at each split for each of the other drivers.

- does the target driver appear to be consistently losing or gaining time compared to another driver?
'''

chartinterrogator['splitdeltawithinchart']= \
'''
Relative to the specified driver, show the delta for each split section relative for each of the other drivers.

- are there any splits on which the target driver seems to be consistently fast or slow compared to other drivers?
- are there any exceptionally fast or slow split sections for any of the drivers that might indicate a push or an incident?
'''

#chartinterrogator['splitdeltaoverchart']= \
#'''
#'''
```

```python
import matplotlib.pyplot as plt
import unidecode
import os.path

stagecodes = stageresults['code']

def stageReport(stagerank_overall,stagerank_stage, stagecodes, stage_status,
                rebase='overallleader', forceAll=False, basepath=basepath ):
    ''' Generate stage level reports and figures for use in stage reports.
        Reports components are saved to newly opened files with structure:
            {}/{}_report{}.md'.format(basepath, ss, dn)
        The dn component is a drivercode as specified by the rebase attribute.
        The filenames are also minted into a SUMMARY.md file in 'Report 2 - Itinerary Basics::initStageReports()''
    '''
    drivercodes = stagerank_overall['drivercode'].unique()
    links=[]
    overallleader = stagerank_overall[stagerank_overall['position']==1].set_index('code')[['drivercode']].to_dict(orient='dict')['drivercode']
    for ss in stagecodes:
        #Check section stages - if TORUN, then break
        if not forceAll and stage_status[ss]['status']=='ToRun': break

        print(ss)
        
        dn=''
        invert_colours=False
        if rebase=='overallleader':
            dc = overallleader[ss]
        elif rebase in drivercodes:
            dc = rebase
            dn = '_'+dc
            invert_colours=True
        else:
            dc = overallleader[ss]
            
        #Should we avoid SS1?
        #SS1 stage bars are borked - disable for now?
        #if ss=='SS1': continue
        currprevstagerank = sp.getCurrPrevStageRank(conn, rally, rc, ss)
        splitdurations = ssd.getSplitDurationsFromSplits(conn,rally,ss,rc)
        # TO DO - SS1 stage bars are borked - disable for now?
        #Need to sort them acc. to driver who ends up first at SS1 end?
        if ss=='SS1': splitdurations=None
            
        #Each stage has its own stage report document
        with open('{}/{}_report{}.md'.format(basepath, ss, dn), 'w') as f:
            #ADD STAGE MAP IMAGE TO REPORT
            if ss in smaps:
                f.write('# Stage Map - {rally}, {year} - SS{ss} - Relative Split Times\n\n'.format(year=year,rally=rally,
                                                                                  ss=str(ss).replace('SS','')))
                img='maps/{}'.format(smaps[ss])
                f.write('![]({})\n'.format(img))

            #for dc in currprevstagerank['drivercode'].unique():
            
            splits = ssd.dbGetSplits(conn,rally,ss,rc)
            #If we have split times for this stage, report on them
            
            # The stage only report is getDriverStageReport
            #However, the following should show splits if it can, degrade if not...
            #previous shows how the stage lead to evolution of the overall lead
            #Only bother if we don't already have the image available: '{}/{}.png'.format(path,fn)
            
            #TO DO: Would it be an idea to create simple functions for creating images
            #  with a cache guard such as forceImages? i.e. if file exists don't bother
            # Would it also make sense to probe an image creator for the filename that would be created?
            fnstub_ds = 'driver_stagesplits_table_{ss}_{dc}'.format(ss=ss,dc=unidecode.unidecode(dc))
            if forceImages or not os.path.exists('{}/{}/{}.png'.format(basepath,imgdir,fnstub_ds)):
                s2 = ds.getDriverSplitsReport(conn, rally, ss, dc, rc, typ='overall',
                                             order='previous', caption='Rally Australia {} - {}'.format(ss, dc))
                #order: overall | previous | roadpos | stage
                _img=ds.getTablePNG(s2,basepath=basepath,
                                    path=imgdir,
                                    fnstub=fnstub_ds)
                f.write('\n![]({})\n'.format(_img))
                
            if splits is not None and not splits.empty:
                
                #TABLE IMAGE - borked atm if there are no splits
                #s2 = ds.getDriverSplitsReport(conn, rally, ss, dc, rc, 'overall',
                 #                             order='previous', caption='Rally Australia {} - {}'.format(ss, dc))
                                              #order: overall | previous | roadpos
                #_img=ds.getTablePNG(s2,basepath=basepath,
                #                    path=imgdir,
                #                    fnstub='driver_splits_table_{ss}_{dc}'.format(ss=ss,dc=unidecode.unidecode(dc)))
                #f.write('\n![]({})\n'.format(_img))

                #print(splits)
                elapseddurations=ssd.getElapsedDurations(splits)
                try:
                    if elapseddurations is not None and not elapseddurations.empty:
                        fig, ax = ssd.plotSplitOverallDelta(elapseddurations,dc,invert_colours=invert_colours,xmin=-180, xmax=180)
                        img='{}/stage_report_split_delta_{}_{}.png'.format(imgdir,str(ss).replace('SS',''), unidecode.unidecode(dc))
                        fig.savefig('{}/{}'.format(basepath,img))   # save the figure to file
                        plt.close(fig)  
                        f.write('# Stage Overall Split Delta Chart - {rally}, {year} - {dc} - SS{ss}\n\n'.format(year=year,rally=rally,
                                                                                              dc=dc,
                                                                                              ss=str(ss).replace('SS','')))
                        f.write(chartinterrogator['splitdeltaoverallchart'])
                        f.write('![]({})\n'.format(img))
                except:
                    print("Error with Overall Split Delta Chart {}".format(ss))

                #try:
                splitdurations = ssd.getSplitDurationsFromSplits(conn,rally,ss,rc)
                if splitdurations is not None and not splitdurations.empty:
                    fig, ax = ssd.plotSplitSectionDelta(splitdurations,dc,invert_colours=invert_colours, maxdelta=MAXINSPLITDELTA,
                                                        xmin=-180, xmax=180,
                                                        title='Delta within each split - {} - {}'.format(str(ss),unidecode.unidecode(dc)))
                    img='{}/stage_report_individual_split_delta_{}_{}.png'.format(imgdir,str(ss).replace('SS',''), unidecode.unidecode(dc))
                    fig.savefig('{}/{}'.format(basepath,img))   # save the figure to file
                    plt.close(fig)  
                    f.write('# Stage Within Split Delta Chart - {rally}, {year} - {dc} - SS{ss}\n\n'.format(year=year,rally=rally,
                                                                                          dc=dc,
                                                                                          ss=str(ss).replace('SS','')))
                    f.write(chartinterrogator['splitdeltawithinchart'])
                    f.write('![]({})\n'.format(img))
                #except:
                #    print("Error with Within Split Delta Chart {}".format(ss))

            fig, ax = sp.rebaseStageProgressBar(currprevstagerank, dc, splitdurations=splitdurations,
                                    neg='lightgreen',pos='pink',invert_colours=invert_colours,xmin=-180, xmax=180,
                         title='Stage Progress Chart - {rally}, {year} - SS{ss}'.format(year=year,
                                                                                        rally=rally,
                                                                                        ss=str(ss).replace('SS','')));
            img='{}/stage_report_{}_{}.png'.format(imgdir,str(ss).replace('SS',''), unidecode.unidecode(dc))
            fig.savefig('{}/{}'.format(basepath,img))   # save the figure to file
            plt.close(fig)  
            f.write('# Stage Progress Chart - {rally}, {year} - {dc} - SS{ss}\n\n'.format(year=year,rally=rally,
                                                                                  dc=dc,
                                                                                  ss=str(ss).replace('SS','')))
            explainer = chartinterrogator['stageprogresschart']
            f.write(explainer)
            f.write('![]({})\n'.format(img))

            
            stage_result_table = get_stage_results_table(stagerank_stage,ss)
            f.write(stage_result_table)
            
            #TO DO - there is no sense of the overall standing at the end of the stage
            #Need to add an overall standing / results table on each stage report
            
            
            #Can we do a rich table into gitbook?
            #Or save as an image?
            #Or embed as an iframe?
            #s2 = ds.getDriverSplitsReport(conn, rally, 'SS12', 'LAT', rc, 'overall')
            #f.write(s2)

                
#stageReport(stagerank_overall, stagecodes, stage_status, rebase='overallleader'  )
#stageReport(stagerank_overall, stagecodes, stage_status, rebase='PAD'  )

stageReport(stagerank_overall, stagerank_stage, stagecodes, stage_status, rebase=  rebase)
#stageReport(stagerank_overall, stagerank_stage, ['SS17','SS18'], stage_status)

```

```python
stagerank_overall['code'].unique()
```

```python
ssd.dbGetSplits(conn,rally,'SS10',rc='RC1').empty
#ssd.getSplitDurationsFromSplits(conn,rally,'SS9',rc)
```

```python
#!/Users/ajh59/anaconda3/bin/pip install pytablewriter
```

```python
#import pytablewriter

#writer = pytablewriter.MarkdownTableWriter()
#writer.from_dataframe(splitdurations.head())
#writer.write_table()
```

```python
#In reports directory
!(cd report/; rm -rf docs; gitbook build;  mv _book docs)

#gitbook serve
```

```python
!ls report/docs
```

```python
df = sr.dbGetStageStart(conn, rally, rc, stages=14)
df
```

```
#df.plot(kind='scatter',x='startpos',y='classrank')
import seaborn as sns; sns.set(color_codes=True)
sns.set_style("whitegrid",{"axes.edgecolor": "1"})

title='Time to stage winner vs Road Position'

df["diffFirstS"] = df["diffFirstMs"]/1000
yval='diffFirstS'
ax=sns.regplot(x="startpos", y=yval, data=df, fit_reg=False)
ax.set_title(title)

ax.set(xlabel='Road position (in class)', ylabel='Time to Class Stage Winner (s)')


ymax=20
ymax=  df[df["diffFirstS"]<=ymax]["diffFirstS"].max()+1 if df["diffFirstS"].max()>ymax else df["diffFirstS"].max() +1

ax.set_ylim(-0.5,ymax)
ax.xaxis.grid(False)

dftmp=df[df[yval]<=ymax]
for i,row in dftmp.iterrows():
     ax.text(row.startpos+0.2, row[yval], row['drivercode'],
             horizontalalignment='left', size='medium', color='black')
figure = ax.get_figure()  

figure.set_size_inches(8, 6)
  
figure.savefig('output.png')
```

```python
#test that the table report works; but how to embed it? as HTML iframe? Or image?
#getDriverSplitsReport(conn, rally, ss, drivercode, rc='RC1', typ='overall', order=None, caption=None)
ss='SS8'
s2 = ds.getDriverSplitsReport(conn, rally, ss, 'PAD', rc, 'overall',
                              order='overall', caption='Rally Australia {}'.format(ss))#order:overall | previous | roadpos
display(HTML(s2))
```

```python
#Grab file snapshot
ds.getTablePNG(s2)
```

```python

```

```python

```
