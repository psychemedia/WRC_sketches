# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # eWRC API
#
# Simple Python API for eWRC results.
#
# Initially built for pulling results at the end of a rally.
#
# What do we need to do to make it work live too, eg to force refesh on certain stages?

import pandas as pd
import re
from dakar_utils import getTime

import requests
import lxml.html as LH
from bs4 import BeautifulSoup
from bs4.element import NavigableString


# ## Generic Utilities
#
# Utility functions.

def soupify(url):
    """Load HTML form URL and make into soup."""
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml') # Parse the HTML as a string
    
    # Remove occasional tags that might appear
    # https://stackoverflow.com/a/40760750/454773
    unwanted = soup.find(id="donate-main")
    unwanted.extract()
    
    return soup


def no_children(node):
    """Extract just the text and no child nodes from a soup node."""
    #https://stackoverflow.com/a/31909680/454773
    text = ''.join([t for t in node.contents if type(t) == NavigableString])
    return text


def dfify(table):
    df = pd.read_html('<html><body>{}</body></html>'.format(table))[0]
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    return df


import unicodedata

def cleanString(s):
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace with single space
    s = ' '.join(s.split())
    
    return s


# ## Timing Utilities

def diffgapsplitter(col):
    #Normalise
    col=col.fillna('+0+0')
    #Remove leading +
    col=col.str.strip('+')
    #Split...
    col = col.str.split('+',expand=True)
    #Rename columns
    col = col.rename(columns={0:'Gap', 1:'Diff'})
    #Convert to numerics
    col['Gap'] = col['Gap'].apply(getTime)#.astype(float)
    col['Diff'] = col['Diff'].apply(getTime)
    return col


# ## Scraping Functions

base_url = 'https://www.ewrc-results.com'


def get_stage_result_links(stub):
    #If navigation remains constant, items are in third list
    
    rally_stage_results_url='https://www.ewrc-results.com/results/{stub}/'.format(stub=stub)
    
    links={}

    soup = soupify(rally_stage_results_url)
    for li in soup.find_all('ul')[2].find_all('li'):
        #if 'class' in li.attrs:
        #    print(li['class'])
        #A class is set for service but not other things
        if 'class' not in li.attrs:
            a = li.find('a')
            if 'href' in a.attrs:
                #links.append(a['href'])
                links[f'SS{a.text}'] = a['href']
                
    return links


# + tags=["active-ipynb"]
# #url='https://www.ewrc-results.com/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
# rally_stub = '54762-corbeau-seats-rally-tendring-clacton-2019'
# rally_stub='61961-mgj-engineering-brands-hatch-winter-stages-2020'
# rally_stub='59972-rallye-automobile-de-monte-carlo-2020'
# tmp = get_stage_result_links(rally_stub)
# tmp
# -

stage_result_cols = ['Pos', 'CarNum', 'Desc', 'Class', 'Time', 'GapDiff', 'Speedkm', 'Stage',
       'StageName', 'StageDist', 'Gap', 'Diff', 'Speed', 'Dist', 'entryId',
       'model', 'navigator', 'PosNum']

stage_overall_cols = ['PosChange', 'CarNum', 'Desc', 'Class', 'Time', 'GapDiff', 'Speedkm',
       'Stage', 'StageName', 'StageDist', 'Pos', 'Change', 'Gap', 'Diff',
       'Speed', 'Dist']

retirement_cols = ['CarNum', 'driverNav', 'Model', 'Status']
retirement_extra_cols = ['Driver', 'CoDriver', 'Stage']

penalty_cols = ['CarNum', 'driverNav', 'Model', 'PenReason']
penalty_extra_cols = ['Driver', 'CoDriver', 'Stage', 'Time','Reason']


from numpy import nan

from parse import parse   

# + tags=["active-ipynb"]
# details = 'SS16 La Cabanette - Col de Braus 2 [Power Stage] - 13.36 km - 26. 1. 12:18'
# #details = 'SS6 Curbans - Venterol 2 - 20.02 km - 24. 1. 13:54'
# pattern = 'SS{stage} {name} - {dist:f} km - {datetime}'
# parse(pattern, details)
# -

def get_stage_results(stub):
    soup = soupify('{}{}'.format(base_url, stub))

    details = soup.find('h4').text

    pattern = 'SS{stage} {name} - {dist:f} km - {datetime}'
    parse_result = parse(pattern, details)
    if parse_result is None:
        pattern = 'SS{stage} - {dist:f} km'
        parse_result = parse(pattern, details)
    print(details, parse_result)
    stage_num = f"SS{parse_result['stage']}"
  
    if 'name' in parse_result:
        stage_name = parse_result['name']
    else:
         stage_name = stage_num
    
    stage_dist =  parse_result['dist']
    if 'datetime' in parse_result:
        stage_datetime = parse_result['datetime']
    else:
        stage_datetime = None
    
    tables = soup.find_all('table')
    stage_result = tables[0]
    stage_overall = tables[1]
    
    # Stage Result
    df_stage_result = dfify(stage_result)
    df_stage_result.columns=['Pos','CarNum','Desc','Class', 'Time','GapDiff', 'Speedkm']
    
    df_stage_result['Stage'] = stage_num
    df_stage_result['StageName'] = stage_name
    df_stage_result['StageDist'] = stage_dist
    
    df_stage_result['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'})
    df_stage_result[['Gap','Diff']] = diffgapsplitter(df_stage_result['GapDiff'])
    df_stage_result[['Speed','Dist']] = df_stage_result['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)')
    
    rows=[]
    for d in stage_result.findAll("td", {"class": "stage-results-drivers"}):
        entryId = d.find('a')['href']
        #print(str(d)) #This gives us the raw HTML in the soup element
        driverNav = d.find('a').text.split('-')
        model=d.find('a').nextSibling.nextSibling
        rows.append( {'entryId':entryId,
                       'model':model,
                      'driver':cleanString(driverNav[0]),
                      'navigator':cleanString(driverNav[1])}) 

    df_stage_result[['driver','entryId','model','navigator']] = pd.DataFrame(rows)
    #Should we cast the Pos to a numeric too? Set = to na then ffill down?
    df_stage_result['PosNum'] = df_stage_result['Pos'].replace('=',nan).astype(float).fillna(method='ffill').astype(int)
    df_stage_result.set_index('driver',drop=True, inplace=True)
    
    # Stage Overall
    df_stage_overall = dfify(stage_overall)

    cols = ['PosChange', 'CarNum', 'Desc','Class', 'Time', 'GapDiff', 'Speedkm' ]
    df_stage_overall.columns = cols

    df_stage_overall['Stage'] = stage_num
    df_stage_overall['StageName'] = stage_name
    df_stage_overall['StageDist'] = stage_dist
    
    df_stage_overall[['Pos','Change']] = df_stage_overall['PosChange'].astype(str).str.extract(r'(?P<Pos>[\d]*)\.\s?(?P<Change>.*)?')
    df_stage_overall['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'})
    df_stage_overall[['Gap','Diff']] = diffgapsplitter(df_stage_overall['GapDiff'])
    df_stage_overall[['Speed','Dist']] = df_stage_overall['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)')

    
    # Retirements
    df_stage_retirements = pd.DataFrame(columns=retirement_cols+retirement_extra_cols)
    retired = soup.find('div',{'class':'retired-inc'})
    if retired:
        df_stage_retirements = dfify(retired.find('table'))
        df_stage_retirements.columns = retirement_cols
        df_stage_retirements[['Driver','CoDriver']] = df_stage_retirements['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')
        df_stage_retirements['Stage'] = stage_num
    
    # Penalties
    df_stage_penalties = pd.DataFrame(columns=penalty_cols+penalty_extra_cols)

    penalty = soup.find('div',{'class':'penalty-inc'})
    if penalty:
        df_stage_penalties = dfify(penalty.find('table'))
        df_stage_penalties.columns = penalty_cols
        df_stage_penalties[['Driver','CoDriver']] = df_stage_penalties['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')
        df_stage_penalties[['Time','Reason']] = df_stage_penalties['PenReason'].str.extract(r'(?P<Time>[^\s]*)\s+(?P<Reason>.*)')
        df_stage_penalties['Stage'] = stage_num
    
    return df_stage_result, df_stage_overall, df_stage_retirements, df_stage_penalties

# + tags=["active-ipynb"]
# partial_stub = '/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
# partial_stub='/results/42870-rallye-automobile-de-monte-carlo-2018/'
# partial_stub='/results/61961-mgj-engineering-brands-hatch-winter-stages-2020/'
# partial_stub='/results/59972-rallye-automobile-de-monte-carlo-2020/'
# #stub = tmp['SS3']
# stage_result, stage_overall, stage_retirements, stage_penalties = get_stage_results(partial_stub)

# + tags=["active-ipynb"]
# stage_result.head()

# + tags=["active-ipynb"]
# stage_overall

# + tags=["active-ipynb"]
# stage_retirements

# + tags=["active-ipynb"]
# stage_penalties
# -



from parse import parse

def check_time_str(txt):
    """Clean a time string."""
    
    # Quick fix to cope with strings of the form:
    # '11:58.0 <a name="" title="Notional time"><span class="c-blue">[N]</span></a>'
    # This should be a proper validator.
    txt = txt.split()[0]
    return txt


def get_stage_times(stub, dropnarow=True):
    url='https://www.ewrc-results.com/times/{stub}/'.format(stub=stub)

    soup = soupify(url)
    
    times = soup.find('div',{'class':'times'}).findChildren('div' , recursive=False)

    #The rows are essentially grouped in twos after the header row
    cols = [c.text for c in times[0].findAll('div')]
    
    groupsize=2
    groups = [times[i:i+groupsize] for i in range(1, len(times), groupsize)]
    
    NAME_SUBGROUP = 0
    TIME_SUBGROUP = 1
    
    carNumMatch = lambda txt: re.search('#(?P<carNum>[\d]*)', cleanString(txt))
    carModelMatch = lambda txt:  re.search('</a>\s*(?P<carModel>.*)</div>', cleanString(txt))
    
    pattern = '''<div class="times-one-time">{stagetime}<br/><span class="times-after">{overalltime}</span><br/>{pos}</div>'''
    
    t=[]
    i=0

    penaltypattern='class="r7_bold_red">{penalty}</span>'

    for g in groups:
        i=i+1
        driverNav_el = g[NAME_SUBGROUP].find('a')
        driverNav = driverNav_el.text
        driver,navigator = driverNav.split(' - ')
        entryId = driverNav_el['href']
        retired = '<span class="r8_bold_red">R</span>' in str(g[NAME_SUBGROUP])
        carNum = carNumMatch(g[NAME_SUBGROUP]).group('carNum')
        carModel = carModelMatch(g[NAME_SUBGROUP]).group('carModel')
        
        #TO DO - may be None?
        try:
            classification = pd.to_numeric(g[NAME_SUBGROUP].find('span').text.replace('R','').strip('').strip('.'))
        except:
            classification = ''
        
        stagetimes = []
        overalltimes = []
        penalties=[]
        positions = []

        for stages in g[TIME_SUBGROUP].findAll('div'):
            txt = cleanString(stages)
            stagetimes_data = parse(pattern, txt )
            if stagetimes_data:
                stagetimes.append(check_time_str(stagetimes_data['stagetime']))
                overalltimes.append(check_time_str(stagetimes_data['overalltime']))

                #Need to parse this
                #There may be penalties in the pos
                penalty = 0
                p = stagetimes_data['pos'].split()
                if p[-1].endswith('</span>'):
                    penalty = parse(penaltypattern, p[-1] )
                    if penalty:
                        #This really needs parsing into a time; currently of form eg 0:10
                        penalty = penalty['penalty']
                    # TO DO - what if we have a replacement? Error codes?
                    if '>R<' in p[-1]:
                        p = 0
                    else:
                        p = int(p[-2].split('.')[0])
                else:
                    p = int(p[-1].strip('.'))
                positions.append(p)
                penalties.append(penalty)
            # TO DO - how do we account for cancelled stages?
            # If we add in blanks we get balnks for un-run stages too?
            #else:
            #    stagetimes.append('')
            #    overalltimes.append('')
             #   positions.append('')
            #    penalties.append('')

        t.append({'entryId': entryId,
                  'driverNav': driverNav,
                  'driver': driver.strip(),
                  'navigator': navigator.strip(),
                  'carNum': carNum,
                  'carModel': carModel,
                  'retired': retired,
                  'Pos': classification,
                  'stagetimes': stagetimes,
                  'overalltimes': overalltimes,
                  'positions': positions,
                  'penalties': penalties})


    df_allInOne = pd.DataFrame(t).set_index(['entryId'])
    
    df_overall = pd.DataFrame(df_allInOne['overalltimes'].tolist(), index= df_allInOne.index)
    df_overall.columns = range(1, df_overall.shape[1]+1)
    
    df_overall_pos = pd.DataFrame(df_allInOne['positions'].tolist(), index= df_allInOne.index)
    df_overall_pos.columns = range(1, df_overall_pos.shape[1]+1)

    df_stages = pd.DataFrame(df_allInOne['stagetimes'].tolist(), index= df_allInOne.index)
    df_stages.columns = range(1, df_stages.shape[1]+1)
    
    df_stages_pos = df_stages.reset_index().drop_duplicates(subset='entryId').set_index('entryId').rank(method='min')
    df_stages_pos.columns = range(1, df_stages_pos.shape[1]+1)

    xcols = df_overall.columns

    for ss in xcols:
        df_overall[ss] = df_overall[ss].apply(getTime)
        df_stages[ss] = df_stages[ss].apply(getTime)

    # TO DO
    #We shouldn't really have to do this - why are there duplicates?
    #We seem to be appending rows over and over for each stage?
    df_allInOne = df_allInOne.reset_index().drop_duplicates(subset='entryId').set_index('entryId')
    df_overall = df_overall.reset_index().drop_duplicates(subset='entryId').set_index('entryId')
    df_stages = df_stages.reset_index().drop_duplicates(subset='entryId').set_index('entryId')
    df_overall_pos = df_overall_pos.reset_index().drop_duplicates(subset='entryId').set_index('entryId')
    
    if dropnarow:
        df_allInOne = df_allInOne.dropna(how='all', axis=1)
        df_overall = df_overall.dropna(how='all', axis=1)
        df_stages = df_stages.dropna(how='all', axis=1)
        df_overall_pos = df_overall_pos.dropna(how='all', axis=1)
        df_stages_pos = df_stages_pos.dropna(how='all', axis=1)
        
    return df_allInOne, df_overall, df_stages, df_overall_pos, df_stages_pos


# + tags=["active-ipynb"]
# url='https://www.ewrc-results.com/times/54762-corbeau-seats-rally-tendring-clacton-2019/'
# #url='https://www.ewrc-results.com/times/42870-rallye-automobile-de-monte-carlo-2018/'

# + tags=["active-ipynb"]
# rally_stub = '42870-rallye-automobile-de-monte-carlo-2018'
# rally_stub='61961-mgj-engineering-brands-hatch-winter-stages-2020'
# rally_stub='59972-rallye-automobile-de-monte-carlo-2020'
# rally_stub='60140-rally-sweden-2020'
#
# df_allInOne, df_overall, df_stages, \
#     df_overall_pos, df_stages_pos = get_stage_times(rally_stub)

# + tags=["active-ipynb"]
# display(df_allInOne)
# display(df_overall)
# display(df_stages)
# display(df_overall_pos)
# display(df_stages_pos)
# -

# ## Final Results
#
# Get overall rankings.

final_path = 'https://www.ewrc-results.com/final/{stub}/'


# + run_control={"marked": false}
def get_final(stub):
    soup = soupify(final_path.format(stub=stub))
    tables = soup.find_all('table')
    #tables = LH.fromstring(html).xpath('//table')
    df_rally_overall = pd.read_html('<html><body>{}</body></html>'.format(tables[0]))[0]
    #df_rally_overall['badge'] = [img.find('img')['src'] for img in tables[0].findAll("td", {"class": "final-results-icon"}) ]
    df_rally_overall.dropna(how='all', axis=1, inplace=True)
    df_rally_overall.columns=['Pos','CarNum','driverNav','ModelReg','Class', 'Time','GapDiff', 'Speedkm']

    #Get the entry ID - use this as the unique key
    #in column 3, <a title='Entry info and stats'>
    df_rally_overall['entryId']=[a['href'] for a in tables[0].findAll("a", {"title": "Entry info and stats"}) ]
    df_rally_overall.set_index('entryId', inplace=True)

    df_rally_overall[['Driver','CoDriver']] = df_rally_overall['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')

    df_rally_overall['Historic']= df_rally_overall['Class'].str.contains('Historic')
    df_rally_overall['Class']= df_rally_overall['Class'].str.replace('Historic','')

    df_rally_overall['Pos'] = df_rally_overall['Pos'].astype(str).str.extract(r'(.*)\.')
    df_rally_overall['Pos'] = df_rally_overall['Pos'].astype(int)

    df_rally_overall[['Model','Registration']]=df_rally_overall['ModelReg'].str.extract(r'(?P<Model>.*) \((?P<Registration>.*)\)')

    df_rally_overall["Class Rank"] = df_rally_overall.groupby("Class")["Pos"].rank(method='min')

    return df_rally_overall
    



# + tags=["active-ipynb"]
# get_final(rally_stub)
# -

# ## Itinerary

itinerary_path = 'https://www.ewrc-results.com/timetable/{stub}/'


def get_itinerary(stub):
    soup = soupify(itinerary_path.format(stub=stub))
    
    event_dist = soup.find('td',text='Event total').parent.find_all('td')[-1].text

    itinerary_df = dfify( soup.find('div', {'class':'timetable'}).find('table') )
    itinerary_df.columns = ['Stage','Name', 'distance', 'Date', 'Time']
    itinerary_df['Leg'] = [nan if 'leg' not in str(x) else str(x).replace('. leg','') for x in itinerary_df['Stage']]
    itinerary_df['Leg'] = itinerary_df['Leg'].fillna(method='ffill')
    itinerary_df['Date'] = itinerary_df['Date'].fillna(method='ffill')
    
    #What if we have no stage name?
    itinerary_df['Name'].fillna('', inplace=True)
    
    itinerary_leg_totals = itinerary_df[itinerary_df['Name'].str.contains("Leg total")][['Leg', 'distance']].reset_index(drop=True)

    full_itinerary_df = itinerary_df[~itinerary_df['Name'].str.contains(". leg")]
    full_itinerary_df = full_itinerary_df[~full_itinerary_df['Date'].str.contains(" km")]
    full_itinerary_df = full_itinerary_df.fillna(method='bfill', axis=1)

    #Legs may not be identified but we may want to identify services
    full_itinerary_df['Service'] = [ 'Service' in i for i in full_itinerary_df['distance'] ]
    full_itinerary_df['Service_Num'] = full_itinerary_df['Service'].cumsum()
    full_itinerary_df.reset_index(drop=True, inplace=True)
    #itinerary_df = full_itinerary_df[~full_itinerary_df['Service']].reset_index(drop=True)
    itinerary_df = full_itinerary_df[full_itinerary_df['Stage'].str.startswith('SS')].reset_index(drop=True)
    itinerary_df['Section'] = itinerary_df['Service_Num'].rank(method='dense')
    itinerary_df.drop(columns=['Service', 'Service_Num'], inplace=True)
    
    itinerary_df[['Distance', 'Distance_unit']] = itinerary_df['distance'].str.extract(r'(?P<Distance>[^\s]*)\s+(?P<Distance_unit>.*)?')
    itinerary_df['Distance'] = itinerary_df['Distance'].astype(float)

    itinerary_df.set_index('Stage', inplace=True)

    return event_dist, itinerary_leg_totals, itinerary_df, full_itinerary_df


# + tags=["active-ipynb"]
# stub='54762-corbeau-seats-rally-tendring-clacton-2019'
# stub='42870-rallye-automobile-de-monte-carlo-2018'
# #stub='61961-mgj-engineering-brands-hatch-winter-stages-2020'
# stub='59972-rallye-automobile-de-monte-carlo-2020'
# event_dist, itinerary_leg_totals, itinerary_df, full_itinerary_df = get_itinerary(stub)

# + tags=["active-ipynb"]
# print(event_dist)
# display(itinerary_leg_totals)
# display(itinerary_df)
# display(full_itinerary_df)

# + tags=["active-ipynb"]
# _tmp = itinerary_df['Distance']
# _tmp.index = [int(i.lstrip('SS')) for i in _tmp.index]
# _tmp
# -

# ## Entry List
#
# Get the entry list.

entrylist_path = "https://www.ewrc-results.com/entries/{stub}/"


def get_entry_list(stub):
    entrylist_url = entrylist_path.format(stub=stub)
    
    soup = soupify(entrylist_url)
    entrylist_table = soup.find('div',{'class':'startlist'}).find('table')
    df_entrylist = dfify(entrylist_table)
    
    base_cols = ['CarNum', 'DriverName','CoDriverName','Team','Car','Class']
    for i in range(len(df_entrylist.columns) - len(base_cols)):
        base_cols.append(f'Meta_{i}')
    df_entrylist.columns = base_cols
    df_entrylist['carNum'] = df_entrylist['CarNum'].str.extract(r'#(.*)')
    
    return df_entrylist


# + tags=["active-ipynb"]
# get_entry_list(stub)
# -

# ## Rebasers
#
# Utils for rebasing

def _rebaseTimes(times, bib=None, basetimes=None):
    ''' Rebase times relative to specified driver. '''
    #Should we rebase against entryId, so need to lool that up. In which case, leave index as entryId
    if bib is None and basetimes is None: return times
    #bibid = codes[codes['Code']==bib].index.tolist()[0]
    if bib is not None:
        return times - times.loc[bib]
    if times is not None:
        return times - basetimes
    return times


# ## `EWRC` Class
# Create a class to that can be used to gran all the results for a particular rally, as required.
#
# We fudge the definition of class functions so that we can separately define and functions in a standalione way. This is probably *not good practice*...!

class EWRC:
    """Class for eWRC data for a particular rally."""

    def __init__(self, stub, live=False):
        self.stub = stub
        self.live = live
        
        self.stage_result_links = None
        
        self.df_rally_overall = None
        
        self.df_allInOne = None #we don't actually use this?
        self.df_overall = None
        self.df_stages = None
        self.df_overall_pos = None
        self.df_stages_pos = None
        
        self.df_overall_rebased_to_leader = None
        self.df_stages_rebased_to_overall_leader = None
        self.df_stages_rebased_to_stage_winner = None
        
        self.event_dist = None
        self.df_itinerary_leg_totals = None
        self.df_itinerary = None
        self.df_full_itinerary =None

        self.stage_distances_all = None
        self.stage_distances = None
        
        self.df_entry_list = None
        self.rally_classes = None
        
        self.entryFromCar = None
        self.carFromEntry = None
 
        self.df_stage_result = pd.DataFrame(columns=stage_result_cols)
        self.df_stage_overall = pd.DataFrame(columns=stage_overall_cols)
        self.df_stage_retirements = pd.DataFrame(columns=retirement_cols+retirement_extra_cols)
        self.df_stage_penalties = pd.DataFrame(columns=penalty_cols+penalty_extra_cols)

    def df_inclass_cars(self, _df, rally_class='all', typ='entryId'):
        """Get cars in particular class."""
        if rally_class != 'all':
            _df = _df[_df.index.isin(self.carsInClass(rally_class, typ=typ))]
        return _df

    def carsInClass(self, qclass, typ='carNum'):
        #Can't we also pass a dict of key/vals to the widget?
        #Omit car 0
        df_entry_list = self.get_entry_list()
        if qclass.lower()=='all':
            return df_entry_list[df_entry_list['CarNum']!='#0']['carNum'].dropna().to_list()
        _cars = df_entry_list[(df_entry_list['CarNum']!='#0') & (df_entry_list['Class']==qclass)]['carNum'].to_list()
        if typ=='entryId':
            _cars = [self.entryFromCar[c] for c in self.entryFromCar if c in _cars]
        return _cars

    def stages_class_winners(self, rally_class='all'):
        """Return stage winners for a specified class."""
        _class_stage_winners = self.df_inclass_cars(self.df_stages_pos,
                                                    rally_class=rally_class).idxmin()
        return _class_stage_winners
    
    def get_class_rebased_times(self, rally_class='all', typ='stagewinner'):
        """
        Get times rebased relative to class.
        Rebaser can be either class stage winner or class stage overall.
        """
        # TO DO  - not yet implemented for class overall
        self.get_stage_times()
        _stage_times = self.df_inclass_cars(self.df_stages, rally_class=rally_class)
        df_stages_rebased_to_stage_winner = _stage_times.apply(_rebaseTimes,
                                                               basetimes=_stage_times.min(), axis=1)
        return df_stages_rebased_to_stage_winner

    def set_rebased_times(self):
        if self.df_stages_rebased_to_overall_leader is None \
                or self.df_stages_rebased_to_stage_winner is None \
                or self.df_stages_rebased_to_stage_winner is None:
            #print('setting rebased times...')
            self.get_stage_times()
            leaderStagetimes = self.df_stages.iloc[0]
            self.df_stages_rebased_to_overall_leader = self.df_stages.apply(_rebaseTimes,
                                                                            basetimes=leaderStagetimes, axis=1)
            #Now rebase to the stage winner
            self.df_stages_rebased_to_stage_winner = self.df_stages_rebased_to_overall_leader.apply(_rebaseTimes, basetimes=self.df_stages_rebased_to_overall_leader.min(), axis=1)

            leaderTimes = self.df_overall.min()
            self.df_overall_rebased_to_leader = self.df_overall.apply(_rebaseTimes,
                                                                      basetimes=leaderTimes, axis=1)

    def _set_car_entry_lookups(self, df, force=False):
        """Look-up dicts between car number and entry."""
        if force or not self.carFromEntry or not self.entryFromCar:
            self.carFromEntry = df['carNum'].to_dict()
            self.entryFromCar =  {v:k for (k, v) in self.carFromEntry.items()}
    
    def get_final(self):
        if self.df_rally_overall is None:
            self.df_rally_overall = get_final(self.stub)
            self._set_car_entry_lookups(self.df_rally_overall)
        return self.df_rally_overall
        
    def get_stage_times(self):
        if self.live or self.df_overall is None or self.df_stages is None or self.df_overall_pos is None:
            self.df_allInOne, self.df_overall, self.df_stages, \
                self.df_overall_pos, self.df_stages_pos = get_stage_times(self.stub)
            self._set_car_entry_lookups(self.df_allInOne)
        return self.df_allInOne, self.df_overall, self.df_stages, self.df_overall_pos
    
    def get_itinerary(self):
        if self.live or self.event_dist is None or self.df_itinerary_leg_totals is None \
            or self.df_itinerary is None or self.df_full_itinerary is None:
                self.event_dist, self.df_itinerary_leg_totals, \
                    self.df_itinerary, self.df_full_itinerary_df = get_itinerary(self.stub)

        _stage_distances = self.df_itinerary['Distance'][~self.df_itinerary['Time'].str.contains('cancelled')]
        # Stage distances do not include cancelled stages
        # The following is the correct stage index
        #_stage_distances.index = [int(i.lstrip('SS')) for i in _stage_distances.index]
        #As a hack, to cope with cancelled stages, reindex
        _stage_distances.reset_index(drop=True, inplace=True)
        _stage_distances.index += 1 
        self.stage_distances = _stage_distances
        self.stage_distances_all = self.df_itinerary['Distance']
    
        return self.event_dist, self.df_itinerary_leg_totals, \
                self.df_itinerary, self.df_full_itinerary
    
    def get_entry_list(self):
        if self.df_entry_list is None:
            self.df_entry_list = get_entry_list(self.stub)
            
        #A list of classes could be useful, so grab it while we can
        self.rally_classes = self.df_entry_list['Class'].dropna().unique()
        
        return self.df_entry_list
    
    def get_stage_result_links(self):
        if self.stage_result_links is None:
            self.stage_result_links = get_stage_result_links(self.stub)
        return self.stage_result_links
    
    def get_stage_results(self, stage=None):
        #for now, just return what we have with stage as None
        if stage is None:
            return self.df_stage_result, self.df_stage_overall, \
                    self.df_stage_retirements, self.df_stage_penalties
        # Could maybe change that to get everything?
        stages = stage if isinstance(stage,list) else [stage]
        if stages:
            links = self.get_stage_result_links()
            if 'all' in stages:
                print('all')
                stages = [k for k in links.keys() if 'leg' not in k]
            elif 'final' in stages or 'last' in stages:
                stages = [k for k in links.keys() if 'leg' not in k][-1]
            for stage in stages:
                if self.live or (stage not in self.df_stage_result['Stage'].unique() and stage in links):
                    df_stage_result, df_stage_overall, df_stage_retirements, \
                        df_stage_penalties = get_stage_results(links[stage])
                    self.df_stage_result = self.df_stage_result.append(df_stage_result, sort=False).reset_index(drop=True)
                    self.df_stage_overall = self.df_stage_overall.append(df_stage_overall, sort=False).reset_index(drop=True)
                    self.df_stage_retirements = self.df_stage_retirements.append(df_stage_retirements, sort=False).reset_index(drop=True)
                    self.df_stage_penalties = self.df_stage_penalties.append(df_stage_penalties, sort=False).reset_index(drop=True)
        
        if stages:
            return self.df_stage_result[self.df_stage_result['Stage'].isin(stages)], \
                    self.df_stage_overall[self.df_stage_overall['Stage'].isin(stages)], \
                    self.df_stage_retirements[self.df_stage_retirements['Stage'].isin(stages)], \
                    self.df_stage_penalties[self.df_stage_penalties['Stage'].isin(stages)]
        
        self._set_car_entry_lookups(self.df_stage_result)
        
        return self.df_stage_result, self.df_stage_overall, \
                self.df_stage_retirements, self.df_stage_penalties

# + tags=["active-ipynb"]
# ewrc=EWRC(rally_stub)

# + tags=["active-ipynb"]
# #df_stage_result, df_stage_overall, df_stage_retirements, df_stage_penalties
# ewrc.get_stage_results()

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS1')[0]

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS1')[1]

# + tags=["active-ipynb"]
# ewrc.set_rebased_times()
# display(ewrc.df_stages_rebased_to_overall_leader)

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS1')[2]

# + tags=["active-ipynb"]
# ewrc.get_stage_result_links()

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS1')[3]

# + tags=["active-ipynb"]
# ewrc.get_stage_times()[0]

# + tags=["active-ipynb"]
# ewrc.get_stage_times()[1]

# + tags=["active-ipynb"]
# ewrc.get_stage_times()[2]

# + tags=["active-ipynb"]
# ewrc.get_stage_times()[3]

# + tags=["active-ipynb"]
# ewrc.get_itinerary()

# + tags=["active-ipynb"]
# ewrc.get_entry_list()

# + tags=["active-ipynb"]
# ewrc=EWRC('54464-corsica-linea-tour-de-corse-2019')

# + tags=["active-ipynb"]
# ewrc.get_entry_list()

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS2')[0]
# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS2')[1]


# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS2')[2]

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS2')[3]
# -


