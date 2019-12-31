# # eWRC API
#
# Simple Python API for eWRC results.

# +
import pandas as pd
import re
from dakar_utils import getTime

import requests
import lxml.html as LH
from bs4 import BeautifulSoup

# -

# ## Generic Utilities
#
# Utility functions.

def soupify(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml') # Parse the HTML as a string
    return soup


def dfify(table):
    df = pd.read_html('<html><body>{}</body></html>'.format(table))[0]
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    return df


# +
import unicodedata

def cleanString(s):
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace with single space
    s = ' '.join(s.split())
    
    return s


# -

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


#url='https://www.ewrc-results.com/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
rally_stub = '54762-corbeau-seats-rally-tendring-clacton-2019'
tmp = get_stage_result_links(rally_stub)
tmp

# +
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
 

# +
from numpy import nan

from parse import parse   
    
def get_stage_results(stub):
    soup = soupify('{}{}'.format(base_url, stub))
    
    pattern = 'SS{stage} {name} - {dist:f} km - {datetime}'
    details = soup.find('h4').text
    parse_result = parse(pattern, details)

    stage_num = f"SS{parse_result['stage']}"
    stage_name = parse_result['name']
    stage_dist =  parse_result['dist']
    stage_datetime = parse_result['datetime']

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
    
    df_stage_overall[['Pos','Change']] = df_stage_overall['PosChange'].str.extract(r'(?P<Pos>[\d]*)\.\s?(?P<Change>.*)?')
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
# -

partial_stub = '/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
partial_stub='/results/42870-rallye-automobile-de-monte-carlo-2018/'
#stub = tmp['SS3']
stage_result, stage_overall, stage_retirements, stage_penalties = get_stage_results(partial_stub)

stage_result.head()

stage_overall

stage_retirements

stage_penalties

# +
from parse import parse

def get_stage_times(stub):
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
        classification = pd.to_numeric(g[NAME_SUBGROUP].find('span').text.replace('R','').strip('').strip('.'))

        stagetimes = []
        overalltimes = []
        penalties=[]
        positions = []

        for stages in g[TIME_SUBGROUP].findAll('div'):
            txt = cleanString(stages)
            stagetimes_data = parse(pattern, txt )
            if stagetimes_data:
                stagetimes.append(stagetimes_data['stagetime'])
                overalltimes.append(stagetimes_data['overalltime'])

                #Need to parse this
                #There may be penalties in the pos
                penalty = 0
                p = stagetimes_data['pos'].split()
                if p[-1].endswith('</span>'):
                    penalty = parse(penaltypattern, p[-1] )
                    if penalty:
                        #This really needs parsing into a time; currently of form eg 0:10
                        penalty = penalty['penalty']
                    p = int(p[-2].split('.')[0])
                else:
                    p = int(p[-1].strip('.'))
                positions.append(p)
                penalties.append(penalty)

        t.append({'entryId':entryId,
                  'driver':driver.strip(),
                 'navigator':navigator.strip(),
                  'carNum':carNum,
                  'carModel':carModel,
                  'retired':retired,
                  'Pos': classification,
                 'stagetimes':stagetimes,
                 'overalltimes':overalltimes,
                 'positions':positions, 'penalties':penalties})


    df = pd.DataFrame(t).set_index(['entryId'])
    
    df_overall = pd.DataFrame(df['overalltimes'].tolist(), index= df.index)
    df_overall.columns = range(1,df_overall.shape[1]+1)
    
    df_overall_pos = pd.DataFrame(df['positions'].tolist(), index= df.index)
    df_overall_pos.columns = range(1,df_overall_pos.shape[1]+1)

    df_stages = pd.DataFrame(df['stagetimes'].tolist(), index= df.index)
    df_stages.columns = range(1,df_stages.shape[1]+1)
    
    xcols = df_overall.columns

    for ss in xcols:
        df_overall[ss] = df_overall[ss].apply(getTime)
        df_stages[ss] = df_stages[ss].apply(getTime)

    return df_overall, df_stages, df_overall_pos


# +
url='https://www.ewrc-results.com/times/54762-corbeau-seats-rally-tendring-clacton-2019/'
#url='https://www.ewrc-results.com/times/42870-rallye-automobile-de-monte-carlo-2018/'

rally_stub = '42870-rallye-automobile-de-monte-carlo-2018'
df_overall, df_stages, df_overall_pos = get_stage_times(rally_stub)
# -

display(df_overall)
display(df_stages)
display(df_overall_pos)

# ## Itinerary

itinerary_path = 'https://www.ewrc-results.com/timetable/{stub}/'


def get_itinerary(stub):
    soup = soupify(itinerary_path.format(stub=stub))
    
    event_dist = soup.find('td',text='Event total').parent.find_all('td')[-1].text

    itinerary_df = dfify( soup.find('div', {'class':'timetable'}).find('table') )
    itinerary_df.columns = ['Stage','Name', 'Distance', 'Date', 'Time']
    itinerary_df['Leg'] = [nan if 'leg' not in str(x) else str(x).replace('. leg','') for x in itinerary_df['Stage']]
    itinerary_df['Leg'] = itinerary_df['Leg'].fillna(method='ffill')
    itinerary_df['Date'] = itinerary_df['Date'].fillna(method='ffill')

    itinerary_leg_totals = itinerary_df[itinerary_df['Name'].str.contains("Leg total")][['Leg', 'Distance']].reset_index(drop=True)

    full_itinerary_df = itinerary_df[~itinerary_df['Name'].str.contains(". leg")]
    full_itinerary_df = full_itinerary_df[~full_itinerary_df['Date'].str.contains(" km")]
    full_itinerary_df = full_itinerary_df.fillna(method='bfill', axis=1)

    #Legs may not be identified but we may want to identify services
    full_itinerary_df['Service'] = [ 'Service' in i for i in full_itinerary_df['Distance'] ]
    full_itinerary_df['Service_Num'] = full_itinerary_df['Service'].cumsum()
    full_itinerary_df.reset_index(drop=True, inplace=True)
    #itinerary_df = full_itinerary_df[~full_itinerary_df['Service']].reset_index(drop=True)
    itinerary_df = full_itinerary_df[full_itinerary_df['Stage'].str.startswith('SS')].reset_index(drop=True)
    itinerary_df['Section'] = itinerary_df['Service_Num'].rank(method='dense')
    itinerary_df.drop(columns=['Service', 'Service_Num'], inplace=True)
    return event_dist, itinerary_leg_totals, itinerary_df, full_itinerary_df


stub='54762-corbeau-seats-rally-tendring-clacton-2019'
stub='42870-rallye-automobile-de-monte-carlo-2018'
event_dist, itinerary_leg_totals, itinerary_df, full_itinerary_df = get_itinerary(stub)

print(event_dist)
display(itinerary_leg_totals)
display(itinerary_df)
display(full_itinerary_df)

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


get_entry_list(stub)


# ## `EWRC` Class
# Create a class to that can be used to gran all the results for a particular rally, as required.
#
# We fudge the definition of class functions so that we can separately define and functions in a standalione way. This is probably *not good practice*...!

class EWRC:
    """Class for eWRC data for a particular rally."""

    def __init__(self, stub):
        self.stub = stub
        
        self.stage_result_links = None
        
        self.df_overall = None
        self.df_stages = None
        self.df_overall_pos = None
        
        self.event_dist = None
        self.df_itinerary_leg_totals = None
        self.df_itinerary = None
        self.df_full_itinerary =None
        
        self.df_entry_list = None
 
        self.df_stage_result = pd.DataFrame(columns=stage_result_cols)
        self.df_stage_overall = pd.DataFrame(columns=stage_overall_cols)
        self.df_stage_retirements = pd.DataFrame(columns=retirement_cols+retirement_extra_cols)
        self.df_stage_penalties = pd.DataFrame(columns=penalty_cols+penalty_extra_cols)
    
    def get_stage_times(self):
        if self.df_overall is None or self.df_stages is None or self.df_overall_pos is None:
            self.df_overall, self.df_stages, self.df_overall_pos = get_stage_times(self.stub)

        return self.df_overall, self.df_stages, self.df_overall_pos
    
    def get_itinerary(self):
        if self.event_dist is None or self.df_itinerary_leg_totals is None \
            or self.df_itinerary is None or self.df_full_itinerary is None:
                self.event_dist, self.df_itinerary_leg_totals, \
                    self.df_itinerary, self.df_full_itinerary_df = get_itinerary(self.stub)
        
        return self.event_dist, self.df_itinerary_leg_totals, \
                self.df_itinerary, self.df_full_itinerary
    
    def get_entry_list(self):
        if self.df_entry_list is None:
            self.df_entry_list = get_entry_list(self.stub)
        return self.df_entry_list
    
    def get_stage_result_links(self):
        if self.stage_result_links is None:
            self.stage_result_links = get_stage_result_links(self.stub)
        return self.stage_result_links
    
    def get_stage_results(self, stage=None):
        #for now, just return what we have with stage as None
        # Could maybe change that to get everything?
        if stage and stage not in self.df_stage_result['Stage'].unique():
            links = self.get_stage_result_links()
            if stage in links:
                df_stage_result, df_stage_overall, df_stage_retirements, \
                    df_stage_penalties = get_stage_results(links[stage])
                self.df_stage_result = self.df_stage_result.append(df_stage_result, sort=False).reset_index(drop=True)
                self.df_stage_overall = self.df_stage_overall.append(df_stage_overall, sort=False).reset_index(drop=True)
                self.df_stage_retirements = self.df_stage_retirements.append(df_stage_retirements, sort=False).reset_index(drop=True)
                self.df_stage_penalties = self.df_stage_penalties.append(df_stage_penalties, sort=False).reset_index(drop=True)
            
        return self.df_stage_result, self.df_stage_overall, \
                self.df_stage_retirements, self.df_stage_penalties


ewrc=EWRC(rally_stub)

ewrc.get_stage_results('SS4')

ewrc.get_stage_result_links()

ewrc.get_stage_times()

ewrc.get_itinerary()

ewrc.get_entry_list()


