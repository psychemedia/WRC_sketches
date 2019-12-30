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


def get_stage_result_links(rally_stage_results_url):
    #If navigation remains constant, items are in third list
    links=[]

    soup = soupify(rally_stage_results_url)
    for li in soup.find_all('ul')[2].find_all('li'):
        #if 'class' in li.attrs:
        #    print(li['class'])
        #A class is set for service but not other things
        if 'class' not in li.attrs:
            a = li.find('a')
            if 'href' in a.attrs:
                links.append(a['href'])
                
    return links


# +
url='https://www.ewrc-results.com/results/54762-corbeau-seats-rally-tendring-clacton-2019/'

get_stage_result_links(url)

# +
from numpy import nan

def get_stage_results(stub):
    soup = soupify('{}{}'.format(base_url, stub))
    tables = soup.find_all('table')
    stage_result = tables[0]
    stage_overall = tables[1]
    stage_retirements = tables[2]
    
    df = dfify(stage_result)
    df.columns=['Pos','CarNum','Desc','Class', 'Time','GapDiff', 'Speedkm']
    df['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'}).head()
    df[['Gap','Diff']] = diffgapsplitter(df['GapDiff'])
    df[['Speed','Dist']] = df['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)').head()
    
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

    df[['driver','entryId','model','navigator']] = pd.DataFrame(rows)
    #Should we cast the Pos to a numeric too? Set = to na then ffill down?
    df['PosNum'] = df['Pos'].replace('=',nan).astype(float).fillna(method='ffill').astype(int)
    df.set_index('driver',drop=True, inplace=True)
    
    return df, stage_overall, stage_retirements


# -

stub = '/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
stub='/results/42870-rallye-automobile-de-monte-carlo-2018/'
stage_result, stage_overall, stage_retirements = get_stage_results(stub)

stage_result

stage_overall

stage_retirements

# +
from parse import parse

def get_stage_times(stage_times_url):
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

# -

url='https://www.ewrc-results.com/times/54762-corbeau-seats-rally-tendring-clacton-2019/'
#url='https://www.ewrc-results.com/times/42870-rallye-automobile-de-monte-carlo-2018/'
df_overall, df_stages, df_overall_pos = get_stage_times(url)

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




