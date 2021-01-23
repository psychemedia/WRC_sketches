# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
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
#
#
# We can tunnel into class and champtionship for stage results, entry list, shakedown and final results.

# +
# #%pip install --upgrade beautifulsoup4
# -

import pandas as pd
import re
from dakar_utils import getTime

# +
import requests
import lxml.html as LH
from bs4 import BeautifulSoup
from bs4.element import NavigableString

from parse import parse


# -

# ## Generic Utilities
#
# Utility functions.

def soupify(url):
    """Load HTML from URL and parse it into a BeautifulSoup object.
    
    :param url: The URL of an HTML page we want to scrape.
    :type url: string, required
    :return: A soup representation of an HTML page.
    :rtype: bs4.BeautifulSoup object
    """
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml') # Parse the HTML as a string
    
    # Remove occasional tags that might appear
    # https://stackoverflow.com/a/40760750/454773
    unwanted = soup.find(id="donate-main")
    if unwanted:
        unwanted.extract()
    
    return soup


def no_children(node):
    """Extract just the text and no child nodes from a soup node."""
    #https://stackoverflow.com/a/31909680/454773
    text = ''.join([t for t in node.contents if type(t) == NavigableString])
    return text


def dfify(table):
    
    # TO DO - TEST - ADD SPACE FOR <br/> - CHECK - MAYBE BROKEN ?
    #table = BeautifulSoup(str(table).replace("<br/>", " "))
    table = str(table).replace("<br/>", " ")
    df = pd.read_html('<html><body>{}</body></html>'.format(table))[0]
    df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
    return df


import unicodedata

def cleanString(s):
    """Clean a string:
     - convert it to a normalized NFKD string;
     - strip string of whitespace;
     - replace multiple space elements with a single space element.
    """
    s = unicodedata.normalize("NFKD", str(s))
    #replace multiple whitespace elements with single space
    s = ' '.join(s.strip().split())
    
    return s


def urljoin(*args, trailing=True):
    """
    Joins given arguments into an url. Trailing but not leading slashes are
    stripped for each argument.
    """
    simple = "/".join(map(lambda x: str(x).strip('/'), args))
    if trailing:
        return f'{simple}/'
    else:
        return simple


base_url = 'https://www.ewrc-results.com'

# +
import urllib

def urlBuilder(stub, params):
    """Build a valid URL."""
    return f'{urljoin(base_url, stub, trailing=True)}?{urllib.parse.urlencode(params)}'


# -

# ## Timing Utilities

def diffgapsplitter(col):
    """Take a dataframe column containing Gap and Diff elements as a single string
    and split them into separate columns.
    """
    #Normalise
    col = col.fillna('+0+0')
    #Remove leading +
    col = col.str.strip('+')
    #Split...
    col = col.str.split('+',expand=True)
    #Rename columns
    col = col.rename(columns={0:'Gap', 1:'Diff'})
    #Convert to numerics
    col['Gap'] = col['Gap'].apply(getTime)#.astype(float)
    col['Diff'] = col['Diff'].apply(getTime)
    return col


# + tags=["active-ipynb", "test"]
# import pandas as pd
# _test_df = diffgapsplitter( pd.DataFrame({'test': [None, '+1.0+2.7'] })['test'])
# _test_df_expected = pd.DataFrame({'Gap': [0.0, 1.0], 'Diff': [0.0, 2.7] })
# pd.testing.assert_frame_equal(_test_df, _test_df_expected)
# -

# ## Scraping Functions

# +
import urllib.parse

def search_events(query):
    """Search ewrc-results events for particular term."""
    if len(query) < 3:
        return {}
    search_url = f'{base_url}/search_event/?find_event={urllib.parse.quote(query)}'
    soup = soupify(search_url)
    links = soup.find_all("div", {'class':'search-event-event'})[0].find('table').find_all('a')
    rally_links = {}
    for a in links:
        if 'href' in a.attrs:
                rally_links[a['href'].strip('/').split('/')[-1]] = a.text.strip()
    return rally_links


# + tags=["active-ipynb"]
# #url='https://www.ewrc-results.com/results/54762-corbeau-seats-rally-tendring-clacton-2019/'
# rally_stub = '54762-corbeau-seats-rally-tendring-clacton-2019'
# rally_stub='61961-mgj-engineering-brands-hatch-winter-stages-2020'
# rally_stub='59972-rallye-automobile-de-monte-carlo-2020'
# rally_stub='62413-rallye-de-ourense-2020/'
# #rally_stub='41079-rallye-automobile-de-monte-carlo-2021'
# homepage_url=f'{base_url}/results/{rally_stub}/'
#

# +
def _get_stages_from_homepage(soup):
    """Retrieve the stage list and keys from the results page."""
    stages = []
    for li in soup.find('div', {'class': 'rzlist50'}).find_all('li'):
        if li.has_attr('class'):
            stages.append((li['class'],'',li['class']))
        else:
            a = li.find('a')
            if not a.has_attr('class'):
                stages.append( (a.text, a['href'], a['title']) )
    return stages

def _get_stages_from_homepage(soup):
    """Retrieve the stage list and keys from the results page."""
    stages = []
    #print(soup.select('body > main > div:nth-child(6)')[0])
    #for li in soup.select('body > main > div:nth-child(6)')[0].find_all('div'):
    for li in soup.find_all('div', {'class': 'd-flex flex-wrap justify-content-center'})[0].find_all('div'):
        if li.has_attr('class'):
            stages.append((li['class'],'',li['class']))
        else:
            #print(li)
            a = li.find('a')
            stages.append( (a.text, a['href'], a['title']) )
    return stages



# +
#There may be diffferent results categories, eg classes, championships
# These are keyed by an extra parameter on the end of the stub
def _get_categories_form_homepage(soup):
    """Retrieve championships and classes."""
    _categories = []
    categories = soup.find_all('div', {'class': 'rzlist40'})
    if categories:
        for category in categories:
            items = []
            for a in category.find_all('a'):
                items.append( (a.text, a['href']) )
            _categories.append(items)
    return _categories

def _get_categories_form_homepage(soup):
    """Retrieve championships and classes."""
    _categories = []
    #categories = soup.find_all('div', {'class': 'rzlist40'})
    categories = soup.find_all('div', {'class': 'd-flex flex-wrap justify-content-center fs-091'})
    if categories:
        for category in categories:
            items = []
            for a in category.find_all('a'):
                if a.text:
                    items.append( (a.text, a['href']) )
            _categories.append(items)
    return _categories


# -

def homepage_quick_scrape(stub, params=None, path=None):
    """Do a quick utility scrape of the homepage."""
    params = '' if not params else urllib.parse.urlencode(params)
    homepage_url=f'{base_url}/results/{stub}/?{params}&{path}'
    print(homepage_url)
    soup = soupify(homepage_url)
    stages = _get_stages_from_homepage(soup)
    categories = _get_categories_form_homepage(soup)
    return stages, categories


# + tags=["active-ipynb"]
# Xstages, Xcategories = homepage_quick_scrape(rally_stub)
# Xstages, Xcategories
# -

def get_stage_result_links(stub, params=None):
    #If navigation remains constant, items are in third list
    params = '' if not params else urllib.parse.urlencode(params)
    rally_stage_results_url= f'{base_url}/results/{stub}/?{params}'
    
    links={}
    soup = soupify(rally_stage_results_url)
    stages = soup.find('div', {'class': 'd-flex flex-wrap justify-content-center'}).find_all('div')
    for li in stages:
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

def get_stage_results(stub, params=None):
    params = '' if not params else urllib.parse.urlencode(params)
    _url = f'{base_url.rstrip("/")}/{stub}/?{params}'
    print(_url)
    soup = soupify(_url)

    #details = soup.find('h4').text
    details = soup.find('h5', {'class':'mt-2'}).text

    pattern = 'SS{stage} {name} - {dist:f} km - {datetime}'
    parse_result = parse(pattern, details)
    if parse_result is None:
        pattern = 'SS{stage} - {dist:f} km'
        parse_result = parse(pattern, details)
    #print(details, parse_result)
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
    
    result_cols = ['Pos','CarNum','Desc','Class', 'Time','GapDiff', 'Speedkm']
    overall_cols = ['PosChange', 'CarNum', 'Desc','Class', 'Time', 'GapDiff', 'Speedkm' ]
    stage_retirement_cols = ['CarNum', 'driverNav', 'Model', 'Status', 'Driver', 'CoDriver', 'Stage']
    stage_penalty_cols = ['CarNum', 'driverNav', 'Model', 'PenReason', 'Driver', 'CoDriver', 'Stage', 'Time', 'Reason']
    
    # Stage Result
    df_stage_result = dfify(stage_result)
    
    # Handle cancelled stage
    cancelled = (df_stage_result.iat[0,0] == 'Stage cancelled')
    if cancelled:
        return pd.DataFrame(columns=result_cols), pd.DataFrame(columns=overall_cols), pd.DataFrame(columns=stage_retirement_cols), pd.DataFrame(columns=stage_penalty_cols)
    
    print(df_stage_result.columns, result_cols)
    display(df_stage_result)
    df_stage_result.columns = result_cols
    
    df_stage_result['Stage'] = stage_num
    df_stage_result['StageName'] = stage_name
    df_stage_result['StageDist'] = stage_dist
    
    df_stage_result['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'})
    df_stage_result[['Gap','Diff']] = diffgapsplitter(df_stage_result['GapDiff'])
    df_stage_result[['Speed','Dist']] = df_stage_result['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)')
    
    rows=[]
    # Separate out the elements from the driver column
    for d in stage_result.findAll("td", {"class": "position-relative"}):
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

    # Reduced cols if the stage is cancelled
    # THis is a crude hack; should really detect properly
    _cancelled_hack = len(df_stage_overall.columns) != len(overall_cols)
    df_stage_overall.columns = overall_cols[:len(df_stage_overall.columns)]
    df_stage_overall['Stage'] = stage_num
    df_stage_overall['StageName'] = stage_name
    df_stage_overall['StageDist'] = stage_dist
    
    df_stage_overall[['Pos','Change']] = df_stage_overall['PosChange'].astype(str).str.extract(r'(?P<Pos>[\d]*)\.\s?(?P<Change>.*)?')
    if not _cancelled_hack:
        df_stage_overall['GapDiff'].fillna('+0+0').str.strip('+').str.split('+',expand=True).rename(columns={0:'Gap', 1:'Diff'})
        df_stage_overall[['Gap','Diff']] = diffgapsplitter(df_stage_overall['GapDiff'])
        df_stage_overall[['Speed','Dist']] = df_stage_overall['Speedkm'].str.extract(r'(?P<Speed>[^.]*\.[\d])(?P<Dist>.*)')

    
    #  TO DO - classes  - at the moment, only use first class?
    df_stage_result["Class"] = df_stage_result["Class"].apply(lambda x: str(x).strip().split()[0])
    df_stage_overall["Class"] = df_stage_overall["Class"].apply(lambda x: str(x).strip().split()[0])
    
    # Retirements
    df_stage_retirements = pd.DataFrame(columns=retirement_cols+retirement_extra_cols)
    # get tag and then next sibling
    _retirementsHeader = soup.find('h6', text=re.compile('Retirement'))
    if _retirementsHeader:
        print('retirement')
        retired = _retirementsHeader.find_next_sibling()#find('table',{'class':'table-retired'})
        if retired:
            df_stage_retirements = dfify(retired)
            df_stage_retirements.columns = retirement_cols
            df_stage_retirements[['Driver','CoDriver']] = df_stage_retirements['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')
            df_stage_retirements['Stage'] = stage_num

    # Penalties
    df_stage_penalties = pd.DataFrame(columns=penalty_cols+penalty_extra_cols)
    _penaltiesHeader = soup.find('h6', text=re.compile('Penalty'))
    if _penaltiesHeader:
        penalty = _penaltiesHeader.find_next_sibling() # ('table',{'class':'table-retired'})
        if penalty:
            df_stage_penalties = dfify(penalty)
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
# partial_stub='/results/61089-rally-islas-canarias-2020/'
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
def check_time_str(txt):
    """Clean a time string."""
    
    # Quick fix to cope with strings of the form:
    # '11:58.0 <a name="" title="Notional time"><span class="c-blue">[N]</span></a>'
    # This should be a proper validator.
    txt = txt.strip()
    if txt:
        txt = txt.split()[0]
    return txt


# +
# TO DO

def get_stage_times(stub, dropnarow=True):
    url=f'https://www.ewrc-results.com/times/{stub}/'
    #print(url)
    soup = soupify(url)
    
    groups = soup.find_all('div',{'class':'times-driver'})
    # Each list member in groups is the data for one driver
    #The rows are essentially grouped in twos after the header row
    #cols = [c.text for c in times[0].findAll('div')]
    
    #groupsize=2
    #groups = [times[i:i+groupsize] for i in range(1, len(times), groupsize)]
    
    NAME_SUBGROUP = 0
    TIME_SUBGROUP = 1
    
    carNumMatch = lambda txt: re.search('#(?P<carNum>[\d]*)', cleanString(txt))
    carModelMatch = lambda txt:  re.search('</a>\s*(?P<carModel>.*)</div>', cleanString(txt))
    
    #pattern = '''<div class="times-one-time">{stagetime}<br/><span class="times-after">{overalltime}</span><br/>{pos}</div>'''
    
    t=[]
    i=0

    penaltypattern='class="r7_bold_red">{penalty}</span>'
    timepattern1 = '<div class="times-one-time font-weight-bold p-1 fs-091">{stagetime}'
    timepattern2 = '<span class="fs-09 text-muted">{cumtime}</span>'
    for gg in groups:
        g = gg.findChildren(recursive=False)
        #print('g0',g[0],'\nand g1\n', g[1], '\nxx\n')
        # g now has two divs
        i=i+1
        driverNav_el = g[NAME_SUBGROUP].find('a')
        driverNav = driverNav_el.text
        driver,navigator = driverNav.split(' - ')
        entryId = driverNav_el['href']
        retired = '<span class="r8_bold_red">R</span>' in str(g[NAME_SUBGROUP])
        carNum = carNumMatch(g[NAME_SUBGROUP]).group('carNum')
        
        #carModel = carModelMatch(g[NAME_SUBGROUP]).group('carModel')
        _carModel = g[NAME_SUBGROUP].find('div',{'class':'times-car mx-1'})
        carModel = _carModel.text.strip() if _carModel else ''
        
        #TO DO - may be None?
        try:
            classification = g[NAME_SUBGROUP].select('div:first-child div:first-child')[0].text
            classification = int(classification.strip().strip('.')[0]) if classification else nan
            #classification = pd.to_numeric(g[NAME_SUBGROUP].find('span').text.replace('R','').strip('').strip('.'))
        except:
            classification = ''
        
        stagetimes = []
        overalltimes = []
        penalties=[]
        positions = []
        for stage in g[TIME_SUBGROUP].findChildren(recursive=False)[:-1]:
            txt = cleanString(stage)
            if 'cancelled' in txt:
                stagetimes.append(nan)
                overalltimes.append(nan)
                positions.append(nan)
                penalties.append(nan)
            else:
                stagetimes_data = [t.strip() for t in txt.split('<br/>')]
                #print(txt, stagetimes_data,'\n')
                #stagetimes_data = parse(timepattern, txt )
                if stagetimes_data:
                    #stagetimes.append(check_time_str(stagetimes_data['stagetime']))
                    #overalltimes.append(check_time_str(stagetimes_data['overalltime']))
                    if len(stagetimes_data)<2:
                        if stagetimes_data[0]=='<div class="times-one-time font-weight-bold p-1 fs-091"><span class="font-weight-bold text-danger">R</span></div>':
                            # retired BUT may return in following stages
                            # example https://www.ewrc-results.com/times/61089-rally-islas-canarias-2020/?s=265920
                            stagetimes.append(nan)
                            overalltimes.append(nan)
                            positions.append(nan)
                    else:
                        # We may now be running as retired
                        if 'R</span>' in stagetimes_data[2]:
                            # running as retired
                            stagetimes.append(check_time_str(parse(timepattern1,stagetimes_data[0] )['stagetime']))
                            overalltimes.append(nan)
                            positions.append(nan)
                        else:
                            stagetimes.append(check_time_str(parse(timepattern1,stagetimes_data[0] )['stagetime']))
                            overalltimes.append(check_time_str(parse(timepattern2,stagetimes_data[1] )['cumtime']))
                            # There may be up/down arrow before position
                            positions.append(int(stagetimes_data[2].split('.')[0].split('>')[-1].strip()))
                    if len(stagetimes_data)>3:
                        penalties.append(stagetimes_data[3])
                    else:
                        penalties.append('')
                # TO DO - how do we account for cancelled stages?
                # If we add in blanks we get blanks for un-run stages too?
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
        # BUT, we don't want to drop a cancelled stage?
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
# rally_stub='61089-rally-islas-canarias-2020' #breaks
#
# df_allInOne, df_overall, df_stages, \
#     df_overall_pos, df_stages_pos = get_stage_times(rally_stub)

# + tags=["active-ipynb"]
# display(df_allInOne.head(2))
# display(df_overall.head(2))
# display(df_stages.head(2))
# display(df_overall_pos.head(2))
# display(df_stages_pos.head(2))
# -

# ## Final Results
#
# Get overall rankings.

final_path = 'https://www.ewrc-results.com/final/{stub}/?{params}'


# + run_control={"marked": false}
def get_final(stub, params=None):
    params = '' if not params else urllib.parse.urlencode(params)
    print(final_path.format(stub=stub, params=params))
    soup = soupify(final_path.format(stub=stub, params=params))
    html_table = soup.find('table', {'class': 'results'})
    
    # There are actually several tables contained in one
    # A rowspan straddles them
    
    tables = []
    table = []
    entryIds = []
    for row in html_table.find_all('tr'):
        row_data = []
        for cell in row:
            # New table
            if cell.text.startswith('Retirements'):
                tables.append(table)
                table = []
            else:
                row_data.append(cell.text.strip())
            if len(tables) == 0:
                tmp = cell.find('a', {'title': 'Entry info and stats'})
                if tmp:
                    entryIds.append(tmp['href'])
        table.append(row_data)
    
    # Process results table
    #tables = LH.fromstring(html).xpath('//table')
    #df_rally_overall = pd.read_html('<html><body>{}</body></html>'.format(tables[0]))[0]
    #df_rally_overall['badge'] = [img.find('img')['src'] for img in tables[0].findAll("td", {"class": "final-results-icon"}) ]
    #df_rally_overall.dropna(how='all', axis=1, inplace=True)
    #print(df_rally_overall[2])
    df_rally_overall = pd.DataFrame(tables[0]).replace('', nan).dropna(how='all', axis=1)
    df_rally_overall.columns=['Pos','CarNum','driverNav','Model',
                              'Reg', 'Class', 'Time','GapDiff', 'Speedkm']

    display(df_rally_overall)
    #Get the entry ID - use this as the unique key
    #in column 3, <a title='Entry info and stats'>
    df_rally_overall['entryId']= entryIds # [a['href'] for a in tables[0].findAll("a", {"title": "Entry info and stats"}) ]
    df_rally_overall.set_index('entryId', inplace=True)

    df_rally_overall[['Driver','CoDriver']] = df_rally_overall['driverNav'].str.extract(r'(?P<Driver>.*)\s+-\s+(?P<CoDriver>.*)')

    df_rally_overall['Historic']= df_rally_overall['Class'].str.contains('Historic')
    df_rally_overall['Class']= df_rally_overall['Class'].str.replace('Historic','')

    df_rally_overall['Pos'] = df_rally_overall['Pos'].astype(str).str.extract(r'(.*)\.')
    df_rally_overall['Pos'] = df_rally_overall['Pos'].astype(int)

    #df_rally_overall[['Model','Registration']]=df_rally_overall['ModelReg'].str.extract(r'(?P<Model>.*) \((?P<Registration>.*)\)')

    df_rally_overall["Class Rank"] = df_rally_overall.groupby("Class")["Pos"].rank(method='min')

    
    # Process retirements table
    if len(tables) > 1:
        pass
        #pd.DataFrame(tables[1])
    
    
    return df_rally_overall
    



# + tags=["active-ipynb"]
# rally_stub='59972-rallye-automobile-de-monte-carlo-2020'
# get_final(rally_stub, {'sct': 1682})
# -

# ## Itinerary

itinerary_path = 'https://www.ewrc-results.com/timetable/{stub}/'


# +

def get_itinerary(stub, params=None):
    """Scrape intinerary page."""
    soup = soupify(itinerary_path.format(stub=stub))
    
    # We now need to find this explicitly by text
    #event_dist = soup.find('td',text='Event total').parent.find_all('td')[-1].text
    event_dist = soup.find('div', text=re.compile('Event total.*')).text
    
    # This is no longer a table... need to scrape divs...
    # Maybe: find divs, iterate until we hit next table etc?
    itinerary_rows = soup.find('div', {'class':'harm-main'}).find_all('div', {'class':'harm'})
    row_items = []
    for row in itinerary_rows:
        items = []
        #each row contains 6 divs
        for i in row.find_all('div'):
            items.append(i.text)
        row_items.append(items)
    #print(pd.DataFrame(row_items))
    # 
    itinerary_df = pd.DataFrame(row_items)
    itinerary_df.columns = ['Stage','Name', 'distance', 'Date', 'Time', 'Other']
    itinerary_df['Date'] = itinerary_df['Date'].replace('', nan).ffill()
    
    itinerary_df['Leg'] = [nan if 'leg' not in str(x) else str(x).replace('. leg','') for x in itinerary_df['Stage']]
    itinerary_df['Leg'] = itinerary_df['Leg'].fillna(method='ffill')
    itinerary_df['Date'] = itinerary_df['Date'].fillna(method='ffill')
    
    #What if we have no stage name?
    itinerary_df['Name'].fillna('', inplace=True)
    itinerary_df['Cancelled'] = itinerary_df['Name'].str.contains('(?i)cancelled') #(?i) ignore case
    itinerary_leg_totals = itinerary_df[itinerary_df['Name'].str.contains("Leg total")][['Leg', 'distance']].reset_index(drop=True)
    
    # The full itinerary includes shakedown, service etc
    full_itinerary_df = itinerary_df[~itinerary_df['Name'].str.contains(". leg")]
    full_itinerary_df = full_itinerary_df[~full_itinerary_df['Date'].str.contains(" km")]
    full_itinerary_df = full_itinerary_df.fillna(method='bfill', axis=1)
    #Legs may not be identified but we may want to identify services
    full_itinerary_df['Service'] = [n.startswith('Flexi') or n.startswith('Service') for n in full_itinerary_df['Name']]
    #full_itinerary_df['Service'] = [ 'Service' in i for i in full_itinerary_df['distance'] ]
    full_itinerary_df['Service_Num'] = full_itinerary_df['Service'].cumsum()
    full_itinerary_df.reset_index(drop=True, inplace=True)
    # TO DO - handle this better
    full_itinerary_df['Leg'] = full_itinerary_df['Leg'].fillna(0)

    itinerary_df = full_itinerary_df[~full_itinerary_df['Service']].reset_index(drop=True)
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
#
# # TO DO - are we missing legs?
# -

# ## Entry List
#
# Get the entry list.

entrylist_path = "https://www.ewrc-results.com/entries/{stub}/?{params}"


def get_entry_list(stub, params=None):
    params = urllib.parse.urlencode(params) if params else ''
    entrylist_url = entrylist_path.format(stub=stub, params=params)
    print(entrylist_url)
    soup = soupify(entrylist_url)
    if not soup:
        return pd.DataFrame()

    
    base_cols = ['CarNum', 'DriverName','CoDriverName','Team','Car','Class', 'Category', 'Type']
    
    entrylist_table = soup.find('div',{'class':'mt-1'})
    if entrylist_table:
        entrylist_table = entrylist_table.find('table', {'class': 'results'}).find_all('tr')
    else:
        return pd.DataFrame(columns=base_cols+['carNum'])
    
    # TO DO parse into the structure of the table by iterating each row
    table_rows = []
    for row in entrylist_table:
        items = []
        items.append(row.find('td', {'class':'text-left'}).text) # Car number
        items.append(row.find('a', {'title': 'Show driver profile'}).text)
        _codriver = row.find('a', {'title': 'Show codriver profile'})
        if _codriver:
            items.append(_codriver.text)
        items.append(row.find('td', {'class':'lh-130'}).text) #car
        items.append(row.find('td', {'class':'lh-130'}).text) #team
        items.append(row.find('td', {'class':'fs-091'}).text) #class
        items.append(row.find('td', {'class':'startlist-m'}).text) #category
        items.append(row.find('td', {'class':'entry-sct'}).text) #type
        table_rows.append(items)
    df_entrylist = pd.DataFrame(table_rows)
    for i in range(len(df_entrylist.columns) - len(base_cols)):
        base_cols.append(f'Meta_{i}')
    df_entrylist.columns = base_cols
    df_entrylist['carNum'] = df_entrylist['CarNum'].str.extract(r'#(.*)')
    
    # TEST CLASS CHECK TO DO
    #df_entrylist["Class"] = df_entrylist["Class"].apply(lambda x: str(x).strip().split()[0])
    
    return df_entrylist.dropna(subset=['carNum'])


# + tags=["active-ipynb"]
# get_entry_list(stub)
# #get_entry_list('66881-aci-rally-monza-2020/') #Breaks
# #61089-rally-islas-canarias-2020  breaks
# get_entry_list('61089-rally-islas-canarias-2020/')
# -

# ## Rebasers
#
# Utils for rebasing

def _rebaseTimes(times, bib=None, basetimes=None):
    ''' Rebase times relative to specified driver. '''
    # Sometimes this errors depending on driver - check why...
    #Should we rebase against entryId, so need to lool that up. In which case, leave index as entryId
    if (bib is None and basetimes is None): return times
    #bibid = codes[codes['Code']==bib].index.tolist()[0]
    if bib is not None:
        return times - times.loc[bib]
    if times is not None and basetimes is not None:
        return times - basetimes
    return times


# ## `EWRC` Class
# Create a class to that can be used to gran all the results for a particular rally, as required.
#
# We fudge the definition of class functions so that we can separately define and functions in a standalione way. This is probably *not good practice*...!

# +
# TO DO
# At the moment, we handle cacheing in the class
# Instead, cache using requests and always make the call from the class
# -

class EWRC:
    """Class for eWRC data for a particular rally."""

    def __init__(self, stub='', base='https://www.ewrc-results.com', path='', live=False, stages=None):
        _path = stub.split('?')
        self.base_url = base
        self.path = _path[1] if len(_path)==2 else path
        self.stub = [s for s in _path[0].split('/') if s][-1]
        self.live = live
        self.stages = [] if stages is None else stages
        self.stage_result_links = None
        
        self.raw_base_stages = []
        self.raw_base_categories = []
        self.base_stages = []
        self.base_classes = []
        self.base_championships = []
        self.rally_championship = ''
        self.rally_class = ''
        try:
            self.raw_base_stages, self.raw_base_categories = homepage_quick_scrape(stub)
            self.set_base_stages()
            self.set_base_classes()
            self.set_base_championships()
        except:
            pass
        
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
        self.df_full_itinerary = None

        self.stage_distances_all = None
        self.stage_distances = None
        
        self.df_entry_list = None
        self.rally_classes = None
        
        self.entryFromCar = None
        self.carFromEntry = None
        self.driverNumFromEntry = None
 
        self.df_stage_result = pd.DataFrame(columns=stage_result_cols)
        self.df_stage_overall = pd.DataFrame(columns=stage_overall_cols)
        self.df_stage_retirements = pd.DataFrame(columns=retirement_cols+retirement_extra_cols)
        self.df_stage_penalties = pd.DataFrame(columns=penalty_cols+penalty_extra_cols)

    def generate_url(self, params):
        """
        Generate a URL for a particular selection.
        """
        # There seems o be a snafu in the arg name used on different paths for the class/championship.
        # This means we need to craft params by hand depneding on the page we want to load.
        return f'{urljoin(self.base_url, self.stub, trailing=True)}?{urllib.parse.urlencode(params)}'
        
    def set_base_stages(self):
        """Report base stages."""
        
        # TO DO  - what do "base stages" mean?
        self.base_stages = {}
        for s in self.raw_base_stages:
            if s[1]:
                self.base_stages[s[2].split()[0]] = {'name': ' '.join(s[2].split()[1:]).split('-')[0].strip(), 'link':s[1]} 
        return self.base_stages
    
    def set_base_classes(self):
        """Retrieve base classes."""
        i = len(self.raw_base_categories)
        self.base_classes = {}
        for c in self.raw_base_categories[i-1]:
            if '&' in c[1]:
                self.base_classes[c[0]] = c[1].split('&')[-1]
            else:
                self.base_classes[c[0]] = ''
        return self.base_classes
    
    def set_base_championships(self):
        """Retrieve base championships."""
        # TO DO - this is broken; the new scraper can access each as a list
        self.base_championships = {}
        if len(self.raw_base_categories) == 2:
            for c in self.raw_base_categories[0]:
                if '&' in c[1]:
                    self.base_championships[c[0]] = c[1].split('&')[-1]
                else:
                    self.base_championships[c[0]] = ''
        else:
            self.base_championships = {'All':''}
        return self.base_championships

    def annotateEntryWithEventDriverId(self):
        """Add class entry ID to df_entry_list."""
        if hasattr(self, 'entryFromCar'):
            self.get_stage_times()
            
        self.df_entry_list['driverEntry']  = self.df_entry_list['carNum'].map(self.entryFromCar)
        self.df_entry_list['driverCar'] = self.df_entry_list['carNum'] + ': '+self.df_entry_list['DriverName']
        self.driverNumFromEntry = self.df_entry_list[['driverCar','driverEntry']].set_index('driverEntry').to_dict()['driverCar']
        
    def df_inclass_cars(self, _df, rally_class='all', typ='entryId'):
        """Get cars in particular class."""
        if rally_class != 'all':
            _df = _df[_df.index.isin(self.carsInClass(rally_class, typ=typ))]
        return _df

    def carsInClass(self, qclass, typ='carNum'):
        #Can't we also pass a dict of key/vals to the widget?
        #Omit car 0
        df_entry_list = self.get_entry_list()
        if not qclass:
            return []
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
            _carFromEntry = df['carNum'].to_dict()
            if not self.carFromEntry:
                self.carFromEntry = {}
            self.carFromEntry = {**self.carFromEntry, **_carFromEntry}
            if not self.entryFromCar:
                self.entryFromCar = {}
            _entryFromCar = {v:k for (k, v) in self.carFromEntry.items()}
            self.entryFromCar = {**self.entryFromCar, **_entryFromCar}
    
    def get_final(self):
        if self.df_rally_overall is None:
            self.df_rally_overall = get_final(self.stub, params={'sct':self.rally_championship, 'ct':self.rally_class})
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
        # Stage distances do not identify cancelled stages
        # The following is the correct stage index
        #_stage_distances.index = [int(i.lstrip('SS')) for i in _stage_distances.index]
        #As a hack, to cope with cancelled stages, reindex
        _stage_distances.reset_index(drop=True, inplace=True)
        _stage_distances.index += 1 
        self.stage_distances = _stage_distances
        self.stage_distances_all = self.df_itinerary['Distance']

        return self.event_dist, self.df_itinerary_leg_totals, \
                self.df_itinerary, self.df_full_itinerary_df

    def get_entry_list(self):
        if self.df_entry_list is None:
            #self.df_entry_list = get_entry_list(self.stub, self.path)
            self.df_entry_list = get_entry_list(self.stub, params={'sct':self.rally_championship, 'cat':self.rally_class})
        #A list of classes could be useful, so grab it while we can
        self.rally_classes = self.df_entry_list['Class'].dropna().unique()
        
        self.annotateEntryWithEventDriverId()
        return self.df_entry_list
    
    def get_stage_result_links(self):
        if self.stage_result_links is None:
            self.stage_result_links = get_stage_result_links(self.stub, params={'sct':self.rally_championship, 'ct':self.rally_class})
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
            #print(links)
            if 'all' in stages:
                #print('all')
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
# print(rally_stub+'/') #60140-rally-sweden-2020/
# print('66881-aci-rally-monza-2020/'+'/') # stages 10 and 12 cancelled TO TEST BREAKS
# # Also breaks '61089-rally-islas-canarias-2020/
# ewrc=EWRC('61089-rally-islas-canarias-2020')

# + tags=["active-ipynb"]
# ewrc.base_stages
# # Expect {'SS2': {'name': 'Hof', 'link': '/results/60140-rally-sweden-2020/?s=250433'},
# # 'SS3': {'name': 'Finnskogen 1',
# #  'link': '/results/60140-rally-sweden-2020/?s=250434'}, etc

# + tags=["active-ipynb"]
# #sct=10
# #ewrc.rally_championship = 10
# ewrc.get_stage_times()
# ewrc.df_allInOne

# + tags=["active-ipynb"]
# '''
# {'All': '',
#  'RC1': 'ct=1017',
#  'RC2': 'ct=1018',
#  'RC4': 'ct=1020',
#  'NAT4': 'ct=4742'}
#  '''
#
# ewrc.base_classes

# + tags=["active-ipynb"]
# '''
# {'All': '',
#  'WRC 2': 'sct=1682',
#  'JWRC': 'sct=10',
#  'WRC 3': 'sct=1681',
#  'M': 'group=M'}
# '''
# ewrc.base_championships

# + tags=["active-ipynb"]
# #df_stage_result, df_stage_overall, df_stage_retirements, df_stage_penalties
#
# # FOr some reason ths was giving the penalties table?
# ewrc.get_stage_results('SS3')

# + tags=["active-ipynb"]
# not pd.isnull(ewrc.df_itinerary)

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS2')[0]

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS11')#[0] # Get an error on cancelled stage - rogue results?

# + tags=["active-ipynb"]
# ewrc.set_rebased_times()
# display(ewrc.df_stages_rebased_to_overall_leader)

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS1')#[2]

# + tags=["active-ipynb"]
# ewrc.get_stage_result_links()

# + tags=["active-ipynb"]
# ewrc.stub

# + tags=["active-ipynb"]
# ewrc.get_stage_results('SS1')[3]

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

