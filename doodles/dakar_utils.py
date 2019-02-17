# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.6
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import matplotlib.pyplot as plt

def _get_col_loc(df, col=None, pos=None, left_of=None, right_of=None):
    ''' Return column position number. '''
    if col in df.columns:
        return df.columns.get_loc(col)
    elif pos and pos <len(df.columns):
        return pos
    else:
        pos = 0
    if left_of in df.columns:
        pos = df.columns.get_loc(left_of)
    elif right_of in df.columns:
        pos = min(df.columns.get_loc(right_of)+1,len(df.columns)-1)
    return pos

def moveColumn(df, col, pos=None, left_of=None, right_of=None):
    ''' Move dataframe column adjacent to a specified column. '''
    pos = _get_col_loc(df, None, pos, left_of, right_of)
    
    data = df[col].tolist()
    df.drop(col, axis=1, inplace=True)
    df.insert(pos, col, data)

def insertColumn(df, col, data, pos=None, left_of=None, right_of=None):
    ''' Insert data in dataframe column at specified location. '''
    pos =  _get_col_loc(df, col, pos, left_of, right_of)
    print(pos)
    df.insert(pos, col, data)


#https://github.com/iiSeymour/sparkline-nb/blob/master/sparkline-nb.ipynb
import matplotlib.pyplot as plt

from io import BytesIO
import urllib
import base64

import scipy
import pandas as pd

def fig2inlinehtml(fig):
    figfile = BytesIO()
    fig.savefig(figfile, format='png')
    figfile.seek(0) 
    figdata_png = base64.b64encode(figfile.getvalue())
    #imgstr = '<img src="data:image/png;base64,{}" />'.format(figdata_png)
    imgstr = '<img src="data:image/png;base64,{}" />'.format(urllib.parse.quote(figdata_png))
    return imgstr

def sparkline2(data, figsize=(2, 0.5), colband=(('red','green'),('red','green')),
               dot=False, typ='line', **kwargs):
    """
    Returns a HTML image tag containing a base64 encoded sparkline style plot
    """
    #data = [0 if pd.isnull(d) else d for d in data]
    
    fig, ax = plt.subplots(1, 1, figsize=figsize, **kwargs)
    
    if typ=='bar':
        color=[ colband[0][0] if c<0 else colband[0][1] for c in data  ]
        ax.plot(range(len(data)), [0]*len(data), linestyle='-', color='lightgrey')
        ax.bar(range(len(data)),data, color=color, width=0.8)
        if dot:
            dot = colband[1][0] if data[len(data) - 1] <0 else colband[1][1]
            for idx, val in enumerate(data):
                if val==0.0:
                    ax.plot(idx, 0,'green', marker='.')#, dot)
    else:
        #Default is line plot
        ax.plot(data, linewidth=0.0)
        d = scipy.zeros(len(data))

        #If we don't interpolate, we get a gap in the sections/waypoints
        #  where times change sign compared to the previous section/waypoint
        ax.fill_between(range(len(data)), data, where=data<d, interpolate=True, color=colband[0][0])
        ax.fill_between(range(len(data)), data, where=data>d, interpolate=True,  color=colband[0][1])

        if dot:
            dot = colband[1][0] if data[len(data) - 1] <0 else colband[1][1]
            plt.plot(len(data) - 1, data[len(data) - 1], dot)

    for k,v in ax.spines.items():
        v.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])

    #No print
    plt.close(fig)
    
    return fig2inlinehtml(fig)

def sparklineStep(data, figsize=(2, 0.5), dot=False, **kwags):
    #data = [0 if pd.isnull(d) else d for d in data]
    
    fig, ax = plt.subplots(1, 1, figsize=figsize, **kwags)
    
    plt.axhspan(-1, -3, facecolor='lightgrey', alpha=0.5)
    #ax.plot(range(len(data)), [-3]*len(data), linestyle=':', color='lightgrey')
    #ax.plot(range(len(data)), [-1]*len(data), linestyle=':', color='lightgrey')
    ax.plot(range(len(data)), [-10]*len(data), linestyle=':', color='lightgrey')
    ax.step(range(len(data)), data, where='mid')

    ax.set_ylim(top=-0.9)
        
    for k,v in ax.spines.items():
        v.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    
    #if dot:
    #    for idx, val in enumerate(data):
    #        #print(val,type(val))
    #        if val==-1:
    #            ax.plot(idx, -0.5,'o')
    
    #No print
    plt.close(fig)
    
    return fig2inlinehtml(fig)


import seaborn as sns

from IPython.core.display import HTML
import seaborn as sns

from numpy import NaN
from math import nan

def bg_color(s):
    ''' Set background colour sensitive to time gained or lost.
    '''
    attrs=[]
    for _s in s:        
        if _s < 0:
            attr = 'background-color: green; color: white'
        elif _s > 0: 
            attr = 'background-color: red; color: white'
        else:
            attr = ''
        attrs.append(attr)
    return attrs

#https://pandas.pydata.org/pandas-docs/stable/style.html
def color_negative(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    if isinstance(val, str) or pd.isnull(val): return ''
    
    
    val = val.total_seconds() if isinstance(val,pd._libs.tslibs.timedeltas.Timedelta) else val
    
    if val and (isinstance(val,int) or isinstance(val,float)):
        color = 'green' if val < 0 else 'red' if val > 0  else 'black'
    else:
        color='white'
    return 'color: %s' % color


def moreStyleDriverSplitReportBaseDataframe(rb2,ss, caption=None):
    ''' Style the driver split report dataframe. '''
    
    if rb2.empty: return ''
        
    def _subsetter(cols, items):
        ''' Generate a subset of valid columns from a list. '''
        return [c for c in cols if c in items]
    
    
    #https://community.modeanalytics.com/gallery/python_dataframe_styling/
    # Set CSS properties for th elements in dataframe
    th_props = [
      ('font-size', '11px'),
      ('text-align', 'center'),
      ('font-weight', 'bold'),
      ('color', '#6d6d6d'),
      ('background-color', '#f7f7f9')
      ]

    # Set CSS properties for td elements in dataframe
    td_props = [
      ('font-size', '11px'),
      ]

    # Set table styles
    styles = [
      dict(selector="th", props=th_props),
      dict(selector="td", props=td_props)
      ]
    
    #Define colour palettes
    #cmg = sns.light_palette("green", as_cmap=True)
    #The blue palette helps us scale the Road Position column
    # This may help us to help identify any obvious road position effect when sorting stage times by stage rank
    cm=sns.light_palette((210, 90, 60), input="husl",as_cmap=True)
    s2=(rb2.style
        .background_gradient(cmap=cm, subset=_subsetter(rb2.columns, ['Road Position', 'Pos','Overall Position', 'Previous Overall Position']))
        .applymap(color_negative,
                  subset=[c for c in rb2.columns if rb2[c].dtype==float and (not c.startswith('D') and c not in ['Overall Position','Overall Gap','Road Position', 'Pos'])])
        .highlight_min(subset=_subsetter(rb2.columns, ['Overall Position','Previous Overall Position']), color='lightgrey')
        .highlight_max(subset=_subsetter(rb2.columns, ['Overall Time', 'Overall Gap']), color='lightgrey')
        .highlight_max(subset=_subsetter(rb2.columns, ['Previous']), color='lightgrey')
        .apply(bg_color,subset=_subsetter(rb2.columns, ['{} Overall'.format(ss), 'Overall Time','Overall Gap', 'Previous', 'Stage Overall']))
        .bar(subset=[c for c in rb2.columns if str(c).startswith('D')], align='zero', color=[ '#5fba7d','#d65f5f'])
        .bar(subset=[c for c in rb2.columns if str(c).startswith('SS') and not str(c).endswith('_overall')], align='zero', color=[ '#5fba7d','#d65f5f'])
        .set_table_styles(styles)
        #.format({'total_amt_usd_pct_diff': "{:.2%}"})
       )
    
    if caption is not None:
        s2.set_caption(caption)

    #nan issue: https://github.com/pandas-dev/pandas/issues/21527
    return s2.render().replace('nan','')

import os
import time
from selenium import webdriver

#Via https://stackoverflow.com/a/52572919/454773
def setup_screenshot(driver,path):
    # Ref: https://stackoverflow.com/a/52572919/
    original_size = driver.get_window_size()
    required_width = driver.execute_script('return document.body.parentNode.scrollWidth')
    required_height = driver.execute_script('return document.body.parentNode.scrollHeight')
    driver.set_window_size(required_width, required_height)
    # driver.save_screenshot(path)  # has scrollbar
    driver.find_element_by_tag_name('body').screenshot(path)  # avoids scrollbar
    driver.set_window_size(original_size['width'], original_size['height'])


def getTableImage(url, fn='dummy_table', basepath='.', path='.', delay=5, scale_factor=2, height=420, width=800):
    ''' Render HTML file in browser and grab a screenshot. '''
    
    opt = webdriver.ChromeOptions()
    opt.add_argument('--force-device-scale-factor={}'.format(scale_factor))

    browser = webdriver.Chrome(options=opt)
    
    #browser.set_window_size(width, height)
    browser.get(url)
    #Give the map tiles some time to load
    time.sleep(delay)
    imgpath='{}/{}.png'.format(path,fn)
    imgfn = '{}/{}'.format(basepath, imgpath)
    imgfile = '{}/{}'.format(os.getcwd(),imgfn)
    
    setup_screenshot(browser,imgfile)
    browser.quit()
    os.remove(imgfile.replace('.png','.html'))
    #print(imgfn)
    return imgpath



def getTablePNG(tablehtml,basepath='.', path='testpng', fnstub='testhtml', scale_factor=2):
    ''' Save HTML table as file. '''
    if not os.path.exists(path):
        os.makedirs('{}/{}'.format(basepath, path))
    fn='{cwd}/{basepath}/{path}/{fn}.html'.format(cwd=os.getcwd(), basepath=basepath, path=path,fn=fnstub)
    tmpurl='file://{fn}'.format(fn=fn)
    with open(fn, 'w') as out:
        out.write(tablehtml)
    return getTableImage(tmpurl, fnstub, basepath, path, scale_factor=scale_factor)
