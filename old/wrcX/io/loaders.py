import requests
import requests_cache
from pandas import read_html, DataFrame
import wrcX.core

import pandas.io.html as ih
import re

def _debug(msg):
    print(msg)

NOCACHE=False  
DEBUG = True

if wrcX.core.cachedb is not None:
    requests_cache.install_cache(wrcX.core.cachedb,old_data_on_error=True,expire_after=None)
        
def validateLoadedDF(df):
    if (len(df.columns)==1) | (not df.size):
        return False
    return True

### PATCHES
#via http://stackoverflow.com/a/28173933/454773
def stringify_children(self,node):
    from lxml.etree import tostring
    from itertools import chain
    parts = ([node.text] +
            list(chain(*([tostring(c, with_tail=False), c.tail] for c in node.getchildren()))) +
            [node.tail])
    txt='\n'.join(str(v.decode()) if isinstance(v, bytes) else str(v) for v in filter(None, parts))
    txt=txt.replace('<br/>','\n')
    txt=re.sub('\n\n+','\n',txt)
    return txt

def nostrip(self, rows):
        """Parse the raw data into a list of lists.
        Parameters
        ----------
        rows : iterable of node-like
            A list of row elements.
        text_getter : callable
            A callable that gets the text from an individual node. This must be
            defined by subclasses.
        column_finder : callable
            A callable that takes a row node as input and returns a list of the
            column node in that row. This must be defined by subclasses.
        Returns
        -------
        data : list of list of strings
        """
        #TH: omit the whitespace stripper from the original function
        data = [[self._text_getter(col) for col in
                 self._parse_td(row)] for row in rows]
        return data

ih._LxmlFrameParser._text_getter=stringify_children
ih._HtmlFrameParser._parse_raw_data=nostrip

#### END PATCHES   


    
def robustLoad(url):

    if NOCACHE or wrcX.core.cacheReset:
        #Try live load else fall back
        try:
            with requests_cache.disabled():
                r=requests.get(url)
                if r.status_code!= requests.codes.ok:
                    raise ValueError('No good')
                #If this is good, we really want to then add it to the cache?
        except:
            #Problem with live URL so fall back to cache, if it's enabled...
            #If it isn't, we may get an error and fail anyway
            r=requests.get(url)
    else:
        r=requests.get(url)
    #We can check whether the cache result was used via:
    if DEBUG:
        _debug(('Used cache for {}:'.format(url),
                     getattr(r, 'from_cache', False)))
    return r


def loader(url,**kwargs):
    #try:
    if wrcX.core.cachedb is not None:
        df=read_html(robustLoad(url).text,encoding='utf-8',**kwargs)
    else:
        df=read_html(url,encoding='utf-8',**kwargs)
    #except:
    #    df=[DataFrame()]
    #The original pandas HTML table reader strips whitespace
    #If we disable that as part of gaining access to cell HTML contents, we need to make up for it
    for ddf in df:
        ddf[ddf.select_dtypes(include=['O']).columns]=ddf.select_dtypes(include=['O']).apply(lambda x: x.str.strip())
    return df
#END loading tools
