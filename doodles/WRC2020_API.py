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

# # WRC 2020 API
#
# Package to support querying the WRC API.
#
# *TO DO - build this up from stuff in `WRC API 2019 Base.ipynb`.*

# ### Query Grammar
#
# Start to try to work out the grammar.
#
# #### ByType
#
# Of the form:  `https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22TYPE%22&maxdepth=2`
#
# A `Season` is made up of `Event`s.
#
# (An `Event` includes a `rosterid` that can be used to reference a `Roster`.)
#
# An `Event` has `Session`s.
#
# A `Session` has `Entry`s.
#
# An `Entry` has a `Car` and `Clip`s.
#
# (There is also a `Car_2016` type for the 2016 season.)
#
# A `Roster` has `RosterEntry`s.
#
# A `RosterEntry` is made from a `Car`.
#
# A `Car` has two `Driver`s (one is the driver, the other the co-driver) and a `Team`.
#
#
#
#
# #### QueryMeta
#
# We can be more focussed with queries into an object class narrowed down by an n.v pair using queries of the form: `https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/queryMeta?t="TYPE"&p={"n":"NOUN","v":"VALUE"}&maxdepth=1`
#
#
#

import requests
import json
import pandas as pd
from pandas.io.json import json_normalize


def nvToDict(nvdict, key='n',val='v', retdict=None):
    """Translate {n: x, v: y} dict to an {x: y} dict."""
    if retdict is None:
        retdict={nvdict[key]:nvdict[val]}
    else:
        retdict[nvdict[key]]=nvdict[val]
    return retdict



assert (nvToDict({'n': "id",'v': "adac-rallye-deutschland"}) ==
        {'id': 'adac-rallye-deutschland'})


# +
def _getEventMetadata():
    ''' Get event metadata as JSON data feed from WRC API. '''
    url='https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22Event%22&maxdepth=1'
    eventmeta = requests.get(url).json()
    
    return eventmeta

def getEventMetadata():
    ''' Get a list of events from WRC as a flat pandas dataframe.
        Itinerary / event data is only available for rallies starting in stated year. '''
    eventMetadata = json_normalize(_getEventMetadata(),
                                   record_path='_meta',
                                   meta='_id'  ).drop_duplicates().pivot('_id', 'n','v').reset_index()

    eventMetadata['date-finish']=pd.to_datetime(eventMetadata['date-finish'])
    eventMetadata['date-start']=pd.to_datetime(eventMetadata['date-start'])
    eventMetadata['year'] = eventMetadata['date-start'].dt.year
    
    return eventMetadata



# -

getEventMetadata().head()

# + tags=["active-ipynb"]
# zz = getEventMetadata()
# zz.head()

# + tags=["active-ipynb"]
# zz[zz['year']==2020].head(2)
# -

externalIdRally	externalIdEvent	timezone	active	countdown	jwrc	images.format16x9.320x180	images.format16x9.160x90	...	winner.birthDate	winner.birthPlace	winner.debutDate	winner.debutPlace	winner.website	winner.driverImageFormats	winner.externalId	winner.page	winner	seasonYear
0	100	Rallye Monte Carlo	153	124

zz[zz['sas-rallyid']=='153']  # externalIdRally

zz[zz['sas-eventid']=="124"]  # externalIdEvent
# Also: zz[zz['sitid']

# + tags=["active-ipynb"]
# zz[zz['sas-eventid']=="124"].to_dict()
# -


