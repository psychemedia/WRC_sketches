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

# # `WRC2020_geo` - Geo Tools for Working With Stage Maps
#
# Official WRC KML route files are available that can be converted to geojson and used to generate a range of geographical views over routes and timing data.
#
# The [`Rally-Maps.com`](https://www.rally-maps.com/home) website has routes for a wide range of rallies which we can also get hold of...

# +
import os
import requests

import pandas as pd
import geopandas as gpd
from sqlite_utils import Database
# -

# ## Input Data Files 
#
# Various tools for coping with geo data files of various formats, for example, geojson and KML.

# #!pip3 install kml2geojson
import kml2geojson


class WRC2020kmlbase:
    """Class for working with KML files."""
    
    def __init__(self):
        """Initialise KML utils."""
        pass
    

    def kml_to_json(kml_slug,indirname='maps', outdirname='geojson'):
        kml2geojson.main.convert('{}/{}.xml'.format(indirname,kml_slug),outdirname)

    def kml_processor(df_rallydata, indirname='maps',outdirname='geojson'):
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        for kml_slug in get_kml_slugs(df_rallydata):
            get_kml_file(kml_slug)
            kml_to_json(kml_slug,indirname,outdirname)


class WRC2020kml(WRC2020kmlbase):
    """Class for working with WRC KML files."""
    
    def __init__(self):
        """Initialise WRC KML tools."""
        WRC2020kmlbase.__init__(self)
        
    def get_kml_slugs(df_rallydata):
        return df_rallydata['kmlfile'].unique().tolist()

    def get_kml_file(kml_slug):
        kmlurl = 'https://webappsdata.wrc.com/web/obc/kml/{}.xml'.format(kml_slug)
        r=requests.get(kmlurl)  
        with open("{}.xml".format(kml_slug), 'wb') as f:
            f.write(r.content)



# kmlfile
kmlstub = zz.loc[zz['sas-eventid']=="124", 'kmlfile'].iloc[0]
display(kmlstub)
kml_to_json(kmlstub,'.','.')

# ## Grabbing Geo Data Files
#
# Tools for accessing geodata files.

# ### WRC Route Files
#
# WRC route maps are available as KML datafiles. A base url provides a commnon path to the file, with an event specific slug identifying the actual filename.
#
# The slug can be found a metadata call to the WRC API.

# +
# # %pip install geojson-to-sqlite
# https://datasette.readthedocs.io/en/stable/spatialite.html
# Linux: apt-get install spatialite-bin libsqlite3-mod-spatialite
# Mac: brew install spatialite-tools

# ! geojson-to-sqlite testgeo.db montecarlo_2020 montecarlo_2020.geojson --spatialite

# +
gdb = Database('testgeo.db')

query = "SELECT * FROM sqlite_master where type='table';"
pd.read_sql(query, gdb.conn)
# -

query = "SELECT * FROM montecarlo_2020 LIMIT 3;"
pd.read_sql(query, gdb.conn)

# +
# #%pip install --upgrade geopandas

gdf = geopandas.read_file("montecarlo_2020.geojson")
gdf.head()

# +
# %matplotlib inline


gdf['geometry'].plot();
# -

gdf['geometry'].iloc[3].length

gdf['geometry'].iloc[3]

# ### Scraping `Rally-Maps.com`
#
# [`Rally-Maps.com`](https://www.rally-maps.com/home) provides access to itineraries and stage maps for a wide range of rallies.
#
# Whilst there is no obvious API for accessing the route data, we can grab it out of the leaflet maps published by the website using a little bit of browser automation...
#
# You may notice that the following recipe asks for no permission to grab the route data, it just extracts it from a public web page. So please don't abuse it. I use the recipe as part of my own rally data junkie habit, which is to say, as a purely personal leisure distraction.

# eg view-source:
# in sl.leaflet.data.storage.addData()

# +

url = "https://www.rally-maps.com/Rallye-Perce-Neige-2020"

# -

# first thought was beautiful soup, script tag, try and parse the js?
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
options = Options()
options.headless = True
browser = webdriver.Firefox(options = options)
browser.get(url)

#https://apimirror.com/javascript/errors/cyclic_object_value
jss = '''const getCircularReplacer = () => {
  const seen = new WeakSet();
  return (key, value) => {
    if (typeof value === "object" && value !== null) {
      if (seen.has(value)) {
        return;
      }
      seen.add(value);
    }
    return value;
  };
};

//https://stackoverflow.com/a/10455320/454773
return JSON.stringify(sl.leaflet.data.storage.stages, getCircularReplacer());

'''


import json
js_data = json.loads(browser.execute_script(jss))
browser.close()

js_data



# # rotate
#
#
# $$
# \begin{bmatrix}
# \cos\theta & -\sin\theta \\
# \sin\theta & \cos\theta
# \end{bmatrix}
# .
# \begin{bmatrix}
# x \\
# y
# \end{bmatrix}
# =
# \begin{bmatrix}
# x\cos\theta - y\sin\theta \\
# x\sin\theta + y\cos\theta
# \end{bmatrix}
# $$

# +
import numpy as np

def rotMatrix(angle):
    """Rotate vector through an angle."""
    c = np.cos(np.radians(angle))
    s = np.sin(np.radians(angle))
    return np.array([[c, -s], [s, c]])

p = [0, 1]
rotMatrix(90).dot(p)
# -
# ## Live Pace
#
# Do the 1km map segment rows, then dist along x, y as stagetime, and plot on-route distance and stage time by driver. Requires knowing the start time. We can get the start time from getSplitTimes first split or startlist.
#



