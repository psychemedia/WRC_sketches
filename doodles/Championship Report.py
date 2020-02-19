# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Championship Reports
#
# Start to construct a series of reports around the championships run each season.

# + {"tags": ["active"]}
import pandas as pd
import sqlite3


# + {"tags": ["active-ipynb"]}
# dbname2='../../wrc-timing/finland19a.db'
# conn = sqlite3.connect(dbname2)
#
# #c2 = conn2.cursor()
#

# +
def get_seasonId():
    q="SELECT * FROM season;"
    return pd.read_sql(q,conn)

get_seasonId()


# +
def get_championship_rounds_by_season(year):
    q='''
    SELECT sc.name, sc.type, s.year FROM season_championships sc
    JOIN season s
    ON sc.seasonId = s.seasonId
    WHERE s.year={year};
    '''.format(year=year)
    return pd.read_sql(q,conn)

get_championship_rounds_by_season(2019)
# -

q="SELECT * FROM championship_lookup;"
pd.read_sql(q,conn)#.columns

q="SELECT * FROM championship_events;"
pd.read_sql(q,conn).columns

# +
qr = '''CREATE TABLE "season" (
  "name" TEXT,
  "seasonId" INTEGER,
  "year" INTEGER
);'''

c = conn.cursor()
c.executescript(qr)
# -


