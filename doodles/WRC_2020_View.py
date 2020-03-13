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

# # WRC2020_View
#
# Visualisers applied directly to `WRCEvent` data object.

# %load_ext autoreload
# %autoreload 2

from WRC_2020 import WRCEvent

# +
# !rm wrc2020swetest1.db
event = WRCEvent(dbname='wrc2020swetest1.db')

event.rallyslurper()
# -

event.getSplitTimes()[1][1566]

# TO DO  - do we have startlists for each stage?
event.getStartlist()

event.stages

dir(event)


