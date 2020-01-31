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

# # MotorsportView2020
#
# A set of base classes for representing motorsport data.

from pandas import Timedelta, NaT
from numpy import nan
import pandas as pd

# ## Basic Time Utilities
#
# We will use `pandas.Timedelta` as the base representation of time.
#
# TO DO - should we return NaT? `if NaT` evaluates as `True` which means null tests would have to ve using `pd.isnull()`.

# + tags=["active-ipynb"]
# assert bool(None) ==  False
# assert bool('') ==  False
#
# assert bool(NaT) == True
# assert bool(nan) == True
#
# assert pd.isnull(NaT) == True 
# assert pd.isnull(nan) == True
# -

# The following function will try to generate timedeltas.

# +
BASEUNIT = 's'  # If the BASEUNIT is NOT 's', some tests will break

def asTimedelta(t, unit=BASEUNIT):
    """Return a timedelta if possible."""
    if isinstance(t, Timedelta):
        return t
    if isinstance(t, int):
        return Timedelta(t, unit=unit)
    if isinstance(t, str):
        try:
            t = Timedelta(int(t), unit=unit)
            return t
        except:
            return None
    if isinstance(t, list):
        t = [asTimedelta(_t) for _t in t]
        return t
    return None


# + tags=["active-ipynb"]
# # Return a timedelta as a timedelta
# assert asTimedelta(Timedelta(1, 's')) == Timedelta(1, 's')
# assert asTimedelta(Timedelta(1000,'ms')) == Timedelta(1, 's')
# # Cast an int to a timedelta
# assert asTimedelta(2) == Timedelta(2, unit='s')
# assert asTimedelta(2000, unit='ms') == Timedelta(2, unit='s')
# # Cast a list of ints to a list of timedeltas
# assert asTimedelta([1, 2]) == [Timedelta(1, 's'), Timedelta(2, 's')]
# # Cast a string to a time delta
# assert asTimedelta("2000", unit='ms') == Timedelta(2, unit='s')
# assert asTimedelta("s", unit='ms') == None
# assert asTimedelta("", unit='ms') == None
# assert asTimedelta(None, unit='ms') == None
# assert asTimedelta(NaT, unit='ms') == None
# # Cast a mixed list of strings and ints to a list of time deltas
# assert (asTimedelta(["2", 2, None, NaT], unit='s') ==
#         [Timedelta(2, unit='s'), Timedelta(2, unit='s'), None, None])
# -

# ## Time
#
# Simplest unit of time, with functions for handling it.

class Basetime:
    """Simplest unit of time. Can be returned as timedelta, s, ms."""

    def __init__(self, t, unit=BASEUNIT):
        """Basic unit of time."""
        self.t = Timedelta(t, unit=unit)
        self.s = self._s(self.t)
        self.ms = self._ms(self.t)

    def _s(self, td=None, unit='s'):
        """Time in seconds."""
        td = asTimedelta(td, unit)
        if td:
            return td.total_seconds()
        return td

    def _ms(self, td=None, unit='s'):
        td = asTimedelta(td, unit)
        if td:
            return 1000*td.total_seconds()
        return td

    def __repr__(self):
        """Display timedelta."""
        return repr(self.t)


# + tags=["active-ipynb"]
# # Check simple equivalence of Time to Timedelta
# assert Basetime(2).t == Timedelta(2, unit='s')
# assert Basetime(2000, unit='ms').t == Timedelta(2, unit='s')
# # Test seconds attribute
# assert Basetime(2).s == 2.0
# # Test milliseconds attribute
# assert Basetime(2).ms == 2000
# assert Basetime(2000, unit='ms').s == 2
# assert Basetime(2000, unit='ms').s == 2.0
# -

def asTime(atime=None, unit=BASEUNIT):
    """Generate a valid Time."""
    if isinstance(atime, tuple) and len(atime) == 3:
        _atime = atime
    else:
        _atime = (None, None, None)
        
    index = _atime[0]
    uid = _atime[1]
    time = asTimedelta(_atime[2], unit=unit)
        
    atime = (index, uid, time)
        
    return atime


# + tags=["active-ipynb"]
# # Check assignment of single time
# assert asTime((1, 2, '3')) == (1, 2, Timedelta(3, 's'))
# # If we don't get a tuple, return one filled with nulls
# assert asTime(1) == (None, None, None)
# # If we get a list, that's wrong too...
# assert (asTime([(1, 2, 3), (4, 5, '6')])) == (None, None, None)
# -

def asTimes(atime=None, unit=BASEUNIT):
    """Generate a list of valid Times."""
    if isinstance(atime, list):
        t = [asTime(_t, unit=unit) for _t in atime]
        return t
    return [asTime(atime, unit=unit)]


# + tags=["active-ipynb"]
# # Check assignment of list of times
# assert (asTimes([(1, 2, 3), (4, 5, '6')]) == 
#         [(1, 2, Timedelta(3, 's')), (4, 5, Timedelta(6, 's'))])
# # If we get a single time, return it as Time list
# assert asTimes((1, 2, '3')) == [(1, 2, Timedelta(3, 's'))]
# # If we get nonsense, retun null tuples
# assert asTimes(1) == [(None, None, None)]
# assert (asTimes([1, (4, 5, 6000), ('a', 1)], unit='ms') ==
#         [(None, None, None), (4, 5, Timedelta(6, 's')), (None, None, None)])
# -

class Time:
    """
    A time unit expresses an indexed time for an identified thing.
    
    Times are given as a 3-tuple: (index, uid, time).
    """

    def __init__(self, atime=None, unit=BASEUNIT):
        """Create a valid time."""
        atime = asTime(atime, unit=BASEUNIT)
        (self.index, self.uid, self.time) = atime
        self.atime = (self.index, self.uid, self.time)

    def __repr__(self):
        """Display time 3-tuple."""
        return repr(self.atime)


# + tags=["active-ipynb"]
# # Check we return a simple Time tuple
# assert Time((1, 2, '3')).atime == (1, 2, Timedelta(3, 's'))

# +
# TO DO  - labelled time - add a  label to the time tuple
# -

# ## Times
#
# Represent a set of times, eg laptimes, stagetimes, splittimes.

class Times():
    """Represent a list of times, eg laptimes, stagetimes, splittimes."""
    
    def __init__(self, times, unit=BASEUNIT):
        """Base representation of a set of times."""
        self.times = asTimes(times, unit=unit)
        
    def rebase(self, times=None, unit=BASEUNIT):
        """Rebase times"""
        pass


# Check a single time is expressed as a list of Times
assert Times((1, 2, 3)).times == [(1, 2, Timedelta(3, 's'))]
# Check a list of times is expressed as a list of Times
assert (Times([(1, 2, 3000), (4, 5, "6000")], unit='ms').times ==
        [(1, 2, Timedelta(3, 's')), (4, 5, Timedelta(6, 's'))])


# ## MultiTimes
#
# `MultiTimes` are used to collect sets of times, for example, the sets of split times within a multi-split rally stage.
# m
# `MultiTimes` arrange individual `Times` lists using an ordered `dict`.
#
# `MultiTimes` may point to `MultiTimes`. For example, a `MultiTimes` object for a rally may contain a simple `dict` of stage times, or it may contain a `dict` of `MultiTimes` each describing the split times for the corresponding stage.
#
# A `levels` argument should label each level, eg `levels = ['stage', 'splits']`.

# +
# For testing levels of MultiTimes?

# https://stackoverflow.com/a/23499101/454773
def depth(d):
    """Find the 'depth' of a dict."""
    if isinstance(d, dict):
        return 1 + (max(map(depth, d.values())) if d else 0)
    return 0

(depth({'a': 1}), depth({'a': {'b': 2}}), depth({'a': {'b': {'a': 3}}}),
 depth({'a': {'b': 2}, 'x': {'b': {'a': 3}}}),)

# + tags=["active-ipynb"]
# def __init__(self, sdbRallyId=None, stageId=None, live=False,
#                  autoseed=False, slurp=False, dbname=None):
#         """Build on classes for each page of API/WRC live timing website."""
#         WRCCars.__init__(self, sdbRallyId=sdbRallyId, live=live,
#                          autoseed=autoseed, dbname=dbname)
