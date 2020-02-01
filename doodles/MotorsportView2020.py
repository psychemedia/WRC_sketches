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

# +
from pandas import Timedelta, NaT
from numpy import nan
import pandas as pd

from WRCUtils2020 import listify
# -

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
        self.t = asTimedelta(t, unit=unit)
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
        """Display Basetime."""
        return f'Basetime: {repr(self.t)}'


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
# Basetime(2000, unit='ms')
# -

# A timing record is minimally a 3-tuple (`(index, uid, timedelta)`) made up from an index value, such as a lap count or stage number, an identifier, such as a car number, and a time given as a timedelta.
#
# The `asTime()` function will take a three tuple and attempt to return it in the form `(vartype, vartype, Timedelta)`.

def asTime(atime=None, unit=BASEUNIT):
    """Generate a valid Time tuple."""
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
# # Check nulls
# assert asTime() ==  (None, None, None)
# assert asTime(()) == (None, None, None)
# # Check we can cope with ourselves...
# assert asTime((1, 2, '3')) == asTime(asTime((1, 2, '3')))
# # Check assignment of single time
# assert asTime((1, 2, '3')) == (1, 2, Timedelta(3, 's'))
# # If we don't get a tuple, return one filled with nulls
# assert asTime(1) == (None, None, None)
# # If we get a list, that's wrong too...
# assert (asTime([(1, 2, 3), (4, 5, '6')])) == (None, None, None)
# -

# The `asTimes()` function takes in a list of 3-tuples and attempts to return them as a list of valid `asTime()` returned tuples.

def asTimes(atime=None, unit=BASEUNIT):
    """Generate a list of valid Times tuples."""
    if isinstance(atime, list):
        t = [asTime(_t, unit=unit) for _t in atime]
        return t
    return [asTime(atime, unit=unit)]


# + tags=["active-ipynb"]
# # Check assignment of list of times
# assert (asTimes([(1, 2, 3), (4, 5, '6')]) == 
#         [(1, 2, Timedelta(3, 's')), (4, 5, Timedelta(6, 's'))])
# # Check we can cope with ourselves
# asTimes([(1, 2, 3), (4, 5, '6')]) == asTimes(asTimes([(1, 2, 3), (4, 5, '6')]))
# # If we get a single time, return it as Time list
# assert asTimes((1, 2, '3')) == [(1, 2, Timedelta(3, 's'))]
# # If we get nonsense, retun null tuples
# assert asTimes(1) == [(None, None, None)]
# assert (asTimes([1, (4, 5, 6000), ('a', 1)], unit='ms') ==
#         [(None, None, None), (4, 5, Timedelta(6, 's')), (None, None, None)])
# -

# As well as the simple 3-tuples, we also acknowledge a 4-tuple labeled time 3-tuple, which adds a label that might be used for display purposes, for example.
#
# The `asLabeledTime()` function attempts to return a labeled time elements in various formats:
#
# - as a 2-tuple: `((index, uid, timedelta), label)`
# - as a 4-tuple: `((index, uid, timedelta, label)`
#
# The `asLabeledTime()` function also seeks to be accommodating as to what it can accept:
#
# - a 2-tuple: `((index, uid, timedelta), label)`;
# - a 3-tuple: `(index, uid, timedelta)`, in which case, we generate the label optionally from the uid, or from a label assigned from a `label` parameter;
# - a 4-tuple: `(index, uid, timedelta, label)`.
#
# If a 3-tuple is passed without an associated `label`, the `label` can be automatically set to the `uid` value.

# + tags=["active-ipynb"]
# # Placeholder to make test run
# class LabeledTime:
#     """Placeholder."""
#     
#     pass
# -

def asLabeledTime(altime=None, label=None, unit=BASEUNIT,
                  useuid=None, singletuple=False, probeuid = False):
    """
    Generate a valid LabeledTimes tuple.
    
    altime must be a two, three or four tuple.
    If no label is provided, and we have a three tuple,
    by default use the uid as the label, or False to return empty string.
    
    If a three tuple or four tuple is provided, use label if provided.
    
    If singletuple is True, return (index, uid, time, label).
    If singletuple is False, return ((index, uid, time), label)).
    
    The label is cast as a string.
    """
    if probeuid:
        if isinstance(altime, tuple) and len(altime) == 3 and label is None:
            useuid = True if useuid is not False else useuid
        return useuid
    
    if isinstance(altime, LabeledTime):
        atime = altime.atime
    elif isinstance(altime, tuple) and len(altime) == 3:
        # 3-tuple: (index, uid, timedelta)
        atime = altime
        if label is None:
            useuid = True if useuid is not False else useuid
    elif isinstance(altime, tuple) and len(altime) in [2, 4]:
        if len(altime) == 2:
            # The 2 tuple is of the form ((index, uid, timedelta), 'label')
            atime = altime[0]
        else:
            #The 4 tuple is of the form (index, uid, timedelta, 'label')
            atime = altime[:-1]
        if not label:
            # If there is a label provided,
            #   then it overrides one in the altime tuple
            label = altime[-1]
    else:
        # Defualt is Nones 3-tuple
        atime = (None, None, None)
        
    if label is not None:
        try:
            label = str(label)
        except:
            label = ''
    elif useuid:
        label = altime[1]
    else:
        label = ''  # Or should we allow None to be returned?

    atime = asTime(atime, unit=unit)
    
    # Force label typing as string
    label = str(label)
    
    if singletuple:
        # Which is to say, a 4-tuple:
        #  (index, uid, timedelta, label)
        return atime + (label,)
    
    # Default is a 2-tuple:
    #  ((index, uid, timedelta), label)
    return (atime, label)


# + tags=["active-ipynb"]
# # Check nulls
# assert asLabeledTime(probeuid=True) == None
# assert asLabeledTime() == ((None, None, None), '')
# assert asLabeledTime(singletuple=True) == (None, None, None, '')
# assert asLabeledTime(label='s') == ((None, None, None), 's')
# # Check we can create a LabeledTime from a 3-tuple and a label
# assert (asLabeledTime((1, 2, 3000), unit='ms') ==
#         ((1, 2, Timedelta(3, 's')), '2'))
# # Check we can cope with ourselves
# assert (asLabeledTime(((1, 2, 3), 'name')) ==
#         asLabeledTime(asLabeledTime(((1, 2, 3), 'name'))))
# assert (asLabeledTime((1, 2, 3000), unit='ms',
#                       useuid=False, probeuid=True) == False)
# assert (asLabeledTime((1, 2, 3000), unit='ms', useuid=False) ==
#         ((1, 2, Timedelta(3, 's')), ''))
# assert (asLabeledTime((1, 2, 3000), unit='ms', useuid=True) ==
#         ((1, 2, Timedelta(3, 's')), '2'))
# # Check we can create a LabeledTime from a 2-tuple
# assert (asLabeledTime(((1, 2, 3), 'name')) ==
#         ((1, 2, Timedelta(3, 's')), 'name'))
# # Check we can create a LabeledTime from a 4-tuple
# assert (asLabeledTime((1, 2, 4000), 'name', unit='ms') ==
#         ((1, 2, Timedelta(4, 's')), 'name'))
# assert (asLabeledTime((1, 2, 3000, 'name'), unit='ms') ==
#         ((1, 2, Timedelta(3, 's')), 'name'))
# # Check we can create a LabeledTime from a 4-tuple and a label
# assert (asLabeledTime((1, 2, 3000, 'name'),
#                       label='name2', unit='ms') ==
#         ((1, 2, Timedelta(3, 's')), 'name2'))
# assert (asLabeledTime((1, 2, 3000, 'name'), label='name2',
#                       useuid=True, unit='ms') ==
#         ((1, 2, Timedelta(3, 's')), 'name2'))
# assert (asLabeledTime((1, 2, 3000, 'name'), label='name2',
#                       useuid=True, unit='ms', singletuple=True) ==
#         (1, 2, Timedelta(3, 's'), 'name2'))
# -

# The `asLabeledTimes()` function takes in a list of times and labels and attempts to return a list of 2-tuples `((index, uid, timedelta), label)` representing labeled times elements of the same length as the supplied list of times.
#
# As with `asLabeledTime()`, we try to be forgiving in what we can accept (2-, 3- or 4-tuple).
#
# If the `label` parameter is a single string item, it may be used as the label for each time. If the `label` is a list of the same length as the list of times, each time is assigned it's own label. If no label is provided, the `uid` may be used as the label.

def asLabeledTimes(atimes=None, labels=None, unit=BASEUNIT,
                   useuid=None, singletuple=False):
    """
    Generate a list of valid LabeledTimes tuples.
    
    The atimes must be None or three tuples.
    """
    if not atimes and not labels:
        return []
    
    atimes = listify(atimes)
    labels = listify(labels)
    
    if not atimes and isinstance(labels, list):
        atimes = [None] * len(labels)
    elif isinstance(atimes, list):
        if isinstance(labels, list) and (len(labels) == len(atimes)):
            # All's good...
            pass
        elif isinstance(labels, str):
            labels = [labels] * len(atimes)
        else:
            labels = [None] * len(atimes)
        # Note: if there are 4 tuples we could try to save those labels?
        
    timelabels = zip(atimes, labels)
    t = [asLabeledTime(_t, label=_label, unit=unit, singletuple=singletuple,
                       useuid=useuid) for (_t, _label) in timelabels]
    return t


# + tags=["active-ipynb"]
# # Check null behaviour
# assert asLabeledTimes() == []
# assert asLabeledTimes(labels='1') == [((None, None, None), '1')]
# assert (asLabeledTimes(labels='1', singletuple=True) ==
#         [(None, None, None, '1')])
# # Check two tuple
# assert (asLabeledTimes(atimes=((1, 2, 3), 'n')) ==
#         [((1, 2, Timedelta(3, 's')), 'n')])
# # Check three tuple
# assert (asLabeledTimes(atimes=(1, 2, 3)) ==
#         [((1, 2, Timedelta(3, 's')), '2')])
# assert (asLabeledTimes(atimes=(1, 2, 4), useuid=False) ==
#         [((1, 2, Timedelta(4, 's')), '')])
# # Check we can cope with ourselves
# assert (asLabeledTimes([(1, 2, 3), (4, 5, '6')]) ==
#        asLabeledTimes(asLabeledTimes([(1, 2, 3), (4, 5, '6')])))
# # Check four tuple
# assert (asLabeledTimes(atimes=(1, 2, 3, 4)) ==
#         [((1, 2, Timedelta(3, 's')), '4')])
# # Cope with multiple items
# assert (asLabeledTimes([(1, 2, 3), (4, 5, '6')]) == 
#         [((1, 2, Timedelta(3, 's')), '2'),
#          ((4, 5, Timedelta(6, 's')), '5')])
# assert (asLabeledTimes([(1, 2, 3), (4, 5, '6')], ['n1', 'n2']) == 
#         [((1, 2, Timedelta(3, 's')), 'n1'),
#          ((4, 5, Timedelta(6, 's')), 'n2')])
# -

# ## Classes

# + run_control={"marked": false}
class Time:
    """
    A time unit expresses an indexed time for an identified thing.
    
    Times are given as a 3-tuple: (index, uid, time).
    """

    def __init__(self, atime=None, unit=BASEUNIT):
        """Create a valid time."""
        self._setTime(atime, unit)

    def _setTime(self, atime, unit):
        """Set attributes on Time object."""
        if isinstance(atime, Time):
            (self.index, self.uid, self.time) = (atime.index, atime.uid, atime.time)
        else:
            atime = asTime(atime, unit=unit)
            (self.index, self.uid, self.time) = atime
        self.atime = (self.index, self.uid, self.time)
        
    def __repr__(self):
        """Display Time 3-tuple."""
        return f'Basetime: {repr(self.atime)}'


# + tags=["active-ipynb"]
# # Check we return a simple Time tuple
# assert Time().atime == (None, None, None)
# assert isinstance(Time(), Time)
# assert isinstance(Time().atime, tuple) and len(Time().atime) == 3
# assert Time((1, 2, '3')).atime == (1, 2, Timedelta(3, 's'))
# assert Time((1, 2, '3000'), unit='ms').atime == (1, 2, Timedelta(3, 's'))
# assert Time(Time((1, 2, '3000'), unit='ms')).atime == (1, 2, Timedelta(3, 's'))
#
# Time((1, 2, '3000'), unit='ms')
# -

class LabeledTime(Time):
    """Extend Time to included a label."""
    
    def __init__(self, altime=None, label=None,
                 unit=BASEUNIT, useuid=None):
        """
        Create a labeled Time.
        
        Labels can be used as display properties in charts, etc.
        We can pass in a Time and a label, or a LabeledTime.
        """
        (atime, label) = asLabeledTime(altime=altime, label=label,
                                       unit=unit, useuid=useuid,
                                       singletuple=False)
        Time.__init__(self, atime=atime, unit=unit)


        self.label = label

        self.ltime = (self.index, self.uid, self.time, self.label)
        
    def _check_label(self, label):
        """Check the label is a string."""
        
    def __repr__(self):
        """Display LabeledTime 4-tuple."""
        return f'Labeledtime: {repr(self.ltime)}'


# + tags=["active-ipynb"]
# # Check empty instantiation
# assert LabeledTime().atime == (None, None, None)
# assert LabeledTime().label == ''
# assert LabeledTime(label='name').label == 'name'
# assert LabeledTime().ltime == (None, None, None, '')
# assert isinstance(LabeledTime(), LabeledTime)
# assert isinstance(LabeledTime().atime, tuple) and len(LabeledTime().atime) == 3
# assert isinstance(LabeledTime().ltime, tuple) and len(LabeledTime().ltime) == 4
# # Check we can create a LabeledTime from a 3-tuple and a label
# assert LabeledTime((1, 2, 3000), 'name', unit='ms').atime == Time((1, 2, 3)).atime
# assert LabeledTime((1, 2, 3), 'name').label == 'name'
# # Check handling of lack of label
# assert LabeledTime((1, 2, 3)).label == '2'
# assert LabeledTime((1, 2, 3), useuid=False).label == ''
# # Check useuid flag
# assert (LabeledTime((1, 2, 3000), unit='ms', 
#                     useuid=False).atime == (1, 2, Timedelta(3, 's')))
# assert (LabeledTime((1, 2, 3000), unit='ms', 
#                     useuid=False).ltime == (1, 2, Timedelta(3, 's'), ''))
# assert (LabeledTime((1, 2, 3000, 'name'), unit='ms', 
#                     useuid=False).ltime == (1, 2, Timedelta(3, 's'), 'name'))
# # Check we can create a LabeledTime from a 4-tuple
# assert LabeledTime((1, 2, 3000, 'name'), unit='ms').atime == Time((1, 2, 3)).atime
# assert LabeledTime((1, 2, 3000, 'name'), unit='ms').label == 'name'
# # Check we can generate label from uid
# assert LabeledTime((1, 2, 3), useuid=True).label == '2'
# # Check we can create a LabeledTime from a LabeledTime
# assert LabeledTime(LabeledTime((1, 2, 3), 'name')).atime == Time((1, 2, 3)).atime
#
# LabeledTime((1, 2, 3), 'name')
# -

# ## Times
#
# Represent a set of times, eg laptimes, stagetimes, splittimes.

class Times():
    """Represent a list of times, e.g. laptimes, stagetimes, splittimes."""
    
    def __init__(self, times=None, unit=BASEUNIT):
        """Base representation of a set of Time elements."""
        if not times:
            self.atimes = []
        else:
            self.atimes = asTimes(times, unit=unit)
        
    def rebase(self, times=None, unit=BASEUNIT):
        """Rebase Times"""
        pass
    
    def __repr__(self):
        """Display Times list."""
        # TO DO need to shorten this if it's too long
        return f'Times: {repr(self.atimes)}'


# +
# Check null
assert Times().atimes == []
assert isinstance(Times(), Times)
# Check a single time is expressed as a list of Times
assert Times((1, 2, 3)).atimes == [(1, 2, Timedelta(3, 's'))]
# Check a list of times is expressed as a list of Times
assert (Times([(1, 2, 3000), (4, 5, "6000")], unit='ms').atimes ==
        [(1, 2, Timedelta(3, 's')), (4, 5, Timedelta(6, 's'))])

Times([(1, 2, 3000), (4, 5, "6000")])


# -

# ## LabeledTimes
#
# Support the labeling of a `Times` list.

class LabeledTimes(Times):
    """Labeled list of times."""

    def __init__(self, times=None, labels=None,
                 unit=BASEUNIT, useuid=None):
        """Create a list of labeled Time elements."""
        Times.__init__(self)
        if times is None:
            self.ltimes = []
        else:
            # By default, asLabeledTimes returns a list of 2-tuples
            _ls = asLabeledTimes(atimes=times, labels=labels,
                                         unit=unit, useuid=useuid)
            self.atimes = [_lt[0] for _lt in _ls]
            self.labels = [_lt[1] for _lt in _ls]
            self.ltimes = [_lt for _lt in zip(self.atimes, self.labels)]

    def __repr__(self):
        """Display LabeledTimes list."""
        # TO DO need to shorten this if it's too long
        return f'LabeledTimes: {repr(self.ltimes)}'


# +
# Check nulls
assert LabeledTimes().atimes == []
assert LabeledTimes().ltimes == []
#Check one item, two tuple
assert LabeledTimes((1, 2, 3), 'name').atimes == [(1, 2, Timedelta(3, unit='s'))]
assert LabeledTimes((1, 2, 3), 'name').labels == ['name']
assert LabeledTimes((1, 2, 3), 'name').ltimes == [(Time((1, 2, 3)).atime, 'name')]
assert LabeledTimes((1, 2, 3), 'name').ltimes == [((1, 2, Timedelta(3, unit='s')), 'name')]
# Check three tuple
assert LabeledTimes((1, 2, 3)).atimes == [(1, 2, Timedelta(3, unit='s'))]
assert LabeledTimes((1, 2, 3)).ltimes == [((1, 2, Timedelta(3, unit='s')), '2')]
assert LabeledTimes((1, 2, 3), useuid=False).ltimes == [((1, 2, Timedelta(3, unit='s')), '')]
# Check 4-tuple
assert LabeledTimes([(1, 2, 3, 'name')]).atimes == [(1, 2, Timedelta(3, unit='s'))]
assert LabeledTimes([(1, 2, 3, 'name')]).ltimes == [(Time((1, 2, 3)).atime, 'name')]
assert LabeledTimes((1, 2, 3, 'name')).atimes == [(Time((1, 2, 3)).atime)]
assert LabeledTimes((1, 2, 3, 'name')).ltimes == [(Time((1, 2, 3)).atime, 'name')]
# Check multi-items
assert (LabeledTimes([((1, 2, 3), 'name'), ((4, 5, 6), 'name2')]).atimes ==
        [(1, 2, Timedelta(3, unit='s')), (4, 5, Timedelta(6, unit='s'))])
assert (LabeledTimes([((1, 2, 3), 'name'), ((4, 5, 6), 'name2')]).labels ==
        ['name', 'name2'])
assert (LabeledTimes([((1, 2, 3)), ((4, 5, 6))],
                     ['name1', 'name2']).labels == ['name1', 'name2'])

LabeledTimes((1, 2, 3), 'name')


# -

# ## MultiTimes
#
# `MultiTimes` are used to collect sets of times, for example, the sets of split times within a multi-split rally stage.
#
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
#
