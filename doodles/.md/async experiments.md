---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.3.1
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# Experiments Around Asynchronous Periodic Function Calls


Asynchronous periodic function calls can be useful when regularly polling a datasource, for example when polling live timing results or timing screens.

This notebooks explores various methods for running asynchronous periodic calls, with arbitrary stopping conditions, particulalry in a Jupyter notebook environment.

(See also: *websockets*; now where did my websocket demos go?!)

<!-- #endregion -->

# Logging

Asynchronously called functions may not have a direct route to notebook `print()` or `display()` output channels, so enable logging.

```python
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
```

## Using the *tornado* `PeriodicCallback`

To start with, let's explore the *tornado* server `PeriodicCallback` [[docs](https://www.tornadoweb.org/en/stable/ioloop.html#tornado.ioloop.PeriodicCallback)] function.

One thing to note is that the `PeriodicCallback` has a `jitter` paramter that may be useful in scraping applciations to slightly randomise the exact frequncy of repeated calls.

```python
from tornado import gen
from tornado.ioloop import PeriodicCallback

from datetime import datetime
```

```python
class Test:
    def __init__(self):
        self.i = 1
        
    @gen.coroutine
    def test(self):
        logging.debug(f'x{self.i}')
        self.i += 1
        

    def monitor(self):
        logging.debug(f'y{self.i}')
        if self.i>=10:
            logging.debug(f'z{self.i}')
            self.cbdf.stop()
            self.monitor.stop()
    
    def mstart(self):
        self.monitor = PeriodicCallback(self.monitor, 750)
        self.monitor.start()
        
    def sstart(self):
        self.cbdf = PeriodicCallback(self.test, 500)
        self.cbdf.start()

    def sstop(self):
        self.cbdf.stop()


x=Test()
x.mstart()
x.sstart()
#x.sstop()
```

```python
x.sstop()
```

```python
x.monitor.stop()
```

```python
x.sstop()
```

```python
class Periodic2:
    def __init__(self):
        self.i = 1

    def backgroundMonitor(self):
        logging.debug(f'y{self.i}')
        if self.i>=10:
            logging.debug(f'z{self.i}')
            self.cbdf.stop()
            self.monitor.stop()
    
    def mstart(self):
        self.monitor = PeriodicCallback(self.backgroundMonitor, 750)
        self.monitor.start()
    
    #we can start an arbitrary function
    def sstart(self, func):
        self.cbdf = PeriodicCallback(func, 500)
        self.cbdf.start()

    def sstop(self):
        self.cbdf.stop()

class Test2(Periodic2):
    def __init__(self):
        Periodic2.__init__(self)

    #We can override the base class monitor
    def backgroundMonitor(self):
        logging.debug(f'y{self.i}')
        if self.i>=6:
            logging.debug(f'z{self.i}')
            self.cbdf.stop()
            self.monitor.stop()
  
    @gen.coroutine
    def test(self):
        logging.debug(f'x{self.i}')
        self.i += 1
        

x=Test2()
x.mstart()
x.sstart(x.test)
```

```python
x.test()
```

```python
class Periodic3:
    def __init__(self):
        self.i = 1

    def backgroundMonitor(self):
        """Default background monitor."""
        logging.debug(f'y{self.i}')
        if self.i>=10:
            logging.debug(f'z{self.i}')
            #for k in self.monitored:
            #    self.monitored[k].stop()
            #for k in self.monitors:
            #    self.monitors[k].stop()
            self.cbdf.stop()
            self.monitor.stop()
            logging.debug(f'Monitoring stopped')
    
    def mstart(self, func=None, monitor_arg='aaa' ):
        """Start a monitor function.
           The monitor can work in the background to shut everything down."""
        if not func:
            self.monitor = PeriodicCallback(self.backgroundMonitor, 750)
            self.monitor.start()
        else:
            logging.debug(f'custom monitor')
            self.monitor = PeriodicCallback(func, 750)
            self.monitor.start()
    
    #we can start an arbitrary function
    def sstart(self, func):
        self.cbdf = PeriodicCallback(func, 500)
        self.cbdf.start()

    def sstop(self):
        self.cbdf.stop()

class Test3(Periodic3):
    def __init__(self):
        Periodic3.__init__(self)
  
    @gen.coroutine
    def test(self):
        logging.debug(f'x{self.i}')
        self.i += 1
        #This could also stop itself
        
    def stopper(self):
        logging.debug(f'monitoringy{self.i}')
        if self.i>=8:
            logging.debug(f'z{self.i}')
            self.cbdf.stop()
            self.monitor.stop()
            
x=Test3()
#x.mstart(x.stopper)
x.mstart()
x.sstart(x.test)
```

```python
# TO DO - see if we can start to add in labelled jobs
# This would allow is to start lots of jobs with their own private monitors
# The background monitor then kills all


class Periodic4:
    def __init__(self):
        self.i = 1

    def backgroundMonitor(self):
        """Default background monitor."""
        logging.debug(f'y{self.i}')
        if self.i>=10:
            logging.debug(f'z{self.i}')
            #for k in self.monitored:
            #    self.monitored[k].stop()
            #for k in self.monitors:
            #    self.monitors[k].stop()
            self.cbdf.stop()
            self.monitor.stop()
            logging.debug(f'Monitoring stopped')
    
    def mstart(self, func=None, monitor_arg='aaa' ):
        """Start a monitor function.
           The monitor can work in the background to shut everything down."""
        if not func:
            self.monitor = PeriodicCallback(self.backgroundMonitor, 750)
            self.monitor.start()
        else:
            logging.debug(f'custom monitor')
            self.monitor = PeriodicCallback(func, 750)
            self.monitor.start()
    
    #we can start an arbitrary function
    def sstart(self, func):
        self.cbdf = PeriodicCallback(func, 500)
        self.cbdf.start()

    def sstop(self):
        self.cbdf.stop()

class Test4(Periodic4):
    def __init__(self):
        Periodic4.__init__(self)
  
    @gen.coroutine
    def test(self):
        logging.debug(f'x{self.i}')
        self.i += 1
        #This could also stop itself
        
    def stopper(self):
        logging.debug(f'monitoringy{self.i}')
        if self.i>=8:
            logging.debug(f'z{self.i}')
            self.cbdf.stop()
            self.monitor.stop()
            
x=Test3()
#x.mstart(x.stopper)
x.mstart()
x.sstart(x.test)
```

```python

```

```python

```

```python

```



```python

```

```python

from tornado import gen
from tornado.ioloop import PeriodicCallback

from holoviews.streams import Buffer

import holoviews as hv
hv.extension('bokeh')


import numpy as np
import pandas as pd

df = pd.DataFrame({'x':range(1000), 'y':np.sin(range(1000))})

rowcount = 0
maxrows = 1000

dfbuffer = Buffer(np.zeros((0, 2)), length=20)

@gen.coroutine
def g():
    global rowcount
    item = df[['x','y']].iloc[rowcount].values.tolist()
    dfbuffer.send(np.array([item]))
    rowcount += 1

    if rowcount>=maxrows:
        cbdf.stop()



#How can we get the thing to stop?

cbdf = PeriodicCallback(g, 500)
cbdf.start()
hv.DynamicMap(hv.Curve, streams=[dfbuffer]).opts(padding=0.1, width=600, color = 'green',)
```

```python
cbdf.stop()
```

## `asyncio`

Newer; issues in Jupyter though because an event loop is already running. The [`nest_asyncio`](https://github.com/erdewit/nest_asyncio) package provides a workaround.

```python
#https://phoolish-philomath.com/asynchronous-task-scheduling-in-python.html

import asyncio
import nest_asyncio


#https://github.com/erdewit/nest_asyncio
#%pip install nest_asyncio
import nest_asyncio # required becuase in jupyter there is an event loop running already
nest_asyncio.apply()

from datetime import datetime as dt

async def run_periodically(wait_time, func, *args):
    """
    Helper for schedule_task_periodically.
    Wraps a function in a coroutine that will run the
    given function indefinitely
    :param wait_time: seconds to wait between iterations of func
    :param func: the function that will be run
    :param args: any args that need to be provided to func
    """
    while True:
        func(*args)
        await asyncio.sleep(wait_time)

def schedule_task_periodically(wait_time, func, *args):
    """
    Schedule a function to run periodically as an asyncio.Task
    :param wait_time: interval (in seconds)
    :param func: the function that will be run
    :param args: any args needed to be provided to func
    :return: an asyncio Task that has been scheduled to run
    """
    return asyncio.create_task(run_periodically(wait_time, func, *args))

async def cancel_scheduled_task(task):
    """
    Gracefully cancels a task
    :type task: asyncio.Task
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    
import datetime

def count_seconds_since(then):
    """
    Prints the number of seconds that have passed since then
    :type then: datetime.datetime
    :param then: Time to count seconds from
    """
    now = datetime.datetime.now()
    print(f"{(now - then).seconds} seconds have passed.")


async def main():

    counter_task = schedule_task_periodically(3, count_seconds_since, dt.now())

    print("Doing something..")
    await asyncio.sleep(10)
    print("Doinsstopg something else..")
    await asyncio.sleep(5)
    print("Shutting down now...")

    await cancel_scheduled_task(counter_task)
    print("Done")

    
#But this is blocking?
asyncio.get_event_loop().run_until_complete(main())
```

```python
#https://stackoverflow.com/a/37512537/454773

async def periodic():
    while True:
        print('periodic')
        await asyncio.sleep(1)

def stop():
    task.cancel()

loop = asyncio.get_event_loop()
loop.call_later(5, stop) #Schedule callback to be called after the given delay number of seconds 
task = loop.create_task(periodic())

try:
    loop.run_until_complete(task)
except asyncio.CancelledError:
    pass
```

Preventing items from trying to access same resource at same time? [`async with lock`](https://docs.python.org/3/library/asyncio-sync.html)

```python

```
