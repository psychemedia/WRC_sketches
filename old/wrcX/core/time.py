#Time wrangling
from pandas import isnull, cut, to_timedelta

def partofday(df):
    return cut(df.dt.hour,
                  [-1,5, 12, 17,19, 24],
                  labels=['Overnight','Morning', 'Afternoon', 'Evening','Night'])

def regularTimeString(strtime):
    if isnull(strtime) or strtime=='' or strtime=='nan':
        return to_timedelta(strtime)

    #Go defensive, just in case we're passed eg 0 as an int
    strtime=str(strtime)
    strtime=strtime.strip('+')

    modifier=''
    if strtime.startswith('-'):
        modifier='-'
        strtime=strtime.strip('-')
    timeComponents=strtime.split(':')
    ss=timeComponents[-1]
    mm=timeComponents[-2] if len(timeComponents)>1 else 0
    hh=timeComponents[-3] if len(timeComponents)>2 else 0
    timestr='{}{}:{}:{}'.format(modifier,hh,mm,ss)
    return to_timedelta(timestr)

#END Time wrangling