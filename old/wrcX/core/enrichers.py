import wrcX.core.data
from wrcX.core.time import *
from wrcX.io.loaders import validateLoadedDF

from pandas import to_datetime, to_timedelta, DataFrame, Series, merge
from numpy import where

def addXfromY(df,col,on='carNo'):
    if col in df.columns: return df
    return df.merge(wrcX.core.data.df_entry[[on,col]], on=on)
    
def addGroupClassFromCarNo(df):
    return addXfromY(df,'groupClass','carNo')

def addTeamFromCarNo(df):
     return addXfromY(df,'team','carNo')

def baseStageDesc():
    df_itinerary = wrcX.core.data.df_itinerary
    fragment=(df_itinerary['firstCar'].notnull()) & (df_itinerary['firstCar']!='')
    for col in ['firstCar']:
        df_itinerary.loc[fragment,'time_'+col]=df_itinerary[fragment].apply(lambda x: regularTimeString(x[col]+':00'),
                                                                            axis=1)

    fragment=df_itinerary['stage'].str.startswith('Day')
    df_itinerary.loc[fragment,'date']=df_itinerary[fragment].apply(lambda x: to_datetime(x['stage'].split('-')[1].strip(),dayfirst=True), axis=1)
    df_itinerary['date'].fillna(method='ffill',inplace=True)
    return df_itinerary

def stageDesc():
    df_stagedesc = baseStageDesc()
    if not validateLoadedDF(df_stagedesc): return DataFrame()

    #If on a day's event the time of day delta is less than previous, move to next day by adding a +24 hours offset
    offset=df_stagedesc.groupby('day')['time_firstCar'].apply(lambda x: Series(where( x.diff()<to_timedelta("0:0:0"),to_timedelta("24:0:0"),to_timedelta("0:0:0") ))).reset_index(drop=True)
    df_stagedesc['datetime']=df_stagedesc['date']+df_stagedesc['time_firstCar']+offset

    df_stagedesc['partofday']=partofday(df_stagedesc['datetime'])

    df_stagedesc['stagename'].fillna('',inplace=True)
    df_stagedesc['stagename'] = df_stagedesc['stagename'].apply(lambda x: ' '.join(x.split()))
    df_stagedesc['basestagename']=df_stagedesc['stagename'].str.extract(r'^(.*?)\s*?[0-9]?\s*(\(PS\))?$',expand=True)[0]
    df_stagedesc['basestagename'].value_counts(), df_stagedesc['stagename'].value_counts()

    stagerows=df_stagedesc['stage'].str.startswith('SS')
    df_stagedesc.loc[stagerows,'stagerun']=df_stagedesc[stagerows].groupby('basestagename')['datetime'].rank()

    repeats=df_stagedesc[stagerows].groupby('basestagename')['stage'].apply(lambda x:{y for y in x})
    repeats.rename('repeats',inplace=True)
    repeats.to_frame().reset_index()

    df_stagedesc=merge(df_stagedesc,repeats.to_frame().reset_index(),on='basestagename')
    df_stagedesc=df_stagedesc.sort_values('datetime')

    df_stagedesc['stagenum']=df_stagedesc['stage'].str.replace('SS','').astype(int)
    df_stagedesc['completedstages']=(df_stagedesc['status'].isin(['COMPLETED','INTERRUPTED'])).cumsum()

    return df_stagedesc

#Find winning streaks  - runs of 2 or more consecutive stage wins
#http://stackoverflow.com/a/35428677/454773
def winningstreak(x):
    x['winstreak'] = x.groupby( (x['pos'] != 1).cumsum()).cumcount() +  ( (x['pos'] != 1).cumsum() == 0).astype(int) 
    return x

def _debug_winningstreak(x):
    x['notwin'] = (x['pos'] != 1)
    #The notwincumcount creates a new group for each not win
    #So if we have a streak of wins, the notwincumcount groups those together
    x['notwincumcount'] = x['notwin'].cumsum()
    x['startwithawin'] = (x['notwincumcount'] == 0).astype(int)
    #groupby.cumcount  - number each item in each group from 0 to the length of that group - 1.
    x['streakgroupwincount'] = x.groupby( 'notwincumcount' ).cumcount()

    x['winstreak'] = x.groupby( 'notwincumcount' ).cumcount() + x['startwithawin']
    return x