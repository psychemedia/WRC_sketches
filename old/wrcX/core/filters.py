from wrcX.core.enrichers import addGroupClassFromCarNo, addTeamFromCarNo
from wrcX.reports.utils import getTeamFromCarNo

def groupClassFilter(df,groupClass='',eligibility=None):
    if groupClass!='':
        df=addGroupClassFromCarNo(df)
        df=df[df['groupClass']==groupClass]
    
    # TO DO - eligibility filter
    return df

def teamFilter(df,team=None,carNo=None):
    if 'team' not in df.columns:
        df=addTeamFromCarNo(df)
    if team is None:
        team = getTeamFromCarNo(carNo)
    return df[df['team']==team]