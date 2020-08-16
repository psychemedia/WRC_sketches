from wrcX.reports.utils import listrand, _melter
from pandas import merge

# Flags
def stageflags(df_overallpositions,df_stagepositions, df_itinerary):
    ''' Generate flags to use for parsing stage changes:

        df_stage,df_overallpart=w.getStageResults( year,rallyid,stages)
        df_stagepositions=w.allstagedetails(df_stage)
        df_overallpositions=w.allstagedetails(df_overallpart)
        df_itinerary=w.getItinerary(year,rallyid)
        g=w.stageflags(df_overallpositions,df_stagepositions, df_itinerary)

        cols=['won','inoveralllead','gainedlead','lostlead','retainedlead']
        g.applymap(lambda x: 'T' if x else 'F').reset_index().groupby('carNo')[cols].apply(sum).ix['5']

        COMPLETED | INTERRUPTED | CANCELLED | RUNNING | TO RUN
    '''
    stages=df_itinerary[(df_itinerary['status'].isin(['COMPLETED','INTERRUPTED'])) &
                        #(df_itinerary['stage'].str.startswith('SS')) &
                        (df_itinerary['stage'].isin(df_overallpositions.columns))]['stage']

    overallpositions=df_overallpositions[stages]
    stagepositions=df_stagepositions[stages]

    wonstage= stagepositions==1
    wonstage=_melter(wonstage,'won')#pd.melt(wonstage.reset_index(),id_vars='carNo', var_name='stage',
                #value_vars=wonstage.columns.tolist(),value_name='won')

    secondstage= stagepositions==2
    secondstage=_melter(secondstage,'second')##pd.melt(secondstage.reset_index(),id_vars='carNo', var_name='stage',
                #value_vars=secondstage.columns.tolist(),value_name='second')

    secondoverall= overallpositions==2
    secondoverall=_melter(secondoverall,'secondoverall')#pd.melt(secondoverall.reset_index(),id_vars='carNo', var_name='stage',
                #value_vars=secondoverall.columns.tolist(),value_name='secondoverall')

    inoveralllead = (overallpositions==1)
    inoveralllead=_melter(inoveralllead,'inoveralllead')#pd.melt(inoveralllead.reset_index(),id_vars='carNo', var_name='stage',
                #value_vars=inoveralllead.columns.tolist(),value_name='inoveralllead')

    leadsummary=merge(wonstage,inoveralllead,on=['carNo','stage'],how='outer')
    leadsummary=merge(leadsummary,secondstage,on=['carNo','stage'],how='outer')
    leadsummary=merge(leadsummary,secondoverall,on=['carNo','stage'],how='outer')


    #gained lead if in lead and not in previous
    gainedlead=(overallpositions.diff(axis=1)<0 ) & (overallpositions==1)

    gainedlead=_melter(gainedlead,'gainedlead')#pd.melt(gainedlead.reset_index(),id_vars='carNo',var_name='stage',
                       #value_vars=gainedlead.columns.tolist(),value_name='gainedlead')

    leadsummary=merge(leadsummary,gainedlead,on=['carNo','stage'],how='outer')

    #patch first stage
    leadsummary['gainedlead'] = leadsummary['gainedlead'] | ((leadsummary['stage']==stages.tolist()[0]) & (leadsummary['won']))

    #lost lead - previous in lead but lost positions from lead
    lostlead= (overallpositions!=1) & ((overallpositions-overallpositions.diff(axis=1)==1))
    lostlead=_melter(lostlead,'lostlead')#pd.melt(lostlead.reset_index(),id_vars='carNo',var_name='stage',
                       #value_vars=lostlead.columns.tolist(),value_name='lostlead')

    leadsummary=merge(leadsummary,lostlead,on=['carNo','stage'],how='outer')

    #wongainedlead['lostlead']

    #retained lead
    leadsummary['retainedlead']=leadsummary['inoveralllead'] & ~leadsummary['gainedlead'] & (leadsummary['stage']!=stages.tolist()[0])

    #retained rank position
    retainedpos=overallpositions.diff(axis=1)==0
    retainedpos=_melter(retainedpos,'retainedpos')#pd.melt(retainedpos.reset_index(),id_vars='carNo', var_name='stage',
                #value_vars=retainedpos.columns.tolist(),value_name='retainedpos')
    leadsummary=merge(leadsummary,retainedpos,on=['carNo','stage'],how='outer')

    prevstageOverallPos=overallpositions.shift(1,axis=1)
    prevstageOverallPos=_melter(prevstageOverallPos,'prevRallyOverall')
    leadsummary=merge(leadsummary,prevstageOverallPos,on=['carNo','stage'],how='outer')

    currstageOverallPos=overallpositions
    currstageOverallPos=_melter(currstageOverallPos,'currRallyOverall')
    leadsummary=merge(leadsummary,currstageOverallPos,on=['carNo','stage'],how='outer')


    #increased lead
    #to do - based on time

    return leadsummary.set_index(['carNo','stage'])