# Essentials
import pandas as pd
from utils.io.redshift_access import access_redshift


def get_stakeholders():
    con, cur = access_redshift()
    cur.execute("select stakeholder_id, stakeholder_name, stakeholder_var, full_var_name \
from crt_ds_etl.stakeholder_ids;")
    data=cur.fetchall()
    stakeholders = pd.DataFrame(data)
    stakeholders.columns=['stakeholder_id','stakeholder_names','stakeholder_vars','full_var_name']
    return(stakeholders)

def get_stakeholders_awareness():
    con, cur = access_redshift()
    cur.execute("select stakeholder_id, awareness_var, awareness_condition \
from crt_ds_etl.stakeholder_ids;")
    data = cur.fetchall()
    stakeholders = pd.DataFrame(data)
    stakeholders.columns = ['stakeholder_id', 'awareness_var', 'awareness_condition']
    return(stakeholders)

def organize_stakeholders(df):
    # Get Stakeholder Data
    con, cur = access_redshift()
    cur.execute("select stakeholder_id, stakeholder_name, stakeholder_var, full_var_name \
from crt_ds_etl.stakeholder_ids;")
    data=cur.fetchall()
    stakeholders = pd.DataFrame(data)
    stakeholders.columns=['stakeholder_id','stakeholder_names','stakeholder_vars','full_stakeholder_name']
    stakeholders.drop('full_stakeholder_name',axis=1,inplace=True) #Gotta get rid of it lol
    # lowercase the df
    lowercase_cols = {x:x.lower() for x in df.columns.tolist()}
    df.rename(columns=lowercase_cols, inplace = True)
    # Create results
    final_df=pd.DataFrame()
    for stakeholder in stakeholders.stakeholder_vars.unique():
        tmp=pd.DataFrame()
        if stakeholder in df.columns:
            df[stakeholder]=pd.to_numeric(df[stakeholder],errors='coerce')
            if df[stakeholder].sum()>0:
                tmp=df[df[stakeholder]==1].copy()
                tmp['stakeholder_id']=stakeholders\
                    .loc[stakeholders.stakeholder_vars==stakeholder,'stakeholder_id']\
                    .unique()[0]
                final_df=pd.concat([final_df,tmp])#final_df=final_df.append(tmp,ignore_index=True)
    print('original df: {}\nfinal_df: {}'.format(df.shape,final_df.shape))
    # If we missed any respondents, add them to IGP
    remaining_data = df[~df.global_id.isin(final_df.global_id.to_list())].copy()
    remaining_data['stakeholder_id'] = 1
    final_df = pd.concat([final_df, remaining_data])
    #final_df=final_df.append(remaining_data,ignore_index=True)
    return(final_df)
