import os
import calendar
import re
from datetime import datetime, date

import numpy as np
import pandas as pd
import s3fs

from utils.io.get_stakeholders import organize_stakeholders
from utils.io.redshift_access import access_redshift


def save_final_results(df, path_, file_name_):
    s3 = s3fs.S3FileSystem(anon=False)
    with s3.open("{}/{}.dat".format(path_, file_name_),'w') as f:
        df.to_csv(f, sep = '\t', index = False)
    print("{} saved to {}.".format(file_name_, path_))


def send_to_analyzer(df):
    #Fix Age issue:
    df = df.dropna(axis=1, how='all')
    df = df[~df.COUNTRY.isna()].copy()
    df['COUNTRY'] = df.COUNTRY.astype(int)
    
    if "Age_x" in df.columns:
        df.rename(columns = {"Age_x":"Age"}, inplace = True)
        df.drop("Age_y", axis=1, inplace = True)
    ## NEW CODE - 04Feb21
    table_name='crt_etl.staging_etl'
    con, cur = access_redshift()
    cur.execute("select * \
        from crt_ds_etl.upload_col_names \
        where table_name = '{}';".format(table_name))
    col_data=pd.DataFrame(cur.fetchall())
    col_data.columns=['table_name','columns']
    redshift_columns=col_data.T.loc['columns',:]

    # Pain in the neck
    if 'Unnamed: 0' in df.columns:
        df.drop('Unnamed: 0', axis=1, inplace=True)
    lowercase_cols = {x:x.lower() for x in df.columns.tolist()}
    df.rename(columns=lowercase_cols, inplace = True)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Fix missing Age-group variable
    df['age_group']=df['age']
    if 'study' not in df.columns:
        df['study']=5
    df['study_type']=df['study']
    df['risk_type']=99#df['q700a_select']
    if 'unique_respondent_all_.' in df.columns:
        df['unique_respondent_all_$']=df['unique_respondent_all_.']
    print("Duplicate Columns? {}".format(df.columns[df.columns.duplicated()].tolist()))
    # Fix Industry
    df['industry_project']=df['new_industry1']
    df['industry_flag']=df['industry_index1']
    print("\nChecking Industries:\n{}".format(df.industry_flag.unique()))
    print("{}".format(df.industry_project.unique()))
    if 'industry' in df.columns:
        df['industry']=df['industry_project']

    #   --------------------------------------
    # Organize output name
    today = datetime.strptime(str(date.today()), "%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M:%S")
    timestamp = "{} {}".format(today.strftime('%b %d, %Y'), current_time)
    reporting_string = "NT {}, {} ({})_New Format".format(calendar.month_abbr[round(df.month.unique()[0])],
                                     df.year.unique()[0],
                                     timestamp)
    #   --------------------------------------
    #   Add stakeholders
    print("Applying Stakeholder fix.")
    final_df = organize_stakeholders(df)
    print("Final_DF.shape: {}".format(final_df.shape))
    #   --------------------------------------
    #   Final step - put in order. Just in case
    final_df = final_df.loc[:, ~final_df.columns.duplicated()].copy()
    final_df=final_df.reindex(columns = redshift_columns).copy()
    print("***\nFinal dataset size: {}\n***".format(final_df.shape))
    print("Duplicate Columns? {}".format(final_df.columns[final_df.columns.duplicated()].tolist()))
    print("---\nUnique stakeholders:")
    print(final_df.groupby('stakeholder_id')['global_id'].nunique())
    print('---')
    #   --------------------------------------
    #   Check for issues
    issues  = [col for col in final_df if  col.endswith('_x')]
    print("We probably have issues with these columns: {}".format(issues))
    print("***\nFinal dataset size: {}\n***".format(final_df.shape))
    to_int=[col for col in final_df if (col.startswith('q') and not col.endswith('reb') and not col.endswith('reb_1'))]
    to_int+=['products','innovation','workplace', 'governance', 'citizenship',
            'leadership', 'performance']
    to_int += ['year', 'month', 'country', 'company', 'rating', 'industry_project',
               'region','industry_flag','employment','opinion_influencer','gender',
               'income', 'education', 'global_id', 'occupation', 'risk_type', 'coderesp',
               'familiarity','industry']
    final_df.loc[:,to_int] = final_df.loc[:,to_int].replace('', np.NaN)
    final_df.loc[:,to_int] = final_df.loc[:,to_int].replace(' ', np.NaN)
    final_df.loc[:,to_int] = final_df.loc[:,to_int].fillna(99).astype(int)
    save_final_results(final_df, "s3://reptrak-perception-data/Complete/General_Data_New_Format", reporting_string)
    return(0)
