import os
from datetime import datetime, date
import calendar

import pandas as pd
import numpy as np
import pyreadstat
import s3fs

from utils.io.get_stakeholders import get_stakeholders_awareness


def save_final_results(df, path_, file_name_):
    s3 = s3fs.S3FileSystem(anon=False)
    with s3.open("{}/{}.dat".format(path_, file_name_),'w') as f:
        df.to_csv(f, sep = '\t', index = False)
    print("{} saved to {}.".format(file_name_, path_))


def read_awareness(file):
    df, meta_dy = pyreadstat.read_sav(file, metadataonly=True)
    print("File imported")
    stakeholders = get_stakeholders_awareness()
    fam = [col for col in df if (col.startswith('S105Fielded_'))]
    import_vars = fam.copy()
    essential_vars=['CODERESP','COUNTRY','Month','hSampleType','Region_C']
    stakeholder_vars=[]
    for stakeholder in stakeholders.awareness_var.unique():
        if stakeholder in df.columns:
            stakeholder_vars.append(stakeholder)
        if stakeholder[:-1]+"2" in df.columns:
            stakeholder_vars.append(stakeholder[:-1]+"2")
    import_vars += essential_vars + stakeholder_vars
    if 'Year' in df.columns:
        print("Year in column")
        import_vars.append('Year')
        df, meta_dy = pyreadstat.read_sav(file,
                               usecols=import_vars)
        df=df[df.Month!=''].copy()
    else:
        from datetime import datetime
        today = datetime.today()
        df, meta_dy = pyreadstat.read_sav(file,
                               usecols=import_vars)
        df=df[df.Month!=''].copy()
        df['Month']=df['Month'].astype(int)
        if df.Month.unique()[0] == 12:
            df['Year'] = today.year -1
        else:
            df['Year'] = today.year
    return(df)


def aggregate_awareness(df):
    # Dropping blank columns
    df = df.dropna(axis=1, how = 'all')
    # Collect familiarity columns
    fam = [col for col in df if (col.startswith('S105Fielded_'))]
    # Count familiarity events
    df_fam = pd.DataFrame()

    for f in fam:
        if len(df[f].unique()) !=0:
            df_fam = pd.concat([df_fam, df[f].value_counts()],axis=1)
    df_fam_ok = df_fam.transpose()
    df_fam_ok.reset_index(inplace=True)
    df_fam_ok['Rating'] = df_fam_ok['index'].str.split('_').str[-1]
    df_fam_ok.rename(columns = {1:'n_1',2:'n_2',3:'n_3',4:'n_4',5:'n_5',6:'n_6',7:'n_7',99:'n_99'},inplace=True)
    # ---
    # Patch - Make sure we have all the columns we need
    df_cols=df_fam_ok.columns.tolist()
    n_cols_to_make=[]
    n_cols=['n_1','n_2','n_3','n_4','n_5','n_6','n_7','n_99']
    for n_col in n_cols:
        if n_col not in df_cols:
            print("{} not found in df_cols".format(n_col))
            n_cols_to_make.append(n_col)
    for n in n_cols_to_make:
        #print("Trying to add {} to df_fam_ok".format({n}))
        df_fam_ok[n]=np.nan
    # ---
    # Compute percentages
    for i in df_fam_ok.iterrows():
        for p in range(1,9):
            if p == 8:
                p1 = 99
            else:
                p1=p
            # replace NA with 0 to print also 0 and 0% in results
            df_fam_ok["n_"+str(p1)].fillna(0,inplace=True)
            df_fam_ok['perc_'+str(p1)] = df_fam_ok["n_"+str(p1)]/df_fam_ok[n_cols].sum(axis=1)
    return(df_fam_ok)


def clean_agg(df_agg):
    df_agg = df_agg.set_index(['Rating'])
    df_agg.columns = pd.MultiIndex.from_tuples([col.split('_', 1) for col in df_agg.columns])
    result = df_agg.stack(level=1).reset_index()
    result.rename(columns={'level_1':'familiarity_id'})
    result = result.rename(columns={'level_1':'familiarity_id', 'n':'user_count', 'perc':'familiarity_percent'})
    result['familiarity_percent'] = round(result.familiarity_percent*100,2)
    result['familiarity_label'] = ''
    result.loc[pd.to_numeric(result.familiarity_id) < 4, 'familiarity_label'] = 'Not Familiar'
    result.loc[pd.to_numeric(result.familiarity_id) >=4, 'familiarity_label'] = 'Familiar'
    result.loc[pd.to_numeric(result.familiarity_id) == 99, 'familiarity_label'] = 'Not Familiar'
    return(result)

def upload_awareness(df):
    # Organize output name
    today = datetime.strptime(str(date.today()), "%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M:%S")
    timestamp = "{} {}".format(today.strftime('%b %d, %Y'), current_time)
    month = int(round(df.Month.unique()[0]))
    print(month)
    print(type(month))
    string = "NT Awareness {}, {} ({})".format(calendar.month_abbr[month],
                                     df.Year.unique()[0],
                                     timestamp)
    print("Pre-upload data cleaning.")
    print("Working with the following column names: {}".format(df.columns.tolist()))
    df = df.rename(columns={'Rating':'rating_id', 'Company':'company_id',
                            'Year':'year', 'Month':'month','Country':'country_id'})
    df = df[['year','month','country_id','rating_id','company_id','familiarity_id','familiarity_percent',
            'familiarity_label','user_count','stakeholder_id']]
    print("Uploading! Filename: {}".format(string))
    save_final_results(df, "s3://reptrak-perception-data/Complete/Awareness_Data", string)
    print("Uploading complete.")
