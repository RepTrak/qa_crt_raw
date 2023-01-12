import os
from datetime import datetime

import pandas as pd
import s3fs

from utils.awareness.get_awareness import get_awareness_data
from utils.io.get_stakeholders import get_stakeholders_awareness
from utils.awareness.awareness_functions import aggregate_awareness, clean_agg, upload_awareness



# Awareness computation, organization, and upload
def awareness(df, ALL_FILES_DIC):
    # df = cleaned data file. At minimum it needs to contain rating, company id, month, year.
    print("\n===================================")
    print("=      Processing Awareness       =")
    print("===================================\n")
    if df.Month.unique()[0] == 1:
        df['Year']= datetime.now().year-1
    else:
        df['Year']= datetime.now().year
    if 'Country' not in df.columns:
        df['Country'] = df['COUNTRY']
    df_awareness = df[['Year','Month','Country','Company','Rating']].copy()
    df_awareness = df_awareness.drop_duplicates()
    print("awareness_main.py: Reading raw data and extracting awareness columns.")
    aware_raw = get_awareness_data(ALL_FILES_DIC)
    #print(f"awareness_main.py: {aware_raw.head(2)}")
    #print(f"awareness.py: All columns in aware_raw (from get_awareness.py): {aware_raw.columns.tolist()}")

    # ---
    res = pd.DataFrame()
    stakeholders = get_stakeholders_awareness()
    missing_cases = {}
    no_cases = {}

    for stakeholder in stakeholders.stakeholder_id.unique():
        sh_group = stakeholders[stakeholders.stakeholder_id == stakeholder]
        sh_var1 = sh_group.awareness_var.unique()[0]
        awareness_condition = sh_group.awareness_condition.unique()[0]

        if sh_var1 in aware_raw.columns:
            print(f"awareness_main.py: Found stakeholder variables; Computing Awareness for {sh_var1}")
            aware_raw[sh_var1] = pd.to_numeric(aware_raw[sh_var1], errors = 'coerce')
            df_shg = aware_raw[aware_raw[sh_var1] == awareness_condition].copy()
            check = df_shg.shape[0] > 0
            #print(f"awareness_main: Computing stakeholder id {sh_group} awareness using {sh_var1} = {awareness_condition}")
            #print(f"\nawareness_main.py: df_shg SHAPE after aware_raw subset:\n{df_shg.shape}\n")
            #print(f"Stupid check - does df_shg.shape[0] > 0? : {check}")
            if check:
                df_stakeholder = aggregate_awareness(df_shg)
                #print(f"df_stakeholder after aggregate:\n{df_stakeholder.head(1)}")
                df_stakeholder = clean_agg(df_stakeholder)
                df_stakeholder['stakeholder_id'] = sh_group.stakeholder_id.unique()[0]
                res = pd.concat([res,df_stakeholder])#res.append(, ignore_index=True)
            else:
                no_cases[sh_var1] = sh_group.stakeholder_id.unique()[0]
                #print(f"awareness_main.py: Column {sh_var1} not found for stakeholder ID "
                #      f"{sh_group.stakeholder_id.unique()[0]}.")
        else:
            missing_cases[sh_var1] = sh_group.stakeholder_id.unique()[0]
            #print(f"awareness_main.py: Stakeholder {sh_var1} ({sh_group.stakeholder_id.unique()[0]})"
            #      f" not found in awareness data.")
    print(f"\nStakeholder variables found in the data but no cases identified:\n{no_cases}")
    print(f"Stakeholder variables completely missing from the data:\n{missing_cases}\n")
    #---------------------------------------------------------------------------
    print("awareness_main.py: Awareness computations complete. Cleaning up results.")
    res=res[['Rating','familiarity_id','user_count','familiarity_percent',
            'familiarity_label','stakeholder_id']].copy()
    res.dropna(how = 'any', axis = 0, inplace=True)
    print(f"awareness_main.py:\n{res.head(5)}")
    df_awareness['Rating'] = pd.to_numeric(df_awareness['Rating'])
    res.Rating = res.Rating.astype(int, copy = False)
    awareness = pd.merge(res, df_awareness, on=['Rating'], how='left') # NOT OPTIMIZED
    awareness.dropna(how = 'any', axis = 0, inplace=True)
    awareness = awareness.drop_duplicates()
    to_int = ['Year', 'Month','Country','Company','Rating','user_count']
    awareness[to_int] = awareness[to_int].astype(int)
    #print(" --- ")
    #print("Checking Awareness:")
    #print(awareness.groupby(['stakeholder_id'],as_index=False)['user_count'].count())
    #print(" --- ")
    clean_cmd = upload_awareness(awareness)
    print("===================================")
    print("=        Awareness COMPLETE       =")
    print("===================================")
    #return(awareness)
