# Essentials
import os
import path
import collections
import re
from itertools import islice

import numpy as np
import pandas as pd

from utils.io.redshift_access import get_available_data, drop_month, get_month, upload

# Columns from redshift, just in case
def get_redshift_columns():
    redshift_columns = ['year','month','company','country','crt','global_id','pulse_g_reb',
                'q215_10r_g_reb','q215_11r_g_reb','q215_15r_g_reb','q215_17r_g_reb',
                'q215_2r_g_reb','q215_3r_g_reb','q215_4r_g_reb','q215_5r_g_reb',
                'q215_6r_g_reb','q215_7r_g_reb','q215_8r_g_reb','q215_9r_g_reb',
                'q310_1r_g_reb','q310_2r_g_reb','q310_3r_g_reb','q310_4r_g_reb',
                'q310_5r_g_reb','q310_6r_g_reb','q310_7r_g_reb','q320_10r_g_reb',
                'q320_11r_g_reb','q320_12r_g_reb','q320_13r_g_reb','q320_14r_g_reb',
                'q320_15r_g_reb','q320_16r_g_reb','q320_17r_g_reb','q320_18r_g_reb',
                'q320_19r_g_reb','q320_1r_g_reb','q320_20r_g_reb','q320_21r_g_reb',
                'q320_22r_g_reb','q320_23r_g_reb','q320_24r_g_reb','q320_26r_g_reb',
                'q320_27r_g_reb','q320_28r_g_reb','q320_29r_g_reb','q320_2r_g_reb',
                'q320_3r_g_reb','q320_4r_g_reb','q320_5r_g_reb','q320_6r_g_reb',
                'q320_7r_g_reb','q320_8r_g_reb','q320_999r_g_reb','q320_9r_g_reb',
                'q360_32r_g_reb','q360_42r_g_reb','q360_43r_g_reb','q360_44r_g_reb',
                'q360_45r_g_reb','q410_10r_g_reb','q410_1r_g_reb','q410_2r_g_reb',
                'q410_3r_g_reb','q410_4r_g_reb','q410_5r_g_reb','q410_6r_g_reb',
                'q410_9r_g_reb','q710a_10r_g_reb','q710a_1r_g_reb','q710a_2r_g_reb',
                'q710a_3r_g_reb','q710a_4r_g_reb','q710a_5r_g_reb','q710a_6r_g_reb',
                'q710a_7r_g_reb','q710a_8r_g_reb','q710a_9r_g_reb','q800_2r_g_reb',
                'q800_3r_g_reb','q800_4r_g_reb','q800_5r_g_reb','q800_6r_g_reb',
                'q800_7r_g_reb','q800_8r_g_reb','q800_9r_g_reb','weight_final']
    return(redshift_columns)

# Compute historic crt months
def comp_crt_month(data):
    return((data.year-2015)*12 + data.month)

def get_cols(df1, df2):
    return([df1.columns.tolist(), df2.columns.tolist()])

def align(new_df, archive_df):
    # Prep new
    old_dims = new_df.shape
    archive_columns, new_df_columns = get_cols(archive_df, new_df)
    not_in_archive = np.setdiff1d(new_df_columns, archive_columns).tolist()
    add_to_archive = {i:np.nan for i in not_in_archive}
    archive_df = archive_df.assign(**add_to_archive)
    archive_columns, new_df_columns = get_cols(archive_df, new_df)
    not_in_new = np.setdiff1d(archive_columns, new_df_columns).tolist()
    add_to_new = {i:np.nan for i in not_in_new}
    new_df = new_df.assign(**add_to_new)
    # Check
    if(collections.Counter(new_df.columns.tolist()) == collections.Counter(archive_df.columns.tolist())):
        print("Dataframes aligned!")
    else:
        print("\n------\nWARNING: DATAFRAMES DID NOT ALIGN\n------\n")
    return(new_df, archive_df)

# Check data & archive, merge to archive
def add_to_archive(new, archive, upload_cmd):
    # Dumb bug - June 04, 2020
    print("\n=========")
    archive[['crt']] = archive[['crt']].apply(pd.to_numeric, errors = 'coerce')
    print("archive crt values: {}".format(archive.crt.unique()))
    print("=========\n")
    new_ym = new.groupby(['month','crt']).size().reset_index().rename(columns={0:'count'})
    ym = get_available_data()
    ym['crt'] = ym.Month
    min_month = archive.crt.min()
    print("Months in archive: {} || Month of new dataframe: {}".format(archive.crt.unique(), new.crt.unique()))

    ########################################################################
    # Check new data validity (1 month):
    if(new_ym.shape[0] != 1):
        raise Exception('New dataset has several months; should only have 1. Size: {}'.format(new_ym.shape))
    current_month = new_ym.crt.unique()[0]
    ########################################################################
    #                               ALIGNMENT
    # Identify missing columns and add them (np.nan)
    redshift_columns = get_redshift_columns()
    new, archive = align(new, archive)
    new = new[redshift_columns]
    archive = archive[redshift_columns]
    new=new[~new.global_id.isna()].copy()
    new['global_id']=new['global_id'].astype('Int64')
    new['company']=new['company'].astype(int)
    new['year']=new['year'].astype(int)
    new['country']=new['country'].astype(int)
    new['crt']=new['crt'].astype(int)
    print(new.head(1))
    if(ym.crt.max() == current_month):
        # If we're re-adding the same month's data, we're going to replace it in the archive.
        # The assumption here is that we're re-running data because our previous run is bad.
        print("Data for the month {}, {}(crt), already exists.".format(
            new_ym.month.unique()[0],
            current_month))
        if upload_cmd:
            print("Updating the archive. Dropping month {}".format(current_month))
            drop_month(current_month)
            print("Checks met! Uploading new data to Redshift.")
            upload(new, show = False)
            print("Uploading complete!\n***")
        else:
            print("Dropping/Updating current month skipped!")
        # Merge into output data
        new_archive = archive.append(new, ignore_index = True, sort=True)
        new_archive.dropna(axis=1, how='all', inplace=True)
        del archive
    # If everything is alright, we just append the data
    else:
        if upload_cmd:
            print("Checks met! Uploading new data to Redshift.")
            upload(new, show = False)
            print("Uploading complete!")
        else:
            print("Upload skipped!")

        new_archive = archive.append(new, ignore_index = True, sort=True)
        new_archive.dropna(axis=1, how='all', inplace=True)
        del archive
    new.to_csv("upload_to-crt_ds_etl.etl_unrolled.csv",index=False)
    del new
    rs_cols = new_archive.columns.tolist()
    new_archive[rs_cols] = new_archive[rs_cols].apply(pd.to_numeric, errors='coerce')
    print("Redshift-Archive process complete!")
    return(new_archive)


# Consistency checks: Makes sure the data we're about to add is valid.
# IE current (not older than a month) and there is historical data to support it.
def fetch_archive(current_month):
    available = get_available_data()
    print("Working on current month:{}.".format(current_month))
    if(available.Month.max() > current_month):
        raise Exception("Old data uploaded to dataset. Rolling halted!")
    elif( current_month-2 not in list(available.Month) ):
        print(available.Month)
        raise Exception("Missing previous data in archive! Month number 2 missing: {}".format(current_month-2))
    elif( current_month-1 not in list(available.Month) ):
        print(available.Month)
        raise Exception("Missing data in archive! Month number 1 missing: {}".format(current_month-1))
    rs_cols = get_redshift_columns()
    print("Current month (CRT): {}".format(current_month))
    print("Fetching Current -1 ({})".format(current_month - 1))
    m1 = get_month(current_month - 1, rs_cols)
    print("Fetching Current -2 ({})".format(current_month - 2))
    m2 = get_month(current_month - 2, rs_cols)
    print("Months have been downloaded from Redshift.\n-----")
    historical = m1.append(m2)
    del m1
    del m2
    return(historical)


# Wrap it all up
def get_data(df,upload_cmd):
    df.month = df.month.astype(int, copy = False)
    df['crt'] = comp_crt_month(df)
    current_month = df.crt.unique()[0]
    return(add_to_archive(df,fetch_archive(current_month),upload_cmd))
