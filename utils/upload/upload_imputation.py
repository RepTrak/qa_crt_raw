# Essentials
import calendar
import os
from datetime import date

import numpy as np
import pandas as pd
import pyreadstat

# Get keys
from utils.io.io import Zip
from utils.io.get_stakeholders import organize_stakeholders
from utils.io.redshift_access import access_redshift, upload


def get_col_names(table_name):
    con, cur = access_redshift()
    cur.execute("select * \
    from crt_ds_etl.upload_col_names \
    where table_name = '{}';".format(table_name))
    col_data = pd.DataFrame(cur.fetchall())
    col_data.columns = ['table_name', 'columns']
    redshift_columns = col_data.T.loc['columns', :]
    return (redshift_columns)

def fix_industries(dataframe):
    if 'new_industry1' in dataframe.columns:
        dataframe['gics'] = dataframe['new_industry1']
    elif 'gics' in dataframe.columns:
        print('gics already in data')
    else:
        dataframe['gics'] = 99
    dataframe['industry_project'] = 0
    dataframe['industry_id'] = dataframe['gics']
    return (dataframe)

def type_enforcement(dataframe):
    # Type enforcement
    imp_cols = [col for col in dataframe if col.endswith('_imp')]
    dataframe[imp_cols] = dataframe[imp_cols].astype(float)
    dataframe['global_id'] = dataframe['global_id'].astype('Int64')

    int_cols = ['year', 'month', 'country', 'company', 'rating',
                'industry_project', 'gics', 'stakeholder_id',
                'age', 'gender', 'q600a', 'income', 'education',
                'global_id', 'modeling_sample_$']
    q600s = [q600 for q600 in dataframe if q600.startswith('q600')]
    tps = [tp for tp in dataframe if tp.startswith('tp_')]
    weird_cols = ['race', 'occupation', 'q600b']
    all_cols = int_cols + q600s + tps + weird_cols
    
    for int_column in all_cols:
        if int_column in dataframe.columns:
            # print(int_column)
            try:
                dataframe[int_column] = pd.to_numeric(dataframe[int_column])
            except:
                print(f"Failed at {int_column}")
                print(f"Values: {dataframe[int_column].unique()}")
        else:
            # Some of these columns are garbage. Should be removed
            dataframe[int_column] = 99
    return (dataframe)


def correct_modeling_sample(dataframe):
    # Making sure Global_ID is an integer
    dataframe['global_id'] = dataframe.global_id.astype('Int64')

    # Identify ratings with less than 100 samples
    dataframe['rating_stakeholder'] = dataframe.rating.astype(str) \
                                      + "_" + dataframe.stakeholder_id.astype(str)
    counts = dataframe.groupby('rating_stakeholder', as_index=False)['global_id'].count()
    low_counts = counts[counts.global_id < 100]
    low_counts_rating_stakeholder = counts[counts.global_id < 100].rating_stakeholder.unique().tolist()

    # Isolate from other samples, and make all modeling_sample= 1
    low_count_dataframe = dataframe[dataframe.rating_stakeholder.isin(low_counts.rating_stakeholder)].copy()
    dataframe = dataframe[~dataframe.rating_stakeholder.isin(low_counts_rating_stakeholder)].copy()
    low_count_dataframe['modeling_sample_$'] = 1

    # For remaining dataset, sample 100 random cases
    dataframe['modeling_sample_$'] = 0
    dataframe['gid_sid'] = dataframe.global_id.astype(str) \
                           + "_" + dataframe.stakeholder_id.astype(str)
    print("group by check:")
    #print(dataframe.groupby(['rating', 'stakeholder_id'])['global_id'].count().sort_values())
    if dataframe.shape[0] > 0:
        ones = dataframe.groupby(['rating', 'stakeholder_id']) \
            .sample(100, replace=False, random_state=617) \
            .reset_index(drop=True) \
            .gid_sid \
            .tolist()
        dataframe.loc[dataframe.gid_sid.isin(ones), 'modeling_sample_$'] = 1
        # Append back to main dataframe
        dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()].copy()
        low_count_dataframe = low_count_dataframe.loc[:, ~low_count_dataframe.columns.duplicated()].copy()
        dataframe = pd.concat([dataframe, low_count_dataframe])  # ,ignore_index=True)
    else:
        print("Warning! No cases (rating+stakeholder_id) >= 100!")
        dataframe = low_count_dataframe.copy()
    return (dataframe)


def fix_modeling_samples(dataframe, modeling_sample_df):
    modeling_samples = modeling_sample_df[
        ['global_id', 'stakeholder_id', 'modeling_sample_$']].copy().drop_duplicates()
    modeling_samples.rename({'global_id': 'Global_ID',
                             'modeling_sample_$': 'Modeling_Sample_$'},
                            axis=1, inplace=True)
    mo1 = modeling_samples[modeling_samples.stakeholder_id == 1].copy()
    mo2 = modeling_samples[modeling_samples.stakeholder_id != 1].copy()
    mo2 = mo2[~mo2.Global_ID.isin(mo1.Global_ID)]
    mo2 = mo2.drop_duplicates(subset='Global_ID')
    mo1 = pd.concat([mo1, mo2])  # .append(mo2)
    #print(mo1[['Global_ID','stakeholder_id']].head(1))
    #print(dataframe[['Global_ID','stakeholder_id']].head(1))
    dataframe.drop('Modeling_Sample_$', axis=1, inplace=True, errors='ignore')
    dataframe['Global_ID'] = dataframe['Global_ID'].astype('Int64')
    mo1['Global_ID'] = mo1['Global_ID'].astype('Int64')
    dataframe = dataframe.merge(mo1, on=['Global_ID'])#,'stakeholder_id'])
    return(dataframe)



def upload_imp(df, upload_cmd,
               paths_dic, meta):

    # Get rid of junk
    invalid = [x for x in df if x.startswith('@')]
    df.drop(invalid, axis=1, inplace=True)
    df = df[~df.COUNTRY.isna()].copy()
    df['COUNTRY'] = df.COUNTRY.astype(int)

    backup_df = df.copy()
    table_name = 'crt_ds_etl.etl_driver'
    imp_cols = get_col_names(table_name)

    #   --------------------------------------
    # Lowercasing column names:
    df.columns = map(str.lower, df.columns)
    if df.month.unique()[0] > 12:
        df['month'] = df.month - (df.year - 2015) * 12
    df = df.loc[:, ~df.columns.duplicated()].copy()

    #   --------------------------------------
    # Type enforcement
    df = type_enforcement(df)

    #   --------------------------------------
    # Resolving GICs
    df = fix_industries(df)

    #   --------------------------------------
    #   Add stakeholders
    print("Applying Stakeholder fix.")
    final_df = organize_stakeholders(df)

    #   --------------------------------------
    #   Fix modeling sample due to stakeholder
    final_df = correct_modeling_sample(final_df)

    #   --------------------------------------
    #   Final step - put in order. Just in case
    #   Upload to standard driver database.
    imputed_df = final_df[imp_cols].copy()
    imputed_df['industry_id'] = imputed_df['industry_id'].astype('Int64')
    print("***\nImputed dataset size: {}".format(imputed_df.shape))
    if upload_cmd:
        print("\t\t==========\nUploading Imputed Data to Redshift\n\t\t==========")
        upload(imputed_df, table_name=table_name)
        print("Imputed variables upload complete!\n***")
    else:
        print("Upload skipped! (OFF)\n***")

    #   --------------------------------------
    #   Database 2 - all_imputation_vars
    new_table_name = 'crt_ds_etl.all_imputation_vars'
    new_imp_cols = get_col_names(new_table_name)
    final_df['study'] = 5
    final_df['unique_respondent'] = final_df['unique_respondent_all_$']

    extra_cols = ['stakeholder_id', 'modeling_sample_$', 'unique_respondent', 'study',
                  'year', 'month', 'country', 'company', 'rating', 'company_type', 'gics', 'industry_project',
                  'gender', 'age', 'familiarity', 'race', 'education',
                  'employment', 'occupation', 'income', 'region']

    for int_column in extra_cols:
        if int_column in final_df.columns:
            #final_df[int_column] = pd.to_numeric(final_df[int_column], errors='coerce')#.astype('Int64')
            final_df[int_column] = final_df[int_column].astype('Int64')
        else:
            print(f"Typecast: Could not find column {int_column}")
            # Some of these columns are garbage. Should be removed
            final_df[int_column] = 99
    missing_columns = []
    print("Last check for missing columns")
    for missing_column in new_imp_cols:
        if missing_column not in final_df.columns:
            final_df[missing_column] = 99
            missing_columns.append(missing_column)
    print(f"The following columns not found in final_df: {missing_columns}")

    #   --------------------------------------
    # Upload
    imputed_df = final_df[new_imp_cols].copy()
    print("***\nNew imputed dataset size: {}".format(imputed_df.shape))
    if upload_cmd:
        print("\t\t==========\nUploading Imputed Data to Redshift\n\t\t==========")
        upload(imputed_df, table_name=new_table_name)
        print("Imputed variables upload complete!\n***")
    else:
        print("Upload skipped! (OFF)\n***")

    #   --------------------------------------
    #   Prepare SPSS file!
    #   Fixing Modeling Samples
    spss_df = fix_modeling_samples(backup_df, final_df)
    spss_df = spss_df.loc[:, ~spss_df.columns.duplicated()].copy()

    # --------------------------------------
    # Setting up filenames
    month = calendar.month_abbr[final_df.month.unique()[0].astype(int)]
    year = final_df.year.unique()[0].astype(str)
    zip_filename = "./" + month + year + "_CRT Results.zip"
    spss_csv_file = "SPSS_" + paths_dic['complete_filename'][2:]

    # --------------------------------------
    # Saving/zipping csv
    print(f"Saving csv with correct modeling samples: {spss_csv_file}")
    spss_df.to_csv(spss_csv_file, index=False)
    print(f"Zipping results: {zip_filename}")
    Zip(spss_csv_file,zip_filename)
    print("Deleting unzipped df.")

    if os.path.exists(paths_dic['complete_filename']):
        os.remove(paths_dic['complete_filename'])
    if os.path.exists(spss_csv_file):
        os.remove(spss_csv_file)  # This might be a overstep.

    # --------------------------------------
    # Saving/zipping SAV
    spss_sav_filename = paths_dic['complete_filename'][2:] + '.sav'
    print(f"Saving and zipping .sav file: {spss_sav_filename}")

    # Double check:Get rid of junk
    invalid = [x for x in spss_df if x.startswith('@')]
    spss_df.drop(invalid, axis=1, inplace=True)
    
    pyreadstat.write_sav(spss_df,
                         dst_path=spss_sav_filename,
                         column_labels=meta['column_labels'],
                         variable_value_labels=meta['variable_value_labels'],
                         missing_ranges=meta['missing_ranges'],
                         variable_display_width=meta['variable_display_width'],
                         variable_measure=meta['variable_measure'])
    zip_filename_spss = "./" + month + year + "_SAV Results.zip"
    print(f"Zipping results {zip_filename_spss}")
    Zip(spss_sav_filename, zip_filename_spss)
    print("Deleting unzipped df.")
    if os.path.exists(spss_sav_filename):
        os.remove(spss_sav_filename)

    return([zip_filename, zip_filename_spss])