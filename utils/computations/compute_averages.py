import os
from itertools import islice

import numpy as np
import pandas as pd


# Compute Rolling Averages
def identify_cols(df):
    q320 = [col for col in df if (col.startswith('q320') and col.endswith('_g_reb'))]
    q215 = [col for col in df if (col.startswith('q215') and col.endswith('_g_reb'))]
    q410 = [col for col in df if (col.startswith('q410') and col.endswith('_g_reb'))]
    q800 = [col for col in df if (col.startswith('q800') and col.endswith('_g_reb'))]
    scoring = ['pulse_g_reb'] + q320 + q215 + q410 + q800
    listvar = ['company','month','country','pulse_g_reb'] + q320 +\
            q215 + q410 + q800 + ['weight_final']
    return([scoring,listvar])

# Apply weights [Sergio]
def weighted_average(group):
    if group.iloc[:,4].sum()>0: #Should check for is not null.
        return (group.iloc[:,3] * group.iloc[:,4]).sum() / group.iloc[:,4].sum()
    elif group.iloc[:,3].sum()==0:
        return np.nan
    elif group.iloc[:,3].sum()==0:
        return np.nan

# Function to CAREFULLY apply weights
def weight(data, score_cols):
    data_copy = data.copy()
    res = pd.DataFrame()
    group_by_cols = ['month','country','company']

    for v in score_cols:
        to_group = data_copy[pd.notna(data_copy[v])][['month','country','company', v,'weight_final']]
        if to_group.shape[0] == 0:
            res[v] = np.nan
        else:
            res[v] = to_group\
                .groupby(group_by_cols)\
                .apply(weighted_average)
    res.reset_index(inplace = True)
    return(res)


# Function for simple rolling averages
def simple_means(data, all_cols, score_cols):
    data_copy = data.copy()
    res = data_copy.groupby(['country','company']).mean().reset_index()
    return(res)


def compute_rolling_avg(data):
    # Enforce data-type, these are sensitive
    enforced_months = ['month','country','company']
    data[enforced_months] = data[enforced_months].apply(pd.to_numeric, errors = 'coerce')
    # Get latest month, important columns.
    current_month = data.crt.max()
    current = data[data.crt == current_month].copy()
    print("Current month before weighting: {}, crt: {}".format(current.month.unique(),
                                                               current.crt.unique()))
    merging_months = ['country','company']
    # Weigh the data
    score_cols, all_cols = identify_cols(current)
    # Fix a bug - June 4, 2020
    current[score_cols] = current[score_cols].apply(pd.to_numeric, errors = 'coerce')
    data[score_cols] = data[score_cols].apply(pd.to_numeric, errors = 'coerce')
    print("Applying weights")
    current_month_weighted = weight(current, score_cols)
    all_months_weighted = weight(data, score_cols)
    rolled_months = simple_means(all_months_weighted, all_cols, score_cols)
    rolled_months.drop(columns=['month'], inplace=True)
    #==============================
    # EXTREMELY Careful Deltas computation
    # There are definitely clever ways of pulling this off,
    # but today we focus on accuracy.
    # Merge all_months_weighted into current_month weighted
    rename_averaged_columns = {x: "aws_"+x for x in all_months_weighted[score_cols].columns}
    rolled_months.rename(columns=rename_averaged_columns, inplace = True)
    current_month_weighted = pd.merge(current_month_weighted, rolled_months,
                                      on=merging_months , validate='1:1' , how='left')
    # Calculate Deltas
    deltas = []
    for var, rolled in rename_averaged_columns.items():
        dlt = "dlt_" + var
        deltas.append(dlt)
        current_month_weighted[dlt] = current_month_weighted[rolled] - current_month_weighted[var]
    delta_vars = merging_months + deltas
    current = pd.merge(current, current_month_weighted[delta_vars],
                       on = merging_months, validate='m:1', how = 'left')
    # Apply Deltas
    for var, rolled in rename_averaged_columns.items():
        orig = "orig_" + var
        dlt = "dlt_" + var
        current[orig] = current[var]
        current[var] = current[orig] + current[dlt]

    print("Done with rolling.\n---")
    #==============================
    return(current)
