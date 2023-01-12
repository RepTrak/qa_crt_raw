from datetime import datetime as dt
from functools import reduce
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

import numpy as np
import pandas as pd

from utils.computations.compute_main import compute_ESG


def impute_em(X, max_iter = 500, eps = 1e-03):
    '''(np.array, int, number) -> {str: np.array or int}

    Precondition: max_iter >= 1 and eps > 0

    Return the dictionary with five keys where:
    - Key 'mu' stores the mean estimate of the imputed data.
    - Key 'Sigma' stores the variance estimate of the imputed data.
    - Key 'X_imputed' stores the imputed data that is mutated from X using
      the EM algorithm.
    - Key 'C' stores the np.array that specifies the original missing entries
      of X.
    - Key 'iteration' stores the number of iteration used to compute
      'X_imputed' based on max_iter and eps specified.
    '''
    nr, nc = X.shape
    C = np.isnan(X) == False
    # Collect M_i and O_i's
    one_to_nc = np.arange(1, nc + 1, step = 1)
    M = one_to_nc * (C == False) - 1
    O = one_to_nc * C - 1
    # Generate Mu_0 and Sigma_0
    Mu = np.nanmean(X, axis = 0)
    observed_rows = np.where(np.isnan(sum(X.T)) == False)[0]
    S = np.cov(X[observed_rows, ].T)
    if np.isnan(S).any():
        S = np.diag(np.nanvar(X, axis = 0))
    # Start updating
    Mu_tilde, S_tilde = {}, {}
    X_tilde = X.copy()
    no_conv = True
    iteration = 0
    while no_conv and iteration < max_iter:
        for i in range(nr):
            S_tilde[i] = np.zeros(nc ** 2).reshape(nc, nc)
            if set(O[i, ]) != set(one_to_nc - 1): # missing component exists
                M_i, O_i = M[i, ][M[i, ] != -1], O[i, ][O[i, ] != -1]
                S_MM = S[np.ix_(M_i, M_i)]
                S_MO = S[np.ix_(M_i, O_i)]
                S_OM = S_MO.T
                S_OO = S[np.ix_(O_i, O_i)]
                Mu_tilde[i] = Mu[np.ix_(M_i)] +\
                    S_MO @ np.linalg.inv(S_OO) @\
                    (X_tilde[i, O_i] - Mu[np.ix_(O_i)])
                X_tilde[i, M_i] = Mu_tilde[i]
                S_MM_O = S_MM - S_MO @ np.linalg.inv(S_OO) @ S_OM
                S_tilde[i][np.ix_(M_i, M_i)] = S_MM_O
        Mu_new = np.mean(X_tilde, axis = 0)
        S_new = np.cov(X_tilde.T, bias = 1) +\
            reduce(np.add, S_tilde.values()) / nr
        no_conv =\
            np.linalg.norm(Mu - Mu_new) >= eps or\
            np.linalg.norm(S - S_new, ord = 2) >= eps
        Mu = Mu_new
        S = S_new
        iteration += 1

    result = {
        'mu': Mu,
        'Sigma': S,
        'X_imputed': X_tilde,
        'C': C,
        'iteration': iteration
    }

    return result

def check_for_nan(dataset):
    # Identify empty columns - remove them
    nan_values=dataset.isna()
    nan_columns=nan_values.all()
    columns_with_nan=dataset.columns[nan_columns].tolist()
    if len(columns_with_nan)>0:
        #print("Identified the following columns to be completely empty: {}".format(columns_with_nan))
        dataset.drop(columns_with_nan,inplace=True,axis=1)
    return(dataset)

def imputation_loop(data,verbose):
    # So annoying!!
    warnings.filterwarnings("ignore")

    res=pd.DataFrame()
    for variable in data['ImpLoopVar'].unique():
        subset=data[data['ImpLoopVar']==variable].copy()
        #if verbose:
        #print("Working with {}, shape: {} \r".format(variable,subset.shape))
        subset.drop(['ImpLoopVar'],axis=1,inplace=True)
        subset_column_names=subset.columns.tolist()
        subset=check_for_nan(subset)
        subset_columns_after_removing_nan=subset.columns.tolist()
        # Run and time imputations
        #print("Imputing on these columns: {}".format(subset_columns_after_removing_nan))

        try:
            start=dt.now()
            result_imputed = impute_em(np.array(subset))
            result_imputed = pd.DataFrame(result_imputed['X_imputed'])
            result_imputed.columns=subset_columns_after_removing_nan
            end=dt.now()
            # Done
            #print("Imputed {} in {} \r".format(variable, end-start))
            result_imputed=result_imputed.reindex(columns=subset_column_names).copy()
        except:
            #print("***\n{} - Case failed. Insufficient Samples: {}\nWill attempt to SMEAN.\n***"\
            #    .format(variable, subset.shape[0]))
            result_imputed=subset.replace(99,np.nan).transform(lambda x: x.fillna(x.mean()))
            result_imputed=result_imputed.reindex(columns=subset_column_names).copy()
        res=pd.concat([res,result_imputed])#res.append(result_imputed)
    return(res)


def impute(data,variables_for_imputation,var1:str,verbose=False,suffix='_imp'):
    # Clean data
    # NEEDS TO BE FIXED FOR IMPLEMENTATION
    data['ImpLoopVar']=data[var1].astype(str)
    data[variables_for_imputation]=data[variables_for_imputation].replace(['',' ',99],np.nan)
    data[variables_for_imputation]=data[variables_for_imputation].astype('float64')
    complete_dataset_columns=['Global_ID']+variables_for_imputation
    data=data[complete_dataset_columns+['ImpLoopVar']].copy()
    # Check for invalid cases at a global level.
    data=check_for_nan(data)
    start=dt.now()
    results=imputation_loop(data,verbose)
    results=results.reindex(columns=complete_dataset_columns).copy()
    renaming_dic={}
    for variable in variables_for_imputation:
        renaming_dic[variable]=variable+suffix
    results.rename(renaming_dic,axis=1,inplace=True)
    end=dt.now()
    print("imputation.py: Battery imputation completed in {}".format(end-start))
    return(results)


def get_battery(data,variable):
    res=[col for col in data if (col.startswith(variable)
        and not col.endswith('G_Reb') and not col.endswith('G_Reb_1'))]
    return(res)


def get_battery_crude():
    q320=['Q320_1', 'Q320_2', 'Q320_3', 'Q320_4',
    'Q320_5', 'Q320_6', 'Q320_7', 'Q320_8',
    'Q320_9', 'Q320_10', 'Q320_11', 'Q320_12',
    'Q320_13', 'Q320_14', 'Q320_15', 'Q320_16',
    'Q320_17', 'Q320_18', 'Q320_19', 'Q320_20',
    'Q320_21', 'Q320_22', 'Q320_23','Q320_24']
    q215=['Q215_3', 'Q215_4', 'Q215_5', 'Q215_6',
        'Q215_7', 'Q215_8', 'Q215_10']
    q410=['Q410_1', 'Q410_2', 'Q410_3', 'Q410_5', 'Q410_6']
    q800=['Q800_2','Q800_3','Q800_4','Q800_5',
        'Q800_6', 'Q800_7','Q800_8','Q800_9']
    return([q320,q215,q410,q800])


def run_imputations(data, constructs, var="Rating", verbose=False):
    # This is the master function for imputing our surveys' data.
    res=data.copy()
    #
    q320,q215,q410,q800=get_battery_crude()
    print("imputation.py: Data shape: {}".format(res.shape))
    print(f"imputation.py: Running imputations on Q320 by {var}")
    imp=impute(data,q320,var,verbose)
    print("imputation.py: Completed imputations on Q320.")
    #print("imputation.py: Data shape: {}".format(res.shape))
    res=res.merge(imp,on='Global_ID',validate='1:1')
    #
    print("imputation.py: Running imputations on Q215")
    imp=impute(data,q215,var,verbose)
    #print("imputation.py: Completed imputations on Q215.\-")
    #print("imputation.py: Data shape: {}".format(res.shape))
    res=res.merge(imp,on='Global_ID',validate='1:1')
    #
    print("imputation.py: Running imputations on Q410")
    imp=impute(data,q410,var,verbose)
    #print("imputation.py: Completed imputations on Q410.\n---")
    #print("imputation.py: Data shape: {}".format(res.shape))
    res=res.merge(imp,on='Global_ID',validate='1:1')
    #
    print("imputation.py: Running imputations on Q800")
    imp=impute(data,q800,var,verbose)
    #print("imputation.py: Completed imputations on Q800.\n---")
    res=res.merge(imp,on='Global_ID',validate='1:1')

    # Fix imputed results.
    print("imputation.py: Fixing out of bounds results in imputations.")
    imp_cols=[col for col in res if col.endswith('_imp')]
    for imp_col in imp_cols:
        res.loc[res[imp_col]>7,imp_col]=7
        res.loc[res[imp_col]<1,imp_col]=1

    dimensions = constructs.construct.unique().tolist()
    for construct in dimensions:
        if construct == "dep_var":
            print("imputation.py: Skipping dep_var_imp")
        else:
            #print("imputation.py: Computing {}".format(construct+"_imp"))
            variables=constructs[constructs.construct==construct].variables.tolist()
            use_these=[var+"_imp" for var in variables]
            res[construct+"_imp"]=res[use_these].mean(axis=1)

    res = compute_ESG(res)
    print("imputation.py: Imputations are complete.")
    print("imputations.py: saving backup data 'post_imputations.csv'")
    res.to_csv("post_imputations.csv", index=False)
    return(res)
