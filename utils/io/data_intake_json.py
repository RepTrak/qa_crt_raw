import os
import re
import json
import pandas as pd
# from utils.io.redshift_access import access_redshift


def get_json_integers(json_meta):
    json_variables = json_meta['variables']
    integer_cols = []
    for json_variable in json_variables:
        if 'values' in json_variable.keys():
            if type(json_variable['values'][0]['value']) == int:
                integer_cols.append(json_variable['label'])
    return(integer_cols)

def crazy_convert(frame, col):
    try:
        res = frame2[col].astype(np.float64)
        res[res<-1] = np.nan
    except:
        res = frame[col].values
    return(res)

def get_stakeholders_awareness():
    con, cur = access_redshift()
    cur.execute("SELECT full_var_name, awareness_var, awareness_condition FROM crt_ds_etl.stakeholder_ids;")
    data=cur.fetchall()
    stakeholders = pd.DataFrame(data)
    stakeholders.columns=['full_var_name', 'awareness_var', 'awareness_condition']
    return(stakeholders)


def check_issues(data, stakeholder_check, respondent_check):
    # Checks false positives
    issues = data[(stakeholder_check & respondent_check)].shape[0] / data[stakeholder_check].shape[0]
    return (issues)


def subset_audience_flags(data,
                          stakeholder_var, sv_condition,
                          respondent_var, rv_condition):
    stakeholder_check = (data[stakeholder_var] == sv_condition)
    respondent_check = (data[respondent_var] != rv_condition)
    issues = check_issues(data, stakeholder_check, respondent_check)

    if issues == 0:
        print(f"No issues detected with stakeholder {stakeholder_var}.")
    else:
        print("{}(stakeholder_var) - False Positive rate: {:.02f}".format(stakeholder_var, issues))
        data.loc[(stakeholder_check & respondent_check), stakeholder_var] = 0
        stakeholder_check = (data[stakeholder_var] == sv_condition)
        respondent_check = (data[respondent_var] != rv_condition)
        if data[data[stakeholder_var] == 1].shape[0] == 0:
            print("No issues (No stakeholders!)")
        else:
            remaining_issues = check_issues(data, stakeholder_check, respondent_check)
            print("Remaining issues: {:.02f}\n---".format(remaining_issues))
    return (data)


def correct_stakeholder_flags(data, stakeholders):
    for i in range(stakeholders.shape[0]):
        if i != 0:
            stakeholder_var = stakeholders.loc[i, 'full_var_name']
            respondent_var = stakeholders.loc[i, 'awareness_var']
            respondent_cond = stakeholders.loc[i, 'awareness_condition']

            if stakeholder_var in data.columns.tolist():
                data[stakeholder_var] = pd.to_numeric(data[stakeholder_var], errors='coerce')
                #data[stakeholder_var] = data[stakeholder_var].astype('Int32',errors='ignore')
                data[stakeholder_var] = crazy_convert(data,stakeholder_var)

            if respondent_var in data.columns.tolist():
                data[respondent_var] = pd.to_numeric(data[respondent_var], errors='coerce')
                #data[respondent_var] = data[respondent_var].astype('Int32',errors='ignore')
                #data[respondent_var] = crazy_convert(data, respondent_var)

            if stakeholder_var not in data.columns.tolist():
                print(f"{stakeholder_var} not in the dataframe")
            elif data[data[stakeholder_var] == 1].shape[0] == 0:
                print(f"{stakeholder_var} has 0 cases.")
            elif respondent_var in data.columns.tolist():
                data = subset_audience_flags(data, stakeholder_var, 1, respondent_var, respondent_cond)
            else:
                print(f"{respondent_var}(Respondent variable) is not in the dataframe.")
    return (data)


def listdir_nohidden(path):
    for f in os.listdir(path):
        if not f.startswith('.') and f.endswith('.json'):
            yield f


def get_customQ_renaming_dic(data_frame):
    rename_custom_dic = {}
    customs = re.compile('Q_.*A_.*')  # cute
    col_names = data_frame.columns.tolist()

    for col in col_names:
        if customs.findall(col):
            drop_this = col.replace('A', '')  # this pulls Q_N_123A_001 (Q_A)
            rename_custom_dic[col] = drop_this
    return (rename_custom_dic)


def Merge(dict1, dict2):
    return (dict2.update(dict1))

def stack_jsons(data_frame):
    '''
    Order of Operation
    1. Duplicate frame (DF1, DF2)
    2. DF2 - Drop _first questions rating
    3. DF2 - Drop _first stakeholders
    4. DF2 - Assign RATINGORDER
    5. DF2 - Rename _second to _first variables
    6. DF1 - Drop _second questions, _ratings, _stakeholders
    7. DF1 - Assign RATINGORDER
    8. Merge and return
    ''';

    # 1
    print(f"JSON: Original Shape of dataframe: {data_frame.shape}")
    df1 = data_frame.copy()
    df2 = data_frame.copy()
    rename_custom_q = get_customQ_renaming_dic(data_frame)

    # 2, 3
    extras = dict(zip([x for x in data_frame.columns if x.startswith('Q311CEO')],
                      [x for x in data_frame.columns if x.startswith('Q310CEO')]))
    purpose1 = dict(zip([x for x in data_frame.columns if x.startswith('Q3001P')],
                        [x for x in data_frame.columns if x.startswith('Q3000P')]))
    purpose2 = dict(zip([x for x in data_frame.columns if x.startswith('Q3101P')],
                        [x for x in data_frame.columns if x.startswith('Q3100P')]))
    nl = dict(zip([x for x in data_frame.columns if x.startswith('Q207OE')],
                  [x for x in data_frame.columns if x.startswith('Q206OE')]))

    manual_renaming = {
        'CRT2': 'CRT1',
        'Company_Type2': 'Company_Type1',
        'Q331x1_99': 'Q330x1_99',
        'Rated_306': 'Rated_305',
        'Q601A': 'Q600A',
        'S106CEO': 'S105CEO',
        'Q306CEO': 'Q305CEO',
        'Q306CEO_1': 'Q305CEO_1',
        'Q316CEO':'Q315CEO'}

    Merge(purpose1, extras)
    Merge(purpose2, extras)
    Merge(manual_renaming, extras)
    Merge(nl, rename_custom_q)
    Merge(extras, rename_custom_q)

    batteries = ['Q305_', 'Q320_', 'Q215_', 'Q410_', 'Q420_', 'Q600_','Q605_', 'Q800_']
    drop_these_questions = []
    df1_batteries = []
    for battery in batteries:
        df1_batteries += [x for x in df2.columns.tolist() if x.startswith(battery)]

    stakeholders1 = [x for x in data_frame.columns if x.startswith('Stakeholder') &
                     x.endswith('1')]
    stakeholders1 += ['Company1', 'GlobalCompany1', 'GroupBenchmark_COMPANY1',
                      'INDUSTRY_INDEX1', 'KYLIE_COMPANY1', 'NASDAQ_COMPANY1',
                      'GRT100_COMPANY1', 'NEW_INDUSTRY1', 'NL_30_1', 'APAC_TOP1']

    drop_these_questions += df1_batteries
    drop_these_questions += stakeholders1
    drop_these_questions = [x for x in drop_these_questions if x in df2.columns]
    df2.drop(drop_these_questions, axis=1, inplace=True)
    # Check if drop was sucessful
    for col_name in drop_these_questions:
        if col_name in df2:
            print(f"JSON: {col_name} was found in df2 after dropping")

    # 4
    df2['RATINGORDER'] = 2

    # 5
    df2_batteries = []
    for question in df1_batteries:
        temp = list(question)
        temp[3] = str(int(temp[3]) + 1)
        question2 = "".join(temp)
        df2_batteries.append(question2)
    stakeholders2 = []
    for stakeholder in stakeholders1:
        stakeholders2.append(stakeholder[:-1] + "2")
    rename_battery_dic = dict(zip(df2_batteries, df1_batteries))
    rename_stakeho_dic = dict(zip(stakeholders2, stakeholders1))

    df2.rename(rename_battery_dic, axis=1, inplace=True)
    df2.rename(rename_stakeho_dic, axis=1, inplace=True)
    print(f"JSON: Dropping/Renaming custom questions from df2 ({len(rename_custom_q)} total)")
    drop_from_df2 = list(rename_custom_q.values())
    drop_from_df2 = [x for x in drop_from_df2 if x in df2.columns.tolist()]
    df2.drop(drop_from_df2, axis=1, inplace=True)
    df2.rename(rename_custom_q, axis=1, inplace=True)

    # 6
    df1.drop(stakeholders2, axis=1, inplace=True)
    df1.drop(df2_batteries, axis=1, inplace=True)
    drop_from_df1 = list(rename_custom_q.keys())
    drop_from_df1 = [x for x in drop_from_df1 if x in df1.columns.tolist()]
    df1.drop(drop_from_df1, axis=1, inplace=True)

    # 7
    df1['RATINGORDER'] = 1

    # 8
    df1 = df1.loc[:, ~df1.columns.duplicated()].copy()
    df2 = df2.loc[:, ~df2.columns.duplicated()].copy()
    print(f"---\nJSON: Shape of dataframes pre-merging:\n1: {df1.shape}, 2: {df2.shape}")
    final_data = pd.concat([df1, df2], ignore_index=True)
    final_data.reset_index(inplace=True, drop=True)
    print("JSON: Checking columns for stuff that should not be there.")
    invalid_columns = ['Q306_1', 'Company2', 'GlobalCompany2',
                       'Q321_1', 'APAC_TOP2']
    for invalid_column in invalid_columns:
        if invalid_column in final_data.columns:
            print(f"\n!!!\n{invalid_column} found in the final dataframe.")
    return(final_data)



def df_to_int64(df, json_metadata):

    print("JSON: Getting valid integers from metadata")
    valid_integers = get_json_integers(json_metadata)

    # ---
    print("JSON: Converting all columns to Int64")
    data_frame_cols = [col for col in valid_integers if
                       ('OE' not in col and col in df.columns)]
    print(f"JSON: Identified {len(data_frame_cols)} that need to be converted to integers")
    i = 0
    for col in data_frame_cols:
        i += 1
        if i % 1000 == 0:
            print(f"JSON: Data conversion complete on {i}th column, {col}")
        df[col] = df[col].astype('Int32',errors='ignore')#pd.to_numeric(df[col].values, errors='ignore')
        #df[col] = crazy_convert(df,col)
    return(df)
    # ---


def get_json(DATA_FOLDER,
                    json_metadata,
                    save=False,
                    stop=False,
                    individually=True):
    if individually:
        all_jsons = pd.DataFrame()
    FILES = listdir_nohidden(DATA_FOLDER)
    result = []
    all_jsons = pd.DataFrame()
    for file in FILES:
        print(f"FILE: {file}")
        result = json.load(open(DATA_FOLDER + file))
        print(f"JSON: Turning json ({file}, 1-by-1) (len: {len(result)}) to pandas dataframe!")
        result = pd.json_normalize(result)
        # result['COUNTRY'] = pd.to_numeric(result['COUNTRY'],errors='coerce')
        # result['respStatus'] = pd.to_numeric(result['respStatus'],errors='coerce')
        # result = result[result.respStatus==3].copy() #Experimental
        # result = df_to_int64(result, json_metadata)
        all_jsons = pd.concat([all_jsons, result])

    result = all_jsons.copy()
    del all_jsons
    # print(f"JSON: Stacking dataframe (current shape: {result.shape})")
    # result = stack_jsons(result)
    # result.reset_index(inplace=True,drop=True)
    # print(f"JSON: Data loaded. (shape: {result.shape})\n---")
    # result.rename({'GlobalCompany1':'Company',
    #                'Company1':'Rating',
    #                'EDUCATION_C':'Education_C'}, axis=1, inplace=True)

    # # ------------
    # result['respStatus'] = pd.to_numeric(result['respStatus'], errors='coerce')
    # result.loc[result.respStatus==2,'respStatus'] = 5 # IDK what to do with this!!
    # result.loc[result.respStatus==3,'respStatus'] = 2
    # # ------------
    # print(f"JSON: Deleting NA country cases! (shape: {result.shape[0]})")
    # result = result[result.COUNTRY.notna()].copy()
    # print(f"JSON: Final shape: {result.shape[0]}")

    # print("JSON: Correcting Stakeholder Flags!")
    # stakeholders = get_stakeholders_awareness()
    # df = correct_stakeholder_flags(result, stakeholders)
    # print("---\nJSON: Stakeholders have been corrected. Fixing IGP\n---")
    # df['Stakeholder_Sum1'] = pd.to_numeric(df['Stakeholder_Sum1'], errors='coerce')
    # df['hSampleType'] = pd.to_numeric(df['hSampleType'], errors='coerce')
    # print(f"hSampleType dtype: {df.hSampleType.dtype}\nStakeholder_Sum: {df.Stakeholder_Sum1.dtype}")
    # df.loc[df.Stakeholder_Sum1.isna(), 'Stakeholder_Sum1'] = 1
    # df.Stakeholder_Sum1.unique()
    # df.loc[((df.Stakeholder_Sum1 > 1) & (df.hSampleType == 2)), 'Stakeholder_IGP1'] = 0

    # print("JSON: Fixing internal research flags.")
    # internal_research_flags1 = ['INDUSTRY_INDEX1', 'NL_30_1', 'APAC_TOP1', 'KYLIE_COMPANY1', 'GroupBenchmark_COMPANY1']

    # for irf in internal_research_flags1:
    #     df[irf] = pd.to_numeric(df[irf], errors='coerce')
    #     df.loc[((df.Stakeholder_IGP1 == 0) | (df.hSampleType == 2)), irf] = 0
    # print("JSON: Done cleaning JSON file!\n---\n")
    # return(df)
    return result