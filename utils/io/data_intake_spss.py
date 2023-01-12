import re
import pandas as pd

def check_company_country(data_frame, sheet_name, writer):
    if 'COUNTRY' in data_frame.columns:
        data_frame['country']=data_frame.COUNTRY
    if 'Company' in data_frame.columns:
        data_frame['company'] = data_frame.Company
    if 'Global_ID' in data_frame.columns:
        data_frame['global_id'] = data_frame.Global_ID
    else:
        data_frame['global_id'] = data_frame.index
    gb = data_frame.groupby(['country','company'],as_index=False)['global_id'].count()
    gb.to_excel(writer, sheet_name=sheet_name)
    if 'Q320_1' in data_frame.columns:
        data_frame['q320_1'] = data_frame.Q320_1
    gb = data_frame.groupby(['country', 'company','q320_1'], as_index=False)['global_id'].count()
    gb.to_excel(writer, sheet_name=sheet_name+'q')


def get_spss_integers(spss_meta):
    integer_cols = []
    for var_name, variables in spss_meta.variable_value_labels.items():
        for value, value_label in variables.items():
            if type(int(value)) == int and var_name not in integer_cols:
                integer_cols.append(var_name)
    return(integer_cols)


def get_customQ_renaming_dic(data_frame):
    rename_custom_dic = {}
    customs = re.compile('Q_.*A_.*') # cute
    col_names = data_frame.columns.tolist()

    for col in col_names:
        if customs.findall(col):
            rename_this = col.replace('A', '') # this pulls (Q_N_123_001 from Q_N_123A_001)
            rename_custom_dic[col] = rename_this
    return(rename_custom_dic)

def get_customQ_renaming_dic_Q_A(data_frame):
    rename_custom_dic = {}
    customs = re.compile('Q_.*A$') # cute
    col_names = data_frame.columns.tolist()

    for col in col_names:
        if customs.findall(col):
            rename_this = col[:-1] # this pulls (Q_N_123_001 from Q_N_123A_001)
            rename_custom_dic[col] = rename_this
    return(rename_custom_dic)

def get_customQ_renaming_dic_aus(data_frame):
    rename_custom_dic = {}
    customs = re.compile('AP_.*A_.*') # cute
    col_names = data_frame.columns.tolist()

    for col in col_names:
        if customs.findall(col):
            rename_this = col.replace('A_', '_') # this pulls (Q_N_123_001 from Q_N_123A_001)
            rename_custom_dic[col] = rename_this
    return(rename_custom_dic)


def get_customQ_renaming_dic_endingA(data_frame):
    rename_custom_dic = {}
    customs = re.compile('AUS_.*_A') # cute
    col_names = data_frame.columns.tolist()

    for col in col_names:
        if customs.findall(col):
            rename_this = col.replace('_A', '') # this pulls (Q_N_123_001 from Q_N_123A_001)
            rename_custom_dic[col] = rename_this
    return(rename_custom_dic)


def get_customQ_renaming_dic_aus_A(data_frame):
    rename_custom_dic = {}
    customs = re.compile('A*_.*A$') # cute
    col_names = data_frame.columns.tolist()

    for col in col_names:
        if customs.findall(col):
            rename_this = col[:-1]
            rename_custom_dic[col] = rename_this
    return(rename_custom_dic)



def stack_aus_data(data_frame, spss_metadata):
    '''
    Order of Operation
    1. Duplicate frame (DF1, DF2)
    2. DF2 - Drop first questions rating
    3. DF2 - Drop first stakeholders
    4. DF2 - Assign RATINGORDER
    5. DF2 - Rename
    6. DF1 - Drop second questions, ratings
    7. DF1 - Assign RATINGORDER
    8. Merge and return
    ''';

    # 1
    print("---\nSPSS: Original Shape of dataframe: {}".format(data_frame.shape))
    print("SPSS: Getting valid integers from metadata")
    valid_integers = get_spss_integers(spss_metadata)
    #print(f"SPSS: Converting all columns to Int64: {valid_integers}")
    data_frame_cols = [col for col in valid_integers if
                       ('OE' not in col and col in data_frame.columns)]
    print(f"SPSS: Identified {len(data_frame_cols)} that need to be converted to integers")
    i = 0
    for col in data_frame_cols:
        i += 1
        if i % 1000 == 0:
            print(f"SPSS: Data conversion complete on {i}th column, {col}")
        data_frame[col] = pd.to_numeric(data_frame[col].values, errors='ignore')
        data_frame[col] = data_frame[col].astype('Int64')

    df1 = data_frame.copy()
    df2 = data_frame.copy()

    rename_custom_q = get_customQ_renaming_dic(data_frame)
    rename_custom_aus_q = get_customQ_renaming_dic_aus(data_frame)
    aus_custom_questions = get_customQ_renaming_dic_endingA(data_frame)
    ending_a2 = get_customQ_renaming_dic_aus_A(data_frame)
    more_garbage = get_customQ_renaming_dic_Q_A(data_frame)

    for k, v in ending_a2.items():
        if k not in aus_custom_questions.keys():
            aus_custom_questions[k] = v

    for k, v in rename_custom_aus_q.items():
        if k not in aus_custom_questions.keys():
            aus_custom_questions[k] = v

    for k, v in rename_custom_q.items():
        if k not in aus_custom_questions.keys():
            aus_custom_questions[k] = v

    for k, v in more_garbage.items():
        if k not in aus_custom_questions.keys():
            aus_custom_questions[k] = v

    manual_renaming = {'CODERESP_1': 'CODERESP',
                       'CRT2': 'CRT1',
                       'Company_Type2': 'Company_Type1',
                       'Counter_306': 'Counter_305',
                       'EXPERIMENTAL_CO2': 'EXPERIMENTAL_CO1',
                       'GroupBenchmark2': 'GroupBenchmark1',
                       'GroupBenchmarkGroup2': 'GroupBenchmarkGroup1',
                       'GroupBenchmarkGroup_COMPANY2': 'GroupBenchmarkGroup_COMPANY1',
                       'Q306CEO_1': 'Q305CEO_1',
                       'Q331x1_99': 'Q330x1_99',
                       'Rated_306': 'Rated_305',
                       'Q601A': 'Q600A',
                       'S106CEO': 'S105CEO',
                       'Q306CEO': 'Q305CEO',
                       'Q316CEO': 'Q315CEO',
                       'AP_AW1_2': 'AP_AW1_1',
                       'AP_AW2_2_1': 'AP_AW2_1_1',
                       'AP_AW2_2x99_99': 'AP_AW2_1x99_99',
                       'AP_AW3_2': 'AP_AW3_1'
                       }

    for k, v in manual_renaming.items():
        if k not in aus_custom_questions.keys():
            aus_custom_questions[k] = v

    ceo = dict(zip([x for x in data_frame.columns if x.startswith('Q310CEO')],
                   [x for x in data_frame.columns if x.startswith('Q311CEO')]))

    for k, v in ceo.items():
        if k not in aus_custom_questions.keys():
            aus_custom_questions[k] = v

    # 2, 3
    batteries = ['Q305_' ,'Q320_','Q215_' ,'Q410_' ,'Q420_' ,'Q600_', 'Q605' ,'Q800_']
    drop_these_questions = []
    df1_batteries = []
    for battery in batteries:
        df1_batteries += [x for x in df2.columns.tolist() if x.startswith(battery)]

    stakeholders1 = [x for x in data_frame.columns if x.startswith('Stakeholder') &
                     x.endswith('1')]
    stakeholders1 += ['Company1', 'GlobalCompany1', 'Industry1','GroupBenchmark_COMPANY1',
                      'INDUSTRY_INDEX1', 'KYLIE_COMPANY1' ,'NASDAQ_COMPANY1',
                      'GRT100_COMPANY1', 'NEW_INDUSTRY1' ,'NL_30_1' ,'APAC_TOP1']
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
    # Renaming variables in DF2 - we have a problem with Company2, etc.
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

    df2.rename(rename_battery_dic ,axis=1 ,inplace=True)
    df2.rename(rename_stakeho_dic ,axis=1 ,inplace=True)

    # Custom Question renaming/dropping
    print("SPSS: Dropping/Renaming custom questions from df2 ({} total)".format(len(aus_custom_questions)))
    drop_from_df2 = list(aus_custom_questions.values())  # NOTE: using values, which are the non _A values
    drop_from_df2 = [x for x in drop_from_df2 if x in df2.columns.tolist()]
    df2.drop(drop_from_df2, axis=1, inplace=True)
    df2.rename(aus_custom_questions, axis=1, inplace=True)

    # 6
    stakeholders2 = [x for x in data_frame.columns if x.startswith('Stakeholder') &
                     x.endswith('2')]
    stakeholders2 += ['Company2', 'GlobalCompany2', 'Industry2','GroupBenchmark_COMPANY2',
                      'INDUSTRY_INDEX2', 'KYLIE_COMPANY2' ,'NASDAQ_COMPANY2',
                      'GRT100_COMPANY2', 'NEW_INDUSTRY2' ,'NL_30_2' ,'APAC_TOP2']
    #df1.drop(stakeholders2, axis=1, inplace=True)
    df1.drop(df2_batteries, axis=1, inplace=True)
    drop_from_df1 = list(rename_custom_q.keys()) + list(rename_custom_aus_q.keys()) + stakeholders2  # drop all the _A questions
    drop_from_df1 = [x for x in drop_from_df1 if x in df1.columns.tolist()]
    df1.drop(drop_from_df1 ,axis=1 ,inplace=True)

    # 7
    df1['RATINGORDER'] = 1

    # 8
    df1 = df1.loc[:, ~df1.columns.duplicated()].copy()
    df2 = df2.loc[:, ~df2.columns.duplicated()].copy()
    print(f"SPSS: Shape of dataframes pre-merging:\n1: {df1.shape}, 2: {df2.shape}")
    final_data = pd.concat([df1 ,df2], ignore_index=True)
    final_data.reset_index(inplace=True, drop=True)
    final_data.rename({'GlobalCompany1': 'Company',
                       'Company1': 'Rating',
                       'Industry1':'NEW_INDUSTRY1'}, axis=1, inplace=True)
    del df1
    del df2
    print(f"SPSS: Shape of final dataframe: {final_data.shape}\n---\n")
    return(final_data)


def get_spss(dataframe, spss_metadata):
    # 0 Remove trash columns
    bad_s105s = ['S105_','S105x_','S105a_', 'S105Fielded_',
                 'S105_Qualified_','S105_DummyCompany_',
                 'S105_Eligible', 'S105_Shown_',  'S105_Qualified_',
                  'S105_SECOND_','S105a_SECOND_',
                  'XS105_Companies_',
                 'hidQ320_Company1', 'hidQ320_Company2',
                 'Q320x', 'Q320x1', 'XQ320', 'Q320x2',
                 'Q321x', 'Q321x1', 'Q321x2', 'Q320x',
                 'IndRepSecLF',
                 'AP_IND_REP1', 'AP_IND_REP2',
                 'INDREP', 'FLAGLATAM', 'ZEROQUOTA',
                 'hidMediaSplit',
                 'AP_MEDIA', 'tplVariables',
                 'Companies_','Companies2_']

    # Need to clean df_spss ---
    drop_all = []
    for bad_var in bad_s105s:
        drop_these = [x for x in dataframe.columns if x.startswith(bad_var)]
        drop_all += drop_these
    print(f"DI-SPSS: Identified {len(drop_all)} columns to be removed from SPSS data.")
    print(f"DI-SPSS: Size of dataframe before removing junk: {dataframe.shape}")  # from approx 8662
    dataframe.drop(drop_all, axis=1, inplace=True)
    print(f"DI-SPSS: Size of dataframe after removing junk: {dataframe.shape}")  # down to approx 3315

    # Rename Q320s
    rename_q320s = dict([[x, x.replace('_Selected_', '_')] for x in dataframe.columns
                         if (x.startswith('Q320_Selected') or x.startswith('Q321_Selected'))])
    dataframe.rename(rename_q320s, axis=1, inplace=True)

    # Rename S105s
    rename_s105s = dict([[x, x.replace('S105Fieldeda_', 'S105Fielded_')] for x in dataframe.columns
                         if x.startswith('S105Fieldeda_')])
    dataframe.rename(rename_s105s, axis=1, inplace=True)

    # Rename essential variables
    dataframe.rename({'Country': 'COUNTRY',
                      'STATUS': 'respStatus',
                      'AGE':'Age',
                      'AGE_ORI_1':'AGE_ORI'},
                     axis=1, inplace=True)
    dataframe['country_temp'] = pd.to_numeric(dataframe['COUNTRY'])
    dataframe.loc[dataframe.country_temp.isna(), 'country_temp'] = 99
    dataframe['country_temp'] = dataframe.country_temp.astype(int)
    dataframe['CODERESP'] = dataframe.country_temp.astype(str) + dataframe.index.astype(str)
    dataframe.drop('country_temp',axis=1,inplace=True)
    print(f"DI-SPSS: Stacking SPSS Data!")
    df_aus = stack_aus_data(dataframe, spss_metadata)
    df_aus['INDUSTRY_INDEX1'] = 0
    return(df_aus)