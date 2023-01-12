import re
from datetime import datetime as dt

import numpy as np
import numpy.ma as ma
import pandas as pd
import quantipy as qp
from quantipy.core.weights.rim import Rim

from utils.computations.compute_averages import compute_rolling_avg
from utils.io.archive_redshift import get_data


############################################################################
#                       Helper functions
# Compute Dimensions (Distributions)
def average99(vals):
    '''
    Average non-99's.
    '''
    return (np.nanmean(np.ma.masked_values(vals, 99), axis=1).data)


# Computing the scores for attributes:
def score_attributes(vals):
    '''
    Adjusted for correct computations circa Sept2019 NT processing file.
    '''
    gm = 69.3488470  # .39923075#64.2166120983878
    gst = 24.1109  # 97296123#26.5665245764245
    reb = ((((vals[:, 1] - 1) / 6 * 100) - vals[:, 2]) / vals[:, 3]) * gst + gm
    return (reb)


# Compute "smeans" - replace missing values with means
def smean(c):
    '''
    Replace nans with mean of columns.
    '''
    # Extremely fast, but can't perform by group. Darn!
    return (np.where(np.isnan(c), ma.array(c, mask=np.isnan(c)).mean(axis=0), c))


def score_dims(vals):
    return (smean(vals).mean(axis=1))




def compute_weights(data, country_codebook):
    weighted_res=pd.DataFrame()
    for country in data.COUNTRY.unique():
        start = dt.now()
        country_df=data[data.COUNTRY==country].copy()
        codebook_df=country_codebook[country_codebook.Country==country].copy()
        age_targets={'Age':{1:codebook_df['Age4_1'].values[0]*100,2:codebook_df['Age4_2'].values[0]*100,
                        3:codebook_df['Age4_3'].values[0]*100,4:codebook_df['Age4_4'].values[0]*100,
                        5:codebook_df['Age4_5'].values[0]*100}}
        gender_targets={'Gender':{1:codebook_df['Gender4_M'].values[0]*100,
                                  2:codebook_df['Gender4_F'].values[0]*100}}
        print("Settting up quantipy data for country {}.".format(country))
        dataset_country=qp.DataSet("CID-{} Dataset".format(country))
        dataset_country.from_components(country_df)
        scheme = Rim('gender_and_age')
        scheme.set_targets(targets=[age_targets, gender_targets])
        dataset_country.weight(scheme,unique_key='Global_ID',
                   weight_name="Weight_Final",
                   inplace=True, report=False, verbose=False)
        weighted_res=pd.concat([weighted_res,dataset_country.data()])
        #weighted_res.append(dataset_country.data())
        del dataset_country
        del scheme
        end = dt.now()
        print(end-start)
        print("---")
    return(weighted_res)

def get_rolled_unrolled_cols(data, with_pulse=True):
    # Identify (presently) rolled scores
    q320=[col for col in data if (col.startswith('Q320') and col.endswith('G_Reb'))]
    q215=[col for col in data if (col.startswith('Q215') and col.endswith('G_Reb'))]
    q410=[col for col in data if (col.startswith('Q410') and col.endswith('G_Reb'))]
    q800=[col for col in data if (col.startswith('Q800') and col.endswith('G_Reb'))]
    if with_pulse:
        rolled_cols=['Pulse_G_Reb']+q320+q215+q410+q800
    else:
        rolled_cols=q320+q215+q410+q800

    # Identify (presently) unrolled scores - orig
    oq320=[col for col in data if (col.startswith('orig_q320') and col.endswith('g_reb'))]
    oq215=[col for col in data if (col.startswith('orig_q215') and col.endswith('g_reb'))]
    oq410=[col for col in data if (col.startswith('orig_q410') and col.endswith('g_reb'))]
    oq800=[col for col in data if (col.startswith('orig_q800') and col.endswith('g_reb'))]
    if with_pulse:
        unrolled_cols=['orig_pulse_g_reb']+oq320+oq215+oq410+oq800
    else:
        unrolled_cols=oq320+oq215+oq410+oq800
    return(rolled_cols,unrolled_cols)

# ESG is new - October 19, 2020
# This is used after imputation!
def compute_ESG(data_frame):
    # Environment
    data_frame['Env1_G_reb'] = data_frame['Q800_5r_G_Reb_1']*.5 + data_frame['Q800_9r_G_Reb_1']*.5
    data_frame['Env2_G_reb'] = data_frame['Q320_14r_G_Reb_1']*.6 + data_frame['Q320_16r_G_Reb_1']*.4
    data_frame['Env3_G_reb'] = data_frame['Q320_15r_G_Reb_1']*.4 + data_frame['Q800_8r_G_Reb_1']*.6
    # Social
    data_frame['Social1_G_reb'] = data_frame['Q800_3r_G_Reb_1']*.5 + data_frame['Q800_2r_G_Reb_1']*.5
    data_frame['Social2_G_reb'] = data_frame['Q320_9r_G_Reb_1']*.5 + data_frame['Q320_10r_G_Reb_1']*.5
    data_frame['Social3_G_reb'] = data_frame['Q320_8r_G_Reb_1']
    # Gov
    data_frame['Gov1_G_reb'] = data_frame['Q800_7r_G_Reb_1']*.5 + data_frame['Q800_6r_G_Reb_1']*.5
    data_frame['Gov2_G_reb'] = data_frame['Q320_11r_G_Reb_1']*.5 + data_frame['Q320_12r_G_Reb_1']*.5
    data_frame['Gov3_G_reb'] = data_frame['Q320_13r_G_Reb_1']*.6 + data_frame['Q800_4r_G_Reb_1']*.4
    # Main Constructs
    data_frame['Env_G_reb'] = data_frame[['Env1_G_reb','Env2_G_reb','Env3_G_reb']].mean(axis=1)
    data_frame['Social_G_reb'] = data_frame[['Social1_G_reb','Social2_G_reb','Social3_G_reb']].mean(axis=1)
    data_frame['Gov_G_reb'] = data_frame[['Gov1_G_reb','Gov2_G_reb','Gov3_G_reb']].mean(axis=1)
    data_frame['ESG_G_reb'] = data_frame[['Env_G_reb','Social_G_reb','Gov_G_reb']].mean(axis=1)
    #   --------------
    #   Imputations
    data_frame['Env1_imp'] = data_frame['Q800_5_imp']*.5 + data_frame['Q800_9_imp']*.5
    data_frame['Env2_imp'] = data_frame['Q320_14_imp']*.6 + data_frame['Q320_16_imp']*.4
    data_frame['Env3_imp'] = data_frame['Q320_15_imp']*.4 + data_frame['Q800_8_imp']*.6
    # Social
    data_frame['Social1_imp'] = data_frame['Q800_3_imp']*.5 + data_frame['Q800_2_imp']*.5
    data_frame['Social2_imp'] = data_frame['Q320_9_imp']*.5 + data_frame['Q320_10_imp']*.5
    data_frame['Social3_imp'] = data_frame['Q320_8_imp']
    # Gov
    data_frame['Gov1_imp'] = data_frame['Q800_7_imp']*.5 + data_frame['Q800_6_imp']*.5
    data_frame['Gov2_imp'] = data_frame['Q320_11_imp']*.5 + data_frame['Q320_12_imp']*.5
    data_frame['Gov3_imp'] = data_frame['Q320_13_imp']*.6 + data_frame['Q800_4_imp']*.4
    # Main Constructs
    data_frame['Env_imp'] = data_frame[['Env1_imp','Env2_imp','Env3_imp']].mean(axis=1)
    data_frame['Social_imp'] = data_frame[['Social1_imp','Social2_imp','Social3_imp']].mean(axis=1)
    data_frame['Gov_imp'] = data_frame[['Gov1_imp','Gov2_imp','Gov3_imp']].mean(axis=1)
    data_frame['ESG_imp'] = data_frame[['Env_imp','Social_imp','Gov_imp']].mean(axis=1)
    return(data_frame)


def compute(df, codebook, constructs, upload, roll_cmd):
    '''
    This function computes the constructs, scores, and pulse.
        NOTE: This function has two components; Helper-Functions & UDFS, and the
              actual execution component. Trouble shoot this on a jupyternotebook
              before going ham on rewriting stuff. It'll save you a lot of headache.
    Inputs: df after filtering and cleaning[DF], codebook[Pandas DF], and constructs[JSON]
    Output: DF

    # Total time: Constructs have been computed in 50.87s ~ 1min
    # Previously (Unvectorized): 'Constructs': 8887.73s ~ 2Hours 45 mins.
    '''

    df['Month']=pd.to_numeric(df.Month)
    df['Month']=df.Month.astype('Int64')
    months = df.Month.value_counts()
    if months.shape[0]>1:
        print("compute_main.py: Found multiple months in the data!")
        print(months)
        correct_month = months[months==months.max()].index[0]
        print(f"compute_main.py: Using month {correct_month}")
        df['Month'] = int(correct_month)

    # ==========================================================================
    #   Computing Constructs; Distributions
    
    # Safety Check
    df = df[~df.COUNTRY.isna()].copy()
    df['COUNTRY'] = df.COUNTRY.astype(int)
    df = df[~df.Company.isna()].copy()
    df['Company'] = df.Company.astype(int)

    construct_dic = {}
    pd.options.mode.chained_assignment = None
    dimensions = constructs.construct.unique().tolist()
    for construct in dimensions:
        variables=constructs[constructs.construct==construct].variables.tolist()
        construct_dic[construct]=variables
        if all([x in df.columns for x in variables]):
            if construct == "dep_var":
                df[construct] = average99(df[variables].astype('float64').values)
            else:
                df[construct] = np.floor(average99(df[variables].astype('float64').values)+0.5)
            df.loc[df[construct]==0, construct] = np.nan
        else:
            print("Unable to compute {} with {}.".format(bucket, variables))
    pd.options.mode.chained_assignment = "warn"

    # ==========================================================================
    #   Computing Scores
    print("Joining codebook with data...")
    df['COUNTRY'] = pd.to_numeric(df['COUNTRY'], errors='coerce')
    codebook['Country'] = pd.to_numeric(codebook['Country'],errors = 'coerce')
    missing_countries=set(df.COUNTRY[-df.COUNTRY.isin(codebook.Country)])
    print("Non-matching codebook countries:{}".format(missing_countries))
    if len(missing_countries)>0:
        raise Exception("\n***\nMissing Country Detected: {}\nPlease Check Codebook\n***".format(missing_countries))
    df = pd.merge(df, codebook, left_on = 'COUNTRY', right_on = 'Country')
    # List of items to score
    standard_battery = re.findall(r"(Q320_\d+|Q360_\d+|Q710a_\d+|Q410_\d+|Q215_\d+|Q800_\d+)",
        ' '.join(df.columns))

    # Compute the score for attributes
    print("Computing scores.")
    for score_this in standard_battery:
        if score_this in df.columns:
            params = ['COUNTRY', score_this, 'CM_Reb', 'CSD_Reb']
            df[score_this + 'r_G_Reb'] = score_attributes(df[params].\
                                                apply(pd.to_numeric, errors = 'coerce').replace(99,np.nan).values)
    # Pulse
    df['Pulse_G_Reb'] = score_attributes(df[['COUNTRY','dep_var', 'CM_Reb', 'CSD_Reb']].\
                                apply(pd.to_numeric, errors = 'coerce').values)
    df = df.loc[:,~df.columns.duplicated()].copy()

    # ==========================================================================
    #   Computing Logistics
    print("Computing Logistics")
    # Construct Global+ID, using the method from cRT
    df['File'] = 5
    df['Month']=df['Month'].astype(int)
    if df.Month.unique()[0]==12:
        df['Year']= dt.now().year-1
    else:
        df['Year']= dt.now().year
    print("\n---\nCRT Month and year: {}, {}\n---".format(df.Month.unique()[0],df.Year.unique()[0]))
    logistics = ['Month', 'COUNTRY', 'CODERESP', 'RATINGORDER', 'Year']
    df.loc[:,logistics] = df.loc[:,logistics].apply(pd.to_numeric, errors = 'coerce')
    df.dropna(axis=1, how='all', inplace=True)
    df['crt_Month'] = (df.Year-2015)*12 + df.Month #Historic purposes
    df = df.sort_values(['COUNTRY', 'CODERESP', 'Month'], ascending = [True, True, True])
    df['RCODERES'] =  df.groupby("COUNTRY")["CODERESP"].rank("dense", ascending=True)
    df['index_id'] = df.index
    df['Global_ID'] = df['File'] * 1000000000000 + df['crt_Month'] * 10000000000 + \
                      df['COUNTRY'] * 1000000000 + df['RCODERES'] * 10000000 + df['RATINGORDER'] * 1000000 \
                      + df.index_id
    #df['Global_ID'] = df['File'] * 1000000000000 + df['crt_Month'] * 10000000000 +\
    #    df['COUNTRY'] *10000000 + df['RCODERES']*100 + df['RATINGORDER']
    #df.drop_duplicates(subset='Global_ID',inplace=True,ignore_index=True)

    # Compute UNIQUE Respondants, cRT method
    df['Unique_Respondent_All_$'] = 0
    MCR = ['Month', 'COUNTRY', 'CODERESP']
    df = df.sort_values(MCR, ascending = [True, True, True])
    df['Unique_Respondent_All_$'] = np.where(df[MCR].duplicated(), '0', '1')

    # ==========================================================================
    # Identify missing gender-age combinations.

    cag=df.groupby(['COUNTRY','Age','Gender'],as_index=False)['Year'].count()
    age_groups=[1,2,3,4,5]
    global_id_replacement=999
    global_id_list=[]
    for country in cag.COUNTRY.unique().tolist():
        country_df=cag[cag.COUNTRY==country].copy()
        if country_df.shape[0]!=10:
            if len(country_df.Gender.unique().tolist())!=2:
                print("Missing a gender for country: {}".format(country))
            # Loop through gender to identify age group missing;
            # We'll always have both genders, but frequently miss an age.
            for gender in country_df.Gender.unique().tolist():
                gender_df=country_df[country_df.Gender==gender].copy()
                if gender_df.shape[0]!=5:
                    # Identify age missing. If it's 1, replace with age=2,
                    # Otherwise replacement = missing_age-1
                    for age in age_groups:
                        if age not in gender_df.Age.unique().tolist():
                            print("Missing an age group in country: \
{} with gender {}, age {}".format(country, gender,age))
                            if age == 1:
                                rr = df[(df.COUNTRY == country)&(df.Gender == gender)&(df.Age==age+1)].iloc[0, :].copy()
                                rr['Age']=age
                            else:
                                if age-1 in gender_df.Age.unique().tolist():
                                    print("Missing age {} for country {}, replacing with {}".format(age,country,age-1))
                                    rr=df[(df.COUNTRY==country)&(df.Gender==gender)&(df.Age==age-1)].iloc[0,:].copy()
                                    rr['Age']=age
                                else:
                                    print("Missing age {} for country {}, replacing with {}".format(age,country,age-2))
                                    rr=df[(df.COUNTRY==country)&(df.Gender==gender)&(df.Age==age-2)].iloc[0,:].copy()
                                    rr['Age']=age
                            rr['Global_ID']=global_id_replacement
                            global_id_list.append(global_id_replacement)
                            global_id_replacement-=1
                            rr=pd.DataFrame(rr)
                            df = pd.concat([df, rr])

    df=df[~df.Global_ID.isna()].copy()
    print("\n---\nComputing Weights via Python.\n---")
    df['Age']=df.Age.astype(int)
    df['Gender']=df.Gender.astype(int)
    df=compute_weights(df,codebook)
    df=df[-(df.Global_ID.isin(global_id_list))]
    df.loc[df.Weight_Final>3.3,'Weight_Final'] = 3.33
    #   Weights Complete!

    # ==========================================================================
    #   Prep for rolling
    if roll_cmd:
        print("Weights Complete.\nPreparing for rolling; lowercasing dataset")
        df=df.loc[:,~df.columns.duplicated()].copy()
        #print(df.columns.tolist())
        drop_these = []#[x for x in df.columns if x.isnumeric()]
        for x in df.columns.tolist():
            if type(x)==int:
                drop_these.append(x)
        if len(drop_these)>0:
            print(f"Found weird numeric columns: {drop_these}")
            df.drop(drop_these,axis=1,inplace=True)
        lowercase_cols={x:x.lower() for x in df.columns.tolist()}
        original_cols={}
        for k, v in lowercase_cols.items():
            if v not in original_cols:
                original_cols[v] = k
            else:
                print("Found two similar columns: {} and {}".format(v, k))
                print("In dic: {}".format(original_cols[v]))
                original_cols[v + "_duplicated"] = k + "_duplicated"
        df.rename(columns=lowercase_cols, inplace = True)
        df = df.loc[:,~df.columns.duplicated()].copy()

        # ==========================================================================
        #   Rolling Code START
        print("\n#  ===================================")
        print("         Rolling Results")
        print("0. Priming data for rolling")
        # Identify only important columns:
        cols = ['global_id','country','company','month', 'year','weight_final']
        rgrebs = [col for col in df if col.endswith('_g_reb')]
        # Create new df for rolling: Global_ID, Country, Company,
        #     Month, weight_final, r_g_rebs and pass that along
        for_archiving = df[cols + rgrebs].copy()
        # Save everything, minus r_g_reb (will be re-added)
        df.drop(rgrebs+['weight_final'], axis = 1, errors='ignore', inplace = True)
        df.to_csv("./tmp/PRE-ROLLING-SAVE.csv",index = False)
        print("PRE-ROLLING-SAVE.csv has been saved.\n---")
        del df
        # Archive, Roll, etc.
        print("1. Obtaining Archive")
        three_months = get_data(for_archiving,upload)
        print("2. Rolling Data")
        df = compute_rolling_avg(three_months)
        del three_months
        # Merge original data back in
        print(" - Adding data back into rolled results")
        full_df = pd.read_csv("./tmp/PRE-ROLLING-SAVE.csv")
        print(f"compute.py: Missing global IDs? {full_df[full_df.global_id.isna()].shape}")
        full_df = full_df[~full_df.global_id.isna()].copy()
        full_df['global_id']=full_df['global_id'].astype('Int64')
        full_df['company']=full_df['company'].astype('Int64')
        df = pd.merge(full_df, df, on=['global_id','country','company','month','year'], validate='1:1' , how='left')
        #Fix Age issue:
        if "Age_x" in df.columns:
            df.rename(columns = {"Age_x":"Age"}, inplace = True)
            df.drop("Age_y", axis=1, inplace = True)
        print("         Rolling Complete")
        print("#  ===================================\n")
        #   Rolling Code END
        df.rename(columns=original_cols, inplace = True)

    # ==========================================================================
    df.Month = df.Month.astype(int, copy=False)
    df.Year = df.Year.astype(int, copy=False)

    # ==========================================================================
    #           Need to switch all var with orig_var;
    #   Currently: non-rolled==orig_var, rolled=var. Needs to be switched.
    #                   ----------NOTE----------
    #       Need to fix this as per roll_cmd
    df.dropna(axis=1,inplace=True,how='all')
    if roll_cmd:
        rolled_cols,unrolled_cols=get_rolled_unrolled_cols(df)
        print("Size of normal battery: {}, size of orig_battery: {}".format(len(rolled_cols),len(unrolled_cols)))
        if len(rolled_cols)!=len(unrolled_cols):
            raise Exception("Number of Rolled columns do not match Unrolled columns!")
        # Create dictionaries to rename variables.
        rolled_to_placeholder=dict(zip(rolled_cols,[q+"_x" for q in rolled_cols]))
        unrolled_fix=dict(zip(unrolled_cols,rolled_cols))
        rolled_fix=dict(zip([q+"_x" for q in rolled_cols],["orig_"+str.lower(q) for q in rolled_cols]))
        # Rename
        df.rename(rolled_to_placeholder,axis=1,inplace=True)
        df.rename(unrolled_fix,axis=1,inplace=True)
        df.rename(rolled_fix,axis=1,inplace=True)
    else:
        print("3-month rolling has been skipped; not renaming orig_ columns.")

    # ==========================================================================
    # Add interpolated (smean) variables
    smean_these, smean_these_orig = get_rolled_unrolled_cols(df, with_pulse=False)
    #if roll_cmd:
    smean_all = smean_these+smean_these_orig
    smean_names = [q+"_1" for q in smean_all]
    bad_vars = ['orig_q320_28r_g_reb', 'orig_q320_29r_g_reb', 'orig_q215_17r_g_reb', 'orig_q320_27r_g_reb',
    'orig_q410_4r_g_reb', 'orig_q215_11r_g_reb', 'orig_q215_15r_g_reb', 'orig_q410_9r_g_reb',
    'orig_q215_2r_g_reb', 'orig_q215_9r_g_reb', 'orig_q410_10r_g_reb']
    for var in bad_vars:
        if var in smean_all:
            smean_all.remove(var)
        if var+"_1" in smean_names:
            smean_names.remove(var+"_1")
    print("Length of smean_names, smean_all: {}, {}".format(len(smean_names),len(smean_all)))
    print("Duplicate Columns? {}".format(df.columns[df.columns.duplicated()].tolist()))
    df = df.loc[:,~df.columns.duplicated()].copy()
    df[smean_names] = df.groupby(['Rating'])[smean_all].transform(lambda x: x.fillna(x.mean()))

    # ==========================================================================
    # Compute scores for constructs
    print("Computing scores for constructs.")
    for k, v in construct_dic.items():
        if k == "dep_var":
            pass
        elif k == "CSR":
            # CSR is a special case - just take the average of the dimension scores
            use_these = [x+"_G_Reb" for x in v]
            to_score_this = k + "_G_Reb"
            df[to_score_this] = df.groupby(['Rating'])[use_these]\
                .transform(lambda x: x.fillna(x.mean())).mean(axis=1)
            if roll_cmd:
                # Compute rolled construct too
                rolled_construct="orig_"+to_score_this
                orig_vars=['orig_'+var for var in use_these]
                df[rolled_construct] = df.groupby(['Rating'])[orig_vars]\
                    .transform(lambda x: x.fillna(x.mean())).mean(axis=1)
        else:
            use_these = [x+"r_G_Reb_1" for x in v]
            if all(x in smean_names for x in use_these):
                # Non-rolled
                to_score_this=k+"_G_Reb"
                df[to_score_this]=df[use_these].mean(axis=1)
                if roll_cmd:
                    # Rolled
                    to_score_this='orig_'+to_score_this
                    use_these=["orig_"+str.lower(v) for v in use_these]
                    df[to_score_this]=df[use_these].mean(axis=1)
            else:
                use_these = [x+"r_G_Reb" for x in v]
                to_score_this = k + "_G_Reb"
                df[to_score_this] = df[use_these].mean(axis=1)
                if roll_cmd:
                # Rolled
                    to_score_this='orig_'+to_score_this
                    use_these=["orig_"+str.lower(v) for v in use_these]
                    df[to_score_this]=df[use_these].mean(axis=1)

    # Compute Pulse Groups
    df['Pulse_Group'] = 1
    df.loc[df.Pulse_G_Reb > 40,'Pulse_Group'] = 2
    df.loc[df.Pulse_G_Reb > 60,'Pulse_Group'] = 3
    df.loc[df.Pulse_G_Reb > 70,'Pulse_Group'] = 4
    df.loc[df.Pulse_G_Reb > 80,'Pulse_Group'] = 5

    # Fixing dimensions
    df[dimensions] = df[dimensions].apply(pd.to_numeric, errors = 'coerce')
    for dim in dimensions:
        df.loc[np.isnan(df[dim]), dim] = 99

    # Drop junk
    df = df.dropna(axis=1, how='all')

    # Add dimension_Multi values
    if "dep_var" in dimensions:
        dimensions.remove("dep_var")
    for dimension in dimensions:
        if dimension != "dep_var":
            _multi=dimension + "_multi"
            _g_reb_multi=dimension + "_G_Reb_multi"
            df[_multi]=df[dimension]
            df[_g_reb_multi]=df[dimension + "_G_Reb"]
            if roll_cmd:
                df["orig_"+_g_reb_multi]=df["orig_"+dimension+"_G_Reb"] # Added rolled scores.
        else:
            print("{} skipped!".format(dimension))

    # Touchpoints
    '''
    direct : q600_16+q600_a
    paid: q600_19+q600_20+q600_34
    Earned = Q600_21, Q600_14, Q600_10, Q600_22, Q600_33
    Owned: Q600_4, Q600_6, Q600_18
    '''
    if 'Q600_35' in df.columns.tolist():
        q600_35_1 = (df['Q600_35']==1)
        df.loc[q600_35_1,'Q600_4'] = 1
    if 'Q600_33' not in df.columns.tolist():
        print("Compute.py: Q600_33 not in file.")
        df['Q600_33'] = 0
    if 'Q600_34' not in df.columns.tolist():
        print("Compute.py: Q600_34 not in file.")
        df['Q600_34'] = 0

    print("Computing Touchpoints.")
    df.loc[df.Q600A.isna(),'Q600A'] = 0
    df['Q600_0'] = df['Q600A']  # <----
    touchpoints = {'TP_Direct':['Q600_16','Q600_0'],
                   'TP_Earned':['Q600_10','Q600_14','Q600_21','Q600_22','Q600_33'],
                   'TP_Owned':['Q600_4','Q600_6','Q600_18'],
                   'TP_Paid':['Q600_19','Q600_20', 'Q600_34']}
    for tp, q600s in touchpoints.items():
        # Make sure Q600s are integers
        for q600 in q600s:
            df.loc[df[q600].isna(), q600] = np.nan
            df[q600] = pd.to_numeric(df[q600])
        df[tp] = df[q600s].sum(axis=1)
        df.loc[df[tp]>=1,tp] = 1

    df['TP_Grouped_Count'] = df[['TP_Direct','TP_Earned','TP_Owned','TP_Paid']].sum(axis=1)
    df.TP_Grouped_Count.unique()
    print("All basic computations complete!")
    df = df.loc[:,~df.columns.duplicated()].copy()
    df.reset_index(drop=True, inplace=True)
    print("compute_main.py: Saving backup dataframe 'post_computations.csv'")
    df.to_csv('post_computations.csv',index=False)
    return(df)
