import sys
import re

import numpy as np
import pandas as pd

def familiarity_filter(df):
    '''
    Compute the Familiarity column, delete S105s.
    Input: Dask Dataframe after trim_down
    Output: Dask Dataframe with Familiarity column

    # Notes:
        # Getting the familiarity for the RATED Company:
        #   0 - Drop blank Ratings
        #   1 - Identify the Rated Company (Fam_Comp)
        #   1.5-Make sure all Fam_Comps are valid; drop those that aren't.
        #   2 - Use the ID to get the familiarity (df[df.Fam_Comp])
        #   3 - Drop useless S105's.
    '''
    # Check which file we're working with. Warning/Fail if we can't figure this out
    if "CompanyGlobal" in df.columns:
        print(":::::::::::::::::::::::::::::::::::::::::")
        print("::::::: Operating on Toluna file. :::::::")
        print(":::::::::::::::::::::::::::::::::::::::::")
        df.rename(columns ={'Rating':'rating_old'}, inplace = True)

        print("Opinion Influencer check: {}".format(df.OPINION_INFLUENCERW2018))
        # These are junk data, the real varaibels are labeled as Q110W2018.
        df = df.drop(['OPINION_INFLUENCER',
                      'Q110_1','Q110_2','Q110_3','Q110_4','Q110_5',
                      'Q110_6','Q110_7','Q110_8','Q110_9','Q110_10',
                      'Q110_11','Q110_12','Q110_13','Q110_14','Q110_15',
                      'Q110_16','Q110_17','Q110_18','Q110_19','Q110_20', 'Q110_99'], axis=1, errors='ignore')
        df.rename(columns ={'level':'RATINGORDER',
                          'Local_Industry1New':'Industry_Project',
                          'CompanyGlobal':'Company', 'CompanyFielded':'Rating',
                          'OPINION_INFLUENCERW2018':'OPINION_INFLUENCER',
                          'Q700a_Select':'Risk_Type','Q110W2018_2':'Q110_2','Q110W2018_3':'Q110_3',
                          'Q110W2018_4':'Q110_4', 'Q110W2018_5':'Q110_5', 'Q110W2018_8':'Q110_8',
                          'Q110W2018_10':'Q110_10', 'Q110W2018_13':'Q110_13', 'Q110W2018_14':'Q110_14',
                          'Q110W2018_15':'Q110_15','Q110W2018_16':'Q110_16', 'Q110W2018_17':'Q110_17',
                          'Q110W2018_99':'Q110_99', 'QhidMonth':'Month'}, inplace=True)
        df['Rating'].astype(str, copy = False)

    elif "AGEGENDER" in df.columns:
        print(":::::::::::::::::::::::::::::::::::::::::")
        print("::::::: Operating on Dynata file. :::::::")
        print(":::::::::::::::::::::::::::::::::::::::::")
        dynata = True
        # July 1st: Missing countries?
        df.COUNTRY.replace(' ', np.nan, inplace=True)
        #print("Number of countres: {}, available countries: {}. ".format(len(df.COUNTRY.unique()),df.COUNTRY.unique()))
        print(f"Size of data before dropping missing 'COUNTRY' cases: {df.shape}")
        df = df.dropna(subset=['COUNTRY']).copy()
        print(f"Size of data after dropping missing 'COUNTRY' cases: {df.shape}")
        # March 9th: Dynata fixed all their column names.
        #df.rename(columns ={"Company1":"Rating", "Industry1":"Industry_Project", "Company_Type":"Company_Type",
        #           "GlobalCompany1":"Company", "Country":"COUNTRY", "STATUS":"respStatus","AGE":"Age"}, inplace=True)
        df.rename(columns ={"RatingOrder":"RATINGORDER"}, inplace=True)
        df['COUNTRY'] = df.COUNTRY.astype(int, copy=False)
        df['Country'] = df.COUNTRY
        # Rating fixes
        print(f"Size of data before dropping missing 'Rating' cases: {df.shape}")
        df = df.dropna(subset=['Rating']).copy()
        print(f"Size of data after dropping missing 'Rating' cases: {df.shape}")
        # Looks stupid but we need to remove decimal places
        df.Rating.astype(int, copy = False)
        df.Rating.astype(str, copy = False)
    else:
        print(":::::::::::::::::::::::::::::::::::::::::")
        print("::::::: ! FAILED TO DETECT FILE ! :::::::")
        print(":::::::::::::::::::::::::::::::::::::::::")

    qs = re.findall(r"(S105_\d+|Q310_\d+|Q320_\d+|Q360_\d+|Q710a_\d+|Q410_\d+|Q215_\d+|Q800_\d+)",
                    ' '.join(df.columns))

    # 0
    df.Rating.replace('',  np.nan, inplace=True)
    df.Rating.replace(' ', np.nan, inplace=True)

    # ============================
    #       "inplace=True" ISSUE
    # This is good ??
    pre_blank_rating = df.shape[0]
    df = df.dropna(subset=['Rating']).copy()
    # ============================

    # ISSUE
    # 1
    pd.options.mode.chained_assignment = None
    print("FILTERS-Fam: Creating Fam_Company")
    df.Rating = df.Rating.astype(int)
    #print(f"FILTERS-Fam: First few ratings: {df.Rating[1:4]}, datatype: {df.Rating.dtypes}")
    df.loc[:,'Fam_Company'] = "S105Fielded_" + df.Rating.astype(str).values
    s105s = [x for x in df.columns if x.startswith('S105Fielded')]
    S105Fielded = list(df.Fam_Company.unique())
    pd.options.mode.chained_assignment = "warn"
    #########

    # 1.5
    invalid_S105 = [a for a in S105Fielded if a not in s105s]
    print("FILTERS-Fam: Could not find the following S105 columns: {}".format(invalid_S105))
    S105Fielded_Clean = [x for x in S105Fielded if x not in invalid_S105]
    print(f"FILTERS-Fam: Dropping cases of missing familiarity columns: {df.shape}")
    df = df[~df.Fam_Company.isin(invalid_S105)].copy()
    print(f"FILTERS-Fam: After dropping cases of missing familiarity columns: {df.shape}")

    # 2
    #print("[Before familiarity] DF Size Check: {}GB".format(round(sys.getsizeof(df)/1024**3,2)))
    df.loc[:,'Familiarity'] = 0
    print("FILTERS-Fam: Running Familiarity-finder")
    for comp in S105Fielded_Clean:
        # Dynata started giving us NANs in the company's familiarity for some reason.
        #print("Working with {} | Unique Familiarity Scores: {}".format(comp, df.loc[df.Fam_Company == comp][comp].unique()))
        df.loc[df.Fam_Company == comp, 'Familiarity'] = df.loc[df.Fam_Company == comp][comp]
    df.loc[:,'Familiarity'] = pd.to_numeric(df.loc[:,'Familiarity'], errors = 'coerce', downcast = 'integer')
    print("FILTERS-Fam: Setting Familiarity")

    # 3
    df = df.drop(['Fam_Company'], axis = 1).copy()
    del_s105s = re.findall("(S105Fielded_\d+)", ''.join(df.columns))
    df = df.drop(del_s105s, axis = 1)
    df = df[df.Familiarity > 3]
    print("FILTERS-Fam: [After dropping S105s] DF Size Check: {}GB".format(
        round(sys.getsizeof(df)/1024**3,2)))
    print("FILTERS-Fam: After dropping unfamiliar ratings: {}, unique values: {}".format(
        len(df), df.Familiarity.unique()))
    #print("Opinion Influencer check: {}".format(df.OPINION_INFLUENCER))
    return(df)

def demographic_filters(df):
    '''
    Removes cases with invalid RespStatus, Ages, and Incomplete Pulse
    Input: dask df
    Output: dask df
    '''
    def count99(x): return(list(x).count(99))
    df = df.copy()
    print("FILTERS-Demo: Unique respStatus:\n{}".format(df.respStatus.unique()))
    df['respStatus'] = pd.to_numeric(df['respStatus'], errors='coerce')
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce')

    # Filter away incomplete cases:
    print("FILTERS-Demo: Before dropping respStatus != 2: {}".format(len(df)))

    df = df[df.respStatus == 2]
    print("FILTERS-Demo: After dropping respStatus != 2: {}".format(len(df)))

    # Filter away Age - babies and boomers
    good_ages = [1,2,3,4,5]
    print("FILTERS-Demo: BEFORE DROP - Unique ages: {}".format(df.Age.unique()))
    df = df[df.Age.isin(good_ages)]
    print("FILTERS-Demo: AFTER DROP - Unique ages: {}".format(df.Age.unique()))
    print("FILTERS-Demo: Shape after dropping Ages (0 and 5's): {}\n---".format(df.shape))

    # Filter away incomplete pulse
    pulse_vars=['Q305_1','Q305_2','Q305_3','Q305_4']
    df[pulse_vars]=df[pulse_vars].apply(pd.to_numeric,errors='coerce')
    df[pulse_vars]=df[pulse_vars].replace(np.nan,99)
    df['pulse_missing']=df.apply(lambda x: count99(x[pulse_vars].astype('int',errors='ignore')),axis=1)#, meta = ('pulse_missing', 'int'))
    print("FILTERS-Demo: Unique Pulse Missings: {}".format(df.pulse_missing.unique()))
    df = df[df.pulse_missing<2].copy()

    # Additionally, we need to clear CODERESP and Country cases. Thanks Dynata
    df.dropna(subset=['COUNTRY'],inplace=True)
    df.dropna(subset=['CODERESP'],inplace=True)
    df.COUNTRY=df.COUNTRY.astype(int,copy=False)
    df.CODERESP=df.CODERESP.astype(int,copy=False)
    return(df)
