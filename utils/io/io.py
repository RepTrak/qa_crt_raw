import os
import subprocess
import json
import s3fs

import pandas as pd
import multiprocessing
import pyreadstat

from utils.io.redshift_access import access_redshift
from utils.io.organize_metadata import prepare_metadata


def resume(stage, ALL_FILES):
    if stage == 0 or stage == 1:
        print("---")
        return(None)
    if stage == 2 or stage == 3:
        print("resume: opening post_filter_data.csv")
        df = pd.read_csv("post_filter_data.csv")
    if stage == 4 or resume == 5:
        print("resume: opening post_computations.csv")
        df = pd.read_csv("post_computations.csv")
    if stage == 6:
        print("resume: opening post_imputations.csv")
        df = pd.read_csv("post_imputations.csv")

    if stage > 1:
        print("resume: Reading metadata.")
        _, spss_meta = pyreadstat.read_sav(ALL_FILES['.sav'][0], metadataonly = True)
        json_meta = json.load(open(ALL_FILES['.txt'][0]))
        meta_data = prepare_metadata(json_meta, spss_meta, df.columns.tolist())
        
    return(df, meta_data)

def Unzip(zipFile, destinationDirectory='./tmp/'):
    # BE CAREFUL - DONT CHANGE OLD PROCESS
    try:
        print("Attempting to extract using 7za")
        process=subprocess.Popen(["7za", "e", f"{zipFile}","-aoa", f"-o{destinationDirectory}"])
    except:
        print("7za failed, using 'unzip'")
        process=subprocess.Popen(["unzip",  f"{zipFile}", "-d" ,f"{destinationDirectory}"])
    process.wait()

def Zip(final_results_filename, zip_filename='./Processing_Results.zip'):
    try:
        print("Attempting to extract using 7za")
        process=subprocess.Popen(["7za", "a", f"{zip_filename}", f"{final_results_filename}"])
    except:
        print("7za failed, using 'zip'")
        process = subprocess.Popen(["zip", f"{zip_filename}", f"{final_results_filename}"])
    process.wait()


def open_codebooks(year):
    '''
    Opens codebook and constructs file.
    '''
    # -- Constructs
    con, cur = access_redshift()
    cur.execute("select * \
        from crt_ds_etl.constructs;")
    constructs=pd.DataFrame(cur.fetchall())
    constructs.columns=['construct','variables']
    # -- Codebook
    codebook_cols=['Year','Country',
    'Region_Continental','Region_Emerging','Region_Bric','Region_G8',
    'global_mean','global_sd', 'CM_Reb', 'CSD_Reb',
    'Gender4_M','Gender4_F',
    'Age4_1','Age4_2','Age4_3','Age4_4','Age4_5']
    con, cur = access_redshift()
    cur.execute("select * \
        from crt_ds_etl.cultural_codebook \
        where year = {};".format(year))
    codebook=pd.DataFrame(cur.fetchall())
    codebook.columns=codebook_cols
    codebook.drop('Year',axis=1,inplace=True)
    return(codebook, constructs)


def transfer_from_s3(export_str):
    '''
    downloads the file form s3
    '''
    msg = export_str
    subprocess.call(msg, shell = True)
    return("Exported from s3 using the cmd '{}'!".format(msg))

def append_csv(df, file_):
    df.to_csv(file_, mode = 'a', header=False, index = False)

def save_csv(df, file_):
    '''
    Silly, but i need to save with index off and i don't know how to deal with kwargs
    '''
    df.to_csv(file_, index = False)

def get_vars_from_json(constructs):
    '''
    pulls out variables from json, removes the constructs
    '''
    variables = []
    for const_group in constructs['Constructs']:
        construct = constructs['Constructs'][const_group]
        for bucket in construct:
            if len(variables) == 0:
                variables = construct[bucket]['vars']
            else:
                variables += construct[bucket]['vars']
    variables = list(set(variables)) # Get rid of dupes, and constructs (CSR variables)
    variables.remove('Workplace')
    variables.remove('Governance')
    variables.remove('Citizenship')
    return(variables)


def put_it_all_together(constructs):
    '''
    Piece R results together with Python results.
    '''
    variables = get_vars_from_json(constructs)
    r=pd.DataFrame()
    imp_files=os.getcwd()+"/imp_files/"
    for imp_file in os.listdir(imp_files):
        print("Reading file: {}".format(imp_file))
        r=r.append(pd.read_csv(os.path.join(imp_files,imp_file)))
    r.drop(variables,axis=1,inplace=True,errors='ignore')
    print("Reading temp_file")
    df=pd.read_csv("./tmp/Temp_File_for_Merging.csv")
    df=pd.merge(df,r,on='Global_ID')
    #Fix Age issue:
    if "Age_x" in df.columns:
        df.rename(columns={"Age_x":"Age"},inplace=True)
        df.drop("Age_y",axis=1,inplace=True)
    return(df)
