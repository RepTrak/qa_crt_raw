import json
import multiprocessing
import pyreadstat
import pandas as pd

from utils.io.get_stakeholders import get_stakeholders_awareness

def get_awareness_data(ALL_FILES):
    awareness_df = pd.DataFrame()
    essentials = ['CODERESP', 'COUNTRY', 'Month', 'hSampleType', 'Region_C']
    stakeholders = list(set(get_stakeholders_awareness().awareness_var.tolist()))
    print(stakeholders)

    if '.sav' in ALL_FILES.keys():
        # ------------------------------------------------------------------------
        # SPSS first!
        for spss_file in ALL_FILES['.sav']:
            if (len(ALL_FILES['.sav']) > 1):
                raise Exception("get_awareness.py: Can't read more than 1 spss file!")
            print(f'get_awareness.py: Opening SPSS file {spss_file} via pyreadstat.multiprocessing().')
            num_processes = multiprocessing.cpu_count()
            df_spss, spss_meta = \
                pyreadstat.read_file_multiprocessing(pyreadstat.read_sav,
                                                     spss_file, num_processes)
            df_spss.rename({'Country': 'COUNTRY',
                            'STATUS': 'respStatus',
                            'AGE':'Age'}, axis=1, inplace=True)
            s105s = [x for x in df_spss.columns if x.startswith('S105Fieldeda')]
            all_cols = essentials + stakeholders + s105s
            all_cols = [x for x in all_cols if x in df_spss.columns]
            if 'hSampleType' in df_spss.columns:
                print("hSampleType found in AUS SPSS data.")
            else:
                print("hSampleType not found, adding it to AUS SPSS data.")
                df_spss['hSampleType'] = 1
            df_spss = df_spss[all_cols].copy()
            df_spss.reset_index(inplace=True, drop=True)
            df_spss = df_spss.loc[:, ~df_spss.columns.duplicated()].copy()
            rename_s105s = dict([[x, x.replace('S105Fieldeda', 'S105Fielded_')] for x in df_spss.columns
                                 if x.startswith('S105Fieldeda')])
            print(f"get_awareness.py: Renaming S105Fieldeda to S105Fielded (AUS)")
            df_spss.rename(rename_s105s,inplace=True,axis=1)
            #print(f"get_awareness.py: First 20 df_spss colnames: {df_spss.columns.tolist()[0:20]}")
            awareness_df = pd.concat([awareness_df, df_spss])
            del df_spss

    # LOAD JSON
    if '.json' in ALL_FILES.keys():
        JSON = True
        json_files = ALL_FILES['.json']
        print(f'---\nget_awareness.py: Opening all JSON files {json_files}.')
        for file in ALL_FILES['.json']:
            print(f"get_awareness.py: Reading and normalizing {file}")
            json_df = pd.json_normalize(json.load(open(file)))
            s105s = [x for x in json_df.columns if x.startswith('S105Fielded')]
            all_cols = essentials + stakeholders + s105s
            all_cols = [x for x in all_cols if x in json_df.columns]
            json_df = json_df[all_cols].copy()
            print("get_awareness.py: Appending json to awareness data.")
            json_df = json_df.loc[:, ~json_df.columns.duplicated()].copy()
            json_df.reset_index(inplace=True, drop=True)
            awareness_df = pd.concat([awareness_df, json_df])
            awareness_df.reset_index(inplace=True, drop=True)
    print(f"get_awareness.py: Finished creating awareness dataframe.")
    print(f"get_awareness.py: Enforcing datatypes on awareness_df.")
    s105s = [x for x in awareness_df.columns if x.startswith('S105Fielded')]
    all_cols = essentials + stakeholders + s105s
    all_cols = [x for x in all_cols if x in awareness_df.columns]
    for col in all_cols:
        awareness_df[col] = pd.to_numeric(awareness_df[col], errors = 'coerce')
    return(awareness_df)