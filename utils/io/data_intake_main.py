import os
import numpy as np
import pandas as pd
import multiprocessing
import pandas as pd
import pyreadstat

from utils.io.data_intake_spss import get_spss
from utils.io.data_intake_json import get_json
# from utils.io.organize_metadata import prepare_metadata


def get_files_dic(FOLDER):
    # Identify files.
    FILE_FORMATS = ['.sav', '.json']
    ALL_FILES = {}

    print('---')
    for file_format in FILE_FORMATS:
        if len([FOLDER + x for x in os.listdir(FOLDER) if x.endswith(file_format)]) != 0:
            print(f"{file_format} files found!")
            files = [FOLDER + x for x in os.listdir(FOLDER) if (x.endswith(file_format)
                                                                and not x.startswith('._'))]
            print(f"Working with the {file_format} files: {files}")
            ALL_FILES[file_format] = files
    print('---\n')
    return(ALL_FILES)


def etl(ALL_FILES, FOLDER, json_meta):
    too_many_spss_files_error_msg = \
        "\n---\nHEY! Chill out!\nWe can't handle more than 1 spss file right now.\n---"
    df = pd.DataFrame()
    SPSS = False
    JSON = False

    # LOAD SPSS
    if '.sav' in ALL_FILES.keys():
        for spss_file in ALL_FILES['.sav']:
            if (len(ALL_FILES['.sav']) > 1):
                raise Exception(too_many_spss_files_error_msg)
            SPSS = True
            print(f'data_intake_maine.py: Opening SPSS file {spss_file} via pyreadstat.multiprocessing().')
            num_processes = multiprocessing.cpu_count()
            df_spss, spss_meta = \
                pyreadstat.read_file_multiprocessing(pyreadstat.read_sav,
                                                     spss_file, num_processes)
            # misleading name but get_spss cleans up the spss file.
            df_spss = get_spss(df_spss, spss_meta)

    # LOAD JSON
    if '.json' in ALL_FILES.keys():
        JSON = True
        json_files = ALL_FILES['.json']
        print(f'---\ndata_intake_maine.py: Opening all JSON files {json_files}.')
        df_json = get_json(FOLDER, json_meta)

    # if SPSS:
    #     print('data_intake_maine.py: Appending SPSS data.')
    #     df_spss['File_Origin'] = 'SPSS'
    #     df = pd.concat([df, df_spss], ignore_index=True)
    #     df.reset_index(inplace=True, drop=True)
    #     print(f'Dataframe shape with only SPSS (au) data: {df.shape}')
    #     del df_spss
    # if JSON:
    #     print('data_intake_maine.py: Appending JSON data.')
    #     df_json['File_Origin'] = 'JSON'
    #     df_json.reset_index(inplace=True, drop=True)
    #     df.reset_index(inplace=True, drop=True)
    #     df = df.loc[:, ~df.columns.duplicated()].copy()
    #     df_json = df_json.loc[:, ~df_json.columns.duplicated()].copy()
    #     df = pd.concat([df, df_json], ignore_index=True)
    #     df.reset_index(inplace=True, drop=True)
    #     print(f'Dataframe shape after appending JSON data: {df.shape}')
    #     del df_json

    # print("data_intake_maine.py: Dropping invalid countries.")
    # df = df[~df.COUNTRY.isna()].copy()
    # df['COUNTRY'] = df.COUNTRY.astype(int)
    # critical_cols = [x for x in df.columns if
    #                  (x.startswith('Q320') or x.startswith('Q305') or x.startswith('215')
    #                   or x.startswith('410') or x.startswith('Q800'))]
    # print(f"data_intake_main.py: Transforming <NA> to np.nan for critical columns:\n{critical_cols}")
    # for cc in critical_cols:
    #     df.loc[df[cc].isna(), cc] = np.nan
    # meta_data = prepare_metadata(json_meta, spss_meta, df.columns.tolist())

    # return([df,meta_data,SPSS,JSON])
    return df_json, df_spss