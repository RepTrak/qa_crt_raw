from utils.io.setup import get_logistics
from utils.io.io import Unzip, transfer_from_s3, resume
import json
from utils.io.data_intake_main import get_files_dic, etl
# from utils.filters.filters_main import filters
# from utils.awareness.awareness_main import awareness
# from utils.computations.compute_main import compute
# from utils.upload.upload_platform_file import send_to_analyzer
# from utils.computations.imputation import run_imputations
# from utils.upload.upload_imputation import upload_imp
# from utils.io.clean_up import clean_up
import datetime as dt
from dateutil.relativedelta import relativedelta
import boto3
import pandas as pd

class QA_Raw_CRT():

    def __init__(self, execution_year_month_str):

        execution_year_month = dt.datetime.strptime(execution_year_month_str, '%Y-%m').date()
        self.cur_exe_year = execution_year_month.year
        self.cur_exe_month = execution_year_month.month

        previous_execution_year_month = execution_year_month + relativedelta(months=-1)
        self.pre_exe_year = previous_execution_year_month.year
        self.pre_exe_month = previous_execution_year_month.month
    
    def get_json_data_s3(self, year, month, bucketname):

        s3 = boto3.client('s3')
        subfolder =  str(year) + '/' + str(month)+ '/' + 'raw_data/'
        
        # s3.put_object(Bucket=bucketname, Key= subfolder)

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketname)
        prefix_objs = bucket.objects.filter(Prefix=subfolder)

        dflist = []

        for obj in prefix_objs:
            if obj.key.split('.')[-1] == 'json':
                fullfilename = "s3://" + bucketname +'/'+ obj.key
                print(fullfilename)
                # df_spss = pd.read_spss(fullfilename)
                dftemp = pd.read_json(obj.get()['Body'])
                dflist.append(dftemp)
        df = pd.concat(dflist)

        return df
    

        
        
            


    def run(S3_PATH, start, stop, upload, roll):

        # Organize paths, get important details - always run
        paths_dic, codebook_df, constructs_df, export_cmd = get_logistics(S3_PATH)
        ALL_FILES = get_files_dic('./tmp/')
        if start > 1:
            df, meta = resume(start, ALL_FILES)

        # ---------------------------------------------------
        # 0: Downloading data
        if start == 0:
            print(f"Passing dataset from s3 ({paths_dic['filename_with_path']}) "
                f"to ec2 (tmp) and unzipping")
            transfer_from_s3(export_cmd)
            Unzip(paths_dic['tmp_path'])
            ALL_FILES = get_files_dic('./tmp/') # Need this here!
            start +=1
            if stop:
                start=8

        # ---------------------------------------------------
        # 1: Reading data
        if start == 1:
            json_meta = json.load(open(ALL_FILES['.txt'][0]))
            # df, meta, SPSS, JSON = etl(ALL_FILES, './tmp/', json_meta)
            df_json, df_spss = etl(ALL_FILES, './tmp/', json_meta)
            # df = filters(df)
            start += 1
            if stop:
                start = 8
        return df_json, df_spss

        # # ---------------------------------------------------
        # # 2: Awareness
        # if start == 2:
        #     awareness(df, ALL_FILES)
        #     start += 1
        #     if stop:
        #         start = 8

        # # ---------------------------------------------------
        # # 3: Computations
        # if start == 3:
        #     df = compute(df, codebook_df, constructs_df,
        #                 upload = upload, roll_cmd = roll)
        #     start += 1
        #     if stop:
        #         start = 8

        # # ---------------------------------------------------
        # # 4: Upload scores to Platform
        # if start == 4:
        #     send_to_analyzer(df)
        #     start += 1
        #     if stop:
        #         start = 8

        # # ---------------------------------------------------
        # # 5: Imputations
        # if start == 5:
        #     df = run_imputations(df, constructs_df)
        #     start += 1
        #     if stop:
        #         start = 8

        # # ---------------------------------------------------
        # # 6: Upload Imputations
        # if start == 6:
        #     zip_filenames = upload_imp(df, upload_cmd=upload,
        #                             paths_dic=paths_dic, meta=meta)
        #     start += 1
        #     if stop:
        #         start = 8

        # # ---------------------------------------------------
        # # 7: Clean up!
        # if start == 7:
        #     clean_up(zip_filenames, paths_dic)

        # print("\n---\nCRT Processing is complete!\n---")
