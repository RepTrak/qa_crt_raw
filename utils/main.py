import datetime as dt
from dateutil.relativedelta import relativedelta
import boto3, os 
import pandas as pd
import numpy as np

class QA_Raw_CRT():

    def __init__(self):
        pass

    def new_path_4_download_file(self, parent_dir, child_dir):
        # parent_dir = os.getcwd()
        path = parent_dir + child_dir
        os.mkdir(path)
        return path

    def delete_tmp_folder_file(self, full_dir, filename):
        os.remove(full_dir+filename)
        os.rmdir(full_dir)
        return 
    
    # def get_json_data_s3(self, year, month, bucketname, parent_dir):
    def get_json_data_s3(self, year, month, bucketname):

        s3 = boto3.client('s3')
        subfolder =  str(year) + '/' + str(month)+ '/' + 'raw_data/'
        
        # s3.put_object(Bucket=bucketname, Key= subfolder)

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketname)
        prefix_objs = bucket.objects.filter(Prefix=subfolder)

        dflist = []
        tmp_path = '/tmp/'

        for obj in prefix_objs:
            if obj.key.split('.')[-1] == 'json':
                dftemp = pd.read_json(obj.get()['Body'])
                dflist.append(dftemp)
                del dftemp

            # if obj.key.split('.')[-1] == 'sav':
          
            #     filename = obj.key.split('/')[-1]
            #     full_path = self.new_path_4_download_file(parent_dir, tmp_path)
            #     bucket.download_file(obj.key, full_path+filename)
            #     df_spss = pd.read_spss(full_path+filename)
            #     self.delete_tmp_folder_file(full_path, filename)

        df_json = pd.concat(dflist)
        del dflist

        # return df_json, df_spss
        return df_json
    
    def compare_col(self, cur_cols, lst_cols):

        # new columns for this month

        new_cols = list(set(cur_cols)-set(lst_cols))

        # missing columns for this month

        missing_cols = list(set(lst_cols)-set(cur_cols))

        return new_cols, missing_cols
    
    def remove_nan_value_in_common(self, final_list, cur_list, lst_list):

        if np.nan in cur_list and np.nan in lst_list:
            if len(final_list) > 1:
               final_list_no_nan = [x for x in final_list if ~np.isnan(x)]
            elif len(final_list) == 1:
                final_list_no_nan = []
        
        else:
            pass 

        return final_list_no_nan

    def compare_unique_values_by_col(self, df_cur, df_lst, cols_cur, cols_lst):

        # df_unique_value_new, df_unique_value_missing = pd.DataFrame(), pd.DataFrame()
        dict_unique_value_new, dict_unique_value_missing = {}, {}

        common_col_names = [col for col in cols_cur if col in cols_lst]
        for col in common_col_names:
            value_list_cur = df_cur[col].unique().tolist()
            value_list_lst = df_lst[col].unique().tolist()
            new_unique_values, missing_unique_values = self.compare_col(value_list_cur, value_list_lst)
            # new_unique_values = self.remove_nan_value_in_common(new_unique_values, value_list_cur, value_list_lst)
            # missing_unique_values = self.remove_nan_value_in_common(missing_unique_values, value_list_cur, value_list_lst)
            if len(new_unique_values) != 0 :
                dict_unique_value_new[col] = new_unique_values
            if len(missing_unique_values) != 0:
                dict_unique_value_missing[col] = missing_unique_values

        return dict_unique_value_new, dict_unique_value_missing

    def add_column(self, dictionary, new_column_value, new_col_type):

        new_column_name = new_col_type + '_Column_Name'

        # df.insert(loc=0, column= new_column_name, value=new_column_value)
        dictionary[new_column_name] = new_column_value

        return dictionary
    
    def get_compared_results_from_df(self, df_cur, df_lst):

        cols_cur, cols_lst = df_cur.columns, df_lst.columns

        new_cols, missing_cols = self.compare_col(cols_cur, cols_lst)

        dict_new, dict_missing = self.compare_unique_values_by_col(df_cur, df_lst, cols_cur, cols_lst)

        dict_new = self.add_column(dict_new, new_cols, new_col_type='New')

        dict_missing = self.add_column(dict_missing, missing_cols, new_col_type='Missing')

        return dict_new, dict_missing
    
    def main(self, execution_year_month_str, bucketname):

        parent_dir = os.getcwd()

        execution_year_month = dt.datetime.strptime(execution_year_month_str, '%Y-%m').date()
        cur_exe_year = execution_year_month.year
        cur_exe_month = execution_year_month.month

        previous_execution_year_month = execution_year_month + relativedelta(months=-1)
        pre_exe_year = previous_execution_year_month.year
        pre_exe_month = previous_execution_year_month.month
        
        
        df_cur_json = self.get_json_data_s3(cur_exe_year, cur_exe_month, bucketname)

        df_lst_json = self.get_json_data_s3(pre_exe_year, pre_exe_month, bucketname)


        # df_cur_json, df_cur_spss = self.get_json_data_s3(self.cur_exe_year, self.cur_exe_month, bucketname, parent_dir)

        # df_lst_json, df_lst_spss = self.get_json_data_s3(self.pre_exe_year, self.pre_exe_month, bucketname, parent_dir)
        
        dict_new_json, dict_missing_json = self.get_compared_results_from_df(df_cur_json, df_lst_json)

        del df_cur_json, df_lst_json

        # df_new_spss, df_missing_spss = self.get_compared_results_from_df(df_cur_spss, df_lst_spss)

        # del df_cur_spss, df_lst_spss

        # return df_new_json, df_missing_json, df_new_spss, df_missing_spss  
        return dict_new_json, dict_missing_json