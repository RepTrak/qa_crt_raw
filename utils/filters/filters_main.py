from utils.filters.filters_delete_cols import delete_survey_cols
from utils.filters.filters_demographics import familiarity_filter, demographic_filters

def filters(dataframe):
    dataframe = delete_survey_cols(dataframe)
    print(f"filters_main.py: Size of data before deleting missing familiarities: {dataframe.shape}")
    dataframe = familiarity_filter(dataframe)
    print(f"filters_main.py: Size of data after familiarity filters: {dataframe.shape}")
    dataframe = demographic_filters(dataframe)
    print(f"filters_main.py: Size of data after demographic filters: {dataframe.shape}")
    print(f"filters_main.py: Saving backup file - 'post_filter_data.csv'")
    dataframe.to_csv('post_filter_data.csv',index=False)
    return(dataframe)