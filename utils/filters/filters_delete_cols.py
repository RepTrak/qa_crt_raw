import re


def delete_survey_cols(df):
    '''
    Removes survey-coding variables, recodes numerical variables
            from StringType to IntegerType
    Input: dask/pandas dataframe
    Output: Reduced dataframe
    '''
    # Remove variables
    bad_vars = ["CompS105_", "CompS105Quota_", "CompS105x2_", "Qualified_Companies_",
                "CUSTOMER_",  "CustomerType_", "RatedCustomerQuota_",
                "NONNATIONALCustomer_", "Companies_", "TPSCompany_", #"S105Fielded_",
                "NonFamiliar_CompaniesFielded_", "NatFamiliar_CompaniesFielded_",
                "Familiar_CompaniesFielded_", "NonFamiliar_CompaniesGlobal_",
                "NatNonFamiliar_CompaniesFielded_",
                "S105Global_", "NatNonFamiliar_CompaniesGlobal_", "Familiar_CompaniesGlobal_",
                "NonFamiliar_Companies_", "Familiar_Companies_","NatNonFamiliar_Companies_",
                "NatFamiliar_Companies_","NatFamiliar_CompaniesGlobal_",
               "CompaniesRating_", "Bank_Rated_Customer_", "Bank_Rated_Non_Customer_",
               "Bank_Rated_CustomerAll_", "Bank_Rated_Non_CustomerAll_",
               "S105_Eligible_","S105_Shown_","S105_SECOND_", "S105_","S105_Qualified"]#,
               #"Companies_", "Companies2_", "XS105_Companies_"]
    bad_str = "("+"\d+|".join(bad_vars)+"\d+)"
    del_these = re.findall(bad_str, ''.join(df.columns))
    df = df.drop(del_these, axis = 1)
    return(df)
