# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will create the files needed to create comorbidity scores in SAS. The goal is to merge the raw
# diagnosis data from the previous script with the analytical file and keep diagnosis codes up to one year prior to the
# injury event. Lastly, I will change the data from wide to long since the SAS script requires a long data format,
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

################# MANIPULATE DATAFRAME IN PREPARATION FOR COMORBIDITY CALCULATIONS IN SAS ##############################

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

# Define columns
col = ['BENE_ID','SRVC_BGN_DT','UNIQUE_ID']

for y in years:

    # Read in analytical sample with the three columns defined above
    df_for_sas = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}_major_trauma',engine='fastparquet',columns=col)

    # Rename column
    df_for_sas=df_for_sas.rename(columns={'SRVC_BGN_DT':'SRVC_BGN_DT_HOS'}) # HOS is hospital. This will help identify which date column is from the analytical and which is from the raw dataset containing the diagnosis information

    # Convert to date time
    df_for_sas['SRVC_BGN_DT_HOS'] = pd.to_datetime(df_for_sas['SRVC_BGN_DT_HOS'])

    # Create a column 12 months prior to service date
    df_for_sas['12_m_prior'] = df_for_sas['SRVC_BGN_DT_HOS'] - pd.DateOffset(months=12)

    #--- Match in same year and last year (e.g. 2012 match with 2011 to grab icd codes 12 months prior)---#

    # Need to do if/then since 2011 doesn't have data in 2010
    if y in [2011]:

        # Read in same year only since 2010 is not available
        df_last_12m = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/raw_dx_for_comorbidity/{y}/',engine='fastparquet')

    elif y in [2012,2013,2014,2015,2016,2017]:

        # Read in same year
        df_same_year = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/raw_dx_for_comorbidity/{y}/',engine='fastparquet')

        # Read in data from last year
        df_last_year = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/raw_dx_for_comorbidity/{y-1}/',engine='fastparquet') # the "-1" is the read in data from previous year

        # Concat together
        df_last_12m = pd.concat([df_same_year,df_last_year],axis=0)

        # Recover memory
        del df_same_year
        del df_last_year

    # Convert raw file to date time
    df_last_12m['SRVC_BGN_DT'] = pd.to_datetime(df_last_12m['SRVC_BGN_DT'])

    # Merge analytical with raw
    df_merge_last12m = pd.merge(df_for_sas,df_last_12m,on=['BENE_ID'],how='left')

    # Keep only dx that are within 12 months. I.e. Keep only diagnosis information if it's within 12 months prior to injury event
    df_merge_last12m = df_merge_last12m[(df_merge_last12m['SRVC_BGN_DT']>=df_merge_last12m['12_m_prior'])&
                                      (df_merge_last12m['SRVC_BGN_DT']<=df_merge_last12m['SRVC_BGN_DT_HOS'])]

    # Clean DF
    df_merge_last12m = df_merge_last12m.drop(['BENE_ID','SRVC_BGN_DT','SRVC_BGN_DT_HOS','12_m_prior'],axis=1)

    # Convert data to long format for SAS
    df_long = pd.melt(df_merge_last12m, id_vars='UNIQUE_ID', value_name='DX',var_name='column_names')

    # Clean DF by dropping column_names and renaming unique claim identifier
    df_long = df_long.drop(['column_names'],axis=1)
    df_long = df_long.rename(columns={'UNIQUE_ID':'patid'})

    # Add a label if the diagnosis code is ICD9 or ICD10. 2015 will be automatically dropped in SAS program because I specified '09' for icd9
    if y in [2011,2012,2013,2014,2015]:
        df_long['Dx_CodeType'] = '09'
    if y in [2016,2017]:
        df_long['Dx_CodeType'] = '10'

    # Remove any rows with missing or empty strings in DX
    df_long = df_long[(~(df_long['DX'].isna())) & (df_long['DX']!='') & (df_long['DX']!=' ')]

    # Drop duplicated icd codes
    df_long = df_long.drop_duplicates(subset=['patid','DX'],keep='first')

    # Convert all to string
    df_long = df_long.astype(str)

    # Export data to csv before importing in SAS
    df_long.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/comorbidity_score/{y}_for_sas.csv', index=False,index_label=False)

