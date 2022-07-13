# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will create the files (in long format) needed to calculate comorbidity scores in SAS.
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

################### CREATE LONG FORMAT IN PREPARATION FOR COMORBIDITY CALCULATIONS IN SAS ##############################

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

# Define columns
col = ['BENE_ID','ADMSN_DT','MEDPAR_ID','DRG_CD']

# Loop for each year
for y in years:

    # Read in data
    df_for_sas = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/bene_with_surgical_drgs/{y}'
                                   ,engine='fastparquet',columns=col)

    # Convert to date time
    df_for_sas['ADMSN_DT'] = pd.to_datetime(df_for_sas['ADMSN_DT'])

    # Create a column one year prior to service date. This column will be used to gather dx codes one year prior to their service date to calculate the comorbidity scores.
    df_for_sas['12_m_prior'] = df_for_sas['ADMSN_DT'] - pd.DateOffset(months=12)

    #--- Match in same year and last year (e.g. 2012 match with 2011 to grab icd codes 12 months prior)---#

    # Need to do if/then since since there was no 2010 file for 2011 claims
    if y in [2011]:

        # Read in same year
        df_last_12m = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/final_raw_dx_for_comorbidity/{y}/',engine='fastparquet')

    else: # the other years 2012-2017

        # Read in same yearp
        df_same_year = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/final_raw_dx_for_comorbidity/{y}/',engine='fastparquet')

        # Read in data from last year
        df_last_year = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/final_raw_dx_for_comorbidity/{y-1}/',engine='fastparquet')

        # Concat together
        df_last_12m = pd.concat([df_same_year,df_last_year],axis=0)

        # Recover memory
        del df_same_year
        del df_last_year

    # Convert to date time
    df_last_12m['SRVC_BGN_DT'] = pd.to_datetime(df_last_12m['SRVC_BGN_DT'])

    # Merge to obtain dx (diagnosis codes) 12 months prior
    df_merge_last12m = pd.merge(df_for_sas,df_last_12m,on=['BENE_ID'],how='left')

    # Keep only those that are within 12 months to gather all dx necessary to calculate comorbidity scores
    df_merge_last12m = df_merge_last12m[(df_merge_last12m['SRVC_BGN_DT']>=df_merge_last12m['12_m_prior'])&
                                      (df_merge_last12m['SRVC_BGN_DT']<=df_merge_last12m['ADMSN_DT'])]

    # Clean DF
    df_merge_last12m = df_merge_last12m.drop(['BENE_ID','SRVC_BGN_DT','ADMSN_DT','12_m_prior'],axis=1)

    # Convert data to long format
    df_long = pd.melt(df_merge_last12m, id_vars='MEDPAR_ID', value_name='DX',var_name='column_names')

    # Clean DF by dropping column_names and renaming unique medpar id
    df_long = df_long.drop(['column_names'],axis=1)
    df_long = df_long.rename(columns={'MEDPAR_ID':'patid'})

    # Add a label if the diagnosis code is ICD9 or ICD10
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
    df_long.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/comorbidity_score/{y}_for_sas.csv', index=False,index_label=False)




