# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will drop any claims containing a trauma ICD code in the first four columns. The reasoning
#              is that we will use the risk-adjusted surgical mortality as a covariate in our analysis. Thus, we should
#              not have outcomes for the same sample on both sides of the equation (i.e. outcomes of mortality on left
#              side and risk-adjust surgical mortality outcome on the right from the same sample).
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

#################################### DROP IF CLAIMS CONTAIN TRAUMA DIAGNOSIS CODES #####################################

# Specify years
years=[2012,2013,2014,2015,2016,2017]

for y in years:

    # Read in data
    ip_df_surgical = pd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/claims_w_comorbid/{y}.csv',dtype=str)

    # Convert columns to datetime format
    date_list=['ADMSN_DT','BENE_BIRTH_DT','BENE_DEATH_DT','BENE_DEATH_DT_FOLLOWING_YEAR','DT_1_MONTH']
    for d in date_list:
        ip_df_surgical[f'{d}'] = pd.to_datetime(ip_df_surgical[f'{d}'])

    # Convert columns to float
    num_list = ['combinedscore', 'thirty_day_death_ind', 'AGE']
    for n in num_list:
        ip_df_surgical[f'{n}'] = ip_df_surgical[f'{n}'].astype(float)

    # Rename combinedscore to comorbidity_score
    ip_df_surgical = ip_df_surgical.rename(columns={'combinedscore':'comorbidity_score'})

    # Define list of first three diagnosis columns plus the admission column (4 columns total since the admission column may be duplicates of the first dx column). Will be used when dropping claims with injury
    diag_first_four_cols = ['ADMTG_DGNS_CD'] + [f'DGNS_{i}_CD' for i in range(1, 4)]

    # Need to convert the columns to str again to bypass AttributeError: 'float' object has no attribute 'startswith'
    ip_df_surgical[diag_first_four_cols] = ip_df_surgical[diag_first_four_cols].astype(str)

    #___ ICD9: Drop claims if first three columns contain any dx code starting with 8 or 9 ___#

    if y in [*range(2012,2016,1)]: # 2012-2015

        # Keep if first three columns plus admitting column does NOT begin with 8 or 9
        ip_df_surgical = ip_df_surgical.loc[~(ip_df_surgical[diag_first_four_cols].applymap(lambda x: x.startswith(('8','9'))).any(axis='columns'))]

    #___ ICD10: Drop claims if first three columns contain any dx code starting with S, T, M97, or O9A2-O9A5 ___#

    else: # 2016-2017

        # Keep if first three columns plus admitting column does NOT begin with S, T, M97, or O9A2-O9A5
        ip_df_surgical = ip_df_surgical.loc[~(ip_df_surgical[diag_first_four_cols].applymap(lambda x : x.startswith(('S','T','M97','O9A2','O9A3','O9A4','O9A5'))).any(axis='columns'))]

    # Clean DF: Drop unnecessary columns
    ip_df_surgical = ip_df_surgical.drop(['VALID_DEATH_DT_SW','VALID_DEATH_DT_SW_FOLLOWING_YEAR','DT_1_MONTH','ADMTG_DGNS_CD']+[f'DGNS_{i}_CD' for i in range(1,26)],axis=1)

    # Create year variable for year Fixed Effects (I did NOT use a loop since there were some 2011 in 2012 data, 2012 in 2013 data, etc... so this way was easier but may not look as clean)
    ip_df_surgical['year_fe'] = 0
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2011, 2011)
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2012, 2012)
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2013, 2013)
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2014, 2014)
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2015, 2015)
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2016, 2016)
    ip_df_surgical['year_fe'] = ip_df_surgical['year_fe'].mask(ip_df_surgical['ADMSN_DT'].dt.year == 2017, 2017)

    # Read out data to stata
    ip_df_surgical.to_stata(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/data_to_run_glm_in_stata/{y}.dta',write_index=False)
