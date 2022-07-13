# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will merge original analytical claims containing surgical code with data from SAS containing
# the calculated comorbidity scores.
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

##################################### MERGE DRG CLAIMS WITH COMORBIDITY SCORES #########################################

# Specify years
years=[2012,2013,2014,2015,2016,2017] # no 2011 since we require one year prior when calculating comorbidity scores and there is no 2010 data.

for y in years:

    # Read in data
    ip_df_surgical = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/bene_with_surgical_drgs/{y}'
                                   ,engine='fastparquet')

    # CHECK denominator
    print(ip_df_surgical.shape[0])

    # Relabel unique_id columns to easily merge
    ip_df_surgical = ip_df_surgical.rename(columns={'MEDPAR_ID':'patid'})

    # Read in data from SAS containing comorbidity scores
    comorbid_scores = pd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/comorbidity_score/files_output_from_sas/{y}_sas_output.csv')

    # CHECK denominator
    print(comorbid_scores.shape[0])

    # Merge scores with claims on pat id to obtain comorbidity scores
    ip_surgical_w_comorbidity = pd.merge(ip_df_surgical,comorbid_scores,how='inner',on=['patid'])

    # CHECK denominator
    print(ip_surgical_w_comorbidity.shape[0])

    # Recover memory
    del comorbid_scores
    del ip_df_surgical

    #___ Create Death Indicators 30 day ___#

    # Convert to datetime
    ip_surgical_w_comorbidity['BENE_DEATH_DT'] = pd.to_datetime(ip_surgical_w_comorbidity['BENE_DEATH_DT'])
    ip_surgical_w_comorbidity['ADMSN_DT'] = pd.to_datetime(ip_surgical_w_comorbidity['ADMSN_DT'])
    ip_surgical_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR'] = pd.to_datetime(ip_surgical_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR'])

    # Create 30 days from emergency response (i.e. service begin date and not service end date)
    ip_surgical_w_comorbidity['DT_1_MONTH'] = ip_surgical_w_comorbidity['ADMSN_DT'] + pd.DateOffset(months=1)

    # Create mortality indicator (w/in 30 days)
    ip_surgical_w_comorbidity['thirty_day_death_ind'] = np.where(
                                                       (ip_surgical_w_comorbidity['BENE_DSCHRG_STUS_CD']=='B') |
                                                       ((ip_surgical_w_comorbidity['BENE_DEATH_DT']<=ip_surgical_w_comorbidity['DT_1_MONTH']) & (ip_surgical_w_comorbidity['VALID_DEATH_DT_SW']=='V')) |
                                                       ((ip_surgical_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR']<=ip_surgical_w_comorbidity['DT_1_MONTH']) & (ip_surgical_w_comorbidity['VALID_DEATH_DT_SW_FOLLOWING_YEAR']=='V')),
                                                       1,0)

    # Remove observations with multiple surgeries on same day. (Best I can do is drop all duplicates where bene and admin date is the same suggesting two procedures occurred in a single day. Medpar file doesn't have an indicator for multiple surgeries)
    ip_surgical_w_comorbidity = ip_surgical_w_comorbidity.drop_duplicates(subset=['BENE_ID','ADMSN_DT'],keep=False)

    # Calculate age using bene birthday
    ip_surgical_w_comorbidity['ADMSN_DT'] = pd.to_datetime(ip_surgical_w_comorbidity['ADMSN_DT'])
    ip_surgical_w_comorbidity['BENE_BIRTH_DT'] = pd.to_datetime(ip_surgical_w_comorbidity['BENE_BIRTH_DT'])
    ip_surgical_w_comorbidity['AGE'] = (ip_surgical_w_comorbidity['ADMSN_DT'] - ip_surgical_w_comorbidity['BENE_BIRTH_DT']) / np.timedelta64(1, 'Y')

    # Read out data to remove any surgeries that were trauma related
    ip_surgical_w_comorbidity.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/claims_w_comorbid/{y}.csv',index=False,index_label=False)












