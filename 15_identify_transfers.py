# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will identify any claims in the analytical file that had a transfer using a data-driven approach.
# Specifically, I first merged the analytical sample with the raw IP and OP claims created in the previous script. Any claim
# from the analytical sample that matched, has a different provider id from the raw IP/OP claims, and is within two days of
# the emergency response end date is considered to have a transfer. The "two days" is arbitrary, but it's reasonable that transfers
# may happen within two days of the analytical claim's discharge date.
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
import pandas as pd
import numpy as np

########################################### IDENTIFY TRANSFERS #########################################################

# Define years
years = [2012,2013,2014,2015,2016,2017]

for y in years:

    #___ Import analytical claims ___#

    # Read in data
    claims_w_comorbid_scores = pd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_scores/{y}.csv', dtype=str)

    # Convert to Datetime
    date = ['SRVC_BGN_DT', 'SRVC_END_DT', 'BENE_BIRTH_DT', 'BENE_DEATH_DT', 'BENE_DEATH_DT_FOLLOWING_YEAR',
            'DT_1_MONTH', 'DT_3_MONTHS', 'DT_6_MONTHS', 'DT_12_MONTHS']
    for d in date:
        claims_w_comorbid_scores[f'{d}'] = pd.to_datetime(claims_w_comorbid_scores[f'{d}'])

    # Convert numeric string columns to floats
    num = ['niss', 'AGE', 'riss', 'maxais', 'mxaisbr_HeadNeck','BLOOD_PT_FRNSH_QTY','mxaisbr_Extremities','mxaisbr_Face',
           'mxaisbr_Abdomen', 'mxaisbr_Chest', 'combinedscore', 'discharge_death_ind', 'thirty_day_death_ind','ninety_day_death_ind',
           'oneeighty_day_death_ind','threesixtyfive_day_death_ind', 'fall_ind', 'motor_accident_ind','firearm_ind', 'IP_IND','parts_ab_ind',
           'fall_ind_CDC','motor_accident_traffic_ind_CDC','firearm_ind_CDC','X58_ind']
    for n in num:
        claims_w_comorbid_scores[f'{n}'] = claims_w_comorbid_scores[f'{n}'].astype('float')

    # to_csv may have issues exporting this particular column. Need to convert to string again to export it.
    claims_w_comorbid_scores['VALID_DEATH_DT_SW_FOLLOWING_YEAR'] = claims_w_comorbid_scores['VALID_DEATH_DT_SW_FOLLOWING_YEAR'].astype('str')

    # Convert provider ID and NPI to object
    claims_w_comorbid_scores['PRVDR_NUM'] = claims_w_comorbid_scores['PRVDR_NUM'].astype('str')
    claims_w_comorbid_scores['ORG_NPI_NUM'] = claims_w_comorbid_scores['ORG_NPI_NUM'].astype('str')

    # Create unique identifier in order to merge data with transfer info back to analytical claims
    claims_w_comorbid_scores = claims_w_comorbid_scores.reset_index(drop=True)
    claims_w_comorbid_scores['unique_id'] = claims_w_comorbid_scores.reset_index().index

    #___ Create new DF with relevant columns from analytical sample to identify transfers ___#
    # While the original analytical sample is untouched (claims_w_comorbid_scores), I created a new DF (trnsfr_col_fr_final) to conveniently identify which claim
    # has a transfer. Ultimately, I will merge the resulting DF (called merged_claims) back with the original analytical sample (claims_w_comorbid_scores)

    # Keep relevant columns to identify transfer cases
    trnsfr_col_fr_final = claims_w_comorbid_scores[['unique_id', 'BENE_ID', 'SRVC_BGN_DT', 'SRVC_END_DT', 'PRVDR_NUM', 'ORG_NPI_NUM',
                                                  'DSCHRG_CD']]

    # Create date plus two days after service end date. Again, two days is arbitrary, but reasonable for a transfer to occur.
    trnsfr_col_fr_final['day_plustwo'] = trnsfr_col_fr_final['SRVC_END_DT'] + pd.DateOffset(days=2)

    #___ Read in raw hospital claims created from previous script ___#

    # Specify columns
    columns_hos = ['BENE_ID', 'TRNSFR_SRVC_BGN_DT', 'TRNSFR_SRVC_END_DT', 'TRNSFR_PRVDR_NUM', 'TRNSFR_ORG_NPI_NUM',
                   'TRNSFR_DSCHRG_STUS', 'TRNSFR_IP_IND']

    # Read in HOS claims
    hos = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/raw_hos_for_transfer/{y}/',
        engine='fastparquet', columns=columns_hos)

    # Convert to Datetime
    hos['TRNSFR_SRVC_BGN_DT'] = pd.to_datetime(hos['TRNSFR_SRVC_BGN_DT'])
    hos['TRNSFR_SRVC_END_DT'] = pd.to_datetime(hos['TRNSFR_SRVC_END_DT'])

    # Convert provider ID and NPI to object
    hos['TRNSFR_PRVDR_NUM'] = hos['TRNSFR_PRVDR_NUM'].astype('str')
    hos['TRNSFR_ORG_NPI_NUM'] = hos['TRNSFR_ORG_NPI_NUM'].astype('str')

    #___ Merge and identify transfers___#

    # Merge left to keep all trnsfr_col_fr_final claims
    merged_claims = pd.merge(trnsfr_col_fr_final, hos, on=['BENE_ID'], how='left')

    # Identify and keep claims that had a transfer if provider number (and NPI) is different, within two days of the emergency response discharge date, transfer date was at least the emergency response discharge date, and patients did not die.
    merged_claims = merged_claims[(merged_claims['PRVDR_NUM'] != merged_claims['TRNSFR_PRVDR_NUM']) &
                                  (merged_claims['ORG_NPI_NUM'] != merged_claims['TRNSFR_ORG_NPI_NUM']) &
                                  (merged_claims['TRNSFR_SRVC_BGN_DT'] <= merged_claims['day_plustwo']) &
                                  (merged_claims['TRNSFR_SRVC_BGN_DT'] >= merged_claims['SRVC_END_DT']) &
                                  (merged_claims['DSCHRG_CD'] != '20') &
                                  (merged_claims['DSCHRG_CD'] != 'B')]

    #--- Obtaining second stop only (i.e. those closest to discharge date) ---#
    # To obtain second stop (not third stop...), we first need to sort the date column (so that the earliest date is on the top) and drop duplicates on unique id while keeping first.
    # This assumes that the claim with a different provider ID and closest service date to the emergency response discharge date is a transfer claim.

    # Sort transfer datetime column so that the earliest/closest date is on top and ip claim is on top (in case CMS did not roll up claims correctly)
    merged_claims = merged_claims.sort_values(by=['TRNSFR_SRVC_BGN_DT', 'TRNSFR_IP_IND'], ascending=[True, False])

    # Drop duplicate and keep first to obtain the claim with the earliest service begin date that's closest to the analytical's discharge date. This is assumed to be the second stop.
    merged_claims = merged_claims.drop_duplicates(subset=['unique_id'], keep='first') # I can drop using unique_id because I merged trnsfr_col_fr_final and hos DFs on BENE_ID. Thus, there may be duplicates if the beneficiary had multiple transfers or multiple claims in one year.

    #___ Keep relevant columns from transfer side and merge back with original analytical claim ___#

    # Keep relevant columns for transfer
    merged_claims = merged_claims[['unique_id', 'TRNSFR_SRVC_BGN_DT', 'TRNSFR_SRVC_END_DT', 'TRNSFR_PRVDR_NUM', 'TRNSFR_ORG_NPI_NUM',
                                   'TRNSFR_DSCHRG_STUS', 'TRNSFR_IP_IND']]

    # Create indicator to help identify claims that had a transfer
    merged_claims['ind_trnsfr'] = 1

    # Merge the transfer df back with the original final sample
    final_merge_transfer_and_original = pd.merge(claims_w_comorbid_scores, merged_claims, on=['unique_id'], how='left')

    # Replace all nan in ind_trnsfr with 0
    final_merge_transfer_and_original['ind_trnsfr'] = final_merge_transfer_and_original['ind_trnsfr'].fillna(0)

    # CHECK proportion of transfer each year.
    print(final_merge_transfer_and_original.shape[0], claims_w_comorbid_scores.shape[0]) # should be similar
    print(final_merge_transfer_and_original['ind_trnsfr'].sum() / final_merge_transfer_and_original.shape[0])  # Check proportion of transfers each year

    # Clean df by dropping unique id
    final_merge_transfer_and_original = final_merge_transfer_and_original.drop(['unique_id'], axis=1)

    # Export data
    final_merge_transfer_and_original.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_and_transfers/{y}.csv',
        index=False, index_label=False)
    print(list(final_merge_transfer_and_original.columns))


    # Note that I do have the TRNSFR_PRVDR_NUM in case I want to study if transferring to level one is better.








