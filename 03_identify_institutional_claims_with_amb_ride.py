#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script's goal is to create indicators for the hospital visits that resulted from an ambulance ride. First, I kept
# the hospital claims that have an emergency ambulance ride using a specific merging process. Under section "Concat and merge with raw
# hospital claims," I merged the resulting file back with the raw institutional claims. This will allow me to correctly and conveniently
# create indicators for the hospital claim that had an emergency ambulance ride.
#----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import dask.dataframe as dd
import numpy as np
import pandas as pd

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

########################## IDENTIFY HOSPITAL CLAIMS WITH EMERGENCY AMBULANCE CLAIMS ####################################
# This method matches IP, then OP on the same day first. We then added +1 day, then +2, to the date of service on the  #
# unmatched ambulance claims and redid the match. Lastly, we concatenated all of the data for each year.               #
########################################################################################################################

# Specify Years
years=[2011,2012,2013,2014,2015,2016,2017]

for year in years:

    #--- Import ambulance (amb) claims ---#

    # Specify columns
    columns_amb = ['BENE_ID','CLM_THRU_DT','ind_for_mi_match','HCPCS_CD','PRVDR_STATE_CD','PRVDR_ZIP','HCPCS_1ST_MDFR_CD',
                   'HCPCS_2ND_MDFR_CD','CARR_LINE_MTUS_CNT_y']

    # Read in ambulance claims. Duplicated claim id's were dropped when exporting ambulance claims in previous codes. Any other duplicates may be claims that are hospital to hospital (HH) transfers on the same day.
    amb = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/merged_amb_mi/{year}/parquet/',engine='fastparquet', columns=columns_amb)

    # Relabel miles column. The "_y" was the suffix for the miles (see previous code)
    amb = amb.rename(columns={'CARR_LINE_MTUS_CNT_y':'MILES'})

    # Keep only those matched with Mileage information
    amb = amb[amb['ind_for_mi_match'] == 1]

    # Drop mileage matched column (irrelevant column)
    amb = amb.drop(['ind_for_mi_match'], axis=1)

    # Convert to Datetime format
    amb['CLM_THRU_DT'] = dd.to_datetime(amb['CLM_THRU_DT'])

    # Keep only those going to hospital. No HH or QL needed.
    pickup_dropoff_cd = ['EH', 'NH', 'RH', 'SH']
    amb = amb.loc[(amb['HCPCS_1ST_MDFR_CD'].isin(pickup_dropoff_cd)) | (amb['HCPCS_2ND_MDFR_CD'].isin(pickup_dropoff_cd))]

    #___Match amb with IP same day___#

    # Specify columns for IP-MEDPAR (op has principle E code whereas ip does not. make sure you are clear when relabeling and concatenating)
    columns_ip = ['MEDPAR_ID','BENE_ID', 'ADMSN_DT','DSCHRG_DT','ADMTG_DGNS_CD'] + ['DGNS_{}_CD'.format(i) for i in range(1, 26)] + ['DGNS_E_{}_CD'.format(k) for k in range(1, 13)]

    # Read in raw IP claims
    ip = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{year}/parquet/', engine='fastparquet', index=False, columns=columns_ip)

    # Rename service dates so concatenating is easier
    ip=ip.rename(columns={'ADMSN_DT':'SRVC_BGN_DT'})
    ip=ip.rename(columns={'DSCHRG_DT':'SRVC_END_DT'})

    # Convert to Datetime format
    ip['SRVC_BGN_DT'] = dd.to_datetime(ip['SRVC_BGN_DT'])
    ip['SRVC_END_DT'] = dd.to_datetime(ip['SRVC_END_DT'])

    # Add columns of one's in IP. This will help to not only calculate the proportion matched, but also drop institutional claims that did not matched with an ambulance claim.
    ip['ind_for_hos_match'] = 1

    # Count the number of diagnosis codes for ip
    diag_col = ['ADMTG_DGNS_CD'] + ['DGNS_{}_CD'.format(i) for i in range(1, 26)] + ['DGNS_E_{}_CD'.format(k) for k in range(1,13)]  # Define diagnosis columns
    ip[diag_col] = ip[diag_col].replace('', np.nan)  # Replace empty strings to count number of diagnosis codes
    ip['num_of_diag_codes'] = ip[diag_col].count(axis='columns')  # Count diagnosis codes (same as doing .count(1))
    ip[diag_col] = ip[diag_col].fillna('')  # Fill nan's with empty strings

    # Drop all diagnosis columns since we do not need it anymore
    ip = ip.drop(['ADMTG_DGNS_CD'] + ['DGNS_{}_CD'.format(i) for i in range(1, 26)] + ['DGNS_E_{}_CD'.format(k) for k in range(1, 13)],axis=1)

    # Add column of consecutive numbers (i.e. unique identifiers) to ensure correct duplicates are dropped after merging. Need to reset index first
    amb=amb.reset_index(drop=True)
    amb['col_consec_num'] = amb.reset_index().index # Need to reset index again to bypass strange error.

    # Merge amb with ip. Keep all of ambulance claims
    amb_merge_ip = dd.merge(ip, amb, left_on=['BENE_ID', 'SRVC_BGN_DT'], right_on=['BENE_ID', 'CLM_THRU_DT'],suffixes=['_IP', '_AMB'], how='right')

    # Recover memory
    del amb

    # Sort in ascending/descending order in each partition. Very expensive operation, but needed to keep earliest claims (i.e. claims not as a result of transfer) and with the most icd info
    amb_merge_ip = amb_merge_ip.map_partitions(lambda x: x.sort_values(by=['BENE_ID','SRVC_END_DT','num_of_diag_codes'], ascending=[True,False,True]))
        # Ascending=False means earliest discharge date at the bottom (to obtain first stop and not transfers)
        # True will have the highest number of diag information at the bottom if SRVC_END_DT were the same
        # This is necessary since we will keep last when dropping duplicates

    # Drop duplicated rows by keeping last (i.e. keep first stop and the most dx-info).
    amb_merge_ip = amb_merge_ip.drop_duplicates(subset=['BENE_ID', 'SRVC_BGN_DT','col_consec_num'], keep='last')
        # Included col_consec_num to correctly identify duplicated information to drop.
        # Included SRVC_BGN_DT as a subset bc I matched on service begin date

    # Clean DF
    amb_merge_ip = amb_merge_ip.drop(['col_consec_num','num_of_diag_codes'], axis=1)

    # Keep those that matched on same day within one DF
    amb_merge_ip_matched = amb_merge_ip[amb_merge_ip['ind_for_hos_match'] == 1]

    # Keep those that did not match on same day within a separate DF
    amb_merge_ip_notmatched = amb_merge_ip[amb_merge_ip['ind_for_hos_match'] != 1]

    # Recover Memory
    del amb_merge_ip

    # Clean DF
    amb_merge_ip_notmatched = amb_merge_ip_notmatched.drop(['SRVC_BGN_DT','MEDPAR_ID','SRVC_END_DT','ind_for_hos_match'],axis=1)
    amb_merge_ip_matched = amb_merge_ip_matched.drop(['BENE_ID','SRVC_BGN_DT', 'SRVC_END_DT', 'ind_for_hos_match', 'CLM_THRU_DT'],axis=1)

    #--- Match with OP same day ---#
    # Now I will take those amb claims that did not match and remerge it with OP claims on the same day.

    # Specify columns
    columns_op = ['CLM_ID','BENE_ID','CLM_THRU_DT', 'CLM_FROM_DT', 'PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD'] + \
                 ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)]

    # Read in raw OP claims
    op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/op_subset/{year}/parquet/', engine='fastparquet', columns=columns_op)

    # Rename service dates so concatenating is easier
    op = op.rename(columns={'CLM_FROM_DT': 'SRVC_BGN_DT', 'CLM_THRU_DT': 'SRVC_END_DT'})

    # Convert to Datetime
    op['SRVC_BGN_DT'] = dd.to_datetime(op['SRVC_BGN_DT'])
    op['SRVC_END_DT'] = dd.to_datetime(op['SRVC_END_DT'])

    # Add columns of one's in OP. This will help to not only calculate the proportion matched, but also drop institutional claims that did not matched with an ambulance claim.
    op['ind_for_hos_match'] = 1

    # Count the number of diagnosis codes for op
    diag_col = ['PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD'] + ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)]  # Define diagnosis columns
    op[diag_col] = op[diag_col].replace('', np.nan)  # Replace empty strings to count number of diagnosis codes
    op['num_of_diag_codes'] = op[diag_col].count(axis='columns')  # Count diagnosis codes (same as doing .count(1))
    op[diag_col] = op[diag_col].fillna('')  # Fill nan's with empty strings

    # Drop all diag columns since we do not need it anymore
    op = op.drop(['PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD'] + ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)],axis=1)

    # Add column of consecutive numbers to ensure correct duplicates are dropped. Need to reset index first
    amb_merge_ip_notmatched=amb_merge_ip_notmatched.reset_index(drop=True)
    amb_merge_ip_notmatched['col_consec_num'] = amb_merge_ip_notmatched.reset_index().index # Need to reset index again to bypass strange error.

    # Merged amb with op. Keep all of ambulance claims
    amb_merge_op = dd.merge(op, amb_merge_ip_notmatched, left_on=['BENE_ID', 'SRVC_BGN_DT'],right_on=['BENE_ID', 'CLM_THRU_DT'],
                            suffixes=['_OP', '_AMB'], how='right')

    # Recover memory by deleting unnecessary DF's
    del amb_merge_ip_notmatched

    # Sort in ascending/descending order in each partition. Very expensive operation, but needed to keep earliest claims (i.e. claims not as a result of transfer) and with the most icd info
    amb_merge_op = amb_merge_op.map_partitions(lambda x: x.sort_values(by=['BENE_ID','SRVC_END_DT','num_of_diag_codes'], ascending=[True,False,True]))
        # Ascending=False means earliest discharge date at the bottom (to obtain first stop) while
        # True will have the highest number of diag information at the bottom if SRVC_END_DT were the same.

    # Drop duplicated rows by keeping last (i.e. keep first stop and the most dx-info).
    amb_merge_op = amb_merge_op.drop_duplicates(subset=['BENE_ID', 'SRVC_BGN_DT','col_consec_num'], keep='last')
        # Included col_consec_num to correctly identify duplicated information to drop
        # Included SRVC_BGN_DT bc I matched on service begin date

    # Clean DF
    amb_merge_op = amb_merge_op.drop(['col_consec_num','num_of_diag_codes'], axis=1)

    # Keep those that matched on same day within one DF
    amb_merge_op_matched = amb_merge_op[amb_merge_op['ind_for_hos_match'] == 1]

    # Keep those that did not match on same day within a separate DF
    amb_merge_op_notmatched = amb_merge_op[amb_merge_op['ind_for_hos_match'] != 1]

    # Recover Memory by deleting unnecessary DF's
    del amb_merge_op

    # Clean DF
    amb_merge_op_notmatched = amb_merge_op_notmatched.drop(['SRVC_BGN_DT','CLM_ID','SRVC_END_DT','ind_for_hos_match'],axis=1)
    amb_merge_op_matched = amb_merge_op_matched.drop(['BENE_ID','SRVC_BGN_DT', 'SRVC_END_DT', 'ind_for_hos_match', 'CLM_THRU_DT'],axis=1)

    #--- Match with IP day +1 ---#
    # Now, repeat the process with IP but add one day to the ambulance claims that were not matched.

    # Add one day to CLM_THRU_DT
    amb_merge_op_notmatched['CLM_THRU_DT_PLUSONE'] = amb_merge_op_notmatched['CLM_THRU_DT'] + timedelta(days=1)

    # Add column of consecutive numbers to ensure correct duplicates are dropped. Need to reset index first
    amb_merge_op_notmatched=amb_merge_op_notmatched.reset_index(drop=True)
    amb_merge_op_notmatched['col_consec_num'] = amb_merge_op_notmatched.reset_index().index # reset index to bypass error, again

    # Merged amb with ip. Keep all of ambulance claims
    amb_merge_ip_plusone = dd.merge(ip, amb_merge_op_notmatched, left_on=['BENE_ID', 'SRVC_BGN_DT'], right_on=['BENE_ID', 'CLM_THRU_DT_PLUSONE'],
                            suffixes=['_IP', '_AMB'], how='right')

    # Recover memory
    del amb_merge_op_notmatched

    # Sort in ascending/descending order in each partition. Very expensive operation, but needed to keep earliest claims (i.e. claims not as a result of transfer) and with the most icd info
    amb_merge_ip_plusone = amb_merge_ip_plusone.map_partitions(lambda x: x.sort_values(by=['BENE_ID','SRVC_END_DT','num_of_diag_codes'], ascending=[True,False,True]))
        # Ascending=False means earliest discharge date at the bottom (to obtain first stop) while
        # True will have the highest number of diag information at the bottom if SRVC_END_DT were the same.

    # Drop duplicated rows by keeping last (i.e. keep first stop and the most dx-info). Included col_consec_num to correctly identify duplicated information to drop.
    amb_merge_ip_plusone = amb_merge_ip_plusone.drop_duplicates(subset=['BENE_ID', 'SRVC_BGN_DT','col_consec_num'], keep='last')
        # included SRVC_BGN_DT bc I matched on service begin date

    # Clean DF
    amb_merge_ip_plusone = amb_merge_ip_plusone.drop(['col_consec_num','num_of_diag_codes'], axis=1)

    # Keep those that matched within one DF
    amb_merge_ip_matched_plusone = amb_merge_ip_plusone[amb_merge_ip_plusone['ind_for_hos_match'] == 1]

    # Keep those that did not match within a separate DF
    amb_merge_ip_notmatched_plusone = amb_merge_ip_plusone[amb_merge_ip_plusone['ind_for_hos_match'] != 1]

    # Recover Memory
    del amb_merge_ip_plusone

    # Clean DF
    amb_merge_ip_notmatched_plusone = amb_merge_ip_notmatched_plusone.drop(['SRVC_BGN_DT','MEDPAR_ID','SRVC_END_DT','ind_for_hos_match','CLM_THRU_DT_PLUSONE'],axis=1)
    amb_merge_ip_matched_plusone = amb_merge_ip_matched_plusone.drop(['BENE_ID','SRVC_BGN_DT', 'SRVC_END_DT', 'ind_for_hos_match', 'CLM_THRU_DT','CLM_THRU_DT_PLUSONE'],axis=1)

    #--- Match with OP plus one ---#
    # Repeat the process but with OP.

    # Add one day to CLM_THRU_DT similar to above.
    amb_merge_ip_notmatched_plusone['CLM_THRU_DT_PLUSONE'] = amb_merge_ip_notmatched_plusone['CLM_THRU_DT'] + timedelta(days=1)

    # Add column of consecutive numbers to ensure correct duplicates are dropped. Need to reset index first
    amb_merge_ip_notmatched_plusone=amb_merge_ip_notmatched_plusone.reset_index(drop=True)
    amb_merge_ip_notmatched_plusone['col_consec_num'] = amb_merge_ip_notmatched_plusone.reset_index().index # reset index to bypass strange error

    # Merged amb with op. Keep all of ambulance claims
    amb_merge_op_plusone = dd.merge(op, amb_merge_ip_notmatched_plusone, left_on=['BENE_ID', 'SRVC_BGN_DT'], right_on=['BENE_ID', 'CLM_THRU_DT_PLUSONE'],
                            suffixes=['_OP', '_AMB'], how='right')

    # Recover memory
    del amb_merge_ip_notmatched_plusone

    # Sort in ascending/descending order in each partition. Very expensive operation, but needed to keep earliest claims (i.e. claims not as a result of transfer) and with the most icd info
    amb_merge_op_plusone = amb_merge_op_plusone.map_partitions(lambda x: x.sort_values(by=['BENE_ID','SRVC_END_DT','num_of_diag_codes'], ascending=[True,False,True]))
        # Ascending=False means earliest discharge date at the bottom (to obtain first stop) while
        # True will have the highest number of diag information at the bottom if SRVC_END_DT were the same.

    # Drop duplicated rows by keeping last (i.e. keep first stop and the most dx-info). Included col_consec_num to correctly identify duplicated information to drop.
    amb_merge_op_plusone = amb_merge_op_plusone.drop_duplicates(subset=['BENE_ID', 'SRVC_BGN_DT','col_consec_num'], keep='last')
        # included SRVC_BGN_DT bc I matched on service begin date

    # Clean DF
    amb_merge_op_plusone = amb_merge_op_plusone.drop(['col_consec_num','num_of_diag_codes'], axis=1)

    # Keep those that matched within one DF
    amb_merge_op_matched_plusone = amb_merge_op_plusone[amb_merge_op_plusone['ind_for_hos_match'] == 1]

    # Keep those that did not match within a separate DF
    amb_merge_op_notmatched_plusone = amb_merge_op_plusone[amb_merge_op_plusone['ind_for_hos_match'] != 1]

    # Recover Memory
    del amb_merge_op_plusone

    # Clean DF
    amb_merge_op_notmatched_plusone = amb_merge_op_notmatched_plusone.drop(['SRVC_BGN_DT','CLM_ID','SRVC_END_DT','ind_for_hos_match','CLM_THRU_DT_PLUSONE'],axis=1)
    amb_merge_op_matched_plusone = amb_merge_op_matched_plusone.drop(['BENE_ID','SRVC_BGN_DT', 'SRVC_END_DT', 'ind_for_hos_match', 'CLM_THRU_DT','CLM_THRU_DT_PLUSONE'],axis=1)

    #--- Match with IP day +2 ---#
    # Now take amb claims that did not match and add two days instead of one and repeat the merge process

    # Add two days to CLM_THRU_DT
    amb_merge_op_notmatched_plusone['CLM_THRU_DT_PLUSTWO'] = amb_merge_op_notmatched_plusone['CLM_THRU_DT'] + timedelta(days=2)

    # Add column of consecutive numbers to ensure correct duplicates are dropped. Need to reset index first
    amb_merge_op_notmatched_plusone=amb_merge_op_notmatched_plusone.reset_index(drop=True)
    amb_merge_op_notmatched_plusone['col_consec_num'] = amb_merge_op_notmatched_plusone.reset_index().index # reset index to bypass strange error

    # Merged amb with ip. Keep all of ambulance claims
    amb_merge_ip_plustwo = dd.merge(ip, amb_merge_op_notmatched_plusone, left_on=['BENE_ID', 'SRVC_BGN_DT'],right_on=['BENE_ID', 'CLM_THRU_DT_PLUSTWO'],
                                    suffixes=['_IP', '_AMB'], how='right')

    # Recover memory
    del amb_merge_op_notmatched_plusone
    del ip

    # Sort in ascending/descending order in each partition. Very expensive operation, but needed to keep earliest claims (i.e. claims not as a result of transfer) and with the most icd info
    amb_merge_ip_plustwo = amb_merge_ip_plustwo.map_partitions(lambda x: x.sort_values(by=['BENE_ID','SRVC_END_DT','num_of_diag_codes'], ascending=[True,False,True]))
        # Ascending=False means earliest discharge date at the bottom (to obtain first stop) while
        # True will have the highest number of diag information at the bottom if SRVC_END_DT were the same.

    # Drop duplicated rows by keeping last (i.e. keep first stop and the most dx-info). Included col_consec_num to correctly identify duplicated information to drop.
    amb_merge_ip_plustwo = amb_merge_ip_plustwo.drop_duplicates(subset=['BENE_ID', 'SRVC_BGN_DT','col_consec_num'], keep='last')
        # included SRVC_BGN_DT bc I matched on service begin date

    # Clean DF
    amb_merge_ip_plustwo = amb_merge_ip_plustwo.drop(['col_consec_num','num_of_diag_codes'], axis=1)

    # Keep those that matched within one DF
    amb_merge_ip_matched_plustwo = amb_merge_ip_plustwo[amb_merge_ip_plustwo['ind_for_hos_match'] == 1]

    # Keep those that did not match within a separate DF
    amb_merge_ip_notmatched_plustwo = amb_merge_ip_plustwo[amb_merge_ip_plustwo['ind_for_hos_match'] != 1]

    # Recover Memory
    del amb_merge_ip_plustwo

    # Clean DF
    amb_merge_ip_notmatched_plustwo = amb_merge_ip_notmatched_plustwo.drop(['SRVC_BGN_DT','MEDPAR_ID','SRVC_END_DT','ind_for_hos_match','CLM_THRU_DT_PLUSTWO'],axis=1)
    amb_merge_ip_matched_plustwo = amb_merge_ip_matched_plustwo.drop(['BENE_ID','SRVC_BGN_DT', 'SRVC_END_DT', 'ind_for_hos_match', 'CLM_THRU_DT','CLM_THRU_DT_PLUSTWO'],axis=1)

    #--- Match with OP Plus Two ---#
    # Repeat the process but with OP.

    # Add two days to CLM_THRU_DT similar to above
    amb_merge_ip_notmatched_plustwo['CLM_THRU_DT_PLUSTWO'] = amb_merge_ip_notmatched_plustwo['CLM_THRU_DT'] + timedelta(days=2)

    # Add column of consecutive numbers to ensure correct duplicates are dropped. Need to reset index first
    amb_merge_ip_notmatched_plustwo=amb_merge_ip_notmatched_plustwo.reset_index(drop=True)
    amb_merge_ip_notmatched_plustwo['col_consec_num'] = amb_merge_ip_notmatched_plustwo.reset_index().index # reset index to bypass strange error

    # Merged amb with op. Keep all of ambulance claims
    amb_merge_op_plustwo = dd.merge(op, amb_merge_ip_notmatched_plustwo, left_on=['BENE_ID', 'SRVC_BGN_DT'],right_on=['BENE_ID', 'CLM_THRU_DT_PLUSTWO'],
                                    suffixes=['_OP', '_AMB'], how='right')

    # Recover memory
    del amb_merge_ip_notmatched_plustwo
    del op

    # Sort in ascending/descending order in each partition. Very expensive operation, but needed to keep earliest claims (i.e. claims not as a result of transfer) and with the most icd info
    amb_merge_op_plustwo = amb_merge_op_plustwo.map_partitions(lambda x: x.sort_values(by=['BENE_ID','SRVC_END_DT','num_of_diag_codes'], ascending=[True,False,True]))
        # Ascending=False means earliest discharge date at the bottom (to obtain first stop) while
        # True will have the highest number of diag information at the bottom if SRVC_END_DT were the same.

    # Drop duplicated rows by keeping last (i.e. keep first stop and the most dx-info). Included col_consec_num to correctly identify duplicated information to drop.
    amb_merge_op_plustwo = amb_merge_op_plustwo.drop_duplicates(subset=['BENE_ID', 'SRVC_BGN_DT','col_consec_num'], keep='last')
        # included SRVC_BGN_DT bc I matched on service begin date

    # Clean DF
    amb_merge_op_plustwo = amb_merge_op_plustwo.drop(['col_consec_num','num_of_diag_codes'], axis=1)

    # Keep those that matched within one DF
    amb_merge_op_matched_plustwo = amb_merge_op_plustwo[amb_merge_op_plustwo['ind_for_hos_match'] == 1]

    # Keep those that did not match within a separate DF
    amb_merge_op_notmatched_plustwo = amb_merge_op_plustwo[amb_merge_op_plustwo['ind_for_hos_match'] != 1]

    # Recover Memory
    del amb_merge_op_plustwo

    # Clean DF
    amb_merge_op_notmatched_plustwo = amb_merge_op_notmatched_plustwo.drop(['SRVC_BGN_DT','CLM_ID','SRVC_END_DT','ind_for_hos_match','CLM_THRU_DT_PLUSTWO'],axis=1)
    amb_merge_op_matched_plustwo = amb_merge_op_matched_plustwo.drop(['BENE_ID','SRVC_BGN_DT', 'SRVC_END_DT', 'ind_for_hos_match', 'CLM_THRU_DT','CLM_THRU_DT_PLUSTWO'],axis=1)

    #_________________________________Concat and merge with raw hospital claims________________________________________#

    # Concat all amb/ip matched separately from amb/op first
    amb_ip = dd.concat([amb_merge_ip_matched, amb_merge_ip_matched_plusone, amb_merge_ip_matched_plustwo], axis=0)
    amb_op = dd.concat([amb_merge_op_matched, amb_merge_op_matched_plusone, amb_merge_op_matched_plustwo], axis=0)

    # Delete extra DFs to recover memory
    del amb_merge_ip_matched
    del amb_merge_op_matched
    del amb_merge_ip_matched_plusone
    del amb_merge_op_matched_plusone
    del amb_merge_ip_matched_plustwo
    del amb_merge_op_matched_plustwo
    del amb_merge_op_notmatched_plustwo

    # Create amb_ind column. This will be used to identify hospital claims that resulted from an emergency ambulance ride.
    amb_ip['amb_ind'] = 1
    amb_op['amb_ind'] = 1

    # Define columns from IP
    ip_columns = ['BENE_ID', 'ADMSN_DT', 'MEDPAR_ID','PRVDR_NUM', 'DSCHRG_DT','ORG_NPI_NUM', 'BENE_DSCHRG_STUS_CD', 'BLOOD_PT_FRNSH_QTY',
                  'ADMTG_DGNS_CD'] + ['DGNS_{}_CD'.format(i) for i in range(1, 26)] + ['DGNS_E_{}_CD'.format(k) for k in range(1, 13)]

    # Define columns from OP
    op_columns = ['BENE_ID', 'CLM_ID', 'CLM_FROM_DT','CLM_THRU_DT', 'ORG_NPI_NUM','PRVDR_NUM', 'NCH_BLOOD_PNTS_FRNSHD_QTY',
                  'PTNT_DSCHRG_STUS_CD', 'PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD','CLM_FAC_TYPE_CD'] + \
                  ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)]

    # Read in raw ip to merge with the amb_ip claims
    raw_ip = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{year}/parquet/',
                             engine='fastparquet', columns=ip_columns)

    # Merge amb_ip file with raw ip to obtain the amb_ind indicator column
    raw_ip_w_amb = dd.merge(raw_ip,amb_ip,on=['MEDPAR_ID'],how='left')

    # Recover memory
    del raw_ip
    del amb_ip

    # Export final DF. This IP claim should now have indicators if ip claim was a result of an amb ride
    raw_ip_w_amb.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/ip_w_amb/{year}/parquet/',compression='gzip',engine='fastparquet')

    # Recover memory
    del raw_ip_w_amb

    # Read in raw op base to merge with the amb_op claims
    raw_op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/opb/{year}/parquet/',
                             engine='fastparquet', columns=op_columns)

    # Merge amb/hos file with raw opb to obtain the amb_ind indicator column
    raw_op_w_amb = dd.merge(raw_op,amb_op,on=['CLM_ID'],how='left')

    # Recover memory
    del raw_op
    del amb_op

    # Drop all raw outpatient claims that are NOT from a hospital
    raw_op_w_amb['CLM_FAC_TYPE_CD'] = raw_op_w_amb['CLM_FAC_TYPE_CD'].astype(str) # ensure that dtype is consistent
    raw_op_w_amb = raw_op_w_amb[(raw_op_w_amb['CLM_FAC_TYPE_CD']=='1')|(raw_op_w_amb['amb_ind']==1)]

    # Export final DF. This OP claim should now have indicators if op claim was a result of an amb ride
    raw_op_w_amb.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/opb_w_amb/{year}/parquet/',compression='gzip',engine='fastparquet')

    # Recover memory
    del raw_op_w_amb









