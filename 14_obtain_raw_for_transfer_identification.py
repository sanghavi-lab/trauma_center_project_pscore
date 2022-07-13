#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: Although I have gathered raw institutional information before, I will gather raw data again but, this time,
# the files exported from here will be specifically created to help identify claims from the analytical sample that
# eventually had a transfer in the next script. This is not relevant for the first paper but we will eventually explore
# another research question on if transferring to another hospital result in higher survivability than going straight to
# a particular hospital type.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
import numpy as np

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

################################# FILTER HOSPITAL CLAIMS TO IDENTIFY TRANSFERS  ########################################

# Define years
years = [2012,2013,2014,2015,2016,2017]

# Specify columns
columns_ip = ['BENE_ID', 'ADMSN_DT', 'DSCHRG_DT', 'PRVDR_NUM', 'ORG_NPI_NUM', 'BENE_DSCHRG_STUS_CD', 'ADMTG_DGNS_CD'] + [f'DGNS_{i}_CD' for i in range(1, 26)]
columns_op = ['BENE_ID', 'CLM_FROM_DT', 'CLM_THRU_DT', 'PRVDR_NUM', 'ORG_NPI_NUM', 'PTNT_DSCHRG_STUS_CD', 'CLM_FAC_TYPE_CD', 'PRNCPAL_DGNS_CD'] + [f'ICD_DGNS_CD{i}' for i in range(1, 26)]

for y in years:

    #___ Read in amb_hos merged data to subset relevant bene ids ___#

    # Define Columns
    columns_processed = ['BENE_ID','SRVC_BGN_DT','PRVDR_NUM','DSCHRG_CD']

    # Read in analytical sample
    processed_df = dd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_scores/{y}.csv'
                                   ,dtype=str,usecols=columns_processed)

    # Convert to Datetime
    processed_df['SRVC_BGN_DT'] = dd.to_datetime(processed_df['SRVC_BGN_DT'])

    #___ Read in raw IP and subset relevant bene_ids ___#

    # Read in IP claims
    ip = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{y}/parquet/',
                         engine='fastparquet', columns=columns_ip)

    # Rename IP data and primary dx column so concatenating is easier
    ip=ip.rename(columns={'ADMSN_DT':'TRNSFR_SRVC_BGN_DT','DSCHRG_DT':'TRNSFR_SRVC_END_DT','BENE_DSCHRG_STUS_CD':'TRNSFR_DSCHRG_STUS',
                          'PRVDR_NUM':'TRNSFR_PRVDR_NUM','ORG_NPI_NUM':'TRNSFR_ORG_NPI_NUM','ADMTG_DGNS_CD':'dx1_t'})

    # Rename remaining dx columns using loop
    for n in range(1,26):
        ip = ip.rename(columns={f'DGNS_{n}_CD': f'dx{n+1}_t'}) # Need the n+1 since the primary diagnosis code is dx1

    # Convert to Datetime
    ip['TRNSFR_SRVC_BGN_DT'] = dd.to_datetime(ip['TRNSFR_SRVC_BGN_DT'])
    ip['TRNSFR_SRVC_END_DT'] = dd.to_datetime(ip['TRNSFR_SRVC_END_DT'])

    # Merge data to keep only relevant bene ids
    rel_bene_ip = dd.merge(ip,processed_df,on=['BENE_ID'],how='inner')

    # Delete extra DFs
    del ip

    # Keep only if the raw file service date is at least the analytical sample's emergency response date because transfers to another hospital only happens after arriving at the first hospital destination
    rel_bene_ip = rel_bene_ip[rel_bene_ip['TRNSFR_SRVC_BGN_DT']>=rel_bene_ip['SRVC_BGN_DT']]

    # Keep those if provider numbers are different because we only want to look for transfer (a second stop) to another hospital
    rel_bene_ip = rel_bene_ip[rel_bene_ip['PRVDR_NUM']!=rel_bene_ip['TRNSFR_PRVDR_NUM']]

    # Keep those where patient did not have an expire indicator prior to transfer (i.e. remove any that died before getting transferred)
    rel_bene_ip = rel_bene_ip[~(rel_bene_ip['DSCHRG_CD'].isin(['B','20']))]

    # Create ip indicator. IP is a one
    rel_bene_ip['TRNSFR_IP_IND'] = 1

    #___ Read in raw OP and subset relevant bene_ids ___#

    # Read in OP claims
    op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/opb/{y}/parquet/',engine='fastparquet', columns=columns_op)
    op['CLM_FAC_TYPE_CD'] = op['CLM_FAC_TYPE_CD'].astype(str)  # ensure that dtype is consistent
    op = op[op['CLM_FAC_TYPE_CD'] == '1']  # keep only hospital op claims
    op = op.drop(['CLM_FAC_TYPE_CD'], axis=1)  # drop column

    # Rename OP data and primary dx column so concatenating is easier
    op = op.rename(columns={'CLM_FROM_DT':'TRNSFR_SRVC_BGN_DT','CLM_THRU_DT':'TRNSFR_SRVC_END_DT','PTNT_DSCHRG_STUS_CD':'TRNSFR_DSCHRG_STUS',
                            'PRVDR_NUM':'TRNSFR_PRVDR_NUM','ORG_NPI_NUM':'TRNSFR_ORG_NPI_NUM','PRNCPAL_DGNS_CD':'dx1_t'})

    # Rename remaining dx columns using loop.
    for n in range(1,26):
        op = op.rename(columns={f'ICD_DGNS_CD{n}': f'dx{n+1}_t'}) # Need the n+1 since the primary diagnosis code is dx1

    # Convert to Datetime
    op['TRNSFR_SRVC_BGN_DT'] = dd.to_datetime(op['TRNSFR_SRVC_BGN_DT'])
    op['TRNSFR_SRVC_END_DT'] = dd.to_datetime(op['TRNSFR_SRVC_END_DT'])

    # Merge data to keep on relevant bene ids
    rel_bene_op = dd.merge(op,processed_df,on=['BENE_ID'],how='inner')

    # Delete extra DFs
    del op

    # Keep only if the raw file service date is at least the analytical sample's emergency response date because transfers to another hospital only happens after arriving at the first hospital destination
    rel_bene_op = rel_bene_op[rel_bene_op['TRNSFR_SRVC_BGN_DT']>=rel_bene_op['SRVC_BGN_DT']]

    # Keep those if provider numbers are different because we only want to look for transfer (a second stop) to another hospital
    rel_bene_op = rel_bene_op[rel_bene_op['PRVDR_NUM']!=rel_bene_op['TRNSFR_PRVDR_NUM']]

    # Keep those where patient did not have an expire indicator prior to transfer (i.e. remove any that died before getting transferred)
    rel_bene_op = rel_bene_op[~(rel_bene_op['DSCHRG_CD'].isin(['B','20']))]

    # Create ip indicator. OP is a zero.
    rel_bene_op['TRNSFR_IP_IND'] = 0

    if y in [2012,2013,2014,2015,2016]:

        #___ Gather raw ip claims January of the following year ___#
        # this is necessary since those in December 31 can still be transferred in Jan 1st of the next year

        # Read in IP claims
        ip_following_year = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{y+1}/parquet/',
                             engine='fastparquet', index=False, columns=columns_ip)

        # Rename IP data and primary dx column so concatenating is easier
        ip_following_year=ip_following_year.rename(columns={'ADMSN_DT':'TRNSFR_SRVC_BGN_DT','DSCHRG_DT':'TRNSFR_SRVC_END_DT','BENE_DSCHRG_STUS_CD':'TRNSFR_DSCHRG_STUS',
                              'PRVDR_NUM':'TRNSFR_PRVDR_NUM','ORG_NPI_NUM':'TRNSFR_ORG_NPI_NUM','ADMTG_DGNS_CD':'dx1_t'})

        # Rename remaining dx columns using loop.
        for n in range(1,26):
            ip_following_year = ip_following_year.rename(columns={f'DGNS_{n}_CD': f'dx{n+1}_t'}) # Need the n+1 since the primary diagnosis code is dx1

        # Convert to Datetime
        ip_following_year['TRNSFR_SRVC_BGN_DT'] = dd.to_datetime(ip_following_year['TRNSFR_SRVC_BGN_DT'])
        ip_following_year['TRNSFR_SRVC_END_DT'] = dd.to_datetime(ip_following_year['TRNSFR_SRVC_END_DT'])

        # Keep if claims are in January of following year
        ip_following_year = ip_following_year[ip_following_year['TRNSFR_SRVC_BGN_DT'].dt.month == 1]

        # Merge data to keep on relevant subset of bene ids
        rel_bene_ip_fy = dd.merge(ip_following_year,processed_df,on=['BENE_ID'],how='inner')

        # Delete extra DFs
        del ip_following_year

        # Keep those where patient did not have an expire indicator prior to transfer
        rel_bene_ip_fy = rel_bene_ip_fy[(rel_bene_ip_fy['DSCHRG_CD'] != 'B')]
        rel_bene_ip_fy = rel_bene_ip_fy[(rel_bene_ip_fy['DSCHRG_CD'] != '20')]

        # Create ip indicator
        rel_bene_ip_fy['TRNSFR_IP_IND'] = 1

        #___ Gather raw op claims January of the following year ___#
        # this is necessary since those in December 31 can still be transferred in Jan 1st of the next year

        # Read in OP claims
        op_following_year = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/op_subset/{y+1}/parquet/',
                             engine='fastparquet', columns=columns_op)

        # Rename OP data and primary dx column so concatenating is easier
        op_following_year = op_following_year.rename(columns={'CLM_FROM_DT':'TRNSFR_SRVC_BGN_DT','CLM_THRU_DT':'TRNSFR_SRVC_END_DT','PTNT_DSCHRG_STUS_CD':'TRNSFR_DSCHRG_STUS',
                                'PRVDR_NUM':'TRNSFR_PRVDR_NUM','ORG_NPI_NUM':'TRNSFR_ORG_NPI_NUM','PRNCPAL_DGNS_CD':'dx1_t'})

        # Rename remaining dx columns using loop.
        for n in range(1,26):
            op_following_year = op_following_year.rename(columns={f'ICD_DGNS_CD{n}': f'dx{n+1}_t'}) # Need the n+1 since the primary diagnosis code is dx1

        # Convert to Datetime
        op_following_year['TRNSFR_SRVC_BGN_DT'] = dd.to_datetime(op_following_year['TRNSFR_SRVC_BGN_DT'])
        op_following_year['TRNSFR_SRVC_END_DT'] = dd.to_datetime(op_following_year['TRNSFR_SRVC_END_DT'])

        # Keep if claims are in January of following year
        op_following_year = op_following_year[op_following_year['TRNSFR_SRVC_BGN_DT'].dt.month == 1]

        # Merge data to keep on relevant bene ids
        rel_bene_op_fy = dd.merge(op_following_year,processed_df,on=['BENE_ID'],how='inner')

        # Delete extra DFs
        del op_following_year

        # Keep those where patient did not have an expire indicator prior to transfer
        rel_bene_op_fy = rel_bene_op_fy[(rel_bene_op_fy['DSCHRG_CD'] != 'B')]
        rel_bene_op_fy = rel_bene_op_fy[(rel_bene_op_fy['DSCHRG_CD'] != '20')]

        # Create ip indicator
        rel_bene_op_fy['TRNSFR_IP_IND'] = 0

    #___ Combine IP and OP claims first ___#

    if y in [2012,2013,2014,2015,2016]: # 2012-2016 has data in the following year

        # Concatenate IP and OP
        ip_op = dd.concat([rel_bene_ip,rel_bene_op,rel_bene_ip_fy,rel_bene_op_fy],axis=0) # _fy is following year

        # Delete extra DFs
        del rel_bene_ip
        del rel_bene_op
        del rel_bene_ip_fy
        del rel_bene_op_fy

    else: # i.e. 2017. 2017 does NOT have data in the following year

        # Concatenate IP and OP
        ip_op = dd.concat([rel_bene_ip,rel_bene_op],axis=0) # unlike 11-16, 2017 does NOT have data in the following year.

        # Delete extra DFs
        del rel_bene_ip
        del rel_bene_op

    # Drop duplicated claims on BENE_ID, SERVICE DATE, and PROVIDER ID
    ip_op = ip_op.drop_duplicates(subset=['BENE_ID','TRNSFR_SRVC_BGN_DT','TRNSFR_PRVDR_NUM'],keep='first') # added provider number to ensure it's the same hospital
        # Dropping duplicate with provider id will allow for one thing:
            # (1) CMS did not roll hospital claims correctly between ip and op (Keep first to retain IP claims instead of OP)

    # Drop date column from processed data
    ip_op = ip_op.drop(['SRVC_BGN_DT','PRVDR_NUM','DSCHRG_CD'],axis=1)

    #___ Export final raw file with dx info ___#

    # Export. This final raw file will be used to identify analytical samples that eventually have a transfer.
    ip_op.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/raw_hos_for_transfer/{y}/',engine='fastparquet',compression='gzip')


