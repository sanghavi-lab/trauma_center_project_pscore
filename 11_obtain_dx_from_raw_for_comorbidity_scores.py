#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script gathers diagnosis columns from the raw IP, OP, and ambulance claims and exports files to parquet
# format. This diagnosis information will be merged back with the analytical file in another script and used when calculating
# comorbidity scores.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
import numpy as np

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

####################### GRAB HOSPITAL AND AMBULANCE DX CODES FOR COMORBIDITY CALCULATIONS ##############################

# Define years
years = [2011,2012,2013,2014,2015,2016,2017]

# Specify columns with dx columns
columns_ip = ['BENE_ID', 'ADMSN_DT', 'PRVDR_NUM', 'ADMTG_DGNS_CD'] + [f'DGNS_{i}_CD' for i in range(1, 26)]             # for IP
columns_op = ['BENE_ID', 'CLM_FROM_DT', 'PRVDR_NUM', 'PRNCPAL_DGNS_CD'] + [f'ICD_DGNS_CD{i}' for i in range(1, 26)]     # for OP
columns_amb = ['BENE_ID', 'CLM_FROM_DT', 'PRNCPAL_DGNS_CD'] + [f'ICD_DGNS_CD{i}' for i in range(1, 13)]                 # for ambulance

for y in years:

    #___ Read in analytical sample to subset relevant bene ids ___#
    # Because IP/OP/Amb raw files are large, I only want a subset of raw files that are relevant with my analytical files.

    # Define Columns
    columns_processed = ['BENE_ID','SRVC_BGN_DT']

    # Read in final analytical data. Since we do not have 2010 data, we can only gather full info for 2012-2017.
    if y in [*range(2011,2017)]: # 2011-2016 since we have the following year
        current_year = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}_major_trauma',
                                   engine='fastparquet',columns=columns_processed) # e.g. for 2013 loop, this will gather everyone from 2013
        next_year = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y+1}_major_trauma',
                                   engine='fastparquet',columns=columns_processed) # e.g. for 2013 loop, this will gather everyone from 2014 to account for the year prior when the 2013 raw is needed for 2014.
        processed_df = dd.concat([current_year,next_year],axis=0)
        del current_year # Recover memory
        del next_year # Recover memory
            # Since comorbidity scores require the current year and one year PRIOR, I need to gather all dx info for everyone in the current year and year prior.
            # E.g. say we want 2012 raw data. We need the 2012 data and all 2011 data. If the currently loop is on 2011, the "next_year" df will account for
            # everyone from 2012 who also have info in 2011. That way, we have FULL information of all dx codes for 2012 from the current year and year prior (2011) when
            # calculating the comorbidity scores.
    else: # 2017 since we don't have 2018 data, but 2017 year prior (which is 2016) will be accounted for in the above code)
        processed_df = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}_major_trauma',
                                   engine='fastparquet',columns=columns_processed)

    # Convert to Datetime
    processed_df['SRVC_BGN_DT'] = dd.to_datetime(processed_df['SRVC_BGN_DT'])

    # Rename column
    processed_df=processed_df.rename(columns={'SRVC_BGN_DT':'SRVC_BGN_DT_HOS'})

    #___ Read in raw IP and subset relevant bene_ids ___#

    # Read in IP claims (planning to use MEDPAR to obtain info from both IP and SNF
    ip = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MEDPAR/csv/medpar_{y}.csv',usecols=columns_ip,sep=',', engine='c',
                                dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)

    # Rename IP data and primary dx column so concatenating is easier
    ip=ip.rename(columns={'ADMSN_DT':'SRVC_BGN_DT','ADMTG_DGNS_CD':'dx1'})

    # Rename remaining dx columns using loop.
    for n in range(1,26):
        ip = ip.rename(columns={f'DGNS_{n}_CD': f'dx{n+1}'}) # Need the n+1 since the primary diagnosis code is dx1

    # Convert to Datetime
    ip['SRVC_BGN_DT'] = dd.to_datetime(ip['SRVC_BGN_DT'])

    # Merge data to keep on relevant subset of bene ids
    rel_bene_ip = dd.merge(ip,processed_df,on=['BENE_ID'],how='inner')

    # Delete extra DFs
    del ip

    # Keep only if the raw file service data is on or before the processed service date. This is because we need diagnosis codes that are one year before (not after) the injury event.
    rel_bene_ip = rel_bene_ip[rel_bene_ip['SRVC_BGN_DT']<=rel_bene_ip['SRVC_BGN_DT_HOS']]

    #___ Read in raw OP and subset relevant bene_ids ___#

    # Read in OP claims
    op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/opb/{y}/parquet/',
                         engine='fastparquet', columns=columns_op)

    # Rename OP data and primary dx column so concatenating is easier
    op = op.rename(columns={'CLM_FROM_DT': 'SRVC_BGN_DT','PRNCPAL_DGNS_CD':'dx1'})

    # Rename remaining dx columns using loop.
    for n in range(1,26):
        op = op.rename(columns={f'ICD_DGNS_CD{n}': f'dx{n+1}'}) # Need the n+1 since the primary diagnosis code is dx1

    # Convert to Datetime
    op['SRVC_BGN_DT'] = dd.to_datetime(op['SRVC_BGN_DT'])

    # Merge data to keep on relevant subset of bene ids
    rel_bene_op = dd.merge(op,processed_df,on=['BENE_ID'],how='inner')

    # Delete extra DFs
    del op

    # Keep only if the raw file service data is on or before the processed service date. This is because we need diagnosis codes that are one year before (not after) the injury event.
    rel_bene_op = rel_bene_op[rel_bene_op['SRVC_BGN_DT']<=rel_bene_op['SRVC_BGN_DT_HOS']]

    #___ Combine IP and OP claims first ___#
    # We concatenate the ip/op claims first to drop duplicates within institutional claims

    # Concatenate IP and OP
    ip_op = dd.concat([rel_bene_ip,rel_bene_op],axis=0)

    # Delete extra DFs
    del rel_bene_ip
    del rel_bene_op

    # Drop duplicated claims on BENE_ID, SERVICE DATE, and PROVIDER ID (PRVDR_NUM was specified to make sure the claims were from the same hospital when dropping duplicates)
    ip_op = ip_op.shuffle(on=['BENE_ID','SRVC_BGN_DT','PRVDR_NUM']).map_partitions(lambda x: x.drop_duplicates(subset=['BENE_ID','SRVC_BGN_DT','PRVDR_NUM'],keep='first'))
        # Sometimes, individuals may have both and IP and OP claim from one hospital. This is an error on CMS side. Thus, dropping duplicates and keeping first will allow me to prioritize keeping inpatient claims (not outpatient) if CMS did make this error.

    # Drop provider id
    ip_op = ip_op.drop(['PRVDR_NUM'],axis=1)

    #___ Read in raw ambulance claims and subset relevant bene_ids ___#

    # Read in Ambulance Claims
    amb = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/emergency_ambulance_claims/{y}/parquet/',
                          engine='fastparquet', columns=columns_amb)

    # Rename data and primary dx column so concatenating is easier
    amb=amb.rename(columns={'CLM_FROM_DT':'SRVC_BGN_DT','PRNCPAL_DGNS_CD':'dx1'})

    # Rename remaining dx columns using loop.
    for n in range(1,13):
        amb = amb.rename(columns={f'ICD_DGNS_CD{n}': f'dx{n+1}'}) # Need the n+1 since the primary diagnosis code is dx1

    # Convert to Datetime
    amb['SRVC_BGN_DT'] = dd.to_datetime(amb['SRVC_BGN_DT'])

    # Merge data to keep on relevant bene ids
    rel_bene_amb = dd.merge(amb,processed_df,on=['BENE_ID'],how='inner')

    # Delete extra DFs
    del amb
    del processed_df

    # Keep only if the raw file service data is on or before the processed service date. This is because we need diagnosis codes that are one year before (not after) the injury event.
    rel_bene_amb = rel_bene_amb[rel_bene_amb['SRVC_BGN_DT']<=rel_bene_amb['SRVC_BGN_DT_HOS']]

    #___ Combine with amb claims ___#

    # Finally, concat ip_op with amb dx columns. Do NOT drop duplicates. Otherwise, I will drop amb or hos dx information that is needed within the same service date.
    ip_op_amb = dd.concat([ip_op,rel_bene_amb],axis=0)

    # Delete extra DFs
    del ip_op
    del rel_bene_amb

    # Drop date column from processed data
    ip_op_amb = ip_op_amb.drop(['SRVC_BGN_DT_HOS'],axis=1)

    #___ Export final raw file with dx info ___#

    # Export all raw dx information. Will be used in another code to calculate the comorbidity scores.
    ip_op_amb.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/raw_dx_for_comorbidity/{y}/',engine='fastparquet',compression='gzip')

    # Recover memory
    del ip_op_amb



