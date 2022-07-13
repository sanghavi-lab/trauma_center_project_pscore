#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will identify any claims that are not transferred claims ("first stop"). The goal of this study
# is to answer the research question of which first destination hospital is the BEST for patients with injuries. This script
# will drop any claims that are a result of a transfer from another hospital.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
from datetime import timedelta

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

############################################### IDENTIFY FIRST STOPS ###################################################

# Specify IP or OP
claim_type = ['ip','opb'] # opb is outpatient base file (not revenue file)

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

for c in claim_type:

    for y in years:

        # Note the assumed definition of a transfer is that the service date within one day of another hospital's discharge date.

        # Read in raw IP. Raw files are needed to help identify which claim is within one day to be considered a transfer.
        raw_ip = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{y}/parquet/', engine='fastparquet', columns=['BENE_ID','ADMSN_DT','DSCHRG_DT','PRVDR_NUM','ORG_NPI_NUM'])

        # Read in raw OP. Raw files are needed to help identify which claim is within one day to be considered a transfer.
        raw_op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/opb/{y}/parquet', engine='fastparquet', columns=['BENE_ID','CLM_FROM_DT','CLM_THRU_DT','PRVDR_NUM','ORG_NPI_NUM','CLM_FAC_TYPE_CD'])
        raw_op['CLM_FAC_TYPE_CD'] = raw_op['CLM_FAC_TYPE_CD'].astype(str)  # ensure that dtype is consistent
        raw_op = raw_op[raw_op['CLM_FAC_TYPE_CD'] == '1'] # keep only hospital op claims
        raw_op = raw_op.drop(['CLM_FAC_TYPE_CD'],axis=1) # drop column

        # Change column name
        raw_ip = raw_ip.rename(columns={'DSCHRG_DT':'END_DT'})
        raw_op = raw_op.rename(columns={'CLM_THRU_DT':'END_DT'})
        raw_ip = raw_ip.rename(columns={'ADMSN_DT':'START_DT'})
        raw_op = raw_op.rename(columns={'CLM_FROM_DT':'START_DT'})

        # Convert to datetime
        raw_ip['END_DT'] = dd.to_datetime(raw_ip['END_DT'])
        raw_op['END_DT'] = dd.to_datetime(raw_op['END_DT'])
        raw_ip['START_DT'] = dd.to_datetime(raw_ip['START_DT'])
        raw_op['START_DT'] = dd.to_datetime(raw_op['START_DT'])

        # Add one day. Will be used to determine if the claims from the analytical file is within this one day. If it is, then it's a transfer and NOT a first destination.
        raw_ip['END_DT_PLUSONE'] = raw_ip['END_DT'].map_partitions(lambda x: x + timedelta(days=1))
        raw_op['END_DT_PLUSONE'] = raw_op['END_DT'].map_partitions(lambda x: x + timedelta(days=1))

        # Concat
        raw_ip_op = dd.concat([raw_ip,raw_op],axis=0)

        # Recover memory
        del raw_ip
        del raw_op

        if c in ['ip']:

            # Read in ip data (analytical sample)
            df_hos_sample = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/{c}/{y}_dup_dropped',engine='fastparquet',
                                           columns=['MEDPAR_ID','BENE_ID','ADMSN_DT','PRVDR_NUM','ORG_NPI_NUM'])

            # Convert to datetime
            df_hos_sample['ADMSN_DT'] = dd.to_datetime(df_hos_sample['ADMSN_DT'])

            # Merge with raw ip and op
            df_hos_sample_merge = dd.merge(raw_ip_op,df_hos_sample,how='right',on=['BENE_ID'],suffixes=['_RAW','_ANALYTICAL'])

            # Recover memory
            del df_hos_sample
            del raw_ip_op

            # Keep "transfer" claims from analytical sample if the analytical sample has a different provider id (and different npi number) and the analytical claim's begin date is within one day of the raw data's end date
            # I am "keeping" to identify the transfers. Later in this code, I will use this dataframe to drop all claims that are "transfers"
            df_hos_sample_merge = df_hos_sample_merge[(df_hos_sample_merge['PRVDR_NUM_RAW']!=df_hos_sample_merge['PRVDR_NUM_ANALYTICAL']) &
                                                      (df_hos_sample_merge['ORG_NPI_NUM_RAW']!=df_hos_sample_merge['ORG_NPI_NUM_ANALYTICAL']) &
                                                      (df_hos_sample_merge['ADMSN_DT']<=df_hos_sample_merge['END_DT_PLUSONE']) & # w/in one day
                                                      (df_hos_sample_merge['ADMSN_DT']>=df_hos_sample_merge['END_DT'])] # at least the analytical file's end date
                # Reiterate: This assumes that the claims from the analytical sample is a "transfer" claim if it matches with the raw file, is within one day of the raw file end date, and has a different provider id from the raw file.

            # Create indicator. This will be used to identify transfers and drop them.
            df_hos_sample_merge['match_ind'] = 1

            # Clean columns
            df_hos_sample_merge = df_hos_sample_merge.drop(['BENE_ID','START_DT', 'END_DT', 'PRVDR_NUM_RAW', 'ORG_NPI_NUM_RAW',
                                                            'END_DT_PLUSONE','ADMSN_DT', 'PRVDR_NUM_ANALYTICAL', 'ORG_NPI_NUM_ANALYTICAL'],axis=1)

            # Read in original ip analytical sample with all columns
            ip_analytical_sample = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/{c}/{y}',
                engine='fastparquet')

            # Match original ip analytical sample with the DF that identified "transfer" claims on MEDPAR ID. Any that matched will be dropped since those are "transfer" claims.
            ip_merged = dd.merge(df_hos_sample_merge,ip_analytical_sample,how='right',on=['MEDPAR_ID'])

            # Recover memory
            del df_hos_sample_merge
            del ip_analytical_sample

            # CHECK: Make sure that I did not drop any claims that was a result of emergency ambulance ride)
            print(ip_merged['amb_ind'].sum().compute())
            denom=ip_merged.shape[0].compute()

            # Keep those that did not match (i.e. the "first stops") using the created match_ind column or if the claim matched with an amb claim (i.e. also keep claims if they were a result of emergency ambulance ride)
            ip_merged = ip_merged[(ip_merged['match_ind'].isna())|(ip_merged['amb_ind']==1)]

            # CHECK: Make sure that I did not drop any claims that was a result of emergency ambulance ride)
            print(ip_merged['amb_ind'].sum().compute())
            print(f'{c} ',ip_merged.shape[0].compute()/denom)

            # Clean columns
            ip_merged = ip_merged.drop(['match_ind'],axis=1)

            # Export only first destinations.
            ip_merged.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/first_stops/{c}/{y}',engine='fastparquet',compression='gzip')

        if c in ['opb']:

            # Read in op data (analytical sample)
            df_hos_sample = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/{c}/{y}_dup_dropped',engine='fastparquet',
                                           columns=['CLM_ID','BENE_ID','CLM_FROM_DT','PRVDR_NUM','ORG_NPI_NUM'])

            # Convert to datetime
            df_hos_sample['CLM_FROM_DT'] = dd.to_datetime(df_hos_sample['CLM_FROM_DT'])

            # Merge with raw ip and op
            df_hos_sample_merge = dd.merge(raw_ip_op,df_hos_sample,how='right',on=['BENE_ID'],suffixes=['_RAW','_ANALYTICAL'])

            # Recover memory
            del df_hos_sample
            del raw_ip_op

            # Keep "transfer" claims from analytical sample if the analytical sample has a different provider id (and different npi number) and the analytical claim's begin date is within one day of the raw data's end date
            # I am "keeping" to identify the transfers. Later in this code, I will use this dataframe to drop all claims that are "transfers"
            df_hos_sample_merge = df_hos_sample_merge[(df_hos_sample_merge['PRVDR_NUM_RAW']!=df_hos_sample_merge['PRVDR_NUM_ANALYTICAL']) &
                                                      (df_hos_sample_merge['ORG_NPI_NUM_RAW']!=df_hos_sample_merge['ORG_NPI_NUM_ANALYTICAL']) &
                                                      (df_hos_sample_merge['CLM_FROM_DT']<=df_hos_sample_merge['END_DT_PLUSONE']) & # w/in one day
                                                      (df_hos_sample_merge['CLM_FROM_DT']>=df_hos_sample_merge['END_DT'])] # at least the analytical file's end date
                # Reiterate: This assumes that the claims from the analytical sample is a "transfer" claim if it matches with the raw file, is within one day of the raw file end date, and has a different provider id from the raw file.

            # Create indicator. This will be used to identify transfers and drop them.
            df_hos_sample_merge['match_ind'] = 1

            # Clean columns
            df_hos_sample_merge = df_hos_sample_merge.drop(['BENE_ID','START_DT', 'END_DT', 'PRVDR_NUM_RAW', 'ORG_NPI_NUM_RAW',
                                                            'END_DT_PLUSONE','CLM_FROM_DT', 'PRVDR_NUM_ANALYTICAL', 'ORG_NPI_NUM_ANALYTICAL'],axis=1)

            # Read in original op analytical sample with all columns
            op_analytical_sample = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/{c}/{y}',
                engine='fastparquet')

            # Match original op analytical sample with the DF that identified "transfer" claims on CLM ID. Any that matched will be dropped since those are "transfer" claims.
            op_merged = dd.merge(df_hos_sample_merge,op_analytical_sample,how='right',on=['CLM_ID'])

            # Recover memory
            del df_hos_sample_merge
            del op_analytical_sample

            # CHECK: Make sure that I did not drop any claims that was a result of emergency ambulance ride)
            print(op_merged['amb_ind'].sum().compute())
            denom=op_merged.shape[0].compute()

            # Keep those that did not match (i.e. the "first stops") using the created match_ind column or if the claim matched with an amb claim (i.e. also keep claims if they were a result of emergency ambulance ride)
            op_merged = op_merged[(op_merged['match_ind'].isna())|(op_merged['amb_ind']==1)]

            # CHECK: Make sure that I did not drop any claims that was a result of emergency ambulance ride)
            print(op_merged['amb_ind'].sum().compute())
            print(f'{c} ',op_merged.shape[0].compute()/denom)

            # Clean columns
            op_merged = op_merged.drop(['match_ind'],axis=1)

            # Export only first destinations.
            op_merged.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/first_stops/{c}/{y}',engine='fastparquet',compression='gzip')


















