#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will remove any "duplicated" outpatient or inpatient claims. There is a distinction between
# duplicates and claims within one stay. Even though this script will drop duplicates (i.e. same bene and service date),
# there may still be claims, especially in outpatient, that are contiguous or overlapping with different service dates).
# Thus, I will take care of these multiple claims within a stay in a later code (end of code 13_obtain_comorbid_and_other_tasks.py)
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
from datetime import timedelta
import numpy as np

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

################################### DROP OP/IP CLAIMS THAT ARE DUPLICATES ##############################################

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

for y in years:

    #___ Drop duplicates for ip ___#

    # Read in ip data
    ip = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/ip/{y}/',engine='fastparquet')

    # CHECK the beginning numbers. This is to ultimately ensure that I did NOT drop claims with an amb_ind == 1 (meaning that I want to make sure, by the end of the code, I keep any claim that was a result of an ambulance ride)
    print('(IP) Number of unique claims where amb_ind == 1: ',ip[ip['amb_ind']==1]['MEDPAR_ID'].nunique().compute())
    print('(IP) Sanity Check. Number should be slightly higher due to duplicates: ',ip['amb_ind'].value_counts().compute())

    # Count the number of diagnosis codes for ip
    diag_col = [f'dx{i}' for i in range(1, 39)]  # Define diagnosis columns
    ip[diag_col] = ip[diag_col].replace('',np.nan)  # Replace empty strings to count number of diagnosis codes
    ip['num_of_diag_codes'] = ip[diag_col].count( axis='columns')  # Count diagnosis codes (same as doing .count(1))
    ip[diag_col] = ip[diag_col].fillna('')  # Fill nan's with empty strings

    # Convert to datetime
    ip['ADMSN_DT'] = dd.to_datetime(ip['ADMSN_DT'])

    # Set index. Setting index will "sort" the claims by amb_ind. 1's at the top and nan's at the bottom since amb_ind consists of 1's and nan's (not 1's and 0's)
    ip = ip.set_index(['amb_ind'])

    # Drop any that are duplicates and keep first. This will allow me to keep claims with ambulance ride if there are duplicates since I "sorted" the df after setting the index.
    ip = ip.drop_duplicates(subset=['MEDPAR_ID','ADMSN_DT'],keep='first')

    # Reset index
    ip = ip.reset_index()

    # Check to make sure none of the claims that matched with amb were dropped. Should be similar to the number of unique claims above
    print('ip',ip[ip['amb_ind']==1].shape[0].compute())

    # Set index to bene this time to sort in each partition
    ip = ip.set_index(['BENE_ID'])

    # Fill in na's with 0 for amb_ind column
    ip['amb_ind'] = ip['amb_ind'].fillna(0)

    # Sort in each partition (amb_ind = 1 and the highest number of diag codes both should be at the top within each partition)
    ip = ip.map_partitions(lambda x: x.sort_values(by=['amb_ind','num_of_diag_codes'],ascending=[False,False]))

    # Reset index
    ip = ip.reset_index()

    # Drop duplicates and keep claims with ambulance ride and/or the one with the most num of diag codes (keep first) after sorting within the partition
    ip = ip.drop_duplicates(subset=['BENE_ID','ADMSN_DT'],keep='first')

    # Check to make sure none of the claims that matched with amb were dropped. Should be similar to the number of unique claims above
    print('ip',ip[ip['amb_ind']==1].shape[0].compute())

    # Clean DF
    ip = ip.drop(['num_of_diag_codes'],axis=1)

    # Export
    ip.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/ip/{y}_dup_dropped/',engine='fastparquet',compression='gzip')

    # Recover memory
    del ip

    #___ Drop duplicates for op ___#

    # Read in op data
    op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/opb/{y}/',engine='fastparquet')

    # CHECK the beginning numbers. This is to ultimately ensure that I did NOT drop claims with an amb_ind == 1 (meaning that I want to make sure, by the end of the code, I keep any claim that was a result of an ambulance ride)
    print('(OP) Number of unique claims where amb_ind == 1: ',op[op['amb_ind']==1]['CLM_ID'].nunique().compute())
    print('(OP) Sanity Check. Number should be slightly higher due to duplicates: ',op['amb_ind'].value_counts().compute())

    # Count the number of diagnosis codes for op
    diag_col = [f'dx{i}' for i in range(1, 39)]  # Define diagnosis columns
    op[diag_col] = op[diag_col].replace('',np.nan)  # Replace empty strings to count number of diagnosis codes
    op['num_of_diag_codes'] = op[diag_col].count( axis='columns')  # Count diagnosis codes (same as doing .count(1))
    op[diag_col] = op[diag_col].fillna('')  # Fill nan's with empty strings

    # Convert to datetime
    op['CLM_FROM_DT'] = dd.to_datetime(op['CLM_FROM_DT'])

    # Set index. Setting index will "sort" the claims by amb_ind. 1's at the top and nan's at the bottom since amb_ind consists of 1's and nan's (not 1's and 0's)
    op = op.set_index(['amb_ind'])

    # Drop any that are duplicates and keep first. This will allow me to keep claims with ambulance ride if there are duplicates since I "sorted" the df after setting the index.
    op = op.drop_duplicates(subset=['CLM_ID','CLM_FROM_DT'],keep='first')

    # Reset index
    op = op.reset_index()

    # Check to make sure none of the claims that matched with amb were dropped. Should be similar to the number of unique claims above
    print('op',op[op['amb_ind']==1].shape[0].compute())

    # Set index to bene this time to sort in each partition
    op = op.set_index(['BENE_ID'])

    # Fill in na's with 0 for amb_ind column
    op['amb_ind'] = op['amb_ind'].fillna(0)

    # Sort in each partition (amb_ind = 1 and the highest number of diag codes both should be at the top within each partition)
    op = op.map_partitions(lambda x: x.sort_values(by=['amb_ind','num_of_diag_codes'],ascending=[False,False]))

    # Reset index
    op = op.reset_index()

    # Drop duplicates and keep claims with ambulance ride and/or the one with the most num of diag codes (keep first) after sorting within the partition
    op = op.drop_duplicates(subset=['BENE_ID','CLM_FROM_DT'],keep='first')

    # Check to make sure none of the claims that matched with amb were dropped. Should be similar to the number of unique claims above
    print('op',op[op['amb_ind']==1].shape[0].compute())

    # Clean DF
    op = op.drop(['num_of_diag_codes'],axis=1)

    # Export
    op.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/opb/{y}_dup_dropped/',engine='fastparquet',compression='gzip')

    # Recover memory
    del op

# NOTE TO SELF ABOUT TRANSFERS:
    # My only concern with this is that the clm thru date may not be the last day of service so identifying transfer may
    # be difficult. However, I don't see in the data that it's the case. Also, I tried to mitigate that by getting the
    # claim with the most days of stay in OP. Also less than 10% were claims that were "overlapped" so I'm not worried about this issue.


