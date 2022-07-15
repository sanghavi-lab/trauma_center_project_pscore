# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: After we created the new injury severity scores using ICDPICR, this script will merge the NISS
# information from ICDPICR with the analytical claims data, keep NISS from 16-75 (major trauma), and export the data.
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
import dask.dataframe as dd

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

################################### MERGE NISS INFORMATION BACK WITH CLAIMS DATA #######################################
# This section will only keep those with NISS from 16 to 75 (major trauma)                                             #
########################################################################################################################

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

# Define columns from icdpicr output
icdpicr_col = ['UNIQUE_ID', 'mxaisbr_HeadNeck', 'mxaisbr_Face', 'mxaisbr_Extremities', 'mxaisbr_Chest', 'mxaisbr_Abdomen',
               'maxais', 'riss', 'niss', 'ecode_1', 'mechmaj1', 'mechmin1', 'intent1', 'ecode_2', 'mechmaj2','mechmin2',
               'intent2', 'ecode_3', 'mechmaj3', 'mechmin3', 'intent3', 'ecode_4', 'mechmaj4', 'mechmin4', 'intent4']

# APPENDIX: Create empty list to store number of rows to calculate the number for the flowchart
num_rows_not_major_trauma = []

for y in years:

    # Read in file from ICDPICR containing NISS information
    if y in [*range(2011,2016)]:
        icdpicr_niss = dd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/niss_calculation/from_R/{y}_icdpic_r_output.csv',
                               dtype='str',usecols=icdpicr_col)
    else: # grab gem_min if 2016,2017
        icdpicr_niss = dd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/appendix/test_different_arguments_in_1_0_1_icdpicr/{y}_gem_min.csv',
                                   dtype='str',usecols=icdpicr_col)


    # Convert to variables to float
    float_col = ['mxaisbr_HeadNeck', 'mxaisbr_Face', 'mxaisbr_Extremities', 'mxaisbr_Chest','mxaisbr_Abdomen', 'maxais',
                 'riss', 'niss']
    for i in float_col:
        icdpicr_niss[f'{i}'] = icdpicr_niss[f'{i}'].astype(float)

    # Read in analytical sample
    df_claims = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}',engine='fastparquet')

    # Merge so that the niss information is connected to the original claim data
    df_merge = dd.merge(df_claims,icdpicr_niss,how='inner',on=['UNIQUE_ID'])

    # CHECK to make sure each df denominator is the same
    print(df_claims.shape[0].compute()) # number will be higher due to duplicated unique id. will drop in code 11
    print(icdpicr_niss.shape[0].compute())
    print(df_merge.shape[0].compute()) # number should be the same as the first one

    # Recover memory
    del df_claims
    del icdpicr_niss

    # APPENDIX: Calculate the number of those who are NOT major trauma
    print(f'Currently at {y}')
    num_rows_not_major_trauma.append(df_merge[df_merge['niss']<=15].shape[0].compute())

    # Keep only major trauma
    df_merge = df_merge[df_merge['niss']>15]

    # Read out
    df_merge.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}_major_trauma',engine='fastparquet',compression='gzip')

# APPENDIX: Print total number of claims that are not major trauma
print('Not major trauma: ',sum(num_rows_not_major_trauma))


