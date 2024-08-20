#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will clean and concatenate ip and op analytical claim files. Additionally, we will create a
#              trauma indicator for a secondary column (specifically the second secondary column which we labeled as
#              dx4 because dx1 is admitting, dx2 is primary, dx3 is the first secondary column, and so on...). The
#              reason is that we may want to drop these individuals since they may or may not be admitted for a trauma
#              related incident. Lastly, I will create an indicator for the dually eligible beneficiaries.
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

######################################### CLEAN AND CONCAT IP AND OP ###################################################

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

for y in years:

    # Read in both data from the same year
    ip = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/first_stops/ip/{y}',engine='fastparquet')
    op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/first_stops/opb/{y}',engine='fastparquet')

    # Rename columns
    ip = ip.rename(columns={'MEDPAR_ID':'UNIQUE_ID','ADMSN_DT':'SRVC_BGN_DT','DSCHRG_DT':'SRVC_END_DT','BENE_DSCHRG_STUS_CD':'DSCHRG_CD'})
    op = op.rename(columns={'CLM_ID':'UNIQUE_ID','CLM_FROM_DT':'SRVC_BGN_DT','CLM_THRU_DT':'SRVC_END_DT','PTNT_DSCHRG_STUS_CD':'DSCHRG_CD','NCH_BLOOD_PNTS_FRNSHD_QTY':'BLOOD_PT_FRNSH_QTY'})

    # Concat both ip and op together
    ip_op = dd.concat([ip,op],axis=0)

    # Recover memory
    del ip
    del op

    # Convert to datetime to fix encoding error when reading out to parquet
    ip_op['SRVC_BGN_DT'] = dd.to_datetime(ip_op['SRVC_BGN_DT'])
    ip_op['SRVC_END_DT'] = dd.to_datetime(ip_op['SRVC_END_DT'])

    # # CHECK: make sure number of rows each year is similar
    # print(ip_op.shape[0].compute())

    # Create an indicator if the second secondary column is a trauma code. See description above on reason why we created this indicator.
    ip_op['sec_secondary_trauma_ind'] = 0
    if y in [*range(2011,2016)]: # ICD9
        ip_op['sec_secondary_trauma_ind'] = ip_op['sec_secondary_trauma_ind'].mask((ip_op[['dx4']].applymap(lambda x: x.startswith(tuple(['8','9']))).any(axis='columns')),1)
        ip_op['sec_secondary_trauma_ind'] = ip_op['sec_secondary_trauma_ind'].mask((ip_op[['dx1','dx2','dx3']].applymap(lambda x: x.startswith(tuple(['8','9']))).any(axis='columns')),0) # since the mask function cannot use the invert operator ~, I will need to repeat the mask function again and remove any 1's if there is an injury code in the admitting, primary, or first secondary column
    if y in [*range(2016,2020)]: # ICD10
        ip_op['sec_secondary_trauma_ind'] = ip_op['sec_secondary_trauma_ind'].mask((ip_op[['dx4']].applymap(lambda x: x.startswith(tuple(['S','T']))).any(axis='columns')),1)
        ip_op['sec_secondary_trauma_ind'] = ip_op['sec_secondary_trauma_ind'].mask((ip_op[['dx1','dx2','dx3']].applymap(lambda x: x.startswith(tuple(['S','T']))).any(axis='columns')),0) # since the mask function cannot use the invert operator ~, I will need to repeat the mask function again and remove any 1's if there is an injury code in the admitting, primary, or first secondary column

    # # Check proportion of sample with this indicator (~5% for each year but number changed to ~2% and ~4% after dropping major trauma)
    # print(ip_op['sec_secondary_trauma_ind'].sum().compute()/ip_op.shape[0].compute())

    #___ Create an indicator for dually eligible beneficiaries ___#

    # Identify all columns needed
    columns_MBSF = ['BENE_ID'] + [f'DUAL_STUS_CD_{i:02}' for i in range(1,13)]

    # Read in MBSF (same year)
    df_MBSF = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFABCD/csv/mbsf_abcd_summary.csv',sep=',', engine='c',dtype='object', na_filter=False,skipinitialspace=True, low_memory=False,usecols=columns_MBSF)

    # Merge Personal Summary with Carrier file
    ip_op = dd.merge(ip_op,df_MBSF,on=['BENE_ID'],how='left')

    # Recover memory. (i.e. take up less RAM space when running the script)
    del df_MBSF

    # Create indicator for full duals
    ip_op['full_dual_ind'] = 0
    ip_op['full_dual_ind'] = ip_op['full_dual_ind'].mask(((ip_op['SRVC_BGN_DT'].dt.month==1) & (ip_op['DUAL_STUS_CD_01'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==2) & (ip_op['DUAL_STUS_CD_02'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==3) & (ip_op['DUAL_STUS_CD_03'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==4) & (ip_op['DUAL_STUS_CD_04'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==5) & (ip_op['DUAL_STUS_CD_05'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==6) & (ip_op['DUAL_STUS_CD_06'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==7) & (ip_op['DUAL_STUS_CD_07'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==8) & (ip_op['DUAL_STUS_CD_08'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==9) & (ip_op['DUAL_STUS_CD_09'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==10) & (ip_op['DUAL_STUS_CD_10'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==11) & (ip_op['DUAL_STUS_CD_11'].isin(['02', '04', '08']))) |
                                                         ((ip_op['SRVC_BGN_DT'].dt.month==12) & (ip_op['DUAL_STUS_CD_12'].isin(['02', '04', '08']))),1)

    # Create indicator for partial duals
    ip_op['partial_dual_ind'] = 0
    ip_op['partial_dual_ind'] = ip_op['partial_dual_ind'].mask(((ip_op['SRVC_BGN_DT'].dt.month==1) & (ip_op['DUAL_STUS_CD_01'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==2) & (ip_op['DUAL_STUS_CD_02'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==3) & (ip_op['DUAL_STUS_CD_03'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==4) & (ip_op['DUAL_STUS_CD_04'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==5) & (ip_op['DUAL_STUS_CD_05'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==6) & (ip_op['DUAL_STUS_CD_06'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==7) & (ip_op['DUAL_STUS_CD_07'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==8) & (ip_op['DUAL_STUS_CD_08'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==9) & (ip_op['DUAL_STUS_CD_09'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==10) & (ip_op['DUAL_STUS_CD_10'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==11) & (ip_op['DUAL_STUS_CD_11'].isin(['01', '03', '05', '06']))) |
                                                                 ((ip_op['SRVC_BGN_DT'].dt.month==12) & (ip_op['DUAL_STUS_CD_12'].isin(['01', '03', '05', '06']))),1)

    # Check proportion of duals
    denom = ip_op.shape[0].compute()
    full = ip_op['full_dual_ind'].sum().compute()
    partial = ip_op['partial_dual_ind'].sum().compute()
    print('full proportion: ',full/denom)
    print('partial proportion: ',partial/denom)
    print('both proportion: ',(partial+full)/denom)

    # Clean DF by dropping columns
    ip_op = ip_op.drop([f'DUAL_STUS_CD_{i:02}' for i in range(1,13)],axis=1)

    # Read out data
    ip_op.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}/',engine='fastparquet',compression='gzip')


