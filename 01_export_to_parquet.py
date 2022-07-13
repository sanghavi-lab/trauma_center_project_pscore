#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script exports files to parquet format for inpatient claims, emergency ambulance rides, outpatient
# claims, and chronic conditions (i.e. converts csv files to parquet format). Mileage information will be obtained when
# ambulance claims merge with mileage.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import dask.dataframe as dd
import numpy as np

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

###################################### CREATE INPATIENT DATA FROM MEDICARE #############################################
# MedPAR has both skilled nursing facilities and IP claims. This script keeps only IP claims.                          #
########################################################################################################################

# Specify years to output files
years=[2011,2012,2013,2014,2015,2016,2017]

# Loop function for each year
for y in years:

    # Define columns from MedPAR
    medpar_columns = ['BENE_ID','ADMSN_DT', 'BENE_AGE_CNT', 'BENE_SEX_CD','BENE_RSDNC_SSA_STATE_CD','MEDPAR_ID',
                      'BENE_RSDNC_SSA_CNTY_CD','BENE_MLG_CNTCT_ZIP_CD','PRVDR_NUM','ER_CHRG_AMT','DSCHRG_DT',
                      'BENE_PRMRY_PYR_AMT', 'ORG_NPI_NUM', 'IP_ADMSN_TYPE_CD', 'BENE_DSCHRG_STUS_CD','ICU_IND_CD',
                      'BENE_IP_DDCTBL_AMT', 'BENE_PTA_COINSRNC_AMT', 'BLOOD_PT_FRNSH_QTY', 'DRG_CD', 'DRG_OUTLIER_STAY_CD',
                      'SRC_IP_ADMSN_CD', 'UTLZTN_DAY_CNT', 'BENE_BLOOD_DDCTBL_AMT', 'ADMTG_DGNS_CD','SS_LS_SNF_IND_CD','GHO_PD_CD',
                      'SRGCL_PRCDR_IND_SW','SRGCL_PRCDR_CD_CNT','SRGCL_PRCDR_DT_CNT'] + \
                     ['DGNS_{}_CD'.format(i) for i in range(1, 26)] + \
                     ['POA_DGNS_{}_IND_CD'.format(j) for j in range(1, 26)] + \
                     ['DGNS_E_{}_CD'.format(k) for k in range(1, 13)] + ['POA_DGNS_E_{}_IND_CD'.format(l) for l in range(1, 13)]

    if y in [*range(2011, 2017, 1)]: # list from 2011-2016

        # Read in data from MedPAR (11-16)
        medpar_df = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MEDPAR/csv/medpar_{y}.csv',usecols=medpar_columns,sep=',', engine='c',
                                dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)

    elif y in [2017]:

        # Read in data from MedPAR (2017 was saved in another directory. Thus, I needed to specify another path)
        medpar_df = dd.read_csv(f'/mnt/labshares/sanghavi-lab/data/medpar/{y}/csv/medpar.csv',usecols=medpar_columns,sep=',', engine='c',
                                dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)

    # Keep only IP claims (i.e. excludes nursing homes)
    ip_df = medpar_df[(medpar_df['SS_LS_SNF_IND_CD']!='N')]

    # Delete medpar_df
    del medpar_df

    # Convert to datetime
    ip_df['ADMSN_DT'] = dd.to_datetime(ip_df['ADMSN_DT'])

    # Read out IP data
    ip_df.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{y}/parquet/', compression='gzip', engine='fastparquet')

############################################## CREATE AMBULANCE CLAIMS #################################################

# Specify years
years = [2011,2012,2013,2014,2015,2016,2017]

# Loop function for each year
for y in years:

    # Identify all columns needed from the carrier line file
    columns_BCARRL = ['BENE_ID','CLM_ID','CLM_THRU_DT', 'PRF_PHYSN_UPIN', 'PRF_PHYSN_NPI', 'HCPCS_CD', 'HCPCS_1ST_MDFR_CD',
                      'HCPCS_2ND_MDFR_CD', 'CARR_LINE_MTUS_CD', 'LINE_LAST_EXPNS_DT', 'CARR_LINE_MTUS_CNT', 'CLM_THRU_DT',
                      'PRVDR_STATE_CD','PRVDR_ZIP', 'LINE_1ST_EXPNS_DT', 'LINE_PRCSG_IND_CD', 'LINE_CMS_TYPE_SRVC_CD','TAX_NUM']
    # Note: Do not need UPIN since NPIs replaced UPINs as the standard provider identifiers beginning in 2007

    if y in [*range(2011, 2017, 1)]: # list from 2011-2016

        # Read in carrier line for the particular year
        df_BCARRL = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/BCARRL/csv/bcarrier_line_k.csv',usecols=columns_BCARRL,sep=',',
                                engine='c', dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)

    elif y in [2017]:

        # Read in carrier line for 2017. (2017 was saved in another directory. Thus, I needed to specify another path)
        df_BCARRL = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/BCAR_RL/csv/bcarrier_line_k.csv',usecols=columns_BCARRL, sep=',',
                                 engine='c', dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)

    # Keep only emergency ambulances and if payment was approved
    em_ambulance_cd = ['A0427', 'A0429', 'A0433']
    payment_allowed_cd = ['A']
    df_BCARRL = df_BCARRL.loc[(df_BCARRL['HCPCS_CD'].isin(em_ambulance_cd)) & (df_BCARRL['LINE_PRCSG_IND_CD'].isin(payment_allowed_cd))]

    # Identify all columns needed from the carrier header file
    columns_BCARRB = ['BENE_ID','CLM_ID', 'CLM_FROM_DT', 'CARR_CLM_PMT_DNL_CD', 'CLM_PMT_AMT','NCH_CARR_CLM_ALOWD_AMT',
                      'PRNCPAL_DGNS_CD', 'CARR_CLM_HCPCS_YR_CD'] + ['ICD_DGNS_CD{}'.format(i) for i in range(1, 13)]

    if y in [*range(2011, 2017, 1)]: # list from 2011-2016

        # Read in BCARRB
        df_BCARRB = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/BCARRB/csv/bcarrier_claims_k.csv',usecols=columns_BCARRB,sep=',',
                                engine='c', dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)
    elif y in [2017]:

        # Read in BCARRB from 2017. (2017 was saved in another directory. Thus, I needed to specify another path)
        df_BCARRB = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/BCAR_RB/csv/bcarrier_claims_k.csv',usecols=columns_BCARRB,sep=',',
                            engine='c', dtype='object', na_filter=False, skipinitialspace=True, low_memory=False)

    # Merge Line item with Base/Header
    df_BCARRL_BCARRB = dd.merge(df_BCARRL,df_BCARRB,on=['BENE_ID','CLM_ID'],how='inner')

    # Recover memory. (i.e. take up less RAM space when running the script)
    del df_BCARRL
    del df_BCARRB

    # Keep claims where claim was NOT denied
    df_BCARRL_BCARRB = df_BCARRL_BCARRB[df_BCARRL_BCARRB['CARR_CLM_PMT_DNL_CD'] != '0']

    # Identify all columns needed from the MBSF for the current year and the following year. Only need death columns for the following year.
    columns_MBSF = ['BENE_ID','STATE_CODE','COUNTY_CD','ZIP_CD','BENE_BIRTH_DT','SEX_IDENT_CD','BENE_RACE_CD','VALID_DEATH_DT_SW',
                    'BENE_DEATH_DT','BENE_ENROLLMT_REF_YR','ENHANCED_FIVE_PERCENT_FLAG','RTI_RACE_CD'] + [f'MDCR_ENTLMT_BUYIN_IND_{i:02}' for i in range(1,13)]
    columns_MBSF_following_year = ['BENE_ID','BENE_DEATH_DT','VALID_DEATH_DT_SW']

    # Read in MBSF (same year)
    df_MBSF = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFABCD/csv/mbsf_abcd_summary.csv',sep=',', engine='c',dtype='object', na_filter=False,skipinitialspace=True, low_memory=False,usecols=columns_MBSF)

    # Merge Personal Summary with Carrier file
    carrier_ps_merge = dd.merge(df_BCARRL_BCARRB,df_MBSF,on=['BENE_ID'],how='left')

    # Recover memory. (i.e. take up less RAM space when running the script)
    del df_BCARRL_BCARRB
    del df_MBSF

    # If current year has data in the following year
    if y in [2011,2012,2013,2014,2015,2016]:

        # Read in MBSF following year. Note the "y+1" to specify the correct path for the next year.
        df_MBSF_following_year = dd.read_csv(f'/mnt/data/medicare-share/data/{y+1}/MBSFABCD/csv/mbsf_abcd_summary.csv',sep=',',
                                             engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False,usecols=columns_MBSF_following_year)

        # Rename columns for the following year
        df_MBSF_following_year = df_MBSF_following_year.rename(columns={'BENE_DEATH_DT': 'BENE_DEATH_DT_FOLLOWING_YEAR',
                                                                        'VALID_DEATH_DT_SW': 'VALID_DEATH_DT_SW_FOLLOWING_YEAR'})

        # Merge with the following year personal summary
        carrier_ps_ps3m_merge = dd.merge(carrier_ps_merge, df_MBSF_following_year, on=['BENE_ID'], how='left')

        # Recover memory. (i.e. take up less RAM space when running the script)
        del df_MBSF_following_year
        del carrier_ps_merge

    elif y in [2017]: # 2018 was not available

        # Create columns of NaN/NaT using numpy/pandas for 2017 since there is no 2018 data.
        carrier_ps_merge['BENE_DEATH_DT_FOLLOWING_YEAR'] = pd.NaT # empty date columns needed to be specified like this to avoid future errors
        carrier_ps_merge['VALID_DEATH_DT_SW_FOLLOWING_YEAR'] = np.nan # Missing values can be specified like this

        # Rename DF to keep consistent with 2011-2016 DFs above
        carrier_ps_ps3m_merge = carrier_ps_merge

        # Recover memory. (i.e. take up less RAM space when running the script)
        del carrier_ps_merge

    # Convert claim thru date to datetime
    carrier_ps_ps3m_merge['CLM_THRU_DT'] = dd.to_datetime(carrier_ps_ps3m_merge['CLM_THRU_DT'])

    # Keep Part A and Part B (also keep state buy-in)
    carrier_ps_ps3m_merge = carrier_ps_ps3m_merge[((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==1) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_01']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_01']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==2) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_02']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_02']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==3) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_03']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_03']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==4) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_04']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_04']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==5) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_05']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_05']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==6) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_06']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_06']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==7) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_07']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_07']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==8) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_08']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_08']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==9) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_09']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_09']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==10) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_10']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_10']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==11) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_11']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_11']=='C'))) |
                                                  ((carrier_ps_ps3m_merge['CLM_THRU_DT'].dt.month==12) & ((carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_12']=='3')|(carrier_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_12']=='C')))]

    # Clean DF by dropping columns
    carrier_ps_ps3m_merge = carrier_ps_ps3m_merge.drop(['MDCR_ENTLMT_BUYIN_IND_0{}'.format(i) for i in range(1,10)],axis=1)
    carrier_ps_ps3m_merge = carrier_ps_ps3m_merge.drop(['MDCR_ENTLMT_BUYIN_IND_{}'.format(i) for i in range(10,13)],axis=1)

    # Export final ambulance claims
    carrier_ps_ps3m_merge.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/emergency_ambulance_claims/{y}/parquet/', compression='gzip', engine='fastparquet')

############################## CONVERT OP BASE AND REVENUE FILES FROM CSV TO PARQUET ###################################

# Specify years
years=[2011,2012,2013,2014,2015,2016,2017]

# Define columns for opb and opr
columns_opb = ['BENE_ID', 'CLM_ID', 'CLM_FROM_DT','CLM_THRU_DT', 'CLM_PMT_AMT', 'NCH_PRMRY_PYR_CLM_PD_AMT', 'ORG_NPI_NUM',
               'PRVDR_NUM', 'CLM_FAC_TYPE_CD', 'DOB_DT', 'NCH_BENE_PTB_DDCTBL_AMT', 'NCH_BENE_PTB_COINSRNC_AMT','NCH_BLOOD_PNTS_FRNSHD_QTY',
               'CLM_SRVC_CLSFCTN_TYPE_CD', 'CLM_MDCR_NON_PMT_RSN_CD','PTNT_DSCHRG_STUS_CD', 'PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD'] + \
              ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)] + \
              ['ICD_PRCDR_CD{}'.format(k) for k in range(1, 26)]

for y in years:

    if y in [*range(2011, 2017, 1)]: # list from 2011-2016

        # Read in OP claims
        opb = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/OPB/csv/outpatient_base_claims_k.csv',usecols=columns_opb,sep=',',
                          engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False)

    elif y in [2017]: # At the time, the 2017 OP file names were different from 2011-2016. Thus, I needed to specify a different path.

        # Read in OP claims
        opb = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/OPB/csv/outpatient_base_claim_k.csv',usecols=columns_opb,sep=',',
                          engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False) # the file does does not have "s" at the end of claim in 2017 unlike 2011-2016

    # Read out file to parquet
    opb.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/opb/{y}/parquet', compression='gzip', engine='fastparquet')

################### CREATE SUBSET OF OP TO KEEP ONLY BENE'S NEEDED TO MERGE WITH AMB CLAIMS ############################
# Retains only a subset of OP bene's matched with amb claims. This saves a lot of space since OP is large and we only  #
# need a subset of bene's that matched with the ambulance claims when we merge OP and ambulance together.              #
########################################################################################################################

# Specify Years
years=[2011,2012,2013,2014,2015,2016,2017]

for y in years:

    # Read in ambulance claims with only BENE_ID
    amb = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/emergency_ambulance_claims/{y}/parquet/', engine='fastparquet', columns=['BENE_ID'])

    # Specify columns (took out procedure code. Won't need it for this project)
    columns_op = ['BENE_ID', 'CLM_ID', 'CLM_FROM_DT','CLM_THRU_DT', 'CLM_PMT_AMT', 'NCH_PRMRY_PYR_CLM_PD_AMT', 'ORG_NPI_NUM',
                   'PRVDR_NUM', 'CLM_FAC_TYPE_CD', 'DOB_DT', 'NCH_BENE_PTB_DDCTBL_AMT', 'NCH_BENE_PTB_COINSRNC_AMT',
                   'CLM_SRVC_CLSFCTN_TYPE_CD', 'CLM_MDCR_NON_PMT_RSN_CD','PTNT_DSCHRG_STUS_CD', 'PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD'] + \
                  ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)]

    # Read in OP claims
    op = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/opb/{y}/parquet', engine='fastparquet', columns=columns_op)

    # Merged amb with op. Keep only OP claims that have the same bene_id's from amb.
    amb_merge_op =  dd.merge(op,amb,on=['BENE_ID'],how='inner')

    # Recover memory
    del amb
    del op

    # Convert to datetime
    amb_merge_op['CLM_FROM_DT'] = dd.to_datetime(amb_merge_op['CLM_FROM_DT'])

    # Export
    amb_merge_op.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/op_subset/{y}/parquet/',compression='gzip',engine='fastparquet')

    # Recover Memory
    del amb_merge_op








