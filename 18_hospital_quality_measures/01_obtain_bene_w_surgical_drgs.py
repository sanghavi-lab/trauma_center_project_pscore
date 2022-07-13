# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script selects patients with surgical DRGs from the inpatient file. These cases are used to generate
# hospital quality measures based on inpatient surgical mortality (specifically, binary 30-day mortality indicator minus
# the modeled risk-adjusted surgical mortality probability).
# ----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
import pandas as pd
import numpy as np

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

###################################### IDENTIFY BENE'S WITH SURGICAL DRGS ##############################################

# Specify columns
col = ['BENE_ID','DRG_CD','ADMSN_DT','SRC_IP_ADMSN_CD','ER_CHRG_AMT','SS_LS_SNF_IND_CD','BENE_RSDNC_SSA_STATE_CD','GHO_PD_CD',
       'BENE_DSCHRG_STUS_CD','PRVDR_NUM','MEDPAR_ID','ADMTG_DGNS_CD'] + [f'DGNS_{i}_CD' for i in range(1, 26)]

# Specify years
years = [2011,2012,2013,2014,2015,2016,2017]

for y in years:

    # Read in inpatient (IP) raw data. IP was extracted from MedPAR in previous codes.
    ip_df = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/ip/{y}/parquet/', engine='fastparquet',columns=col)

    # Convert date columns to date time
    ip_df['ADMSN_DT'] = dd.to_datetime(ip_df['ADMSN_DT'])

    # Define list of DRG codes
    list_surgical_DRG = [f'00{i}' for i in range(1, 10)] + [f'0{i}' for i in range(10, 100)] + [f'{i}' for i in range(100, 971)] # gather all range

    # Keep if claim contains the above DRG code
    ip_df_surgical = ip_df[ip_df['DRG_CD'].isin(list_surgical_DRG)]

    # Recover memory by deleting old DF
    del ip_df

    # List of DRGS we do not want (https://www.cms.gov/icd10m/version37-fullcode-cms/fullcode_cms/P0375.html)
    list_eye = [f'{i}' for i in range(113,118)]
    list_ortho = ['470']
    list_metabolic = [f'{i}' for i in range(614,631)]
    list_male_repoductive = [f'{i}' for i in range(707,718)]
    list_female_repoductive = [f'{i}' for i in range(734,751)]
    list_mental = ['876']
    list_pre_mdc = [f'00{i}' for i in range(1,9)] + [f'0{i}' for i in range(10,18)]
    list_derm = [f'{i}' for i in range(573,586)]
    list_myeloproliferative = [f'{i}' for i in range(820,831)]
    list_parasitic = [f'{i}' for i in range(853, 859)]
    list_poison = [f'{i}' for i in range(901, 910)]
    list_health_stat = [f'{i}' for i in range(939, 942)]
    list_trauma = [f'{i}' for i in range(955, 960)]
    list_HIV = [f'{i}' for i in range(969, 971)]
    list_other_irrelevant = ['009','015','018','019']+[f'0{i}' for i in range(43,100)]+[f'{i}' for i in range(100,113)] +\
                         [f'{i}' for i in range(118,129)]+[f'{i}' for i in range(140,163)]+[f'{i}' for i in range(169,215)]+['230','237','238','265']+\
                         [f'{i}' for i in range(275,319)]+[f'{i}' for i in range(321,326)]+[f'{i}' for i in range(359,405)]+[f'{i}' for i in range(426,453)]+\
                         ['484','490','491']+[f'{i}' for i in range(521,570)]+[f'{i}' for i in range(586,614)]+[f'{i}' for i in range(631,652)]+[f'{i}' for i in range(676,707)]+\
                         [f'{i}' for i in range(719,734)]+[f'{i}' for i in range(751,768)]+[f'{i}' for i in range(771,783)]+[f'{i}' for i in range(789,796)]+\
                         [f'{i}' for i in range(805,817)]+[f'{i}' for i in range(831,853)]+[f'{i}' for i in range(859,870)]+[f'{i}' for i in range(873,876)]+[f'{i}' for i in range(877,901)]+\
                         [f'{i}' for i in range(910,927)]+[f'{i}' for i in range(930,939)]+[f'{i}' for i in range(942,955)]+[f'{i}' for i in range(942,955)]+\
                         [f'{i}' for i in range(960,969)] #Kept sepsis

    # Drop if claim contains the above DRG code (Although sepsis was NOT a surgical code, I included it just because it's closely related to trauma)
    ip_df_surgical = ip_df_surgical[~ip_df_surgical['DRG_CD'].isin(list_eye+list_ortho+list_metabolic+list_male_repoductive+list_female_repoductive+list_mental+list_pre_mdc+list_derm+list_myeloproliferative+
                                                 list_parasitic+list_poison+list_health_stat+list_trauma+list_HIV+list_other_irrelevant)]

    # Remove transfers from other hospitals/hospice, ED visits, no info,
    transfer_and_ED_list = ['4','5','6','7','9','F']
    ip_df_surgical = ip_df_surgical[~ip_df_surgical['SRC_IP_ADMSN_CD'].isin(transfer_and_ED_list)] # the "~" means "opposite"

    # Remove last 30 days only if in 2017 since we will examine 30d death.
    if y in [2017]:
        ip_df_surgical=ip_df_surgical[~(ip_df_surgical['ADMSN_DT'].dt.month==12)] # If admission date is not december for 2016

    # Keep only short stay
    ip_df_surgical = ip_df_surgical[ip_df_surgical['SS_LS_SNF_IND_CD']=='S']

    # Keep only those where ER charge amount is zero
    ip_df_surgical = ip_df_surgical[ip_df_surgical['ER_CHRG_AMT']=='0']

    # Keep only US States
    us_states_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11',
                      '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
                      '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
                      '34', '35', '36', '37', '38', '39', '41', '42', '43', '44', '45',
                      '46', '47', '49', '50', '51', '52', '53']
    ip_df_surgical = ip_df_surgical[ip_df_surgical['BENE_RSDNC_SSA_STATE_CD'].isin(us_states_list)]

    # Keep if GHO (Group Health Organization) has paid the provider for the claim
    ip_df_surgical = ip_df_surgical[ip_df_surgical['GHO_PD_CD']!='1']

    #___ Merge with MBSF to obtain death dates ___#

    # Identify all columns needed in the MBSF for the current year and the following year
    columns_MBSF = ['BENE_ID','BENE_BIRTH_DT','SEX_IDENT_CD','RTI_RACE_CD','BENE_DEATH_DT','VALID_DEATH_DT_SW'] # Need Valid column to confirm death date was checked against SSA
    columns_MBSF_following_year = ['BENE_ID','BENE_DEATH_DT','VALID_DEATH_DT_SW'] # Need Valid column to confirm death date was checked against SSA

    # Read in MBSF
    df_MBSF = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFABCD/csv/mbsf_abcd_summary.csv',sep=',', engine='c',
                          dtype='object', na_filter=False,skipinitialspace=True, low_memory=False,usecols=columns_MBSF)

    # Merge Personal Summary with IP
    ip_df_surgical_ps = dd.merge(ip_df_surgical,df_MBSF,on=['BENE_ID'],how='left')

    # Recover memory
    del ip_df_surgical
    del df_MBSF

    # If current year has data the following year
    if y in [*range(2011,2017,1)]: # list is from 2011-2016

        # Read in MBSF following year
        df_MBSF_following_year = dd.read_csv(f'/mnt/data/medicare-share/data/{y+1}/MBSFABCD/csv/mbsf_abcd_summary.csv',sep=',',
                                             engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False,usecols=columns_MBSF_following_year)

        # Rename columns for the following year
        df_MBSF_following_year = df_MBSF_following_year.rename(columns={'BENE_DEATH_DT': 'BENE_DEATH_DT_FOLLOWING_YEAR',
                                                                        'VALID_DEATH_DT_SW': 'VALID_DEATH_DT_SW_FOLLOWING_YEAR'})

        # Merge with the following year personal summary
        ip_df_surgical_ps = dd.merge(ip_df_surgical_ps, df_MBSF_following_year, on=['BENE_ID'], how='left')

        # Recover memory
        del df_MBSF_following_year

    elif y in [2017]: # 2017 does NOT have data in the following year (i.e. no 2018)

        # Create columns of Nan's using numpy for 2017 since there is no 2018 data
        ip_df_surgical_ps['BENE_DEATH_DT_FOLLOWING_YEAR'] = pd.NaT
        ip_df_surgical_ps['VALID_DEATH_DT_SW_FOLLOWING_YEAR'] = np.nan

    # Clean data: Drop columns not needed anymore
    ip_df_surgical_ps = ip_df_surgical_ps.drop(['SRC_IP_ADMSN_CD','SS_LS_SNF_IND_CD','ER_CHRG_AMT','BENE_RSDNC_SSA_STATE_CD',
                                                'GHO_PD_CD'],axis=1)

    # Need to drop last three months of 2015 (since these months just transitioned to icd10)
    if y in [2015]:
        ip_df_surgical_ps=ip_df_surgical_ps[~((ip_df_surgical_ps['ADMSN_DT'].dt.month==10)|
                                              (ip_df_surgical_ps['ADMSN_DT'].dt.month==11)|
                                              (ip_df_surgical_ps['ADMSN_DT'].dt.month==12))]

    ip_df_surgical_ps['BENE_ID'] = ip_df_surgical_ps['BENE_ID'].astype(str)
    ip_df_surgical_ps['MEDPAR_ID'] = ip_df_surgical_ps['MEDPAR_ID'].astype(str)
    ip_df_surgical_ps['ADMSN_DT'] = dd.to_datetime(ip_df_surgical_ps['ADMSN_DT'])

    # Read out
    ip_df_surgical_ps.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/bene_with_surgical_drgs/{y}',engine='fastparquet',compression='gzip')


































