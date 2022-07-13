#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will use the MBSF to obtain death dates and create indicators for Parts A and B. Then, the
#              script will remove rurals using FORHP's definition. FORHP stands for Federal Office of Rural Health Policy.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
import pandas as pd # only need for NaT
import numpy as np # only need for NaN

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

#################################### MERGE WITH MBSF AND DROP RURALS ###################################################
# This section will merge with mbsf to obtain death dates and create indicators for Parts A and B (FFS only) and       #
# remove rurals by matching with FORHP zip-code data                                                                   #
########################################################################################################################

# Specify IP or OP
claim_type = ['ip','opb'] # opb is outpatient base file (not revenue file)

# Define years
years=[2011,2012,2013,2014,2015,2016,2017]

for c in claim_type:

    for y in years:

        # Note: from this point on, I will not use the last three months of 2015 (icd10)

        # Read in Medicare claims data
        if y in [2011,2012,2013,2014,2015]:
            # ICD9
            hos_claims = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/identify_trauma/icd_9_before_drop_duplicates/{c}/{y}/',engine='fastparquet')
        if y in [2016,2017]:
            #ICD10
            hos_claims = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/identify_trauma/icd_10_before_drop_duplicates/{c}/{y}/',engine='fastparquet')

        #_____________________________________________ Obtain Death Dates _____________________________________________#

        # Identify all columns needed in the MBSF for the current year and the following year
        columns_MBSF = ['BENE_ID', 'STATE_CODE', 'COUNTY_CD', 'ZIP_CD', 'BENE_BIRTH_DT', 'SEX_IDENT_CD',
                        'BENE_RACE_CD', 'VALID_DEATH_DT_SW','BENE_DEATH_DT', 'RTI_RACE_CD'] + [f'MDCR_ENTLMT_BUYIN_IND_{i:02}' for i in range(1,13)]
        columns_MBSF_following_year = ['BENE_ID', 'BENE_DEATH_DT', 'VALID_DEATH_DT_SW'] # Only need death information in the following year

        # Read in MBSF (same year)
        df_MBSF = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFABCD/csv/mbsf_abcd_summary.csv', sep=',',engine='c',
                              dtype='object', na_filter=False, skipinitialspace=True, low_memory=False,usecols=columns_MBSF)

        # Merge Personal Summary with hospital claims
        hos_claims_ps_merge = dd.merge(hos_claims, df_MBSF, on=['BENE_ID'], how='left')

        # Recover memory
        del hos_claims
        del df_MBSF

        # If current year has data the following year...
        if y in [2011,2012,2013,2014,2015,2016]:

            # Read in MBSF following year
            df_MBSF_following_year = dd.read_csv(f'/mnt/data/medicare-share/data/{y + 1}/MBSFABCD/csv/mbsf_abcd_summary.csv', sep=',',
                engine='c', dtype='object', na_filter=False, skipinitialspace=True, low_memory=False,usecols=columns_MBSF_following_year)

            # Rename columns for the following year
            df_MBSF_following_year = df_MBSF_following_year.rename(
                columns={'BENE_DEATH_DT': 'BENE_DEATH_DT_FOLLOWING_YEAR','VALID_DEATH_DT_SW': 'VALID_DEATH_DT_SW_FOLLOWING_YEAR'})

            # Merge with the following year personal summary
            hos_claims_ps_ps3m_merge = dd.merge(hos_claims_ps_merge, df_MBSF_following_year, on=['BENE_ID'], how='left')

            # Recover memory
            del df_MBSF_following_year
            del hos_claims_ps_merge

        # If current year does NOT have data the following year...
        elif y in [2017]: # There is no 2018 data.

            # Create columns of NaN/NaT using numpy/pandas for 2017 since there is no 2018 data
            hos_claims_ps_merge['BENE_DEATH_DT_FOLLOWING_YEAR'] = pd.NaT # Need to create empty column of missing date information to bypass potential errors after concatenating all of the years into one DF.
            hos_claims_ps_merge['VALID_DEATH_DT_SW_FOLLOWING_YEAR'] = np.nan

            # Rename DF to keep consistent with above DF name from 11-16
            hos_claims_ps_ps3m_merge = hos_claims_ps_merge

            # Recover memory
            del hos_claims_ps_merge

        #_____________________________ Create Indicators for FFS only (Parts A and B) _________________________________#
        # i.e. create an indicator to identify ffs claims only

        if c in ['ip']: # IP

            # Convert to datetime
            hos_claims_ps_ps3m_merge['ADMSN_DT'] = dd.to_datetime(hos_claims_ps_ps3m_merge['ADMSN_DT'])

            # Create indicator for Parts A and B (Keeping State Buy-In)
            hos_claims_ps_ps3m_merge['parts_ab_ind']=0
            hos_claims_ps_ps3m_merge['parts_ab_ind'] = hos_claims_ps_ps3m_merge['parts_ab_ind'].mask(((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==1) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_01']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_01']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==2) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_02']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_02']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==3) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_03']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_03']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==4) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_04']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_04']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==5) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_05']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_05']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==6) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_06']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_06']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==7) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_07']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_07']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==8) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_08']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_08']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==9) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_09']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_09']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==10) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_10']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_10']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==11) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_11']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_11']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['ADMSN_DT'].dt.month==12) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_12']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_12']=='C'))),1)
        if c in ['opb']: # OP base file

            # Convert to datetime
            hos_claims_ps_ps3m_merge['CLM_FROM_DT'] = dd.to_datetime(hos_claims_ps_ps3m_merge['CLM_FROM_DT'])

            # Create indicator for Parts A and B (Keeping State Buy-In)
            hos_claims_ps_ps3m_merge['parts_ab_ind']=0
            hos_claims_ps_ps3m_merge['parts_ab_ind'] = hos_claims_ps_ps3m_merge['parts_ab_ind'].mask(((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==1) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_01']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_01']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==2) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_02']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_02']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==3) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_03']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_03']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==4) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_04']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_04']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==5) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_05']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_05']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==6) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_06']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_06']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==7) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_07']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_07']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==8) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_08']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_08']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==9) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_09']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_09']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==10) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_10']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_10']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==11) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_11']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_11']=='C'))) |
                                                                                                     ((hos_claims_ps_ps3m_merge['CLM_FROM_DT'].dt.month==12) & ((hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_12']=='3')|(hos_claims_ps_ps3m_merge['MDCR_ENTLMT_BUYIN_IND_12']=='C'))),1)

        # Clean DF
        hos_claims_ps_ps3m_merge = hos_claims_ps_ps3m_merge.drop([f'MDCR_ENTLMT_BUYIN_IND_{i:02}' for i in range(1,13)],axis=1)

        # CHECK proportion of AB over all in each year
        print(f'{c} {y}',hos_claims_ps_ps3m_merge['parts_ab_ind'].sum().compute()/hos_claims_ps_ps3m_merge.shape[0].compute())

        #______________________________________________ Drop Rurals ___________________________________________________#

        # Read in FORHP rural zip-code data
        forhp_df = dd.read_csv('/mnt/labshares/sanghavi-lab/data/public_data/data/forhp_eligible_zip_codes_crosswalk/forhp-eligible-zips.csv')

        # Convert to string
        forhp_df['ZIP'] = forhp_df['ZIP'].astype(str)
        hos_claims_ps_ps3m_merge['ZIP_CD'] = hos_claims_ps_ps3m_merge['ZIP_CD'].astype(str)

        # Merge rural and Medicare claims to determine which claims are from rural areas. Claims that match are from rural counties
        claims_forhp_merge = dd.merge(forhp_df,hos_claims_ps_ps3m_merge,left_on=['ZIP'],right_on=['ZIP_CD'], how='right')

        # Recover memory
        del hos_claims_ps_ps3m_merge

        # CHECK prop of claims that are rural
        print(f'{c} {y}',claims_forhp_merge[~claims_forhp_merge['ZIP'].isna()].shape[0].compute()/claims_forhp_merge.shape[0].compute())

        # Drop rurals
        not_rural_df = claims_forhp_merge[claims_forhp_merge['ZIP'].isna()] # If claims did NOT match (i.e. contains nan) then those claims are not rural.

        # Recover memory
        del claims_forhp_merge

        # Clean DF of non rural claims
        not_rural_df = not_rural_df.drop(['ZIP','STATE','Unnamed: 0'],axis=1)

        # Read out data in parquet
        not_rural_df.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/{c}/{y}',compression='gzip',engine='fastparquet')

################ APPENDIX: Check total number of hospital claims that are NOT from rural counties #######################

# Import modules
import pandas as pd

# Empty list to store numbers
list_num_rows=[]

# Define a list for IP or OP
claim_type = ['ip','opb']

# Define years 11-17 to loop through
years=[*range(2011,2018,1)]

#___ Loop through each year and calculate the number of observations ___#
for c in claim_type:

    for y in years:

        # Read in data. Note, I already excluded the last three months of 2015.
        df_nonrural = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/nonrural/{c}/{y}',engine='fastparquet',columns=['BENE_ID'])

        # Calculate the number of rows and append to list above
        num_rows = df_nonrural.shape[0]
        list_num_rows.append(num_rows)

        # Check
        print(f'{c} {y}: ',num_rows)


# Print total number of claims (both ip and op) with injury code
print('Claims without rural (includes 2015 icd10 data) (to find the number of rurals, \nI need to subtract this number from Hospital claims with \ninjury code box: ',sum(list_num_rows))
