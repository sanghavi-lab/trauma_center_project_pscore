#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script identifies claims with at least one trauma code based on HCUP's definition. HCUP stands for
# Healthcare Cost and Utilization Project.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
from itertools import chain

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

############################################## IDENTIFY TRAUMA #########################################################

# Specify IP or OP. Will use this list with the loop function since I have not concatenated the IP and OP claims together, yet.
claim_type = ['ip','opb'] # opb is outpatient base file (not revenue file)

# Specify years
years=[2011,2012,2013,2014,2015,2016,2017]

# Define columns from IP
ip_columns = ['BENE_ID', 'ADMSN_DT', 'MEDPAR_ID','PRVDR_NUM', 'DSCHRG_DT','ORG_NPI_NUM', 'BENE_DSCHRG_STUS_CD', 'BLOOD_PT_FRNSH_QTY',
              'ADMTG_DGNS_CD'] + ['DGNS_{}_CD'.format(i) for i in range(1, 26)] + ['DGNS_E_{}_CD'.format(k) for k in range(1, 13)] +\
             ['HCPCS_CD','HCPCS_1ST_MDFR_CD','HCPCS_2ND_MDFR_CD','MILES','amb_ind']

# Define columns from OP
op_columns = ['BENE_ID', 'CLM_ID', 'CLM_FROM_DT','CLM_THRU_DT', 'ORG_NPI_NUM','PRVDR_NUM', 'NCH_BLOOD_PNTS_FRNSHD_QTY',
              'PTNT_DSCHRG_STUS_CD', 'CLM_FAC_TYPE_CD', 'PRNCPAL_DGNS_CD', 'FST_DGNS_E_CD'] + \
             ['ICD_DGNS_CD{}'.format(i) for i in range(1, 26)] + ['ICD_DGNS_E_CD{}'.format(j) for j in range(1, 13)] +\
             ['HCPCS_CD','HCPCS_1ST_MDFR_CD','HCPCS_2ND_MDFR_CD','MILES','amb_ind']

for c in claim_type: # IP or OP

    for y in years:

        # Read in data and standardize diagnosis column names (38 dx columns total)
        if c in ['ip']:

            # Import ip
            hos_df = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/{c}_w_amb/{y}/parquet/', engine='fastparquet', columns=ip_columns)

            # Rename diagnosis columns
            hos_df = hos_df.rename(columns={'ADMTG_DGNS_CD':'dx1'})
            for num in range(1,26):
                hos_df = hos_df.rename(columns={f'DGNS_{num}_CD':f'dx{num+1}'}) # the "+1" is needed to continue consecutively naming the diagnosis columns after naming ADMTG_DGNS_CD as dx1

            # Rename external cause code columns
            for enum in range(1,13):
                hos_df = hos_df.rename(columns={f'DGNS_E_{enum}_CD':f'dx{enum+26}'}) # the "+26" is needed to continue consecutively naming the diagnosis columns

            # Create indicator for ip. IP is one
            hos_df['IP_IND'] = 1

        # Read in data and standardize diagnosis column names (38 dx columns total)
        if c in ['opb']:

            # Import op
            hos_df = dd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/{c}_w_amb/{y}/parquet/', engine='fastparquet', columns=op_columns)

            # Rename diagnosis columns
            hos_df = hos_df.rename(columns={'PRNCPAL_DGNS_CD':'dx1'})
            for num in range(1,26):
                hos_df = hos_df.rename(columns={f'ICD_DGNS_CD{num}':f'dx{num+1}'}) # the "+1" is needed to continue consecutively naming the diagnosis columns after naming PRNCPAL_DGNS_CD as dx1

            # Rename external cause code columns
            for enum in range(1,13):
                hos_df = hos_df.rename(columns={f'ICD_DGNS_E_CD{enum}':f'dx{enum+26}'}) # the "+26" is needed to continue consecutively naming the diagnosis columns

            # Create indicator for op. OP is zero
            hos_df['IP_IND'] = 0

            # Remove the duplicated external cause code column
            hos_df = hos_df.drop(['FST_DGNS_E_CD'],axis=1)

        #___ Due to differences in icd9 vs icd10, we need to identify trauma separately for icd9 vs icd10 ___#

        #--- Identify ICD9. Only 11-15 has icd9 ---#
        if y in [2011,2012,2013,2014,2015]:

            # ICD9 Create list of HCUP Trauma: 800-909.2, 909.4, 909.9; 910-994.9; 995.5-995.59; 995.80-995.85
            lst_include_codes = [str(cd) for cd in chain(range(800, 910),        # 800-909.9 (will exclude 909.3, 909.5-909.8 in lst_ignore_codes_icd9 below)
                                                        range(910, 995))         # 910-994.9
                                                    ] + ['9955',                 # 995.5-995.59
                                                         '9958']                 # 995.80-995.89 (will exclude 995.86-995.89 in lst_ignore_codes_icd9 below)
            lst_ignore_codes_icd9 = ['9093', '9095', '9096', '9097', '9098', '99586','99587', '99588', '99589'] # List to ignore codes 909.3, 909.5-909.8, & 995.86-995.89

            # ICD9 Create list of Ecodes to remove claims: E849.0-E849.9; E967.0-E967.9; E869.4; E870-E879; E930-E949
            ecode_to_remove = ['E849',                                        # E849.0-E849.9
                               'E967',                                        # E967.0-E967.9
                               'E8694'                                        # E869.4
                               ] + ['E{}'.format(i) for i in range(870,880)   # E870-E879
                               ] + ['E{}'.format(i) for i in range(930,950)]  # E930-E949

            # Define list of all diagnosis and ecodes columns (38 columns total)
            diag_ecode_col = [f'dx{i}' for i in range(1,39)]

            # Define list of first three diagnosis columns plus the principal/admission column (4 columns total)
            diag_first_four_cols = [f'dx{i}' for i in range(1,5)]

            # Convert all diagnosis and ecodes columns to string
            hos_df[diag_ecode_col] = hos_df[diag_ecode_col].astype(str)

            # First, we keep based on lst_include_codes, while ignoring lst_ignore_codes_icd9.
            hos_df_trauma = hos_df.loc[(hos_df[diag_first_four_cols].applymap(lambda x: x.startswith(tuple(lst_include_codes)) & (~x.startswith(tuple(lst_ignore_codes_icd9)))).any(axis='columns'))]

            # Recover Memory
            if y in [2011,2012,2013,2014]: # Cannot delete 2015 since we need that df for icd10
                del hos_df

            # Second, for icd9, we obtain our final subset by excluding (with the "~" sign) the claims using the Ecodes defined above (ecode_to_remove)
            hos_df_trauma = hos_df_trauma.loc[~(hos_df_trauma[diag_ecode_col].applymap(lambda x: x.startswith(tuple(ecode_to_remove))).any(axis='columns'))]

            # CHECK to see if dask identified trauma correctly
            print(hos_df_trauma[diag_first_four_cols].head(60))
            print(hos_df_trauma[diag_first_four_cols].tail(60))

            # Read out data
            hos_df_trauma.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/identify_trauma/icd_9_before_drop_duplicates/{c}/{y}/',compression='gzip',engine='fastparquet')

        #--- Identify ICD10. Only 15-17 has icd10 ---#
        if y in [2015,2016,2017]:

            # ICD10 Append to lst_include_codes: S00-S99, T07-T34, T36-T50 (ignoring sixth character of 5, 6, or some X's),T51-T76, T79, M97, T8404, O9A2-O9A5
            lst_include_codes = ['S0{}'.format(i) for i in range(0,10)]+['S{}'.format(i) for i in range(10,100)      # S00-S99
                                 ] + ['T0{}'.format(i) for i in range(7,10)]+['T{}'.format(i) for i in range(10,35)  # T07-T34
                                 ] + ['T{}'.format(i) for i in range(36,51)                                          # T36-T50 (will exclude sixth character of 5, 6, or some X's later)
                                 ] + ['T{}'.format(i) for i in range(51,77)                                          # T51-T76
                                 ] + ['T79','M97','T8404','O9A2','O9A3','O9A4','O9A5']                               # T79, M97, T8404, O9A2-O9A5

            # Define list of all diagnosis and ecodes columns (38 columns total)
            diag_ecode_col = [f'dx{i}' for i in range(1,39)]

            # Define list of first three diagnosis columns plus the principal/admission column (4 columns total)
            diag_first_four_cols = [f'dx{i}' for i in range(1,5)]

            # Convert all diagnosis and ecodes columns to string
            hos_df[diag_ecode_col] = hos_df[diag_ecode_col].astype(str)

            # First, we keep based on lst_include_codes
            hos_df_trauma = hos_df.loc[(hos_df[diag_first_four_cols].applymap(lambda x: x.startswith(tuple(lst_include_codes))).any(axis='columns'))]

            # Recover Memory
            del hos_df

            # Second, for icd10, we exclude the sixth (thus we use str[5]) character of 5 or 6 from the T36-T50 series from first four columns
            hos_df_trauma = hos_df_trauma[~(
                                           ((hos_df_trauma['dx1'].str.startswith(tuple([f'T{i}' for i in range(36,51)]))) & ((hos_df_trauma['dx1'].str[5]=='5') | (hos_df_trauma['dx1'].str[5]=='6'))) |
                                           ((hos_df_trauma['dx2'].str.startswith(tuple([f'T{i}' for i in range(36, 51)]))) & ((hos_df_trauma['dx2'].str[5] == '5') | (hos_df_trauma['dx2'].str[5] == '6'))) |
                                           ((hos_df_trauma['dx3'].str.startswith(tuple([f'T{i}' for i in range(36, 51)]))) & ((hos_df_trauma['dx3'].str[5] == '5') | (hos_df_trauma['dx3'].str[5] == '6'))) |
                                           ((hos_df_trauma['dx4'].str.startswith(tuple([f'T{i}' for i in range(36, 51)]))) & ((hos_df_trauma['dx4'].str[5] == '5') | (hos_df_trauma['dx4'].str[5] == '6')))
                                           )]

            # Third, we exclude some T36-T50 series where the sixth (str[5]) is a character of X (see HCUP definition for specifics: https://www.hcup-us.ahrq.gov/db/vars/siddistnote.jsp?var=i10_multinjury)
            lst_sixth_X_include = ['T369', 'T379', 'T399', 'T414', 'T427', 'T439', 'T459', 'T479', 'T499'] # create a list to include
            hos_df_trauma['incld_some_sixth_X_ind'] = 0 # Create indicator columns starting with zero's first
            hos_df_trauma['incld_some_sixth_X_ind'] = hos_df_trauma['incld_some_sixth_X_ind'].mask((hos_df_trauma['dx1'].str.startswith(tuple(lst_sixth_X_include))) & (hos_df_trauma['dx1'].str[5]=='X') & (hos_df_trauma['dx1'].str[4].isin(['1','2','3','4'])),1) # Replace incld_some_sixth_X_ind based on condition
            hos_df_trauma['incld_some_sixth_X_ind'] = hos_df_trauma['incld_some_sixth_X_ind'].mask((hos_df_trauma['dx2'].str.startswith(tuple(lst_sixth_X_include))) & (hos_df_trauma['dx2'].str[5]=='X') & (hos_df_trauma['dx2'].str[4].isin(['1','2','3','4'])),1) # Replace incld_some_sixth_X_ind based on condition
            hos_df_trauma['incld_some_sixth_X_ind'] = hos_df_trauma['incld_some_sixth_X_ind'].mask((hos_df_trauma['dx3'].str.startswith(tuple(lst_sixth_X_include))) & (hos_df_trauma['dx3'].str[5]=='X') & (hos_df_trauma['dx3'].str[4].isin(['1','2','3','4'])),1) # Replace incld_some_sixth_X_ind based on condition
            hos_df_trauma['incld_some_sixth_X_ind'] = hos_df_trauma['incld_some_sixth_X_ind'].mask((hos_df_trauma['dx4'].str.startswith(tuple(lst_sixth_X_include))) & (hos_df_trauma['dx4'].str[5]=='X') & (hos_df_trauma['dx4'].str[4].isin(['1','2','3','4'])),1) # Replace incld_some_sixth_X_ind based on condition
            hos_df_trauma=hos_df_trauma[~(
                                         ((hos_df_trauma['dx1'].str.startswith(tuple([f'T{i}' for i in range(36,51)]))) & (hos_df_trauma['dx1'].str[5]=='X') & (hos_df_trauma['incld_some_sixth_X_ind']!=1)) |
                                         ((hos_df_trauma['dx2'].str.startswith(tuple([f'T{i}' for i in range(36,51)]))) & (hos_df_trauma['dx2'].str[5]=='X') & (hos_df_trauma['incld_some_sixth_X_ind']!=1)) |
                                         ((hos_df_trauma['dx3'].str.startswith(tuple([f'T{i}' for i in range(36,51)]))) & (hos_df_trauma['dx3'].str[5]=='X') & (hos_df_trauma['incld_some_sixth_X_ind']!=1)) |
                                         ((hos_df_trauma['dx4'].str.startswith(tuple([f'T{i}' for i in range(36,51)]))) & (hos_df_trauma['dx4'].str[5]=='X') & (hos_df_trauma['incld_some_sixth_X_ind']!=1))
                                         )] # Drop any T36-T50 if 6th is X and indicator is not 1 (i.e. not the codes where 6th character is an X that we want to keep)

            # Drop column
            hos_df_trauma=hos_df_trauma.drop(['incld_some_sixth_X_ind'],axis=1)

            # CHECK to see if dask identified trauma correctly
            print(hos_df_trauma[diag_first_four_cols].head(60))
            print(hos_df_trauma[diag_first_four_cols].tail(60))

            # Read out data
            hos_df_trauma.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/identify_trauma/icd_10_before_drop_duplicates/{c}/{y}/',compression='gzip',engine='fastparquet')

########### APPENDIX: Check total number of hospital claims with injury code from 2011-2017 (exclude last 3 months of 2015) ###########

# Import modules
import pandas as pd

# Empty list to store numbers
list_num_rows=[]

# Define a list for IP or OP
claim_type = ['ip','opb']

# Define years 11-17 to loop through
years=[*range(2011,2018,1)]

# Define a list for icd9 vs icd10
icd_type = ['9','10']

#___ Loop through each year and calculate the number of observations ___#
for c in claim_type:

    for y in years:

        for i in icd_type:

            if (i in ['9']) & (y in [*range(2011,2016,1)]): # For ICD9 data

                # Read in data
                df_trauma = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/identify_trauma/icd_{i}_before_drop_duplicates/{c}/{y}/',engine='fastparquet',columns=['BENE_ID'])

                # Calculate the number of rows and append to list above
                num_rows = df_trauma.shape[0]
                list_num_rows.append(num_rows)

                # Check
                print(f'{c} {y} icd_{i}: ',num_rows)

            elif (i in ['10']) & (y in [*range(2016,2018,1)]): # excludes 2015 icd10

                # Read in data
                df_trauma = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/identify_trauma/icd_{i}_before_drop_duplicates/{c}/{y}/',engine='fastparquet',columns=['BENE_ID'])

                # Calculate the number of rows and append to list above
                num_rows = df_trauma.shape[0]
                list_num_rows.append(num_rows)

                # Check
                print(f'{c} {y} icd_{i}: ', num_rows)

            else:

                print(f'Data unavailable for {c} {y} icd_{i}')


# Print total number of claims (both ip and op) with injury code
print('Hospital claims with injury code 2011-2017: ',sum(list_num_rows))







