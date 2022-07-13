# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will merge the trauma data from the American Trauma Society (ATS) with the analytical claims sample to
# categorize the different hospital types and prepare the files for Stata. This a lot of work went into this procedure, but here is the
# overall process: (1) Identifying trauma centers by matching the analytical claims with ATS within each year. Any claims that were
# not matched were then matched again with ATS data in the following year. To make sure the trauma center identification was accurate,
# I removed hospitals that have multiple trauma centers with different levels and hospitals that changed levels between 2013-2017.
# (2) Identifying non-trauma centers was easier. Basically, if the analytical claims data did not match with ATS data in
# any year between 2013-2019 then we are certain that these are not trauma centers.
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

##################################### MERGE CLAIMS WITH ATS TRAUMA DATA ################################################
# First merge the ATS trauma data with the AHA-to-Provider_ID crosswalk. Second, merge the claims data with the        #
# ATS/AHA data on the same year. Take those claims that did not match and merge on the following ATS/AHA year.         #
########################################################################################################################

# Disable SettingWithCopyWarning (i.e. chained assignments)
pd.options.mode.chained_assignment = None

# APPENDIX: Create empty list to store beginning denominators. Will be used to in the end to obtain number of claims not included in final sample due to inability to accurately determine hospital type.
denom_list_appendix = []

#__________________________ Merge ATS and AHA data before matching with claims ________________________________________#
# Here I will obtain the provider id from AHA.

# Define columns for ATS data
cols_ats = ['TCN', 'AHA_num', 'Add_1', 'City/Town', 'State', 'ACS_Ver', 'State_Des']

# Define columns for crosswalk data
cols_xwalk = ['NPINUM', 'MCRNUM', 'ID','MNAME','MLOCADDR','MLOCCITY','MSTATE']

# Create empty dictionary to store the combined ats-aha data for each year
ats_aha_dict = {}

# Create empty dictionary list to store duplicated provider ids with different trauma levels (i.e. hospitals that have multiple trauma centers with different levels)
duplicated_provider_id_dict_list = {}
    # I will eventually use this list of provider id's to remove any hospitals from my analytical sample that have multiple trauma centers with different levels

# Specify years
ats_aha_years = [2012,2013,2014,2015,2016,2017,2018,2019] # We do not have 2012 or 2018 ATS data. Instead we will use 2013 and 2019 as replacements (see below when reading in data)

for y in ats_aha_years:

    # Read in ATS and AHA x-walk data
    if y in [2013,2014,2015,2016,2017,2019]:
        trauma_ats_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/AM_TRAUMA_DATA_FIRST_TAB_UNLOCKED_{y}.xlsx',usecols=cols_ats, dtype=str)
        trauma_xwalk_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/NPINUM_MCRNUM_AHAID_CROSSWALK_{y}.xlsx', header=3,dtype=str, usecols=cols_xwalk)
    elif y in [2012]: # using 2013 data
        trauma_ats_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/AM_TRAUMA_DATA_FIRST_TAB_UNLOCKED_2013.xlsx',usecols=cols_ats, dtype=str)
        trauma_xwalk_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/NPINUM_MCRNUM_AHAID_CROSSWALK_2013.xlsx', header=3,dtype=str, usecols=cols_xwalk)
    elif y in [2018]: # using 2019 data.
        trauma_ats_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/AM_TRAUMA_DATA_FIRST_TAB_UNLOCKED_2019.xlsx',usecols=cols_ats, dtype=str)
        trauma_xwalk_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/NPINUM_MCRNUM_AHAID_CROSSWALK_2019.xlsx', header=3,dtype=str, usecols=cols_xwalk)

    # Put indicator of 1 to observe if crosswalk and ats data matched
    trauma_xwalk_df['ats_match_ind'] = 1

    # Merge trauma data and crosswalk on AHA ID (American Hospital Association ID) AND STATE
    trauma_ats_xwalk_df = pd.merge(trauma_xwalk_df, trauma_ats_df, left_on=['ID', 'MSTATE'],right_on=['AHA_num', 'State'], how='right')

    # Replace nan and empty strings with "-" to keep consistent with original ats data
    trauma_ats_xwalk_df['ACS_Ver'] = trauma_ats_xwalk_df['ACS_Ver'].replace('', '-')
    trauma_ats_xwalk_df['State_Des'] = trauma_ats_xwalk_df['State_Des'].replace('', '-')
    trauma_ats_xwalk_df['ACS_Ver'] = trauma_ats_xwalk_df['ACS_Ver'].replace(np.nan, '-')
    trauma_ats_xwalk_df['State_Des'] = trauma_ats_xwalk_df['State_Des'].replace(np.nan, '-')

    # --- CHECK MERGE PROPORTION BETWEEN ATS AND X-WALK ---#
    print(f'{y} Percent matched between ATS and AHA',100 * trauma_ats_xwalk_df['ats_match_ind'].sum() / trauma_ats_xwalk_df.shape[0], '\n\n')
    # ------------------------------------------#

    # Keep hospitals form ATS who matched with AHA
    ats_aha_dict[y] = trauma_ats_xwalk_df[trauma_ats_xwalk_df['ats_match_ind'] == 1]

    # Create trauma_lvl column using State_Des first
    ats_aha_dict[y]['TRAUMA_LEVEL'] = np.where(ats_aha_dict[y]['State_Des'] == '1', '1',
                                             np.where(ats_aha_dict[y]['State_Des'] == '2', '2',
                                                      np.where(ats_aha_dict[y]['State_Des'] == '3', '3',
                                                               np.where(ats_aha_dict[y]['State_Des'] == '4', '4',
                                                                        np.where(ats_aha_dict[y]['State_Des'] == '5', '5',
                                                                                 np.where(ats_aha_dict[y]['State_Des'] == '-',
                                                                                          '-', '-'))))))

    # Then prioritize ACS (there is no lvl 4 or 5 in ACS_Ver column)
    ats_aha_dict[y].loc[ats_aha_dict[y]['ACS_Ver'] == '1', 'TRAUMA_LEVEL'] = '1'
    ats_aha_dict[y].loc[ats_aha_dict[y]['ACS_Ver'] == '2', 'TRAUMA_LEVEL'] = '2'
    ats_aha_dict[y].loc[ats_aha_dict[y]['ACS_Ver'] == '3', 'TRAUMA_LEVEL'] = '3'

    # # see number of t lvl per year
    # ats_aha_dict[y] = ats_aha_dict[y][ats_aha_dict[y]['TRAUMA_LEVEL']!='-']
    # df = ats_aha_dict[y].TRAUMA_LEVEL.value_counts().to_frame().sort_values(by='TRAUMA_LEVEL')
    # print(df.head(60))
    # print(df.TRAUMA_LEVEL.sum())

    #--- Place into dictionary (duplicated_provider_id_dict_list) hospitals that have multiple trauma centers with different levels ---#
    # Goal is to use this list to drop hospitals under one organization (same provider id) but have multiple hospitals with different trauma levels.

    # Drop any if a hospital has multiple trauma centers with the same level meaning that multiple hospitals under one organizations had the same trauma level.
    duplicated_provider_id = ats_aha_dict[y][(~ats_aha_dict[y].duplicated(subset=['MCRNUM','TRAUMA_LEVEL'], keep=False))]

    # Drop if ACS_Ver and State_Des is '-'. I.e. missing data
    duplicated_provider_id = duplicated_provider_id[~((duplicated_provider_id['ACS_Ver']=='-')&(duplicated_provider_id['State_Des']=='-'))]

    # Drop any if hospital name has "children" for the matched only. Pediatrics were ignored.
    duplicated_provider_id['TCN'] = duplicated_provider_id['TCN'].astype(str) # Convert name of hospital column to str
    duplicated_provider_id = duplicated_provider_id[~duplicated_provider_id['TCN'].str.contains('|'.join(['child', 'Child', 'CHILD']))]

    # Keep those with multiple provider id and drop if provider id is missing info. The remaining duplicated hospitals have multiple different trauma levels.
    duplicated_provider_id = duplicated_provider_id[(duplicated_provider_id.duplicated(subset=['MCRNUM'], keep=False)) & (~duplicated_provider_id['MCRNUM'].isna())]

    # Append to list in a dictionary. Will be used to help remove hospitals that have multiple trauma centers with different levels.
    duplicated_provider_id_dict_list[y] = duplicated_provider_id['MCRNUM'].tolist()
    #-------------#

    # Drop TRAUMA_LEVEL column that we created
    ats_aha_dict[y] = ats_aha_dict[y].drop(['TRAUMA_LEVEL'],axis=1)

    # Recover memory
    del trauma_ats_xwalk_df
    del trauma_xwalk_df
    del trauma_ats_df
    del duplicated_provider_id

#_________________________ Create analytical sample of claims that merge with ATS/AHA _________________________________#
# With the ATS/AHA df's and list of duplicated_provider_id created, I will merge my analytical sample to categorize hospitals with trauma centers and drop those that have multiple trauma centers with different levels

# Create empty dictionary to store analytical claims that matched with ATS/AHA data
matched_claims_dict = {}

# Specify years for analytical sample
claims_years = [2012,2013,2014,2015,2016,2017]

for y in claims_years:

    # Read in data
    df_merge_claims_w_niss = pd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_trnsfr_ind_geo_and_hosquality/{y}.csv',dtype=str)

    # ----- CHECK DENOMINATOR OF CLAIMS ----#
    beginning_denom = df_merge_claims_w_niss.shape[0]
    denom_list_appendix.append(beginning_denom) # Store denom in list for appendix
    print('\n\n','number of claims (should be same as bottom after matching)',beginning_denom, '\n\n')
    # ----------------------------------#

    # Create a list of indicator columns
    ind_list = df_merge_claims_w_niss.columns[df_merge_claims_w_niss.columns.str.endswith('_ind')].tolist()

    # Convert to Datetime
    date = ['SRVC_BGN_DT', 'SRVC_END_DT', 'BENE_BIRTH_DT', 'BENE_DEATH_DT', 'BENE_DEATH_DT_FOLLOWING_YEAR',
            'TRNSFR_SRVC_BGN_DT', 'TRNSFR_SRVC_END_DT']
    for d in date:
        df_merge_claims_w_niss[f'{d}'] = pd.to_datetime(df_merge_claims_w_niss[f'{d}'])

    # Convert numeric string columns to floats.
    num = ['niss', 'AGE', 'riss', 'maxais', 'mxaisbr_HeadNeck','BLOOD_PT_FRNSH_QTY','mxaisbr_Extremities','mxaisbr_Face','amb_ind',
           'mxaisbr_Abdomen', 'mxaisbr_Chest', 'combinedscore', 'discharge_death_ind', 'thirty_day_death_ind','ninety_day_death_ind',
           'oneeighty_day_death_ind','threesixtyfive_day_death_ind', 'fall_ind', 'motor_accident_ind','firearm_ind','TRNSFR_IP_IND',
           'ind_trnsfr','IP_IND','parts_ab_ind','median_hh_inc', 'prop_below_pvrty_in_cnty', 'prop_female_in_cnty', 'prop_65plus_in_cnty',
           'metro_micro_cnty', 'prop_w_cllge_edu_in_cnty', 'prop_gen_md_in_cnty', 'prop_hos_w_med_school_in_cnty', 'POSbeds', 'AHAbeds',
           'IMM_3', 'MORT_30_AMI', 'MORT_30_CABG', 'MORT_30_COPD', 'MORT_30_HF', 'MORT_30_PN', 'MORT_30_STK', 'READM_30_HOSP_WIDE', 'SEP_1',
           'fall_ind_CDC','motor_accident_traffic_ind_CDC','firearm_ind_CDC','X58_ind','sec_secondary_trauma_ind','full_dual_ind',
           'partial_dual_ind','cuts_pierce_ind'] + ind_list
    for n in num:
        df_merge_claims_w_niss[f'{n}'] = df_merge_claims_w_niss[f'{n}'].astype('float')

    # Create Variable for TIME Fixed Effects (for treatment group) (I did NOT use a loop since there were some 2011 in 2012 data, 2012 in 2013 data, etc... so this way was easier but may not look as clean)
    df_merge_claims_w_niss['year_fe']=0
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2011, 2011)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2012, 2012)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2013, 2013)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2014, 2014)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2015, 2015)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2016, 2016)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2017, 2017)

    #--- First: Match on 6-digit Provider ID same year ---#

    # Add consecutive numbers to drop duplicated merges
    df_merge_claims_w_niss['to_drop_dup'] = df_merge_claims_w_niss.reset_index().index

    # Merge the ATS trauma data (those already matched with the AHA crosswalk) with the analytical claims on provider ID
    merge_claims_ats_PID = pd.merge(ats_aha_dict[y], df_merge_claims_w_niss, left_on=['MCRNUM'],right_on=['PRVDR_NUM'],how='right')

    # Recover memory
    del df_merge_claims_w_niss

    # Drop duplicates due to merge
    merge_claims_ats_PID = merge_claims_ats_PID.drop_duplicates(subset=['PRVDR_NUM', 'to_drop_dup'], keep='last')

    # Clean Dataset
    merge_claims_ats_PID = merge_claims_ats_PID.drop(['to_drop_dup'], axis=1)

    # Keep those that matched on provider id
    matched_claims_PID = merge_claims_ats_PID[~merge_claims_ats_PID['ats_match_ind'].isna()]

    #--- Second: Match remaining on NPI same year ---#

    # Take those not matched on Provider ID same year
    notmatched_claims = merge_claims_ats_PID[merge_claims_ats_PID['ats_match_ind'].isna()]

    # Recover memory
    del merge_claims_ats_PID

    # Clean columns in those not matched
    notmatched_claims = notmatched_claims.drop(['NPINUM', 'MCRNUM', 'ID', 'MNAME', 'MLOCADDR', 'MLOCCITY', 'MSTATE', 'ats_match_ind',
                                                'TCN', 'AHA_num', 'Add_1', 'City/Town', 'State', 'ACS_Ver', 'State_Des'], axis=1)

    # Add consecutive numbers to drop duplicated merges
    notmatched_claims = notmatched_claims.reset_index(drop=True)  # To prevent the "SettingWithCopyWarning" message
    notmatched_claims['to_drop_dup'] = notmatched_claims.reset_index().index

    # Merge the ATS trauma data with the analytical claims that did not match before on NPI this time
    merge_claims_ats_NPI = pd.merge(ats_aha_dict[y], notmatched_claims, left_on=['NPINUM'],right_on=['ORG_NPI_NUM'],how='right')

    # Recover memory
    del notmatched_claims

    # Drop duplicates due to merge
    merge_claims_ats_NPI = merge_claims_ats_NPI.drop_duplicates(subset=['ORG_NPI_NUM', 'to_drop_dup'], keep='last')

    # Clean Dataset.
    merge_claims_ats_NPI = merge_claims_ats_NPI.drop(['to_drop_dup'], axis=1)

    # Keep those that matched on NPI
    matched_claims_NPI = merge_claims_ats_NPI[~merge_claims_ats_NPI['ats_match_ind'].isna()]

    #--- Third: Match remaining unmatched on 6-digit Provider ID following year ---#

    # Take those not matched on NPI same year
    notmatched_claims_fr_sameyr = merge_claims_ats_NPI[merge_claims_ats_NPI['ats_match_ind'].isna()]

    # Recover memory
    del merge_claims_ats_NPI

    # Clean columns in those not matched
    notmatched_claims_fr_sameyr = notmatched_claims_fr_sameyr.drop(['NPINUM', 'MCRNUM', 'ID', 'MNAME', 'MLOCADDR', 'MLOCCITY', 'MSTATE', 'ats_match_ind',
                                                'TCN', 'AHA_num', 'Add_1', 'City/Town', 'State', 'ACS_Ver', 'State_Des'], axis=1)

    # Add consecutive numbers to drop duplicated merges
    notmatched_claims_fr_sameyr = notmatched_claims_fr_sameyr.reset_index(drop=True)  # To prevent the "SettingWithCopyWarning" message
    notmatched_claims_fr_sameyr['to_drop_dup'] = notmatched_claims_fr_sameyr.reset_index().index

    # Merge the ATS trauma data with the analytical claims that did not match before on provider id the following year
    merge_claims_ats_PID_nextyr = pd.merge(ats_aha_dict[y+1], notmatched_claims_fr_sameyr, left_on=['MCRNUM'],right_on=['PRVDR_NUM'],how='right') # the "y+1" is to grab the following year ATS data

    # Recover memory
    del notmatched_claims_fr_sameyr

    # Drop duplicates due to merge
    merge_claims_ats_PID_nextyr = merge_claims_ats_PID_nextyr.drop_duplicates(subset=['PRVDR_NUM', 'to_drop_dup'], keep='last')

    # Clean Dataset
    merge_claims_ats_PID_nextyr = merge_claims_ats_PID_nextyr.drop(['to_drop_dup'], axis=1)

    # Keep those that matched on provider id
    matched_claims_PID_nextyr = merge_claims_ats_PID_nextyr[~merge_claims_ats_PID_nextyr['ats_match_ind'].isna()]

    # --- Fourth: Match remaining on NPI following year ---#

    # Take those not matched on provider id following year
    notmatched_claims_nextyr_PID = merge_claims_ats_PID_nextyr[merge_claims_ats_PID_nextyr['ats_match_ind'].isna()]

    # Recover memory
    del merge_claims_ats_PID_nextyr

    # Clean columns in those not matched
    notmatched_claims_nextyr_PID = notmatched_claims_nextyr_PID.drop(['NPINUM', 'MCRNUM', 'ID', 'MNAME', 'MLOCADDR', 'MLOCCITY', 'MSTATE', 'ats_match_ind',
                                                'TCN', 'AHA_num', 'Add_1', 'City/Town', 'State', 'ACS_Ver', 'State_Des'], axis=1)

    # Add consecutive numbers to drop duplicated merges
    notmatched_claims_nextyr_PID = notmatched_claims_nextyr_PID.reset_index(drop=True)  # To prevent the "SettingWithCopyWarning" message
    notmatched_claims_nextyr_PID['to_drop_dup'] = notmatched_claims_nextyr_PID.reset_index().index

    # Merge the ATS trauma data (i.e. those matched with the crosswalk) with the claims on NPI
    merge_claims_ats_NPI_nextyr = pd.merge(ats_aha_dict[y+1], notmatched_claims_nextyr_PID, left_on=['NPINUM'],right_on=['ORG_NPI_NUM'],how='right')

    # Recover memory
    del notmatched_claims_nextyr_PID

    # Drop duplicates due to merge
    merge_claims_ats_NPI_nextyr = merge_claims_ats_NPI_nextyr.drop_duplicates(subset=['ORG_NPI_NUM', 'to_drop_dup'], keep='last')

    # Clean Dataset
    merge_claims_ats_NPI_nextyr = merge_claims_ats_NPI_nextyr.drop(['to_drop_dup'], axis=1)

    # Keep those that matched on NPI
    matched_claims_NPI_nextyr = merge_claims_ats_NPI_nextyr[~merge_claims_ats_NPI_nextyr['ats_match_ind'].isna()]

    #--- CHECK proportion matched using concatenated data ---#
    # To Check, we need to concat with merge_claims_ats_NPI_nextyr and not matched_claims_NPI_nextyr
    df = pd.concat([matched_claims_PID,matched_claims_NPI,matched_claims_PID_nextyr,merge_claims_ats_NPI_nextyr],axis=0) # kept those matched and not matched to calculate proportion matched (merge_claims_ats_NPI_nextyr and NOT matched_claims_NPI_nextyr)
    print('total trauma claims after matching with ats (exclude state when merging) (should be same as above):', df.shape[0])
    print('number matched with ATS (exclude state when merging):', df['ats_match_ind'].sum())
    print('percent of claims matched with ATS (exclude state when merging): ',100*(df['ats_match_ind'].sum())/df.shape[0] , '\n\n')
    del df
    #------------#

    #--- Concatenate only claims that matched with PID (provider id) and NPI same year and next year ---#

    # IMPORTANT: Drop any hospital that have multiple trauma centers with different levels.
    matched_claims_PID = matched_claims_PID[~matched_claims_PID['MCRNUM'].isin(duplicated_provider_id_dict_list[y])]
    matched_claims_PID_nextyr = matched_claims_PID_nextyr[~matched_claims_PID_nextyr['MCRNUM'].isin(duplicated_provider_id_dict_list[y+1])]
        # NOTE: I only dropped hospitals for matched_claims_PID and matched_claims_PID_nextyr (NOT matched_claims_NPI or merge_claims_ats_NPI_nextyr)
        # Reasoning was that there were only ~1% matched using npi so I primarily focused on provider id.

    # Concat only those that matched and store in dictionary
    matched_claims_dict[y] = pd.concat([matched_claims_PID,matched_claims_NPI,matched_claims_PID_nextyr,matched_claims_NPI_nextyr],axis=0) # only kept df matched (matched_claims_NPI_nextyr)

    ####################################################
    print('number matched after dropping hospitals containing more than one different trauma levels (should be slightly less than number matched above): ',matched_claims_dict[y].shape[0],'\n\n')
    ###################################################

    # Recover memory
    del matched_claims_PID
    del matched_claims_NPI
    del matched_claims_PID_nextyr
    del matched_claims_NPI_nextyr

#___________________________ Create analytical sample of claims that do not match ATS/AHA _____________________________#
# I.E. identify hospitals without a trauma center                                                                      #
########################################################################################################################

# First concat all years of AHA and ATS together
ats_aha_list=[] # Create empty list
for y in ats_aha_years:
    ats_aha_list.append(ats_aha_dict[y]) # Append to list
ats_aha_allyears = pd.concat(ats_aha_list,axis=0) # Concat all years of AHA/ATS together

# Create empty dictionary to store analytical claims that did NOT match with ATS/AHA data. i.e. nontrauma centers
unmatched_claims_dict = {}

# Specify years
claims_years = [2012,2013,2014,2015,2016,2017]

for y in claims_years:

    # Read in analytical claims
    df_merge_claims_w_niss = pd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_trnsfr_ind_geo_and_hosquality/{y}.csv',dtype=str)

    # Create a list of indicators
    ind_list = df_merge_claims_w_niss.columns[df_merge_claims_w_niss.columns.str.endswith('_ind')].tolist()

    # Convert to Datetime
    date = ['SRVC_BGN_DT', 'SRVC_END_DT', 'BENE_BIRTH_DT', 'BENE_DEATH_DT', 'BENE_DEATH_DT_FOLLOWING_YEAR',
            'TRNSFR_SRVC_BGN_DT', 'TRNSFR_SRVC_END_DT']
    for d in date:
        df_merge_claims_w_niss[f'{d}'] = pd.to_datetime(df_merge_claims_w_niss[f'{d}'])

    # Convert numeric string columns to floats.
    num = ['niss', 'AGE', 'riss', 'maxais', 'mxaisbr_HeadNeck','BLOOD_PT_FRNSH_QTY','mxaisbr_Extremities','mxaisbr_Face','amb_ind',
           'mxaisbr_Abdomen', 'mxaisbr_Chest', 'combinedscore', 'discharge_death_ind', 'thirty_day_death_ind','ninety_day_death_ind',
           'oneeighty_day_death_ind','threesixtyfive_day_death_ind', 'fall_ind', 'motor_accident_ind','firearm_ind','TRNSFR_IP_IND',
           'ind_trnsfr','parts_ab_ind','median_hh_inc', 'prop_below_pvrty_in_cnty', 'prop_female_in_cnty', 'prop_65plus_in_cnty',
           'metro_micro_cnty', 'prop_w_cllge_edu_in_cnty', 'prop_gen_md_in_cnty', 'prop_hos_w_med_school_in_cnty', 'POSbeds', 'AHAbeds',
           'IMM_3', 'MORT_30_AMI', 'MORT_30_CABG', 'MORT_30_COPD', 'MORT_30_HF', 'MORT_30_PN', 'MORT_30_STK', 'READM_30_HOSP_WIDE', 'SEP_1',
           'fall_ind_CDC','motor_accident_traffic_ind_CDC','firearm_ind_CDC','X58_ind','sec_secondary_trauma_ind','full_dual_ind',
           'partial_dual_ind','cuts_pierce_ind'] + ind_list
    for n in num:
        df_merge_claims_w_niss[f'{n}'] = df_merge_claims_w_niss[f'{n}'].astype('float')

    # Create Variable for TIME FE (for control group)
    df_merge_claims_w_niss['year_fe']=0
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2011, 2011)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2012, 2012)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2013, 2013)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2014, 2014)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2015, 2015)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2016, 2016)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2017, 2017)
    df_merge_claims_w_niss['year_fe'] = df_merge_claims_w_niss['year_fe'].mask(df_merge_claims_w_niss['SRVC_BGN_DT'].dt.year == 2018, 2018)

    # --- First: Match on 6-digit Provider ID all years and keep only unmatched ---#

    # Merge the ATS trauma data (all years) with the claims on provider ID
    merge_claims_ats_PID = pd.merge(ats_aha_allyears, df_merge_claims_w_niss, left_on=['MCRNUM'],right_on=['PRVDR_NUM'],how='right')

    # Recover memory
    del df_merge_claims_w_niss

    # Keep those that did NOT match on provider id
    unmatched_claims_PID = merge_claims_ats_PID[merge_claims_ats_PID['ats_match_ind'].isna()]

    # Recover memory
    del merge_claims_ats_PID

    #--- Second: Match remaining unmatched on NPI same year ---#

    # Clean columns in those NOT matched
    unmatched_claims_PID = unmatched_claims_PID.drop(['NPINUM', 'MCRNUM', 'ID', 'MNAME', 'MLOCADDR', 'MLOCCITY', 'MSTATE', 'ats_match_ind',
                                                'TCN', 'AHA_num', 'Add_1', 'City/Town', 'State', 'ACS_Ver', 'State_Des'], axis=1)

    # Merge the ATS trauma data (all years) with the unmatched claims on NPI
    merge_claims_ats_NPI = pd.merge(ats_aha_allyears, unmatched_claims_PID, left_on=['NPINUM'],right_on=['ORG_NPI_NUM'],how='right')

    # Recover memory
    del unmatched_claims_PID

    # Keep those that did not match on NPI and store in dictionary
    unmatched_claims_dict[y] = merge_claims_ats_NPI[merge_claims_ats_NPI['ats_match_ind'].isna()]

#___ Concat matched data (trauma centers), concat unmatched data (nontrauma centers), prepare data for stata, and export ___#

# Concat all years of matched data (trauma centers)
matched_claims_list=[] # Create empty list
for y in claims_years:
    matched_claims_list.append(matched_claims_dict[y]) # Append to list
final_matched_claims_allyears = pd.concat(matched_claims_list,axis=0) # Concat all years of AHA/ATS together

# Recover memory
del matched_claims_dict
del matched_claims_list

# Concat all years of unmatched data (nontrauma centers)
unmatched_claims_list=[] # Create empty list
for y in claims_years:
    unmatched_claims_list.append(unmatched_claims_dict[y]) # Append to list
final_unmatched_claims_allyears = pd.concat(unmatched_claims_list,axis=0) # Concat all years of AHA/ATS together

# Recover memory
del unmatched_claims_dict
del unmatched_claims_list

# Drop any if hospital name has "children" for the matched only
final_matched_claims_allyears['TCN'] = final_matched_claims_allyears['TCN'].astype(str)
final_matched_claims_allyears = final_matched_claims_allyears[~final_matched_claims_allyears['TCN'].str.contains('|'.join(['child','Child','CHILD']))]

# Keep only US States
us_states=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11',
           '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
           '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
           '34', '35', '36', '37', '38', '39', '41', '42', '43', '44', '45',
           '46', '47', '49', '50', '51', '52', '53']
final_matched_claims_allyears = final_matched_claims_allyears[final_matched_claims_allyears['STATE_CODE'].isin(us_states)]
final_unmatched_claims_allyears = final_unmatched_claims_allyears[final_unmatched_claims_allyears['STATE_CODE'].isin(us_states)]

#--- Create indicator for transfer within two days of service begin date (i.e. "early transfers") ---#
# Important that I used begin date and not end date. If transfer within two days of begin date, then #
# these were considered "early transfers." On the other hand, the ind_trnsfr is if bene's were       #
# transferred within two days of the end date.                                                       #
######################################################################################################

# Create day plus two from begin service date
final_matched_claims_allyears['dayplustwo'] = final_matched_claims_allyears['SRVC_BGN_DT'] + pd.DateOffset(days=2)
final_unmatched_claims_allyears['dayplustwo'] = final_unmatched_claims_allyears['SRVC_BGN_DT'] + pd.DateOffset(days=2)

# Create indicator if transfer happened within two days of BEGIN date
final_matched_claims_allyears['transfer_ind_two_days'] = np.where(((final_matched_claims_allyears['ind_trnsfr']==1)& # ind_trnsfr created from prior codes
                                                          (final_matched_claims_allyears['TRNSFR_SRVC_BGN_DT']<=final_matched_claims_allyears['dayplustwo']))
                                                         ,1,0)
final_unmatched_claims_allyears['transfer_ind_two_days'] = np.where(((final_unmatched_claims_allyears['ind_trnsfr']==1)& # ind_trnsfr created from prior codes
                                                          (final_unmatched_claims_allyears['TRNSFR_SRVC_BGN_DT']<=final_unmatched_claims_allyears['dayplustwo']))
                                                         ,1,0)

#___ IMPORTANT: Drop hospitals that change trauma levels overtime from 13-17 ___#

#--- First: drop using AHA num witin using ATS data (using TCN (hospital name) ---#

# Define columns for ATS data
cols_ats = ['TCN','AHA_num','ACS_Ver', 'State_Des']

# Empty Dict to store df's
df_dict={}

# Create empyty list
df_list=[]

# Specify years (2012 and 2018 were not available)
years=[2013,2014,2015,2016,2017]

for y in years:

    # Read in trauma data from ATS
    df_dict[y] = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/AM_TRAUMA_DATA_FIRST_TAB_UNLOCKED_{y}.xlsx',usecols=cols_ats,dtype=str)

    # Create column specifying year in order to sort values
    df_dict[y]['year'] = f"{y}"

    # Create trauma_lvl column using State_Des
    df_dict[y]['TRAUMA_LEVEL'] = np.where(df_dict[y]['State_Des'] == '1', '1',
                                             np.where(df_dict[y]['State_Des'] == '2', '2',
                                                      np.where(df_dict[y]['State_Des'] == '3', '3',
                                                               np.where(df_dict[y]['State_Des'] == '4', '4',
                                                                        np.where(df_dict[y]['State_Des'] == '5', '5',
                                                                                 np.where(df_dict[y]['State_Des'] == '-',
                                                                                          '-', '-'))))))

    # Prioritize ACS
    df_dict[y].loc[df_dict[y]['ACS_Ver'] == '1', 'TRAUMA_LEVEL'] = '1'
    df_dict[y].loc[df_dict[y]['ACS_Ver'] == '2', 'TRAUMA_LEVEL'] = '2'
    df_dict[y].loc[df_dict[y]['ACS_Ver'] == '3', 'TRAUMA_LEVEL'] = '3'

    # Append df from dictionary to list
    df_list.append(df_dict[y])

# Concat all years of ATS together
ats_13_17 = pd.concat(df_list,axis=0)

# Drop if trauma level is '-'
ats_13_17 = ats_13_17[ats_13_17['TRAUMA_LEVEL']!='-']

# Drop dup on TCN (hospital name) and Trauma level (Use TCN for more specificity since some may have different hospitals but same AHA num)
ats_13_17_dup = ats_13_17.drop_duplicates(subset=['TCN','TRAUMA_LEVEL'])

# Then, keep only the duplicates. The duplicates are the providers that have changed level overtime
ats_13_17_dup = ats_13_17_dup[ats_13_17_dup.duplicated(subset=['TCN','AHA_num'],keep=False)]

# Sort values by hospital name and year
ats_13_17_dup = ats_13_17_dup.sort_values(by=['TCN','year'], ascending=[True,True])

# Put AHA num to a list. These AHA number are hospitals that changed level between 2013-2017
lvl_change_list_aha_num = ats_13_17_dup['AHA_num'].tolist()  # append to list

# Recover memory
del ats_13_17_dup
del ats_13_17
del df_list
del df_dict

# Finally, drop those where there was a change in trauma levels between 13-17 (only for trauma centers) based on AHA number
final_matched_claims_allyears = final_matched_claims_allyears[~final_matched_claims_allyears['AHA_num'].isin(lvl_change_list_aha_num)]

#--- Second: drop remaining hospitals that changed overtime using provider number ---#
# Previously, I used TCN within the ATS data to drop those that changed levels overtime. Now we use the provider id from our analytical sample
# to make sure all hospitals that change levels overtime were dropped

# Create a new columns based on year
final_matched_claims_allyears['year'] = final_matched_claims_allyears['SRVC_BGN_DT'].dt.year

# Again, create trauma_lvl column using state_des
final_matched_claims_allyears['TRAUMA_LEVEL'] = np.where(final_matched_claims_allyears['State_Des'] == '1', '1',
                                                      np.where(final_matched_claims_allyears['State_Des'] == '2', '2',
                                                               np.where(final_matched_claims_allyears['State_Des'] == '3', '3',
                                                                        np.where(final_matched_claims_allyears['State_Des'] == '4', '4',
                                                                                 np.where(final_matched_claims_allyears['State_Des'] == '5', '5',
                                                                                          np.where(final_matched_claims_allyears['State_Des'] == '-',
                                                                                                   '-', '-'))))))

# Again, prioritize ACS
final_matched_claims_allyears.loc[final_matched_claims_allyears['ACS_Ver'] == '1', 'TRAUMA_LEVEL'] = '1'
final_matched_claims_allyears.loc[final_matched_claims_allyears['ACS_Ver'] == '2', 'TRAUMA_LEVEL'] = '2'
final_matched_claims_allyears.loc[final_matched_claims_allyears['ACS_Ver'] == '3', 'TRAUMA_LEVEL'] = '3'

# See if trauma lvl changed overtime based on provider id.
df_match = final_matched_claims_allyears[final_matched_claims_allyears['PRVDR_NUM'].duplicated(keep=False)] # Place duplicated hospital in another df called df_match. These duplicated hospitals may have different trauma levels overtime
df_match = df_match.drop_duplicates(subset=['PRVDR_NUM','TRAUMA_LEVEL'],keep='last') # ATS may have duplicated hospitals with the same trauma level. I want to reduce the DF to just the unique hospital and their trauma level
df_match = df_match.drop_duplicates(subset=['PRVDR_NUM','TRAUMA_LEVEL'],keep=False) # Drop dup again but now keep False. The logic is to drop any hospitals that have the same trauma level over the years between 2013-2017. Thus, leaving only those that changed trauma levels over the years.
df_match = df_match[df_match['TRAUMA_LEVEL']!='-'] # Drop missing data
df_match = df_match[df_match['PRVDR_NUM'].duplicated(keep=False)] # This function would drop any hospital with one row of data (suggesting that they did not change trauma level over the years)
df_match = df_match.sort_values(by=['PRVDR_NUM','year']) # The remaining are hospitals that change levels over the years between 2013-2017

# Put provider num of hospitals that changed trauma level overtime to a list
lvl_change_list_prvdr_num = df_match['PRVDR_NUM'].tolist()

# Delete columns
final_matched_claims_allyears = final_matched_claims_allyears.drop(['year'],axis=1)

# Drop those where there was a change in trauma levels between 13-17 (only for those matched) based on provider number
final_matched_claims_allyears = final_matched_claims_allyears[~final_matched_claims_allyears['PRVDR_NUM'].isin(lvl_change_list_prvdr_num)]
    # Now, I am certain that I accounted for all hospitals that change trauma level overtime because I used both AHA num and provider ID

#___ Clean DF in preparation to merge with hospital quality and analyze using p-score ___#

# Drop unnecessary columns for trauma center and nontrauma center datasets
final_matched_claims_allyears = final_matched_claims_allyears.drop(['dayplustwo','NPINUM','MCRNUM','ID','MNAME','MLOCADDR','MLOCCITY','MSTATE',
                                                                    'TCN','AHA_num','Add_1','City/Town','State','ORG_NPI_NUM','DT_1_MONTH','DT_3_MONTHS',
                                                                    'DT_6_MONTHS','DT_12_MONTHS','ats_match_ind','VALID_DEATH_DT_SW','BENE_RACE_CD',
                                                                    'VALID_DEATH_DT_SW_FOLLOWING_YEAR','DSCHRG_CD','COUNTY_CD','ZIP_CD', 'ecode_1', 'mechmaj1',
                                                                    'mechmin1', 'intent1', 'ecode_2', 'mechmaj2', 'mechmin2', 'intent2', 'ecode_3', 'mechmaj3',
                                                                    'mechmin3', 'intent3', 'ecode_4', 'mechmaj4', 'mechmin4', 'intent4'],axis=1)
final_unmatched_claims_allyears = final_unmatched_claims_allyears.drop(['dayplustwo','NPINUM','MCRNUM','ID','MNAME','MLOCADDR','MLOCCITY','MSTATE',
                                                                        'TCN','AHA_num','Add_1','City/Town','State','ORG_NPI_NUM','DT_1_MONTH','DT_3_MONTHS',
                                                                        'DT_6_MONTHS','DT_12_MONTHS','ats_match_ind','VALID_DEATH_DT_SW','BENE_RACE_CD',
                                                                        'VALID_DEATH_DT_SW_FOLLOWING_YEAR','DSCHRG_CD','COUNTY_CD','ZIP_CD', 'ecode_1', 'mechmaj1',
                                                                        'mechmin1', 'intent1', 'ecode_2', 'mechmaj2', 'mechmin2', 'intent2', 'ecode_3', 'mechmaj3',
                                                                        'mechmin3', 'intent3', 'ecode_4', 'mechmaj4', 'mechmin4', 'intent4'],axis=1)

# Convert all dx columns to strings to resolve ValueError: Column cannot be exported.
dx = [f'dx{i}' for i in range(1,39)]
final_matched_claims_allyears[dx] = final_matched_claims_allyears[dx].astype(str)
final_unmatched_claims_allyears[dx] = final_unmatched_claims_allyears[dx].astype(str)

# Convert to date time
date_col = ['SRVC_BGN_DT', 'SRVC_END_DT', 'BENE_BIRTH_DT', 'TRNSFR_SRVC_BGN_DT','TRNSFR_SRVC_END_DT','BENE_DEATH_DT','BENE_DEATH_DT_FOLLOWING_YEAR']
for d in date_col:
    final_matched_claims_allyears[f'{d}'] = pd.to_datetime(final_matched_claims_allyears[f'{d}'])
    final_unmatched_claims_allyears[f'{d}'] = pd.to_datetime(final_unmatched_claims_allyears[f'{d}'])

# Convert to string
str_col = ['BENE_ID', 'HCPCS_CD', 'STATE_COUNTY_SSA', 'STATE_CODE', 'SEX_IDENT_CD','PRVDR_NUM',
           'RTI_RACE_CD', 'TRNSFR_PRVDR_NUM', 'TRNSFR_ORG_NPI_NUM','TRNSFR_DSCHRG_STUS','ACS_Ver',
           'State_Des']
for s in str_col:
    final_matched_claims_allyears[f'{s}'] = final_matched_claims_allyears[f'{s}'].astype(str)
    final_unmatched_claims_allyears[f'{s}'] = final_unmatched_claims_allyears[f'{s}'].astype(str)

# Rename variables
final_matched_claims_allyears = final_matched_claims_allyears.rename(columns={'combinedscore':'comorbidityscore'})
final_unmatched_claims_allyears = final_unmatched_claims_allyears.rename(columns={'combinedscore':'comorbidityscore'})

# Convert numeric columns to floats
num = ['niss','AGE','riss','maxais','mxaisbr_HeadNeck','mxaisbr_Extremities','BLOOD_PT_FRNSH_QTY','mxaisbr_Face','amb_ind',
       'mxaisbr_Abdomen','mxaisbr_Chest','comorbidityscore','discharge_death_ind','thirty_day_death_ind',
       'ninety_day_death_ind','oneeighty_day_death_ind','threesixtyfive_day_death_ind','TRNSFR_IP_IND','ind_trnsfr','transfer_ind_two_days',
       'fall_ind','motor_accident_ind','firearm_ind','IP_IND','MILES','SH_ind', 'EH_ind', 'NH_ind', 'RH_ind','parts_ab_ind','year_fe','median_hh_inc',
       'prop_below_pvrty_in_cnty', 'prop_female_in_cnty', 'prop_65plus_in_cnty', 'metro_micro_cnty', 'prop_w_cllge_edu_in_cnty', 'prop_gen_md_in_cnty',
       'prop_hos_w_med_school_in_cnty', 'POSbeds', 'AHAbeds', 'IMM_3', 'MORT_30_AMI', 'MORT_30_CABG', 'MORT_30_COPD', 'MORT_30_HF', 'MORT_30_PN',
       'MORT_30_STK', 'READM_30_HOSP_WIDE', 'SEP_1','fall_ind_CDC','motor_accident_traffic_ind_CDC','firearm_ind_CDC','X58_ind','sec_secondary_trauma_ind',
       'full_dual_ind','partial_dual_ind','cuts_pierce_ind']+ind_list
for n in num:
    final_matched_claims_allyears[f'{n}']=final_matched_claims_allyears[f'{n}'].astype('float')
    final_unmatched_claims_allyears[f'{n}']=final_unmatched_claims_allyears[f'{n}'].astype('float')

#--- Create column that sums number of CC/OTCC for each bene ---#
# Specify the 29 CC and OTCC that will be included in model
cc_otcc_list =['AMI_EVER_ind', 'ALZH_EVER_ind', 'ALZH_DEMEN_EVER_ind', 'ATRIAL_FIB_EVER_ind', 'CATARACT_EVER_ind', 'CHRONICKIDNEY_EVER_ind', 'COPD_EVER_ind', 'CHF_EVER_ind', 'DIABETES_EVER_ind', 'GLAUCOMA_EVER_ind',
               'ISCHEMICHEART_EVER_ind', 'DEPRESSION_EVER_ind', 'OSTEOPOROSIS_EVER_ind', 'RA_OA_EVER_ind', 'STROKE_TIA_EVER_ind', 'CANCER_BREAST_EVER_ind', 'CANCER_COLORECTAL_EVER_ind', 'CANCER_PROSTATE_EVER_ind', 'CANCER_LUNG_EVER_ind',
               'CANCER_ENDOMETRIAL_EVER_ind', 'ANEMIA_EVER_ind', 'ASTHMA_EVER_ind', 'HYPERL_EVER_ind', 'HYPERP_EVER_ind', 'HYPERT_EVER_ind', 'HYPOTH_EVER_ind', 'MULSCL_MEDICARE_EVER_ind', 'OBESITY_MEDICARE_EVER_ind', 'EPILEP_MEDICARE_EVER_ind']
    # This is needed to display the total number of chronic conditions for each bene in the balance table.

# For both matched (trauma center) and unmatched (non-trauma center) data, sum the number of CC and OTCC for each bene and put into column labeled cc_otcc_count
final_matched_claims_allyears['cc_otcc_count'] = final_matched_claims_allyears[cc_otcc_list].sum(axis=1)
final_unmatched_claims_allyears['cc_otcc_count'] = final_unmatched_claims_allyears[cc_otcc_list].sum(axis=1)
#---#

# After 2 researchers manually verified the list of hospitals without a trauma center, keep only those we know that the hospital does not have a trauma center
list_possible_t_center = ['050425','100002','100008','100062','330059','390067','030089','050169','070001','100070','130006','310054','330195','390137',
                          '450358','310010','330353','490043','390004','230302','490046','190111','030011','070007','390045','050093','050153']
final_unmatched_claims_allyears = final_unmatched_claims_allyears[~final_unmatched_claims_allyears['PRVDR_NUM'].isin(list_possible_t_center)] # i.e. drop if in the list above

# APPENDIX: Calculate final denominator to put in flowchart (needs to be before I take out those under 65 to obtain correct number since the denom_list_appendix also contains those above 65)
print('APPENDIX: hospital that have multiple levels, change level overtime, or cannot be accurately determines: ', sum(denom_list_appendix)-final_matched_claims_allyears.shape[0]-final_unmatched_claims_allyears.shape[0])

# Keep only those at least 65
final_matched_claims_allyears=final_matched_claims_allyears[final_matched_claims_allyears['AGE']>=65]
final_unmatched_claims_allyears=final_unmatched_claims_allyears[final_unmatched_claims_allyears['AGE']>=65]

# Read out data to stata
final_matched_claims_allyears.to_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_matched_claims_allyears.dta',write_index=False)
final_unmatched_claims_allyears.to_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_unmatched_claims_allyears.dta',write_index=False)

















