#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will create additional indicators for the logit model - chronic conditions and ambulance type.
# Also, the script will use pandas (not dask) to read in the data again and merge AHRF (Area Health Resources
# Files) data containing variables for geographic adjustments, Dartmouth data containing hospital
# characteristics (e.g. hospital beds), and Hospital Compare containing hospital quality measures.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import dask.dataframe as dd
from datetime import timedelta
import pandas as pd
import numpy as np

############################################ MODULE FOR CLUSTER ########################################################

# Read in libraries to use cluster
from dask.distributed import Client
client = Client('127.0.0.1:3500')

################################ CREATE INDICATORS FOR AMBULANCE TYPES, CC, and OTCC ###################################

# Specify years
years = [2012,2013,2014,2015,2016,2017]

for y in years:

    # Read in analytical claims data
    hos_claims = dd.read_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_and_transfers/{y}.csv', dtype=str)

    # Convert to Datetime
    date = ['SRVC_BGN_DT', 'SRVC_END_DT', 'BENE_BIRTH_DT', 'BENE_DEATH_DT', 'BENE_DEATH_DT_FOLLOWING_YEAR',
            'DT_1_MONTH', 'DT_3_MONTHS', 'DT_6_MONTHS', 'DT_12_MONTHS']
    for d in date:
        hos_claims[f'{d}'] = dd.to_datetime(hos_claims[f'{d}'])

    # Convert numeric string columns to floats. If I'm not working with Ellen's sample, then I don't need the sev columns
    num = ['niss', 'AGE', 'riss', 'maxais', 'mxaisbr_HeadNeck','BLOOD_PT_FRNSH_QTY','mxaisbr_Extremities','mxaisbr_Face',
           'mxaisbr_Abdomen', 'mxaisbr_Chest', 'combinedscore', 'discharge_death_ind', 'thirty_day_death_ind','ninety_day_death_ind',
           'amb_ind','oneeighty_day_death_ind','threesixtyfive_day_death_ind', 'fall_ind', 'motor_accident_ind','firearm_ind',
           'TRNSFR_IP_IND', 'ind_trnsfr','IP_IND','parts_ab_ind','fall_ind_CDC','motor_accident_traffic_ind_CDC','firearm_ind_CDC','X58_ind']
    for n in num:
        hos_claims[f'{n}']=hos_claims[f'{n}'].astype('float')

    # Issues exporting this particular column. Need to specify as string again to export it.
    hos_claims['VALID_DEATH_DT_SW_FOLLOWING_YEAR'] = hos_claims['VALID_DEATH_DT_SW_FOLLOWING_YEAR'].astype('str')

    # Convert provider ID and NPI to object in order to merge
    hos_claims['PRVDR_NUM'] = hos_claims['PRVDR_NUM'].astype('str')
    hos_claims['ORG_NPI_NUM'] = hos_claims['ORG_NPI_NUM'].astype('str')

    #--- Create pick up indicators ---#

    # Create a list of modifier codes
    mod_list = ['SH','EH','NH','RH']

    # Create indicator for each pick up locations (only 4 pick locations S,E,N,R)
    for m in mod_list:

        # Create a column of zero first
        hos_claims[f'{m}_ind'] = 0

        # Use Mask function to replace 0 with 1 if modifier begins with a pick up location, ends with dropoff of H (hospital), and the length of the string is two
        if m in ['SH']:
            hos_claims[f'{m}_ind'] = hos_claims[f'{m}_ind'].mask((hos_claims['HCPCS_1ST_MDFR_CD']=='SH')|(hos_claims['HCPCS_2ND_MDFR_CD']=='SH'),1)
        if m in ['EH']:
            hos_claims[f'{m}_ind'] = hos_claims[f'{m}_ind'].mask((hos_claims['HCPCS_1ST_MDFR_CD']=='EH')|(hos_claims['HCPCS_2ND_MDFR_CD']=='EH'),1)
        if m in ['NH']:
            hos_claims[f'{m}_ind'] = hos_claims[f'{m}_ind'].mask((hos_claims['HCPCS_1ST_MDFR_CD']=='NH')|(hos_claims['HCPCS_2ND_MDFR_CD']=='NH'),1)
        if m in ['RH']:
            hos_claims[f'{m}_ind'] = hos_claims[f'{m}_ind'].mask((hos_claims['HCPCS_1ST_MDFR_CD']=='RH')|(hos_claims['HCPCS_2ND_MDFR_CD']=='RH'),1)

    # Drop both mod columns
    hos_claims = hos_claims.drop(['HCPCS_1ST_MDFR_CD','HCPCS_2ND_MDFR_CD'],axis=1)

    # SANITY CHECK: adding up all indicators should get the same proportions from either method
    print('proportion using amb_ind: ',hos_claims['amb_ind'].sum().compute()/hos_claims.shape[0].compute())
    print('proportion using the pick up and drop off indicator (should be similar to above prop matched): ',(hos_claims['SH_ind'].sum().compute()+hos_claims['EH_ind'].sum().compute()+hos_claims['NH_ind'].sum().compute()+hos_claims['RH_ind'].sum().compute())/hos_claims.shape[0].compute())

    #___ CC and OTCC ___#

    # Read in CC and OTCC data
    if y in [2012,2013,2014,2015,2016]:
        MBSFCC = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFCC/csv/mbsf_cc_summary.csv',sep=',',engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False)
        MBSFOTCC = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFOTCC/csv/mbsf_oth_cc_summary.csv',sep=',',engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False)
    else: # 2017. Again, the file for OTCC was labeled differently from 2011-2016 so I just used an if/then function for convenience.
        MBSFCC = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFCC/csv/mbsf_cc_summary.csv',sep=',',engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False)
        MBSFOTCC = dd.read_csv(f'/mnt/data/medicare-share/data/{y}/MBSFOTCC/csv/mbsf_othcc_summary.csv',sep=',',engine='c', dtype='object', na_filter=False,skipinitialspace=True, low_memory=False)

    # Gather only date columns from the MBSF file and put into list
    CC_dates = MBSFCC.columns[MBSFCC.columns.str.endswith('_EVER')].tolist()
    OTCC_dates = MBSFOTCC.columns[MBSFOTCC.columns.str.endswith('_EVER')].tolist()

    # Keep only BENE_ID and date columns using list created above
    MBSFCC = MBSFCC[['BENE_ID']+CC_dates]
    MBSFOTCC = MBSFOTCC[['BENE_ID']+OTCC_dates]

    # Merge on BENE (claims with CC first)
    claims_merge_CC = dd.merge(hos_claims,MBSFCC,on=['BENE_ID'],how='left')

    # Recover memory
    del hos_claims
    del MBSFCC

    # Merge on BENE (claims with OTCC next)
    claims_merge_CC_OTCC = dd.merge(claims_merge_CC,MBSFOTCC,on=['BENE_ID'],how='left')

    # Recover memory
    del claims_merge_CC
    del MBSFOTCC

    # Gather only date columns for CC and OTCC and put into list
    ALL_CC_dates = claims_merge_CC_OTCC.columns[claims_merge_CC_OTCC.columns.str.endswith('_EVER')].tolist()

    # Convert all CC to datetime
    for d in ALL_CC_dates:
        claims_merge_CC_OTCC[f'{d}'] = dd.to_datetime(claims_merge_CC_OTCC[f'{d}'])

    # Create indicator for each CC if the date of occurrence for the CC or OTCC is pre-emergency response (meaning condition was present during emergency response)
    for c in ALL_CC_dates:

        # Create a column of zero's first
        claims_merge_CC_OTCC[f'{c}_ind'] = 0

        # Use Mask function to replace 0 with 1 if date of occurrence for the CC or OTCC is before the service end date (i.e. discharge date). This means that this chronic condition was present.
        claims_merge_CC_OTCC[f'{c}_ind'] = claims_merge_CC_OTCC[f'{c}_ind'].mask(claims_merge_CC_OTCC['SRVC_BGN_DT']>claims_merge_CC_OTCC[f'{c}'],1)

        # Drop CC date column
        claims_merge_CC_OTCC = claims_merge_CC_OTCC.drop([f'{c}'],axis=1)

    # Read out data
    claims_merge_CC_OTCC.to_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_trnsfr_and_ind/{y}/',engine='fastparquet',compression='gzip')


##### MERGE AHRF, DARTMOUTH, & HOSPITAL COMPARE DATA CONTAINING GEOGRAPHIC ADJUSTMENTS AND HOSPITAL CHARACTERISTICS #####
# Note that I am switching to pandas here. Main reason is convenience since the data is not large anymore               #
#########################################################################################################################

# Specify years (no 2011 since we do not have 2010 data to calculate the comorbidity scores)
years = [2012,2013,2014,2015,2016,2017]

for y in years:

    #_________ Merge AHRF file to obtain geographic controls ____________#
    # Note that the columns have names like f12424. I did not convert these columns to the english name yet. This was a bit more convenient for me (a bit lazy lol)

    # Read in AHRF file. File from AHRF is not disaggregated by year. For some geographic controls, AHRF only has an aggregate measure (e.g. 2013-2017 inclusive).
    ahrf_df = pd.read_csv('/mnt/labshares/sanghavi-lab/data/public_data/data/AHRF_SAS/ahrf2019.csv',dtype=str,usecols=['f00002','f12424','f00010','f1322615','f1440813','f1198415','f1390715','f1408315','f1406718',
                                                                                                      'f1448213','f0461015','f0885715','f0886917','f0887017','f0887117','f0888117','f0888217','f0888317'])

    # Convert numeric columns to float
    num_list = ['f1322615','f1440813','f1198415','f1390715','f1408315','f1406718','f1448213','f0461015','f0885715','f0886917','f0887017','f0887117','f0888117','f0888217','f0888317']
    for n in num_list:
        ahrf_df[f'{n}']=ahrf_df[f'{n}'].astype(float)

    # Rename variables according to AHRF 2018-2019 Technical Documentation (see below)
    ahrf_df = ahrf_df.rename(columns={'f1322615':'median_hh_inc','f1440813':'pct_below_pvrty_in_cnty','f1198415':'tot_pop_in_cnty',
                                      'f1390715':'tot_female_in_cnty','f1408315':'tot_65plus_in_cnty','f1406718':'metro_micro_cnty',
                                      'f1448213':'pct_w_cllge_edu_in_cnty','f0461015':'tot_gen_md_in_cnty','f0885715':'tot_md_in_cnty',
                                      'f0886917':'tot_st_g_hos_in_cnty','f0887017':'tot_st_ng_hos_in_cnty','f0887117':'tot_lt_hos_in_cnty',
                                      'f0888117':'tot_st_g_hos_w_med_school_in_cnty','f0888217':'tot_stng_lt_hos_w_med_school_in_cnty','f0888317':'tot_vet_hos_w_med_school_in_cnty',
                                      'f00002':'FIPS_st_cnty_cd','f12424':'state_abbr','f00010':'county_name'})
        # F13226-15 : Median Household Income (2015) (reminder to put in model as log)
        # F14408-13 : % Persons Below Poverty Level (13-17)
        # F11984-15 : Population Estimate (total 2015)
        # F13907-15 : Pop Total Female (2015)
        # F14083-15 : Population Estimate 65+ (2015)
        # F14067-18 : CBSA Indicator Code (0 = Not, 1 = Metro, 2 = Micro)
        # F14482-13 : % Persons 25+ w/4+ Yrs College (13-17)
        # F04610-15 : MD's, Total Gen Pract, Total Non-Fed (2015)
        # F08857-15 : Total Active M.D.'s, Total Non-Fed (2015)
        # F08869-17 : # Short Term General Hosps (2017)
        # F08870-17 : # Short Term Non-General Hosps (2017)
        # F08871-17 : # Long Term Hospitals (2017)
        # F08881-17 : # Hosp W/Medical School Affiln (Short Term General Hospitals) (2017)
        # F08882-17 : # Hosp W/Medical School Affiln (ST Non-Gen + Long Term Hosps) (2017)
        # F08883-17 : # Hosp W/Medical School Affiln (Veterans Hospitals) (2017)

    #--- Create proportions ---#

    # Convert percent to decimal form for the following
    ahrf_df['prop_below_pvrty_in_cnty'] = ahrf_df['pct_below_pvrty_in_cnty']/100 # % Persons Below Poverty Level
    ahrf_df['prop_w_cllge_edu_in_cnty'] = ahrf_df['pct_w_cllge_edu_in_cnty']/100 # % Persons 25+ w/4+ Yrs College

    # Calculate proportions for the following
    ahrf_df['prop_female_in_cnty'] = ahrf_df['tot_female_in_cnty']/ahrf_df['tot_pop_in_cnty'] # Number of female over total population in a county
    ahrf_df['prop_65plus_in_cnty'] = ahrf_df['tot_65plus_in_cnty']/ahrf_df['tot_pop_in_cnty'] # Number of persons 65+ over total population in a county
    ahrf_df['prop_gen_md_in_cnty'] = ahrf_df['tot_gen_md_in_cnty']/ahrf_df['tot_md_in_cnty'] # Number of general practice physicians (MD) over all MD's in a county
    ahrf_df['prop_hos_w_med_school_in_cnty'] = (ahrf_df['tot_st_g_hos_w_med_school_in_cnty']+ahrf_df['tot_stng_lt_hos_w_med_school_in_cnty']+ahrf_df['tot_vet_hos_w_med_school_in_cnty'])/(ahrf_df['tot_lt_hos_in_cnty']+ahrf_df['tot_st_ng_hos_in_cnty']+ahrf_df['tot_st_g_hos_in_cnty']) # Number of hospitals (short and long term) with medical school affiliation over all hospitals (short and long term) in a county

    # Keep only relevant variables for the model
    ahrf_df = ahrf_df[['FIPS_st_cnty_cd','state_abbr','county_name','median_hh_inc','prop_below_pvrty_in_cnty','prop_female_in_cnty','prop_65plus_in_cnty','metro_micro_cnty','prop_w_cllge_edu_in_cnty','prop_gen_md_in_cnty','prop_hos_w_med_school_in_cnty']]

    #--- Merge ssa to fips crosswalk with ahrf dataframe ---#
    # CLaims use SSA format but AHRF use FIPS. So I need a crosswalk

    # Read in ssa to fips crosswalk. I will use SSA format instead of fips
    xwalk_ssa_county = pd.read_excel('/mnt/labshares/sanghavi-lab/data/public_data/data/ssa_fips_state_county2016/FY 2016 FR Cty CBSA Xwalk and CBSA Con Cty.xlsx',usecols=['County','State','SSA State county code','FIPS State county code'],dtype='str')

    # Merge ahrf_df with the crosswalk to obtain the ssa state/county code format
    ahrf_df_w_ssa = pd.merge(ahrf_df,xwalk_ssa_county,how='left',left_on=['FIPS_st_cnty_cd'],right_on=['FIPS State county code'])

    # Keep only those that matched
    ahrf_df_w_ssa = ahrf_df_w_ssa[~ahrf_df_w_ssa['FIPS State county code'].isna()]

    # Drop irrelevant columns
    ahrf_df_w_ssa = ahrf_df_w_ssa.drop(['County', 'State','FIPS State county code','FIPS_st_cnty_cd'],axis=1)

    #--- Create Column of state abbreviations from STATE_CODE for the analytical claims sample ---#

    # Read in claims data after amb, CC, and OTCC indicators were created using dask (see above)
    hos_claims = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_trnsfr_and_ind/{y}/',engine='fastparquet')

    # Needed in order to prevent "ValueError: cannot reindex from a duplicate axis"
    hos_claims = hos_claims.reset_index(drop=True)

    # Rename State Column from SSA to Abbreviation
    hos_claims['STATE_ABB'] = hos_claims['STATE_CODE'].replace(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11',
                                                                '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
                                                                '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33',
                                                                '34', '35', '36', '37', '38', '39', '41', '42', '43', '44', '45',
                                                                '46', '47', '49', '50', '51', '52', '53'],
                                                               ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
                                                                'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
                                                                'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
                                                                'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
                                                                'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'])

    #--- Claims: Combine county column with state column (both SSA format) to uniquely create SSA State county code column for each claim ---#
    # i.e. just combining state column with county column into new column called STATE_COUNTY_SSA

    # Define list of individual states and their SSA codes
    state_dict={'AL':'01', 'AK':'02', 'AZ':'03', 'AR':'04', 'CA':'05', 'CO':'06', 'CT':'07', 'DE':'08', 'DC':'09', 'FL':'10', 'GA':'11',
                'HI':'12', 'ID':'13', 'IL':'14', 'IN':'15', 'IA':'16', 'KS':'17', 'KY':'18', 'LA':'19', 'ME':'20', 'MD':'21', 'MA':'22',
                'MI':'23', 'MN':'24', 'MS':'25', 'MO':'26', 'MT':'27', 'NE':'28', 'NV':'29', 'NH':'30', 'NJ':'31', 'NM':'32', 'NY':'33',
                'NC':'34', 'ND':'35', 'OH':'36', 'OK':'37', 'OR':'38', 'PA':'39', 'RI':'41', 'SC':'42', 'SD':'43', 'TN':'44', 'TX':'45',
                'UT':'46', 'VT':'47', 'VA':'49', 'WA':'50', 'WV':'51', 'WI':'52', 'WY':'53'}

    for i in state_dict:

        # Correct county code to include Stata SSA in order to uniquely identify each county (matched data)
        hos_claims.loc[(hos_claims['COUNTY_CD'].str.len() == 3)&(hos_claims['STATE_ABB']==i), 'STATE_COUNTY_SSA'] = f'{state_dict[i]}'+hos_claims['COUNTY_CD']
        hos_claims.loc[(hos_claims['COUNTY_CD'].str.len() == 2)&(hos_claims['STATE_ABB']==i), 'STATE_COUNTY_SSA'] = f'{state_dict[i]}'+'0'+hos_claims['COUNTY_CD']
        hos_claims.loc[(hos_claims['COUNTY_CD'].str.len() == 1)&(hos_claims['STATE_ABB']==i), 'STATE_COUNTY_SSA'] = f'{state_dict[i]}'+'00'+hos_claims['COUNTY_CD']
            # Long story but claims data is missing some zero's so I needed to re-add the zero's depending on the character length of the COUNTY_CD

    #--- Finally: merge claims data with ahrf file on SSA state/county code to obtain all geographic data ---#

    # Merge on SSA code
    hos_claims_w_geo = pd.merge(hos_claims,ahrf_df_w_ssa,how='left',left_on=['STATE_COUNTY_SSA'],right_on=['SSA State county code'])

    # Keep those that merged
    hos_claims_w_geo = hos_claims_w_geo[~hos_claims_w_geo['county_name'].isna()]

    # Drop irrelevant columns
    hos_claims_w_geo = hos_claims_w_geo.drop(['state_abbr','SSA State county code'],axis=1)

    #____________ Merge Dartmouth file to obtain hospital characteristics _____________#

    # Read in Dartmouth files by year
    dartmouth_hos_char = pd.read_csv(f'/mnt/labshares/sanghavi-lab/data/public_data/data/dartmouth_hospital_data/hosp{y}.csv',usecols=['PROVIDER','PSTATE','AHAbeds','POSbeds'],dtype=str, encoding= 'unicode_escape')

    # Rename variables for consistency
    dartmouth_hos_char = dartmouth_hos_char.rename(columns={'PROVIDER':'PRVDR_NUM','PSTATE':'STATE_ABB'})

    # Merge on provider id and state columns to obtain hospital beds from dartmouth. Keep all claims.
    hos_claims_w_geo_dart = pd.merge(hos_claims_w_geo,dartmouth_hos_char,how='left',on=['PRVDR_NUM','STATE_ABB'])

    #____________ Merge Hospital Compare file to obtain hospital quality measures _____________#

    #--- Timely_and_Effective_Care-Hospital ---#

    # Read in data
    TE_care = pd.read_csv('/mnt/labshares/sanghavi-lab/data/public_data/data/hos_compare_data2021/Timely_and_Effective_Care-Hospital.csv',usecols=['Facility ID','Measure ID','Score'],dtype=str)

    # Replace "Not Available" with nan in score column
    TE_care['Score'] = TE_care['Score'].replace('Not Available',np.nan)

    # Keep only relevant rows before concatenating and transforming to wide data
    TE_care = TE_care[TE_care['Measure ID'].isin(['SEP_1','IMM_3'])]

    # Convert percents to proportion for consistency
    TE_care['Score'] = TE_care['Score'].astype(float) # Convert to float first
    TE_care['Score'] = TE_care['Score']/100 # Convert to decimal form

    #--- Complications_and_Deaths-Hospital ---#

    # Read in data
    comp_death = pd.read_csv('/mnt/labshares/sanghavi-lab/data/public_data/data/hos_compare_data2021/Complications_and_Deaths-Hospital.csv',usecols=['Facility ID','Measure ID','Score'],dtype=str)

    # Replace "Not Available" with nan in score column
    comp_death['Score'] = comp_death['Score'].replace('Not Available',np.nan)

    # Keep only relevant rows before concatenating and transforming to wide data
    comp_death = comp_death[comp_death['Measure ID'].isin(['MORT_30_STK','MORT_30_AMI','MORT_30_CABG','MORT_30_COPD','MORT_30_HF','MORT_30_PN'])]

    # Convert percents to proportion for consistency
    comp_death['Score'] = comp_death['Score'].astype(float) # Convert to float first
    comp_death['Score'] = comp_death['Score']/100 # Convert to decimal form

    #--- Unplanned_Hospital_Visits-Hospital ---#

    # Read in data
    unplanned_visits = pd.read_csv('/mnt/labshares/sanghavi-lab/data/public_data/data/hos_compare_data2021/Unplanned_Hospital_Visits-Hospital.csv',usecols=['Facility ID','Measure ID','Score'],dtype=str)

    # Replace "Not Available" with nan in score column
    unplanned_visits['Score'] = unplanned_visits['Score'].replace('Not Available',np.nan)

    # Keep only relevant rows before concatenating and transforming to wide data
    unplanned_visits = unplanned_visits[unplanned_visits['Measure ID'].isin(['READM_30_HOSP_WIDE'])] # 30 day readmission

    # Convert percents to proportion for consistency
    unplanned_visits['Score'] = unplanned_visits['Score'].astype(float) # Convert to float first
    unplanned_visits['Score'] = unplanned_visits['Score']/100 # Convert to decimal form

    #--- Concat all datasets together from HOSPITAL COMPARE then convert from long to wide ---#

    # Concat
    hos_comp_concat = pd.concat([TE_care,comp_death,unplanned_visits],axis=0)

    # Convert from long to wide
    hos_comp_concat = hos_comp_concat.pivot(index='Facility ID',columns='Measure ID',values='Score').reset_index()

    # Rename columns for consistency
    hos_comp_concat = hos_comp_concat.rename(columns={'Facility ID':'PRVDR_NUM'})


    #--- Finally: merge quality measures with claims ---#

    # Merge to gather quality measure from Hospital Compare data
    hos_claims_w_geo_dart_hoscomp = pd.merge(hos_claims_w_geo_dart,hos_comp_concat,how='left',on=['PRVDR_NUM'])

    # CHECK DENOMINATORS
    print(hos_claims_w_geo_dart_hoscomp.shape[0])
    print(hos_claims_w_geo_dart.shape[0])
    print(list(hos_claims_w_geo_dart_hoscomp.columns))

    # Read out data
    hos_claims_w_geo_dart_hoscomp.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_trnsfr_ind_geo_and_hosquality/{y}.csv',
        index=False, index_label=False)












