# ----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script will merge the analytical sample with comorbidity score data from SAS and begin creating indicators
# for mechanisms of injury and death. Recall that I discussed about the difference between duplicates and claims within
# a stay (i.e. contiguous or overlapping claims). I will deal with the overlapping claims at the end of this script.
# ----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

############################## MERGE CLAIMS WITH COMORBIDITY SCORES FROM SAS ###########################################

# Specify Years (no 2011 since we do not have 2010 data when calculating the comorbidity scores)
years=[2012,2013,2014,2015,2016,2017]

for y in years:

    # Read in analytical data
    analytical_sample = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}_major_trauma',engine='fastparquet')

    # Drop duplicates again since some unique id were duplicated when merging in previous codes
    analytical_sample = analytical_sample.drop_duplicates(subset=['UNIQUE_ID'],keep='first')

    # Convert to date time
    dt_col_list = ['SRVC_BGN_DT','SRVC_END_DT','BENE_BIRTH_DT','BENE_DEATH_DT','BENE_DEATH_DT_FOLLOWING_YEAR']
    for d in dt_col_list:
        analytical_sample[f'{d}'] = pd.to_datetime(analytical_sample[f'{d}'])

    # Convert column to string
    str_col_list = ['DSCHRG_CD']
    for s in str_col_list:
        analytical_sample[f'{s}'] = analytical_sample[f'{s}'].astype(str)

    # Relabel unique_id columns
    analytical_sample = analytical_sample.rename(columns={'UNIQUE_ID':'patid'})
    analytical_sample['patid'] = analytical_sample['patid'].astype(str)

    # Read in data from SAS containing comorbidity scores
    if y in [2016,2017]: # grab gem_min
        comorbid_scores = pd.read_excel(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/appendix/sens_ana_diff_niss_method/comorbidity_score/files_output_from_sas/{y}_gem_min_sas_output.xlsx')
    else:
        comorbid_scores = pd.read_excel(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/comorbidity_score/files_output_from_sas/{y}_sas_output.xlsx')

    # Make sure patid is string
    comorbid_scores['patid'] = comorbid_scores['patid'].astype(str)

    # Merge scores with claims on pat id to obtain the comorbidity scores
    claim_w_comorbidity = pd.merge(analytical_sample,comorbid_scores,how='inner',on=['patid'])

    # # CHECK: make sure all three of the following have the same denominators to ensure that merge was done correctly.
    # print(claim_w_comorbidity.shape[0])
    # print(analytical_sample.shape[0])
    # print(comorbid_scores.shape[0])

    # Recover memory
    del analytical_sample
    del comorbid_scores

    #___ Create indicator to identify different mechanisms of injury ___#

    # Create list of columns
    dx_col_list = [f'dx{i}' for i in range(1,39)] # 38 dx columns

    # Convert dx columns to str
    claim_w_comorbidity[dx_col_list] = claim_w_comorbidity[dx_col_list].astype(str)

    if y in [2012,2013,2014,2015]: # ICD9 (also, no 2011 since we do not have 2010 data to calculate the comorbidity scores)

        #___ Indicators with some based on WISQARS ___#

        # Create indicator for all accidental falls (any starting with E88) for ICD9 (no late effect)
        fall_codes_icd9 = ['E880','E881','E882','E883','E884','E885','E886','E888']
        claim_w_comorbidity['fall_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(fall_codes_icd9))).any(axis='columns'),1,0)

        # Create indicator for all motor vehicle traffic or nontraffic accidents for ICD9 (no late effect) (from wisqars)
        claim_w_comorbidity['motor_accident_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(('E81','E820','E821','E822','E823','E824','E825'))).any(axis='columns'),1,0)

        # Create indicator for all firearm incidents for icd9 (based on cdc: https://www.cdc.gov/nchs/injury/ice/amsterdam1998/amsterdam1998_guncodes.htm)
        claim_w_comorbidity['firearm_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(('E922','E955','E965','E970','E985'))).any(axis='columns'),1,0)

        # Create indicator for cut and pierce
        cuts_pierce_list = ['E920', 'E956', 'E966', 'E986', 'E974', 'E9952']
        claim_w_comorbidity['cuts_pierce_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(cuts_pierce_list))).any(axis='columns'),1,0)

        #___ Indicators based on other CDC E-code groupings (https://www.cdc.gov/injury/wisqars/ecode_matrix.html) and (https://www.cdc.gov/nchs/injury/injury_tools.htm) ___#

        # Create indicator for all accidental falls (any starting with E88) for ICD9 (no late effect)
        fall_codes_icd9_CDC = ['E880','E881','E882','E883','E884','E885','E886','E888'] + ['E957','E9681','E987']
        claim_w_comorbidity['fall_ind_CDC'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(fall_codes_icd9_CDC))).any(axis='columns'),1,0)

        # Create indicator for all motor vehicle traffic only accidents for ICD9 (no late effect) (from wisqars)
        claim_w_comorbidity['motor_accident_traffic_ind_CDC'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(('E81','E9585','E9685','E9885'))).any(axis='columns'),1,0) # wisqars plus cdc

        # Create indicator for all firearm incidents for icd9 (based on cdc: https://www.cdc.gov/nchs/injury/ice/amsterdam1998/amsterdam1998_guncodes.htm)
        claim_w_comorbidity['firearm_ind_CDC'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(('E9220','E9221','E9223','E9228','E9229','E9550','E9551','E9552','E9553','E9554','E9650','E9651','E9652','E9653','E9654'
                                                                                                                            ,'E9794','E9850','E9851','E9852','E9853','E9854','E970'))).any(axis='columns'),1,0)

        #___ Only for icd10 ___#
        claim_w_comorbidity['X58_ind'] = 0 # this ind is zero to concatenate data easily. The X58 indicator will be created for icd 10 2016 and 2017 (see below)

    if y in [2016,2017]: # ICD10

        #___ Indicators with some based on WISQARS ___#

        # Create list of accidental falls from icd10 (based on falls in WISQARS website)
        fall_codes_icd10 = [f'W0{i}' for i in range(0,10)] + [f'W{i}' for i in range(10,20)]+['X80','Y01','Y30']

        # Create indicator for all accidental falls for ICD10
        claim_w_comorbidity['fall_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(fall_codes_icd10))).any(axis='columns'),1,0)

        # Create list of all motor vehicle traffic or nontraffic accidents from icd10. (based on overall motor vehicle codes from wisqars)
        motor_accidents_codes_icd10 = [f'V0{i}' for i in range(2,5)] + [f'V{i}' for i in range(20,80)] + ['V090','V092','V12','V13','V14',
                                       'V190','V191','V192','V194','V195','V196','V803','V804','V805','V810','V811','V820','V821','V83','V84',
                                       'V85','V86','V870','V871','V872','V873','V874','V875','V876','V877','V878','V880','V881','V882','V883',
                                       'V884','V885','V886','V887','V888','V890','V892','X82','Y03','Y32']

        # Create indicator for all accidental motor accidents for ICD10
        claim_w_comorbidity['motor_accident_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(motor_accidents_codes_icd10))).any(axis='columns'),1,0)

        # Create indicator for all firearm incidents for ICD10 (based on firearm codes from wisqars)
        firearm_codes_icd10 = ['W32', 'W33', 'W34', 'X72', 'X73', 'X74', 'X93', 'X94', 'X95', 'Y22', 'Y23', 'Y24', 'Y350']

        # Create indicator for all firearm incidents for ICD10
        claim_w_comorbidity['firearm_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(firearm_codes_icd10))).any(axis='columns'),1,0)

        # Create list for cut and pierce
        cuts_pierce_list_icd10 = ['W25','W26','W27','W28','W29','W45','W46','X78','X99','Y28','Y354']

        # Create indicator for cut and pierce
        claim_w_comorbidity['cuts_pierce_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(cuts_pierce_list_icd10))).any(axis='columns'),1,0)

        #___ Indicators based on GEM crosswalk plus WISQARS ___#

        # Read in gem data
        gem_df = pd.read_stata('/mnt/labshares/sanghavi-lab/data/public_data/data/GEM_conversion/icd10cmtoicd9gem.dta',columns=['icd10cm', 'icd9cm'])
        gem_df['icd10cm'] = gem_df['icd10cm'].astype('str')
        gem_df['icd9cm'] = gem_df['icd9cm'].astype('str')

        # Identify falls only and put into list
        falls = gem_df[gem_df['icd9cm'].str.startswith(tuple(['E880','E881','E882','E883','E884','E885','E886','E888'] + ['E957','E9681','E987']))]
        fall_codes_icd10_CDC = falls['icd10cm'].sort_values().to_list()
        fall_codes_icd10_CDC = fall_codes_icd10_CDC + [f'W0{i}' for i in range(0,10)] + [f'W{i}' for i in range(10,20)]+['X80','Y01','Y30'] # Add other fall codes from wisqars

        # Create indicator for all accidental falls for ICD10
        claim_w_comorbidity['fall_ind_CDC'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(fall_codes_icd10_CDC))).any(axis='columns'),1,0)

        # Identify motor vehicle traffic only and put into list
        motor_df = gem_df[gem_df['icd9cm'].str.startswith(tuple(['E81', 'E9585', 'E9685', 'E9885']))]
        motor_accidents_codes_icd10_CDC = motor_df['icd10cm'].sort_values().to_list()
        motor_accidents_codes_icd10_CDC = motor_accidents_codes_icd10_CDC + ['V304XX','V305XX','V306XX','V307XX','V309XX','V314XX','V315XX','V316XX','V317XX','V319XX','V324XX','V325XX','V326XX','V327XX','V329XX','V334XX','V335XX','V336XX','V337XX','V339XX','V344XX','V345XX',
                                           'V346XX','V347XX','V349XX','V354XX','V355XX','V356XX','V357XX','V359XX','V364XX','V365XX','V366XX','V367XX','V369XX','V374XX','V375XX','V376XX','V377XX','V379XX','V384XX','V385XX','V386XX','V387XX',
                                           'V389XX','V3940X','V3949X','V3950X','V3959X','V3960X','V3969X','V3981X','V3989X','V399XX','V404XX','V405XX','V406XX','V407XX','V409XX','V414XX','V415XX','V416XX','V417XX','V419XX','V424XX','V425XX',
                                           'V426XX','V427XX','V429XX','V4341X','V4342X','V4343X','V4344X','V4351X','V4352X','V4353X','V4354X','V4361X','V4362X','V4363X','V4364X','V4371X','V4372X','V4373X','V4374X','V4391X','V4392X','V4393X',
                                           'V4394X','V444XX','V445XX','V446XX','V447XX','V449XX','V454XX','V455XX','V456XX','V457XX','V459XX','V464XX','V465XX','V466XX','V467XX','V469XX','V474XX','V475XX','V4751X','V4752X','V476XX','V4761X',
                                           'V4762X','V477XX','V479XX','V4791X','V4792X','V484XX','V485XX','V486XX','V487XX','V489XX','V4940X','V4949X','V4950X','V4959X','V4960X','V4969X','V4981X','V4988X','V499XX','V504XX','V505XX','V506XX',
                                           'V507XX','V509XX','V514XX','V515XX','V516XX','V517XX','V519XX','V524XX','V525XX','V526XX','V527XX','V529XX','V534XX','V535XX','V536XX','V537XX','V539XX','V544XX','V545XX','V546XX','V547XX','V549XX',
                                           'V554XX','V555XX','V556XX','V557XX','V559XX','V564XX','V565XX','V566XX','V567XX','V569XX','V574XX','V575XX','V576XX','V577XX','V579XX','V584XX','V585XX','V586XX','V587XX','V589XX','V5940X','V5949X',
                                           'V5950X','V5959X','V5960X','V5969X','V5981X','V5988X','V599XX','V604XX','V605XX','V606XX','V607XX','V609XX','V614XX','V615XX','V616XX','V617XX','V619XX','V624XX','V625XX','V626XX','V627XX','V629XX',
                                           'V634XX','V635XX','V636XX','V637XX','V639XX','V644XX','V645XX','V646XX','V647XX','V649XX','V654XX','V655XX','V656XX','V657XX','V659XX','V664XX','V665XX','V666XX','V667XX','V669XX','V674XX','V675XX',
                                           'V676XX','V677XX','V679XX','V684XX','V685XX','V686XX','V687XX','V689XX','V6940X','V6949X','V6950X','V6959X','V6960X','V6969X','V6981X','V6988X','V699XX','V704XX','V705XX','V706XX','V707XX','V709XX',
                                           'V714XX','V715XX','V716XX','V717XX','V719XX','V724XX','V725XX','V726XX','V727XX','V729XX','V734XX','V735XX','V736XX','V737XX','V739XX','V744XX','V745XX','V746XX','V747XX','V749XX','V754XX','V755XX',
                                           'V756XX','V757XX','V759XX','V764XX','V765XX','V766XX','V767XX','V769XX','V774XX','V775XX','V776XX','V777XX','V779XX','V784XX','V785XX','V786XX','V787XX','V789XX','V7940X','V7949X','V7950X','V7959X',
                                           'V7960X','V7969X','V7981X','V7988X','V799XX','V830XX','V831XX','V832XX','V833XX','V840XX','V841XX','V842XX','V843XX','V850XX','V851XX','V852XX','V853XX','V8601X','V8602X','V8603X','V8604X','V8605X',
                                           'V8606X','V8609X','V8611X','V8612X','V8613X','V8614X','V8615X','V8616X','V8619X','V8621X','V8622X','V8623X','V8624X','V8625X','V8626X','V8629X','V8631X','V8632X','V8633X','V8634X','V8635X','V8636X',
                                           'V8639X','V870XX','V871XX','V872XX','V873XX','V874XX','V875XX','V876XX','V877XX','V878XX','V892XX','V203XX','V204XX','V205XX','V209XX','V213XX','V214XX','V215XX','V219XX','V223XX','V224XX','V225XX',
                                           'V229XX','V233XX','V234XX','V235XX','V239XX','V243XX','V244XX','V245XX','V249XX','V253XX','V254XX','V255XX','V259XX','V263XX','V264XX','V265XX','V269XX','V273XX','V274XX','V275XX','V279XX','V283XX',
                                           'V284XX','V285XX','V289XX','V2940X','V2949X','V2950X','V2959X','V2960X','V2969X','V2981X','V2988X','V299XX','V123XX','V124XX','V125XX','V129XX','V133XX','V134XX','V135XX','V139XX','V143XX','V144XX',
                                           'V145XX','V149XX','V1940X','V1949X','V1950X','V1959X','V1960X','V1969X','V199XX','V0210X','V0211X','V0212X','V02131','V02138','V0219X','V0290X','V0291X','V0292X','V02931','V02938','V0299X','V0310X',
                                           'V0311X','V0312X','V03131','V03138','V0319X','V0390X','V0391X','V0392X','V03931','V03938','V0399X','V0410X','V0411X','V0412X','V04131','V04138','V0419X','V0490X','V0491X','V0492X','V04931','V04938',
                                           'V0499X','V0920X','V0921X','V0929X','V093XX','V8031X','V8032X','V8041X','V8042X','V8051X','V8052X','V811XX','V821XX'] # Add other motor traffic accidents from cdc

        # Create indicator for motor vehicle accident traffic only
        claim_w_comorbidity['motor_accident_traffic_ind_CDC'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(motor_accidents_codes_icd10_CDC))).any(axis='columns'),1,0)

        # Identify guns only and put into list
        guns = gem_df[gem_df['icd9cm'].str.startswith(tuple(['E9220', 'E9221', 'E9223', 'E9228', 'E9229', 'E9550', 'E9551', 'E9552', 'E9553', 'E9554', 'E9650', 'E9651','E9652', 'E9653', 'E9654', 'E9794', 'E9850', 'E9851', 'E9852', 'E9853', 'E9854', 'E970']))]
        firearm_codes_icd10_CDC = guns['icd10cm'].sort_values().to_list()
        firearm_codes_icd10_CDC = firearm_codes_icd10_CDC + ['W320XX','W321XX','W3300X','W3301X','W3302X','W3303X','W3309X','W3310X','W3311X','W3312X','W3313X','W3319X','W3400X','W3409X','W3410X','W3419X'] # Add other gun codes from CDC's list

        # Create indicator for all firearm incidents for ICD10
        claim_w_comorbidity['firearm_ind_CDC'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(tuple(firearm_codes_icd10_CDC))).any(axis='columns'),1,0)

        #___ Check if there are X58's here (dump code, icd10 only) ___#
        # Create indicator for exposure to other specified factors
        claim_w_comorbidity['X58_ind'] = np.where(claim_w_comorbidity[dx_col_list].applymap(lambda x: x.startswith(('X58'))).any(axis='columns'), 1, 0)

    #___ Create Death Indicators ___#

    # Create dates 30, 90, 180, 365 days from emergency response (i.e. service begin date and not service end date)
    claim_w_comorbidity['DT_1_MONTH'] = claim_w_comorbidity['SRVC_BGN_DT'] + pd.DateOffset(months=1)
    claim_w_comorbidity['DT_3_MONTHS'] = claim_w_comorbidity['SRVC_BGN_DT'] + pd.DateOffset(months=3)
    claim_w_comorbidity['DT_6_MONTHS'] = claim_w_comorbidity['SRVC_BGN_DT'] + pd.DateOffset(months=6)
    claim_w_comorbidity['DT_12_MONTHS'] = claim_w_comorbidity['SRVC_BGN_DT'] + pd.DateOffset(months=12)

    # Create death indicator (w/in 30 days....and so on)
    claim_w_comorbidity['discharge_death_ind'] = np.where(
                                                       (claim_w_comorbidity['DSCHRG_CD'].isin(['B','20'])),1,0)
    claim_w_comorbidity['thirty_day_death_ind'] = np.where(
                                                       (claim_w_comorbidity['DSCHRG_CD'].isin(['B','20'])) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT']<=claim_w_comorbidity['DT_1_MONTH']) & (claim_w_comorbidity['VALID_DEATH_DT_SW']=='V')) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR']<=claim_w_comorbidity['DT_1_MONTH']) & (claim_w_comorbidity['VALID_DEATH_DT_SW_FOLLOWING_YEAR']=='V')),
                                                       1,0)
    claim_w_comorbidity['ninety_day_death_ind'] = np.where(
                                                       (claim_w_comorbidity['DSCHRG_CD'].isin(['B','20'])) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT']<=claim_w_comorbidity['DT_3_MONTHS']) & (claim_w_comorbidity['VALID_DEATH_DT_SW']=='V')) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR']<=claim_w_comorbidity['DT_3_MONTHS']) & (claim_w_comorbidity['VALID_DEATH_DT_SW_FOLLOWING_YEAR']=='V')),
                                                       1,0)
    claim_w_comorbidity['oneeighty_day_death_ind'] = np.where(
                                                       (claim_w_comorbidity['DSCHRG_CD'].isin(['B','20'])) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT']<=claim_w_comorbidity['DT_6_MONTHS']) & (claim_w_comorbidity['VALID_DEATH_DT_SW']=='V')) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR']<=claim_w_comorbidity['DT_6_MONTHS']) & (claim_w_comorbidity['VALID_DEATH_DT_SW_FOLLOWING_YEAR']=='V')),
                                                       1,0)
    claim_w_comorbidity['threesixtyfive_day_death_ind'] = np.where(
                                                       (claim_w_comorbidity['DSCHRG_CD'].isin(['B','20'])) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT']<=claim_w_comorbidity['DT_12_MONTHS']) & (claim_w_comorbidity['VALID_DEATH_DT_SW']=='V')) |
                                                       ((claim_w_comorbidity['BENE_DEATH_DT_FOLLOWING_YEAR']<=claim_w_comorbidity['DT_12_MONTHS']) & (claim_w_comorbidity['VALID_DEATH_DT_SW_FOLLOWING_YEAR']=='V')),
                                                       1,0)

    # Calculate Age
    claim_w_comorbidity['SRVC_BGN_DT'] = pd.to_datetime(claim_w_comorbidity['SRVC_BGN_DT'])
    claim_w_comorbidity['BENE_BIRTH_DT'] = pd.to_datetime(claim_w_comorbidity['BENE_BIRTH_DT'])
    claim_w_comorbidity['AGE'] = (claim_w_comorbidity['SRVC_BGN_DT'] - claim_w_comorbidity[
        'BENE_BIRTH_DT']) / np.timedelta64(1, 'Y')  # should equal to age count variable from medpar

    #---- Remove remaining ip duplicates ----#
    # Goal is to remove any remaining duplicates in IP but keep the claim that matched with an ambulance.

    # Subset of ip claims
    ip = claim_w_comorbidity[claim_w_comorbidity['IP_IND']==1]

    # Sort by BENE_ID SRVC_BGN_DT and amb_ind. Within bene's, the 1's in amb_ind are on top and nan's are on the bottom
    ip=ip.sort_values(by=['BENE_ID','amb_ind'])

    # Drop duplicates on BENE_ID and SRVC_BGN_DT
    ip = ip.drop_duplicates(subset=['BENE_ID','SRVC_BGN_DT'],keep='first') # keeping first will allow me to keep the duplicated claim that had amb_ind = 1 (i.e. claim with emergency ambulance ride)

    #---- Remove remaining op duplicates using pandas if claims are contiguous or overlapping ----#
    # Since op is NOT on the stay level, I need to remove claims if two or more claims are contiguous from the beneficiaries staying overnight.
    # While there are many ways to do this, the most convenient way for me was to compare the subsequent row with the previous row (see below for specifics).

    # Subset of op claims
    op = claim_w_comorbidity[claim_w_comorbidity['IP_IND']==0]

    # Count the number of diagnosis codes for op
    diag_col = [f'dx{i}' for i in range(1, 39)]  # Define diagnosis columns
    op[diag_col] = op[diag_col].replace('',np.nan)  # Replace empty strings to count number of diagnosis codes
    op['num_of_diag_codes'] = op[diag_col].count( axis='columns')  # Count diagnosis codes (same as doing .count(1))
    op[diag_col] = op[diag_col].fillna('')  # Fill nan's with empty strings

    # Create number of days in op for each claim. I need this when sorting the dataframe (see below script)
    op['DAYS_IN_HOS'] = op['SRVC_END_DT'] - op['SRVC_BGN_DT']

    # Sort by bene, provider number (hospital), claim from dt (i.e. begin date), number of days in hospital for that claim, and number of diagnosis columns
    op = op.sort_values(by=['BENE_ID','PRVDR_NUM','SRVC_BGN_DT', 'DAYS_IN_HOS', 'num_of_diag_codes'],ascending=[True, True, True, False, False])
        # Within each bene and hospital (provider number), the earliest begin date, the highest number of days in op, and number of diag codes should be at the top
        # This will create a specific ordering so that I can compare the subsequent row with the previous row. If the subsequent row's date of service overlaps or is contiguous then I will
        # mark and drop that row since it's part of a "stay."

    # See notes below on why I needed to loop three times in order to completely remove all contiguous/overlapping claims
    loop = ['1','2','3']
    for i in loop:

        print(f'Loop number {i}') # Track which loop the process is on
        print('# of rows before dropping contiguous/overlapping claims: ', op.shape[0]) # By loop 3, the number of rows before dropping claims should equal the number of rows after dropping claims. Once the rows equal, then I will know that I have dropped all of the contiguous/overlapping claims because there is nothing else to drop.

        # Create indicator if subsequent row is within the previous row's service begin and service end date
        op['overlap_ind'] = 0
        op['overlap_ind'] = op['overlap_ind'].mask((op['SRVC_BGN_DT']>=op['SRVC_BGN_DT'].shift(1)) & # the next row is at least the same date as the previous row
                                                   (op['SRVC_BGN_DT']<=(op['SRVC_END_DT'].shift(1)+timedelta(days=7))) & # the next row's begin date is within seven days after the discharge date of the previous row. I could've used any number but 7 days was a reasonable time interval
                                                   (op['BENE_ID']==op['BENE_ID'].shift(1)) &  # the next row HAS to be the same patient
                                                   (op['PRVDR_NUM']==op['PRVDR_NUM'].shift(1)),1) # the next row HAS to be within the same hospital

        # CHECK to make sure indicator was properly assigned
        # print(op[['overlap_ind','SRVC_BGN_DT','SRVC_END_DT','BENE_ID','PRVDR_NUM']].head(60))
        # print(op[['overlap_ind','SRVC_BGN_DT','SRVC_END_DT','BENE_ID','PRVDR_NUM']].tail(60))

        # Drop those where overlap ind is 1 (i.e. was within the same "visit"/"stay"), but do NOT drop any claims that matched with ambulance
        op = op[~((op['overlap_ind']==1)&(op['amb_ind']!=1))]

        print('# of rows after dropping contiguous claims/overlapping: ', op.shape[0]) # By loop 3, the number of rows before dropping claims should equal the number of rows after dropping claims. Once the rows equal, then I will know that I have dropped all of the contiguous/overlapping claims because there is nothing else to drop.

        # Note on why I need to loop 3 times:
        # - In the above code, I specified that if the subsequent row's claim begin date is within 7 days of the previous row's claim end date, then replace overlap_ind with a 1.
        #   Then, drop the subsequent row if the begin date is within 7 days (overlap_ind==1 ; meaning the subsequent row was contiguous or overlapping with the previous
        #   row) and if the claim did not match with an ambulance claim (amb_ind!=1). However, this script only looks between rows (shift(1)) instead of any contiguous claims
        #   (i.e. it is possible that the next two (or even three) rows is within 7 days). Thus, I would loop through this function three times to ensure that all contiguous
        #   claims were dropped. I chose three since after loop number 3, the number of rows are the same before and after dropping contiguous/overlapping claims. This means that
        #   there are no other contiguous/overlapping claims to drop. There are probably better ways of doing this but this way works perfectly for me.

    # Clean Columns
    op = op.drop(['overlap_ind','num_of_diag_codes','DAYS_IN_HOS'],axis=1)

    # Concat
    ip_op = pd.concat([ip,op],axis=0)

    # Read out data
    ip_op.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/claims_w_comorbid_scores/{y}.csv',index=False,index_label=False)








