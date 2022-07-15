#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will merge the final claim with the risk adjusted Hospital Surgical Quality Scores (which is the binary
#              30-day mortality indicator minus the risk-adjusted surgical predicted mortality (i.e. probability of mortality)).
#              This script will also identify hospitals serving at least 90 bene's with major trauma.
#----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

######################### MERGE FINAL ANALYTICAL CLAIMS WITH HOSPITAL QUALITY MEASURE ##################################
# Hospital quality scores/measure was created in stata

# Disable SettingWithCopyWarning (i.e. chained assignments)
pd.options.mode.chained_assignment = None

# Read in final analytical claims file (the matched contains the trauma levels and the unmatched are all of the nontrauma hospitals)
final_matched_claims_allyears = pd.read_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_matched_claims_allyears.dta')
final_unmatched_claims_allyears = pd.read_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_unmatched_claims_allyears.dta')

#___ Find number of bene served by each hospital within each year ___#
final_matched_claims_allyears['num_bene_served'] = 1
final_unmatched_claims_allyears['num_bene_served'] = 1

volume_t_lvl = final_matched_claims_allyears.groupby(['year_fe','PRVDR_NUM'])['num_bene_served'].sum().to_frame().reset_index()
volume_nt = final_unmatched_claims_allyears.groupby(['year_fe','PRVDR_NUM'])['num_bene_served'].sum().to_frame().reset_index()

final_matched_claims_allyears = final_matched_claims_allyears.drop(['num_bene_served'],axis=1)
final_unmatched_claims_allyears = final_unmatched_claims_allyears.drop(['num_bene_served'],axis=1)
final_matched_claims_allyears = pd.merge(final_matched_claims_allyears,volume_t_lvl,on=['year_fe','PRVDR_NUM'])
final_unmatched_claims_allyears = pd.merge(final_unmatched_claims_allyears,volume_nt,on=['year_fe','PRVDR_NUM'])
#____#

# APPENDIX: Find number of claims with less than or equal to 90 bene's served
print('Appendix: ',final_matched_claims_allyears[(final_matched_claims_allyears['num_bene_served']<=90)&(final_matched_claims_allyears['TRAUMA_LEVEL'].isin(['1','2']))].shape[0]+final_unmatched_claims_allyears[final_unmatched_claims_allyears['num_bene_served']<=90].shape[0])

# Keep only those serving at least 90 bene's within a year
final_matched_claims_allyears = final_matched_claims_allyears[((final_matched_claims_allyears['num_bene_served']>90)&(final_matched_claims_allyears['TRAUMA_LEVEL'].isin(['1','2'])))|
                                                              (final_matched_claims_allyears['TRAUMA_LEVEL'].isin(['3','4','5','-']))]
final_unmatched_claims_allyears = final_unmatched_claims_allyears[final_unmatched_claims_allyears['num_bene_served']>90]
    # Due to sample size, I kept everyone for trauma level 3,4,5 but dropped those serving less than 90 for nontrauma, level 1, and level 2

# # CHECK: mean median and spread (deciles)
# print('nt: mean, median, deciles', final_unmatched_claims_allyears[final_unmatched_claims_allyears['num_bene_served']>90]['num_bene_served'].mean(),final_unmatched_claims_allyears[final_unmatched_claims_allyears['num_bene_served']>90]['num_bene_served'].median(),'\n',final_unmatched_claims_allyears[final_unmatched_claims_allyears['num_bene_served']>90]['num_bene_served'].quantile(np.arange(0.1, 1, 0.1)))
# print('lvl1: mean, median, deciles', final_matched_claims_allyears[(final_matched_claims_allyears['TRAUMA_LEVEL']=='1')&(final_matched_claims_allyears['num_bene_served']>90)]['num_bene_served'].mean(),final_matched_claims_allyears[(final_matched_claims_allyears['TRAUMA_LEVEL']=='1')&(final_matched_claims_allyears['num_bene_served']>90)]['num_bene_served'].median(),'\n',final_matched_claims_allyears[(final_matched_claims_allyears['TRAUMA_LEVEL']=='1')&(final_matched_claims_allyears['num_bene_served']>90)]['num_bene_served'].quantile(np.arange(0.1, 1, 0.1)))
# print('lvl2: mean, median, deciles', final_matched_claims_allyears[(final_matched_claims_allyears['TRAUMA_LEVEL']=='2')&(final_matched_claims_allyears['num_bene_served']>90)]['num_bene_served'].mean(),final_matched_claims_allyears[(final_matched_claims_allyears['TRAUMA_LEVEL']=='2')&(final_matched_claims_allyears['num_bene_served']>90)]['num_bene_served'].median(),'\n',final_matched_claims_allyears[(final_matched_claims_allyears['TRAUMA_LEVEL']=='2')&(final_matched_claims_allyears['num_bene_served']>90)]['num_bene_served'].quantile(np.arange(0.1, 1, 0.1)))

# Drop TRAUMA_LEVEL column
final_matched_claims_allyears = final_matched_claims_allyears.drop(['TRAUMA_LEVEL'],axis=1)
final_unmatched_claims_allyears = final_unmatched_claims_allyears.drop(['TRAUMA_LEVEL'],axis=1)

# Read in data containing risk-adjusted surgical mortality quality measure
hos_qual_measure = pd.read_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/data_to_run_glm_in_stata/hos_qual_calculated_from_stata.dta',columns=['PRVDR_NUM','year_fe','prob_diff_residual'])

# Average the hospital quality measure (prob_diff_residual) by hospital and year
hos_qual_measure = hos_qual_measure.groupby(['year_fe','PRVDR_NUM'])['prob_diff_residual'].mean().to_frame().reset_index()

# Merge with matched analytical sample (trauma centers)
final_matched_claims_allyears_w_hos_qual = pd.merge(final_matched_claims_allyears,hos_qual_measure,how='left',on=['PRVDR_NUM','year_fe'])

# Merge with unmatched analytical sample (non-trauma centers)
final_unmatched_claims_allyears_w_hos_qual = pd.merge(final_unmatched_claims_allyears,hos_qual_measure,how='left',on=['PRVDR_NUM','year_fe'])

# # CHECK denominators
# print(final_matched_claims_allyears_w_hos_qual.shape[0])
# print(final_matched_claims_allyears.shape[0])
# print(final_unmatched_claims_allyears_w_hos_qual.shape[0])
# print(final_unmatched_claims_allyears.shape[0])

#___ Clean DF in preparation to analyze data using p score and regression ___#

# Convert to date time
date_col = ['SRVC_BGN_DT', 'SRVC_END_DT', 'BENE_BIRTH_DT', 'TRNSFR_SRVC_BGN_DT','TRNSFR_SRVC_END_DT','BENE_DEATH_DT','BENE_DEATH_DT_FOLLOWING_YEAR']
for d in date_col:
    final_matched_claims_allyears_w_hos_qual[f'{d}'] = pd.to_datetime(final_matched_claims_allyears_w_hos_qual[f'{d}'])
    final_unmatched_claims_allyears_w_hos_qual[f'{d}'] = pd.to_datetime(final_unmatched_claims_allyears_w_hos_qual[f'{d}'])

# Convert to string
str_col = ['patid','BENE_ID', 'HCPCS_CD', 'STATE_COUNTY_SSA', 'STATE_CODE', 'SEX_IDENT_CD','PRVDR_NUM',
           'RTI_RACE_CD', 'TRNSFR_PRVDR_NUM', 'TRNSFR_ORG_NPI_NUM','TRNSFR_DSCHRG_STUS','ACS_Ver',
           'State_Des']
for s in str_col:
    final_matched_claims_allyears_w_hos_qual[f'{s}'] = final_matched_claims_allyears_w_hos_qual[f'{s}'].astype(str)
    final_unmatched_claims_allyears_w_hos_qual[f'{s}'] = final_unmatched_claims_allyears_w_hos_qual[f'{s}'].astype(str)

# Create a list of indicators to use when converting to float
ind_list = final_matched_claims_allyears_w_hos_qual.columns[final_matched_claims_allyears_w_hos_qual.columns.str.endswith('_ind')].tolist()

# Convert numeric columns to floats
num = ['niss','AGE','riss','maxais','mxaisbr_HeadNeck','mxaisbr_Extremities','BLOOD_PT_FRNSH_QTY','mxaisbr_Face','amb_ind',
       'mxaisbr_Abdomen','mxaisbr_Chest','comorbidityscore','discharge_death_ind','thirty_day_death_ind',
       'ninety_day_death_ind','oneeighty_day_death_ind','threesixtyfive_day_death_ind','TRNSFR_IP_IND','ind_trnsfr','transfer_ind_two_days',
       'fall_ind','motor_accident_ind','firearm_ind','IP_IND','MILES','SH_ind', 'EH_ind', 'NH_ind', 'RH_ind','parts_ab_ind','year_fe','median_hh_inc',
       'prop_below_pvrty_in_cnty', 'prop_female_in_cnty', 'prop_65plus_in_cnty', 'metro_micro_cnty', 'prop_w_cllge_edu_in_cnty', 'prop_gen_md_in_cnty',
       'prop_hos_w_med_school_in_cnty', 'POSbeds', 'AHAbeds', 'IMM_3', 'MORT_30_AMI', 'MORT_30_CABG', 'MORT_30_COPD', 'MORT_30_HF', 'MORT_30_PN',
       'MORT_30_STK', 'READM_30_HOSP_WIDE', 'SEP_1', 'prob_diff_residual','cc_otcc_count']+ind_list
for n in num:
    final_matched_claims_allyears_w_hos_qual[f'{n}']=final_matched_claims_allyears_w_hos_qual[f'{n}'].astype('float')
    final_unmatched_claims_allyears_w_hos_qual[f'{n}']=final_unmatched_claims_allyears_w_hos_qual[f'{n}'].astype('float')

# Read out data to stata
final_matched_claims_allyears_w_hos_qual.to_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_matched_claims_allyears_w_hos_qual.dta',write_index=False)
final_unmatched_claims_allyears_w_hos_qual.to_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_unmatched_claims_allyears_w_hos_qual.dta',write_index=False)



