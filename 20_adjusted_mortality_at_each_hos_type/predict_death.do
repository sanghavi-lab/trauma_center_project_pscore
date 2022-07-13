*------------------------------------------------------------------------------*
* Project: Trauma Center Project
* Author: Jessy Nguyen
* Last Updated: July 11, 2022
* Description: This script will prepare a file with the adjusted 30-day mortality for figure 2
*------------------------------------------------------------------------------*

quietly{ /* Suppress outputs */

* Input in terminal to run
* do /mnt/labshares/sanghavi-lab/Jessy/hpc_utils/codes/python/trauma_center_project/final_codes_python_sas_R_stata/20_adjusted_mortality_at_each_hos_type/predict_death.do

* Define list for each niss calculation method
local diff_niss_methods `" "gem_min" "tqip" "nis" "'

foreach w of local diff_niss_methods {

    *___ Read in analytical final claims data ___*

    * Read in Data for matched claims (i.e. trauma center data) and then append unmatched claims (i.e. nontrauma center data)
    if inlist("`w'", "tqip"){
        noisily di "`w'"

        * Set working directory
        cd "/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/appendix/sens_ana_diff_niss_method/merged_ats_claims_for_stata/"

        use final_matched_w_hos_qual_roc_max_TQIP.dta, clear
        append using final_unmatched_w_hos_qual_roc_max_TQIP
    }
    else if inlist("`w'", "nis"){
        noisily di "`w'"

        * Set working directory
        cd "/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/appendix/sens_ana_diff_niss_method/merged_ats_claims_for_stata/"

        use final_matched_w_hos_qual_roc_max_NIS.dta, clear
        append using final_unmatched_w_hos_qual_roc_max_NIS
    }
    else if inlist("`w'", "gem_min"){
        noisily di "`w'"

        * Set working directory
        cd "/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/"

        use final_matched_claims_allyears_w_hos_qual.dta, clear /* this is the original analytical file which uses gem_min */
        append using final_unmatched_claims_allyears_w_hos_qual /* this is the original analytical file which uses gem_min */
    }

    *___ Clean data ___*

    * Keep on parts A and B (i.e. FFS)
    keep if parts_ab_ind==1

    * Remove observations with NISS >=76 or 0
    drop if niss>75 | niss < 1

    * Drop those matched with ATS but has a "-" (i.e. unclear of trauma center level)
    drop if (State_Des=="-" & ACS_Ver=="-")

    * Remove observations if missing/unknown
    drop if RTI_RACE_CD=="0"
    drop if SEX_IDENT_CD=="0"

    * Remove if admitted with trauma code but not primarily for trauma
    drop if sec_secondary_trauma_ind==1

    * Remove those under 65
    drop if AGE < 65

    * Creating Bands for niss. May not be used in model but create just in case
    gen niss_bands = .
    replace niss_bands = 1 if niss<=15
    replace niss_bands = 2 if niss>15 & niss<=20
    replace niss_bands = 3 if niss>20 & niss<=25
    replace niss_bands = 4 if niss>25 & niss<=30
    replace niss_bands = 5 if niss>30 & niss<=40
    replace niss_bands = 6 if niss>40

    * Label the niss categorical variables
    label define niss_label 1 "1-15" 2 "16-20" 3 "21-25" 4 "26-30" 5 "31-40" 6 "41+"
    label values niss_bands niss_label

    * Destring and rename variables needed in the model
    destring RTI_RACE_CD, generate(RACE)
    destring SEX_IDENT_CD, generate(SEX)
    destring STATE_COUNTY_SSA, generate(COUNTY) /* maybe I don't need since I want state level FE but create just in case */
    destring STATE_CODE, generate(STATE)

    * Create trauma level variable using state_des first.
    gen TRAUMA_LEVEL = 6 /* Any in TRAUMA_LVL that did not get replaced will be assigned a 6 */
    replace TRAUMA_LEVEL=1 if State_Des == "1"
    replace TRAUMA_LEVEL=2 if State_Des == "2"
    replace TRAUMA_LEVEL=3 if State_Des == "3"
    replace TRAUMA_LEVEL=4 if State_Des == "4"
    replace TRAUMA_LEVEL=4 if State_Des == "5" /* designate level 5 in the same category as level 4 */

    * Prioritize ACS_Ver categorization over State_Des. (Same with Ellen's inventory paper)
    replace TRAUMA_LEVEL=1 if ACS_Ver == "1"
    replace TRAUMA_LEVEL=2 if ACS_Ver == "2"
    replace TRAUMA_LEVEL=3 if ACS_Ver == "3"
    replace TRAUMA_LEVEL=4 if ACS_Ver == "4"
    replace TRAUMA_LEVEL=4 if ACS_Ver == "5" /* designate level 5 in the same category as level 4 */

    * Label the variables that were de-stringed
    label define RACE_label 1 "White" 2 "Black" 3 "Other" 4 "Asian/PI" 5 "Hispanic" 6 "Native Americans/Alaskan Native"
    label values RACE RACE_label
    label define SEX_label 1 "M" 2 "F"
    label values SEX SEX_label
    label define TRAUMA_LEVEL_label 1 "LVL 1" 2 "LVL 2" 3 "LVL 3" 4 "LVL 4/5" 6 "Non-trauma"
    label values TRAUMA_LEVEL TRAUMA_LEVEL_label

    * Rename for shorter name labels because -local- function only takes 32 characters max. -local- will be used later when creating macro's to save in an excel table
    ren BLOOD_PT_FRNSH_QTY BLOODPT
    ren comorbidityscore comorbid
    ren median_hh_inc m_hh_inc
    ren prop_below_pvrty_in_cnty pvrty
    ren prop_female_in_cnty fem_cty
    ren prop_65plus_in_cnty eld_cty
    ren prop_w_cllge_edu_in_cnty cllge
    ren prop_gen_md_in_cnty gen_md
    ren prop_hos_w_med_school_in_cnty med_cty
    ren cc_otcc_count cc_cnt

    *--- Create indicators ---*

    * Create indicator variables for each race
    tabulate RACE, generate(r)

    * Rename columns to appropriate name. I tripled checked that r1 is white, r2 is black, ... , r6 is native american
    ren r1 white, replace
    ren r2 black, replace
    ren r3 other, replace
    ren r4 asian_pi, replace
    ren r5 hispanic, replace
    ren r6 native_am, replace

    * Generate a new "other" race column that is designated as 1 if native american/alaskan native or other is 1. Goal is to combine native american with other since it's a small group
    gen other_n = 0
    replace other_n = 1 if other == 1 | native_am == 1

    * Create indicator variable for metro, micro, or none
    tabulate metro_micro_cnty, generate(m)

    * Rename columns to appropriate name. I tripled check that m1 is not_metro_micro, etc...
    ren m1 not_metro_micro, replace
    ren m2 metro, replace
    ren m3 micro, replace

    * Create indicator for female
    gen female = 1
    replace female = 0  if SEX == 1
    replace female = 1 if SEX == 2 /* only to double check */

    * Create treatment indicators for each Trauma level (level 1, level 2, level 3, level 4/5, nontrauma)
    tabulate TRAUMA_LEVEL, generate(t) /* This will create the following dummy variables: t1 = level 1, t2 = level 2, t3 = level 3, t4 = level 4/5, t5 = nontrauma */

    * Create variable for the log of median household income to account for right skewness
    gen median_hh_inc_ln = ln(m_hh_inc)

    * Create splines for niss and riss to allow for flexibility based on bands from past literature
    mkspline riss1 24 riss2 40 riss3 49 riss4=riss

    * Run logit before predicting
    drop if SRVC_BGN_DT >= mdyhms(12, 01, 2017, 0, 0, 0) /* To use thirty day death, need to drop last 30 days (Dec 2017) */
    noisily logit thirty_day_death_ind i.TRAUMA_LEVEL i.niss_bands riss1-riss4 ib2.RACE ib1.SEX c.AGE##c.AGE c.comorbid##c.comorbid c.BLOODPT ///
    c.mxaisbr_HeadNeck c.mxaisbr_Extremities c.mxaisbr_Chest c.mxaisbr_Abdomen c.maxais i.STATE i.year_fe AMI_EVER_ind ///
    ALZH_EVER_ind ALZH_DEMEN_EVER_ind ATRIAL_FIB_EVER_ind CATARACT_EVER_ind CHRONICKIDNEY_EVER_ind COPD_EVER_ind CHF_EVER_ind ///
    DIABETES_EVER_ind GLAUCOMA_EVER_ind ISCHEMICHEART_EVER_ind DEPRESSION_EVER_ind OSTEOPOROSIS_EVER_ind ///
    RA_OA_EVER_ind STROKE_TIA_EVER_ind CANCER_BREAST_EVER_ind CANCER_COLORECTAL_EVER_ind CANCER_PROSTATE_EVER_ind CANCER_LUNG_EVER_ind ///
    CANCER_ENDOMETRIAL_EVER_ind ANEMIA_EVER_ind ASTHMA_EVER_ind HYPERL_EVER_ind HYPERP_EVER_ind HYPERT_EVER_ind HYPOTH_EVER_ind ///
    MULSCL_MEDICARE_EVER_ind OBESITY_MEDICARE_EVER_ind EPILEP_MEDICARE_EVER_ind median_hh_inc_ln pvrty fem_cty eld_cty ///
    ib0.metro_micro_cnty cllge gen_md med_cty full_dual_ind, cformat(%5.3f)

    * Predict using margins to categorize them into niss_bands & save results
    noisily margins niss_bands, at(TRAUMA_LEVEL==(1 2 6)) noesample saving(nissvdeath_predicted_`w'_all, replace) /*noesample just allows to predict on another dataset if like train on medicare and predict on medicaid*/

    }

    }

}

