*------------------------------------------------------------------------------*
* Project: Trauma Center Project
* Author: Jessy Nguyen
* Last Updated: July 11, 2022
* Description: This script will analyze result using p score method and generate tables for the balance table and p score
* results.
*------------------------------------------------------------------------------*

quietly{ /* Suppress outputs */

* for the final code in final folder
* do /mnt/labshares/sanghavi-lab/Jessy/hpc_utils/codes/python/trauma_center_project/final_codes_python_sas_R_stata/23_pscore_analysis_in_stata/p_score_analysis.do

* Set working directory
cd "/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/"

*___ Read in analytical final claims data ___*

* Read in Data for matched claims (i.e. trauma center data)
use final_matched_claims_allyears_w_hos_qual.dta, clear

* Append unmatched claims (i.e. nontrauma center data)
append using final_unmatched_claims_allyears_w_hos_qual

*___ Clean data ___*

* Only if needed
*keep if cont_enroll_ind==1

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

* Creating Bands for niss. May not be used in model but create just in case
gen niss_bands = .
replace niss_bands = 1 if niss<=15
replace niss_bands = 2 if niss>15 & niss<=24
replace niss_bands = 3 if niss>24 & niss<=40
replace niss_bands = 4 if niss>40 & niss<=49
replace niss_bands = 5 if niss>49

* Label the niss categorical variables
label define niss_label 1 "1-15" 2 "16-24" 3 "25-40" 4 "41-49" 5 "50+"
label values niss_bands niss_label

* Gen indicators for niss categories
gen n16_24 = 0
replace n16_24 = 1 if niss_bands == 2
gen n25_40 = 0
replace n25_40 = 1 if niss_bands == 3
gen n41 = 0
replace n41 = 1 if (niss_bands == 4) | (niss_bands == 5)

* Gen indicators for comorbidity  categories
gen cs1 = 0
replace cs1 = 1 if (comorbidityscore<1)
gen cs1_3 = 0
replace cs1_3 = 1 if ((comorbid>=1)&(comorbid<4))
gen cs4 = 0
replace cs4 = 1 if ((comorbid>=4))

* Gen indicators for num chronic condition categories
gen cc1_6 = 0
replace cc1_6 = 1 if (cc_otcc_count<7)
gen cc7 = 0
replace cc7 = 1 if (cc_otcc_count>=7)

* Destring and rename variables needed in the model
destring RTI_RACE_CD, generate(RACE)
destring SEX_IDENT_CD, generate(SEX)
destring STATE_COUNTY_SSA, generate(COUNTY) /* maybe I don't need since I want state level FE but create just in case */
destring STATE_CODE, generate(STATE)

* Create trauma level variable using state_des first. I created this variable in prior scripts but it's more convenient just to create this again as a factor variable in Stata.
gen TRAUMA_LEVEL = 6 /* Any in TRAUMA_LVL that did not get replaced will be assigned a 6 */
replace TRAUMA_LEVEL=1 if State_Des == "1"
replace TRAUMA_LEVEL=2 if State_Des == "2"
replace TRAUMA_LEVEL=3 if State_Des == "3"
replace TRAUMA_LEVEL=4 if State_Des == "4"
replace TRAUMA_LEVEL=4 if State_Des == "5" /* designate level 5 in the same category as level 4 */

* Prioritize ACS_Ver categorization over State_Des. There is no level 4 or 5 in the ACS_Ver column
replace TRAUMA_LEVEL=1 if ACS_Ver == "1"
replace TRAUMA_LEVEL=2 if ACS_Ver == "2"
replace TRAUMA_LEVEL=3 if ACS_Ver == "3"

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

*--- Loop through different analysis groups ---*

* Only the longer list is needed if we want to look at additional subgroups. Otherwise, we use the smaller list.
*local analysis_groups `" "all" "amb" "gun" "cut" "fal" "mot" "m16" "m25" "m41" "ip" "ipF" "ipM" "ipA" "op" "opF" "opM" "opA" "' /* all is all hospital sample, amb is only bene's who came to the hospital on an emergency ambulance, gun is firearms only, cut is nonfirearm-related penetrating injury, fal is short for falls, and mot is short for motor vehicle accidents. m16 is niss 16-24, m25 is 25-40, and m41 is 41+ */
local analysis_groups `" "all" "amb" "gun" "fal" "mot" "' /* all is all hospital sample, amb is only bene's who came to the hospital on an emergency ambulance, gun is firearms only, cut is nonfirearm-related penetrating injury, fal is short for falls, and mot is short for motor vehicle accidents. */

foreach a of local analysis_groups {

    noisily di "Analysis for group: `a'"

    * Since I have nested "preserve" and "restore" functions, I use the "tempfile/save" function instead. Another way to bypass restrictions from "preserve" and "restore" and allow me to restore to original analytical file.
    tempfile all_hospital_file
    save `all_hospital_file'

    if "`a'" == "gun"{
        keep if firearm_ind == 1
    }

    if "`a'" == "cut"{
        keep if cuts_pierce_ind == 1
    }

    if "`a'" == "fal"{
        keep if fall_ind == 1
    }

    if "`a'" == "mot"{
        keep if motor_accident_ind == 1
    }

    if "`a'" == "m16"{
        keep if niss_bands == 2
    }

    if "`a'" == "m25"{
        keep if niss_bands == 3
    }

    if "`a'" == "m41"{
        keep if (niss_bands == 4 | niss_bands == 5)
    }

    if "`a'" == "ip"{
        keep if IP_IND == 1
    }

    if "`a'" == "ipF"{
        keep if (IP_IND == 1 & fall_ind == 1)
    }

    if "`a'" == "ipM"{
        keep if (IP_IND == 1 & motor_accident_ind == 1)
    }

    if "`a'" == "ipA"{
        keep if (IP_IND == 1 & amb_ind == 1)

        * Create variable for ambulance service type
        gen amb_type = .
        replace amb_type = 1 if HCPCS_CD == "A0429"
        replace amb_type = 2 if inlist(HCPCS_CD, "A0427", "A0433")

        * Label the ambulance type columns
        label define amb_type_label 1 "BLS" 2 "ALS"
        label values amb_type amb_type_label
    }

    if "`a'" == "op"{
        keep if IP_IND == 0
    }

    if "`a'" == "opF"{
        keep if (IP_IND == 0 & fall_ind == 1)
    }

    if "`a'" == "opM"{
        keep if (IP_IND == 0 & motor_accident_ind == 1)
    }

    if "`a'" == "opA"{
        keep if (IP_IND == 0 & amb_ind == 1)

        * Create variable for ambulance service type
        gen amb_type = .
        replace amb_type = 1 if HCPCS_CD == "A0429"
        replace amb_type = 2 if inlist(HCPCS_CD, "A0427", "A0433")

        * Label the ambulance type columns
        label define amb_type_label 1 "BLS" 2 "ALS"
        label values amb_type amb_type_label
    }

    if "`a'" == "amb"{
        keep if amb_ind==1

        * Create variable for ambulance service type
        gen amb_type = .
        replace amb_type = 1 if HCPCS_CD == "A0429"
        replace amb_type = 2 if inlist(HCPCS_CD, "A0427", "A0433")

        * Label the ambulance type columns
        label define amb_type_label 1 "BLS" 2 "ALS"
        label values amb_type amb_type_label
    }

    *--- Create indicators ---*
    * Note: these will be used to check balance and create treatment variable for p-score analysis

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

    **************** P-Score Analysis *********************

    * Create 10 new comparison groups (i.e. lvl 1 vs 2, lvl 1 vs 3, ... , lvl 4/5 vs nontrauma)
    if inlist("`a'", "gun", "cut"){ /* because the penetrating subgroup has a small sample size, I cannot use levels 4/5. So, there are only 6 comparison groups instead of 10. */
        local treatment_list `" "one_v_two" "' /* Sample size for t lvl 3/4/5 and nt are too small for the penetrating subgroup. */

        * Comment in if you want to combine nt with lvl 2. Comment out the above
        *local treatment_list `" "one_v_nt" "' /* Sample size for t lvl 3/4/5 are too small for the penetrating subgroup. */
        *replace TRAUMA_LEVEL = 6 if TRAUMA_LEVEL == 2 /* Combine level 2 with nontrauma and label value as nontrauma (i.e. "6") for convenience */
    }
    else if inlist("`a'", "m41"){ /* because the penetrating subgroup has a small sample size, I cannot use levels 4/5. So, there are only 6 comparison groups instead of 10. */
        local treatment_list `" "one_v_two" "one_v_three" "one_v_nt" "two_v_three" "two_v_nt" "three_v_nt" "'
    }
    else{ /* 10 comparison groups if analysis group is NOT firearm */
        local treatment_list `" "one_v_nt" "one_v_two" "two_v_nt" "'
    }

    * Loop through each comparison group
    foreach t of local treatment_list {

        preserve

        if "`t'" == "one_v_two"{
            keep if (TRAUMA_LEVEL == 1 | TRAUMA_LEVEL == 2)
            ren t1 treatment /* t1 is where level 1 is 1 and every other level is zero */
        }

        else if "`t'" == "one_v_three"{
            keep if (TRAUMA_LEVEL == 1 | TRAUMA_LEVEL == 3)
            ren t1 treatment
        }

        else if "`t'" == "one_v_four_five"{
            keep if (TRAUMA_LEVEL == 1 | TRAUMA_LEVEL == 4)
            ren t1 treatment
        }

        else if "`t'" == "one_v_nt"{
            keep if (TRAUMA_LEVEL == 1 | TRAUMA_LEVEL == 6)
            ren t1 treatment
        }

        else if "`t'" == "two_v_three"{
            keep if (TRAUMA_LEVEL == 2 | TRAUMA_LEVEL == 3)
            ren t2 treatment /* t2 is where level 2 is 1 and every other level is zero */
        }

        else if "`t'" == "two_v_four_five"{
            keep if (TRAUMA_LEVEL == 2 | TRAUMA_LEVEL == 4)
            ren t2 treatment
        }

        else if "`t'" == "two_v_nt"{
            keep if (TRAUMA_LEVEL == 2 | TRAUMA_LEVEL == 6)
            ren t2 treatment
        }

        else if "`t'" == "three_v_four_five"{
            keep if (TRAUMA_LEVEL == 3 | TRAUMA_LEVEL == 4)
            ren t3 treatment /* t3 is where level 3 is 1 and every other level is zero */
        }

        else if "`t'" == "three_v_nt"{
            keep if (TRAUMA_LEVEL == 3 | TRAUMA_LEVEL == 6)
            ren t3 treatment
        }

        else if "`t'" == "four_five_v_nt"{
            keep if (TRAUMA_LEVEL == 4 | TRAUMA_LEVEL == 6)
            ren t4 treatment /* t4 is where level 4/5 is 1 and every other level is zero */
        }

        *--------------- Check balance before weighting ---------------*

        * Create list named covariates. Indicators were created above already
        local covariates niss riss AGE comorbid female white black other_n asian_pi hispanic BLOODPT m_hh_inc pvrty fem_cty eld_cty metro cllge gen_md med_cty cc_cnt CFI n16_24 n25_40 n41 cs1 cs1_3 cs4 cc1_6 cc7

        * Loop through each covariate to check mean and if there is statistically significant differences between the two groups (lvl1 and lvl2)
        foreach c of local covariates{

            * Check t test (I also use the regression way to double check [see below])
            ttest `c', by(treatment)
            return list

            * Save the means of the treatment and control groups to put into excel sheet
            if inlist("`c'","female","white", "black", "other_n", "asian_pi", "hispanic")|inlist("`c'","pvrty", "fem_cty", "eld_cty", "metro", "cllge", "gen_md", "med_cty")|inlist("`c'","n16_24", "n25_40", "n41", "cs1", "cs1_3", "cs4", "cc1_6")|inlist("`c'","cc7"){ /* use conditional function to convert these to percents. Also need to use two inlist functions separated with "|" to bypass error "Expression too long" */
                local `a'`t'`c'_p = string(`r(p)',"%3.2f") /* Save macro of p value and keep at 2 decimal places. "_p" is p value */
                local `a'`t'`c'tn = string(`r(mu_2)'*100,"%9.1f") /* Save macro for treatment group's mean and keep decimal at tenth place. "tn" is treatment group's mean before weighting. Multiply by 100 to convert to percent */
                local `a'`t'`c'cn = string(`r(mu_1)'*100,"%9.1f") /* Save macro for control group's mean and keep decimal at tenth place. "cn" is control group's mean before weighting. Multiply by 100 to convert to percent */
            }
            else{ /* While the above requires conversion to percents, other variables like niss or AGE do not */
                local `a'`t'`c'_p = string(`r(p)',"%3.2f") /* Save macro of p value and keep at 2 decimal places. "_p" is p value */
                local `a'`t'`c'tn = string(`r(mu_2)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at tenth place. "tn" is treatment group's mean before weighting */
                local `a'`t'`c'cn = string(`r(mu_1)',"%9.1f") /* Save macro for control group's mean and keep decimal at tenth place. "cn" is control group's mean before weighting */
            }

            * Sanity check: Regression covariates on treatment to see if there is significant difference between hospital types
            reg `c' treatment

            *___ STANDARDIZED DIFFERENCE FOR DICHOTOMOUS VARIABLES ___*
            if inlist("`c'", "female", "white", "black", "other_n", "asian_pi", "hispanic")|inlist("`c'", "metro")|inlist("`c'","n16_24", "n25_40", "n41", "cs1", "cs4", "cc1_6", "cc7"){
            summarize `c' if treatment == 1     /* Apply weights for treatment group */
            local `a'`t'`c'ppt = r(mean)                      /* Obtain weighted probability from treatment */
            summarize `c' if treatment == 0     /* Apply weights for control group */
            local `a'`t'`c'ppc = r(mean)                      /* Obtain weighted probability from control */

            * Calculate STD DIFF
            local `a'`t'`c'SDB = string((``a'`t'`c'ppt' - ``a'`t'`c'ppc')/sqrt(((``a'`t'`c'ppt' * (1 - ``a'`t'`c'ppt')) + (``a'`t'`c'ppc' * (1 - ``a'`t'`c'ppc')))/2),"%9.2f")
            }

            *___STANDARDIZED DIFFERENCE FOR CONTINUOUS VARIABLES___*
            else{
            summarize `c' if treatment == 1     /* Apply weights for treatment group */
            local `a'`t'`c'mmt = r(mean)                      /* Obtain weighted means from treatment */
            local `a'`t'`c'sst = r(sd)                        /* Obtain weighted standard error from treatment */
            summarize `c' if treatment == 0     /* Apply weights for control group */
            local `a'`t'`c'mmc = r(mean)                      /* Obtain weighted means from treatment */
            local `a'`t'`c'ssc = r(sd)                        /* Obtain weighted standard error from treatment */

            * Calculate STD DIFF
            local `a'`t'`c'SDB = string((``a'`t'`c'mmt' - ``a'`t'`c'mmc')/sqrt(( (``a'`t'`c'sst')^2 + (``a'`t'`c'ssc')^2 )/2),"%9.2f")
            }

            *___ Split by categories for niss (16-24 25-40 41+) ___*
            if inlist("`c'","niss"){
                ttest `c' if niss_bands==2, by(treatment) /*16-24*/
                local `a'`t'`c'_p2 = string(`r(p)',"%3.2f")
                local `a'`t'`c'tn2 = string(`r(mu_2)',"%9.1f") /*treatment group*/
                local `a'`t'`c'cn2 = string(`r(mu_1)',"%9.1f") /*control group*/

                ttest `c' if niss_bands==3, by(treatment) /*25-40*/
                local `a'`t'`c'_p3 = string(`r(p)',"%3.2f")
                local `a'`t'`c'tn3 = string(`r(mu_2)',"%9.1f") /*treatment group*/
                local `a'`t'`c'cn3 = string(`r(mu_1)',"%9.1f") /*control group*/

                ttest `c' if (niss_bands==4) | (niss_bands==5), by(treatment) /*41+*/
                local `a'`t'`c'_p45 = string(`r(p)',"%3.2f")
                local `a'`t'`c'tn45 = string(`r(mu_2)',"%9.1f") /*treatment group*/
                local `a'`t'`c'cn45 = string(`r(mu_1)',"%9.1f") /*control group*/
            }

            *___ Split by categories for comorbidity (<1 1-3 >=4) ___*
            if inlist("`c'","comorbid"){
                ttest `c' if comorbid<1, by(treatment)
                local `a'`t'`c'_p1 = string(`r(p)',"%3.2f")
                local `a'`t'`c'tn1 = string(`r(mu_2)',"%9.1f") /*treatment group*/
                local `a'`t'`c'cn1 = string(`r(mu_1)',"%9.1f") /*control group*/

                ttest `c' if (comorbid>=1)&(comorbid<4), by(treatment)
                local `a'`t'`c'_p2 = string(`r(p)',"%3.2f")
                local `a'`t'`c'tn2 = string(`r(mu_2)',"%9.1f") /*treatment group*/
                local `a'`t'`c'cn2 = string(`r(mu_1)',"%9.1f") /*control group*/

                ttest `c' if (comorbid>=4), by(treatment)
                local `a'`t'`c'_p3 = string(`r(p)',"%3.2f")
                local `a'`t'`c'tn3 = string(`r(mu_2)',"%9.1f") /*treatment group*/
                local `a'`t'`c'cn3 = string(`r(mu_1)',"%9.1f") /*control group*/
            }
        }

        *_____________ Naive Regression ______________*

        * Check if the denominator is correct in each loop
        tab TRAUMA_LEVEL
        tab treatment, matcell(dist_table)

        * Save the sample size for the treatment and control groups to put into excel sheet. Note that this is the unweighted number of observations. It may change after using overlap weights for p-score.
        local `a'_`t'_ts = dist_table[2,1] /* Save sample size of treatment group as a macro. "_ts" is treatment sample size */
        local `a'_`t'_cs = dist_table[1,1] /* Save sample size of control group as a macro. "_cs" is control sample size */
        local `a'_`t'_sum = dist_table[2,1] + dist_table[1,1] /* Save total as a macro */

        * Need this to restore file before running naive regression
        tempfile original
        save `original'

        * Outcome: Discharge Death
        reg discharge_death_ind treatment, cformat(%5.3f)

        *--- Outcome: Thirty Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(12, 01, 2017, 0, 0, 0) /* To use thirty day death, need to drop last 30 days (Dec 2017) */
        reg thirty_day_death_ind treatment, cformat(%5.3f) /* Naive Regression */

        * Save results from thirty day death for excel
        matrix list r(table) /* View regression result from thirty day death as a table */
        matrix reg_table = r(table) /* Convert regression result from thirty day death to a table and save as reg_table */
        local `a'_`t'_30cn = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_30cn" is thirty day death treatment coefficient for the naive regression */
        local `a'_`t'_30lbn = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_30lbn" is thirty day death lower bound for the naive regression */
        local `a'_`t'_30ubn = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_30ubn" is thirty day death upper bound for the naive regression */

        *--- Outcome: Ninety Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(10, 01, 2017, 0, 0, 0) /* To use ninety day death, need to drop last 90 days (Oct-Dec 2017) */
        reg ninety_day_death_ind treatment, cformat(%5.3f)

        * Save results from ninety day death for excel
        matrix list r(table) /* View regression result from ninety day death as a table */
        matrix reg_table = r(table) /* Convert regression result from ninety day death to a table and save as reg_table */
        local `a'_`t'_90cn = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_90cn" is ninety day death treatment coefficient for the naive regression */
        local `a'_`t'_90lbn = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_90lbn" is ninety day death lower bound for the naive regression */
        local `a'_`t'_90ubn = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_90ubn" is ninety day death upper bound for the naive regression */

        *--- Outcome: One-Eighty Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(07, 01, 2017, 0, 0, 0) /* To use One-Eighty day death, need to drop last 180 days (Jul-Dec 2017) */
        reg oneeighty_day_death_ind treatment, cformat(%5.3f)

        * Save results from One-Eighty day death for excel
        matrix list r(table) /* View regression result from One-Eighty day death as a table */
        matrix reg_table = r(table) /* Convert regression result from One-Eighty day death to a table and save as reg_table */
        local `a'_`t'_18cn = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_18cn" is shortened for 180 day death treatment coefficient for the naive regression */
        local `a'_`t'_18lbn = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_18lbn" is shortened for 180 day death lower bound for the naive regression */
        local `a'_`t'_18ubn = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_18ubn" is shortened for 180 day death upper bound for the naive regression */

        *--- Outcome: Three hundred Sixty Five Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(01, 01, 2017, 0, 0, 0) /* To use Three hundred Sixty Five day death, need to drop last 365 days (Jan-Dec 2017) */
        reg threesixtyfive_day_death_ind treatment, cformat(%5.3f)

        * Save results from Three hundred Sixty Five day death for excel
        matrix list r(table) /* View regression result from Three hundred Sixty Five day death as a table */
        matrix reg_table = r(table) /* Convert regression result from Three hundred Sixty Five day death to a table and save as reg_table */
        local `a'_`t'_1cn = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_1cn" is shortened for 1 year death treatment coefficient for the naive regression */
        local `a'_`t'_1lbn = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_1lbn" is shortened for 1 year death lower bound for the naive regression */
        local `a'_`t'_1ubn = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_1ubn" is shortened for 1 year death upper bound for the naive regression */

        * Restore file (need clear option)
        use `original',clear

        *____________ Step 1: Model P(D=1|X) using Logit ________________*

        * Create variable for the log of median household income to account for right skewness
        gen median_hh_inc_ln = ln(m_hh_inc)

        * Create splines for niss and riss to allow for flexibility based on bands from past literature
        mkspline niss1 24 niss2 40 niss3 49 niss4=niss
        mkspline riss1 24 riss2 40 riss3 49 riss4=riss

        if inlist("`a'","amb","ipA", "opA"){ /* If subgroup is ambulance rides, then I need to specify a slightly different model with ambulance-specific covariates: amb type, miles, and modifiers */

            noisily di "Creating appendix table for logistic output for group: `a' with comparison group `t'"

            * Step 1 logit (since this is for those who took an ambulance, I controlled for ib1.amb_type c.MILES SH_ind EH_ind NH_ind RH_ind)
            asdoc logit treatment niss1-niss4 riss1-riss4 ib2.RACE ib1.SEX c.AGE##c.AGE c.comorbid##c.comorbid c.BLOODPT ///
            c.mxaisbr_HeadNeck c.mxaisbr_Extremities c.mxaisbr_Chest c.mxaisbr_Abdomen c.maxais i.STATE i.year_fe AMI_EVER_ind ///
            ALZH_EVER_ind ALZH_DEMEN_EVER_ind ATRIAL_FIB_EVER_ind CATARACT_EVER_ind CHRONICKIDNEY_EVER_ind COPD_EVER_ind CHF_EVER_ind ///
            DIABETES_EVER_ind GLAUCOMA_EVER_ind ISCHEMICHEART_EVER_ind DEPRESSION_EVER_ind OSTEOPOROSIS_EVER_ind ///
            RA_OA_EVER_ind STROKE_TIA_EVER_ind CANCER_BREAST_EVER_ind CANCER_COLORECTAL_EVER_ind CANCER_PROSTATE_EVER_ind CANCER_LUNG_EVER_ind ///
            CANCER_ENDOMETRIAL_EVER_ind ANEMIA_EVER_ind ASTHMA_EVER_ind HYPERL_EVER_ind HYPERP_EVER_ind HYPERT_EVER_ind HYPOTH_EVER_ind ///
            MULSCL_MEDICARE_EVER_ind OBESITY_MEDICARE_EVER_ind EPILEP_MEDICARE_EVER_ind median_hh_inc_ln pvrty fem_cty eld_cty ///
            ib0.metro_micro_cnty cllge gen_md med_cty full_dual_ind ib1.amb_type c.MILES SH_ind EH_ind NH_ind RH_ind, save(appendix_logit_table_`a'_`t') replace /* This model does NOT have claims based frailty index */
        }

        else{ /* If NOT ambulance subgroup, then we don't include the ambulance specific covariates */

            noisily di "Creating appendix table for logistic output for group: `a' with comparison group `t'"

            * Step 1 logit
            logit treatment niss1-niss4 riss1-riss4 ib2.RACE ib1.SEX c.AGE##c.AGE c.comorbid##c.comorbid c.BLOODPT ///
            c.mxaisbr_HeadNeck c.mxaisbr_Extremities c.mxaisbr_Chest c.mxaisbr_Abdomen c.maxais i.STATE i.year_fe AMI_EVER_ind ///
            ALZH_EVER_ind ALZH_DEMEN_EVER_ind ATRIAL_FIB_EVER_ind CATARACT_EVER_ind CHRONICKIDNEY_EVER_ind COPD_EVER_ind CHF_EVER_ind ///
            DIABETES_EVER_ind GLAUCOMA_EVER_ind ISCHEMICHEART_EVER_ind DEPRESSION_EVER_ind OSTEOPOROSIS_EVER_ind ///
            RA_OA_EVER_ind STROKE_TIA_EVER_ind CANCER_BREAST_EVER_ind CANCER_COLORECTAL_EVER_ind CANCER_PROSTATE_EVER_ind CANCER_LUNG_EVER_ind ///
            CANCER_ENDOMETRIAL_EVER_ind ANEMIA_EVER_ind ASTHMA_EVER_ind HYPERL_EVER_ind HYPERP_EVER_ind HYPERT_EVER_ind HYPOTH_EVER_ind ///
            MULSCL_MEDICARE_EVER_ind OBESITY_MEDICARE_EVER_ind EPILEP_MEDICARE_EVER_ind median_hh_inc_ln pvrty fem_cty eld_cty ///
            ib0.metro_micro_cnty cllge gen_md med_cty full_dual_ind /* This model does NOT have claims based frailty index */
        }

        *____________ Step 2: Predict Propensity score ________________*

        * Step 2 predict for each bene
        predict propensity_score

        *____________ Step 3: Determine the weights ________________*

        * Step 3 Generate overlap weights
        gen weights = .
        replace weights = 1-propensity_score if treatment==1
        replace weights = propensity_score if treatment==0

        * Save file in order to make distribution graphs of pscore's for appendix
        save "/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/for_pscore_dist_`a'_`t'.dta", replace

        *____________ Step 4: Check Balance after weighting ________________*

        * Loop through each covariate again to check mean between the two groups
        foreach c of local covariates{

            * See mean for each covariates. Need to use -summarize- instead of -tab- or -table- in order to conveniently store means of treatment/control as macro's
            if inlist("`c'","female","white", "black", "other_n", "asian_pi", "hispanic")|inlist("`c'","pvrty", "fem_cty", "eld_cty", "metro", "cllge", "gen_md", "med_cty")|inlist("`c'","n16_24", "n25_40", "n41", "cs1", "cs1_3", "cs4", "cc1_6")|inlist("`c'","cc7"){ /* use conditional function to convert these to percents. Also need to use two inlist functions separated with "|" to bypass error "Expression too long" */
            summarize `c' [aw=weights] if treatment == 1 /* Apply weights for treatment group */
            local `a'`t'`c'tp = string(`r(mean)'*100,"%9.1f") /* Save macro for treatment group's mean and keep decimal at first place. "tp" is treatment group's mean after weighting. Multiple by 100 to convert to percents */
            summarize `c' [aw=weights] if treatment == 0 /* Apply weights for control group */
            local `a'`t'`c'cp = string(`r(mean)'*100,"%9.1f") /* Save macro for control group's mean and keep decimal at first place. "cp" is treatment group's mean after weighting. Multiple by 100 to convert to percents */

            }
            else{ /* Same function to save means after applying weights but for variables that do not require conversion to percentage */
            summarize `c' [aw=weights] if treatment == 1
            local `a'`t'`c'tp = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
            summarize `c' [aw=weights] if treatment == 0
            local `a'`t'`c'cp = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */
            }

            * Sanity check: tab mean for each covariates
            tab treatment [aw=weights], summarize(`c')

            * Sanity check: Regression covariates on treatment to see if there is significant difference between hospital types. DO NOT NEED TO SAVE P VALUE because we "forced" the means to be the same so the p value is always 1.
            reg `c' treatment [pw=weights]
            *noisily reg m_hh_inc treatment [pw=weights] /* Check if median household income was significant after adjusting */

            *___ STANDARDIZED DIFFERENCE FOR DICHOTOMOUS VARIABLES ___*
            if inlist("`c'", "female", "white", "black", "other_n", "asian_pi", "hispanic")|inlist("`c'", "metro")|inlist("`c'","n16_24", "n25_40", "n41", "cs1", "cs4", "cc1_6", "cc7"){
            summarize `c' [aw=weights] if treatment == 1     /* Apply weights for treatment group */
            local `a'`t'`c'ppt = r(mean)                      /* Obtain weighted probability from treatment */
            summarize `c' [aw=weights] if treatment == 0     /* Apply weights for control group */
            local `a'`t'`c'ppc = r(mean)                      /* Obtain weighted probability from control */

            * Calculate STD DIFF
            local `a'`t'`c'SDA = string((``a'`t'`c'ppt' - ``a'`t'`c'ppc')/sqrt(((``a'`t'`c'ppt' * (1 - ``a'`t'`c'ppt')) + (``a'`t'`c'ppc' * (1 - ``a'`t'`c'ppc')))/2),"%9.2f")
            }

            *___STANDARDIZED DIFFERENCE FOR CONTINUOUS VARIABLES___*
            else{
            summarize `c' [aw=weights] if treatment == 1     /* Apply weights for treatment group */
            local `a'`t'`c'mmt = r(mean)                      /* Obtain weighted means from treatment */
            local `a'`t'`c'sst = r(sd)                        /* Obtain weighted standard error from treatment */
            summarize `c' [aw=weights] if treatment == 0     /* Apply weights for control group */
            local `a'`t'`c'mmc = r(mean)                      /* Obtain weighted means from treatment */
            local `a'`t'`c'ssc = r(sd)                        /* Obtain weighted standard error from treatment */

            * Calculate STD DIFF
            local `a'`t'`c'SDA = string((``a'`t'`c'mmt' - ``a'`t'`c'mmc')/sqrt(( (``a'`t'`c'sst')^2 + (``a'`t'`c'ssc')^2 )/2),"%9.2f")
            }

            *___ Split by category for niss (16-24 25-40 41+) ___*
            if inlist("`c'","niss"){
                summarize `c' [aw=weights] if (treatment == 1) & (niss_bands==2)
                local `a'`t'`c'tp2 = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
                summarize `c' [aw=weights] if treatment == 0 & (niss_bands==2)
                local `a'`t'`c'cp2 = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */

                summarize `c' [aw=weights] if (treatment == 1) & (niss_bands==3)
                local `a'`t'`c'tp3 = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
                summarize `c' [aw=weights] if treatment == 0 & (niss_bands==3)
                local `a'`t'`c'cp3 = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */

                summarize `c' [aw=weights] if (treatment == 1) & ((niss_bands==4)|(niss_bands==5))
                local `a'`t'`c'tp45 = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
                summarize `c' [aw=weights] if (treatment == 0) & ((niss_bands==4)|(niss_bands==5))
                local `a'`t'`c'cp45 = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */
            }

            *___ Split by category for comorbidity (<1 1-3 >=4) __*
            if inlist("`c'","comorbid"){
                summarize `c' [aw=weights] if (treatment == 1) & (comorbid<1)
                local `a'`t'`c'tp1 = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
                summarize `c' [aw=weights] if treatment == 0 & (comorbid<1)
                local `a'`t'`c'cp1 = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */

                summarize `c' [aw=weights] if (treatment == 1) & ((comorbid>=1)&(comorbid<4))
                local `a'`t'`c'tp2 = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
                summarize `c' [aw=weights] if treatment == 0 & ((comorbid>=1)&(comorbid<4))
                local `a'`t'`c'cp2 = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */

                summarize `c' [aw=weights] if (treatment == 1) & ((comorbid>=4))
                local `a'`t'`c'tp3 = string(`r(mean)',"%9.1f") /* Save macro for treatment group's mean and keep decimal at second place. "tp" is treatment group's mean after weighting */
                summarize `c' [aw=weights] if (treatment == 0) & ((comorbid>=4))
                local `a'`t'`c'cp3 = string(`r(mean)',"%9.1f") /* Save macro for control group's mean and keep decimal at second place. "cp" is treatment group's mean after weighting */
            }
        }

        *____________ Step 5: Run Analysis ONLY AFTER we are satisfied with model and balance ________________*

        * Check if the denominator is correct in each loop (and check sample size)
        tab TRAUMA_LEVEL
        tab treatment

        * note that applying weights may change number of observations

        * Outcome: Discharge Death
        reg discharge_death_ind treatment [pweight=weights], cformat(%5.3f)

        *--- Outcome: Thirty Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(12, 01, 2017, 0, 0, 0) /* To use thirty day death, need to drop last 30 days (Dec 2017) */
        reg thirty_day_death_ind treatment [pweight=weights], cformat(%5.3f)

        * Save results from thirty day death for excel
        matrix list r(table) /* View regression result from thirty day death as a table */
        matrix reg_table = r(table) /* Convert regression result from thirty day death to a table and save as reg_table */
        local `a'_`t'_30cp = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_30cp" is thirty day death treatment coefficient for the pscore weighting */
        local `a'_`t'_30lbp = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_30lbp" is thirty day death lower bound for the pscore weighting */
        local `a'_`t'_30ubp = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_30ubp" is thirty day death upper bound for the pscore weighting */

        *--- Outcome: Ninety Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(10, 01, 2017, 0, 0, 0) /* To use ninety day death, need to drop last 90 days (Oct-Dec 2017) */
        reg ninety_day_death_ind treatment [pweight=weights], cformat(%5.3f)

        * Save results from ninety day death for excel
        matrix list r(table) /* View regression result from ninety day death as a table */
        matrix reg_table = r(table) /* Convert regression result from ninety day death to a table and save as reg_table */
        local `a'_`t'_90cp = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_90cp" is ninety day death treatment coefficient for the pscore weighting */
        local `a'_`t'_90lbp = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_90lbp" is ninety day death lower bound for the pscore weighting */
        local `a'_`t'_90ubp = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_90ubp" is ninety day death upper bound for the pscore weighting */

        *--- Outcome: One-Eighty Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(07, 01, 2017, 0, 0, 0) /* To use One-Eighty day death, need to drop last 180 days (Jul-Dec 2017) */
        reg oneeighty_day_death_ind treatment [pweight=weights], cformat(%5.3f)

        * Save results from One-Eighty day death for excel
        matrix list r(table) /* View regression result from One-Eighty day death as a table */
        matrix reg_table = r(table) /* Convert regression result from One-Eighty day death to a table and save as reg_table */
        local `a'_`t'_18cp = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_18cp" is shortened for 180 day death treatment coefficient for the pscore weighting */
        local `a'_`t'_18lbp = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_18lbp" is shortened for 180 day death lower bound for the pscore weighting */
        local `a'_`t'_18ubp = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_18ubp" is shortened for 180 day death upper bound for the pscore weighting */

        *--- Outcome: Three hundred Sixty Five Day Death ---*
        drop if SRVC_BGN_DT >= mdyhms(01, 01, 2017, 0, 0, 0) /* To use Three hundred Sixty Five day death, need to drop last 365 days (Jan-Dec 2017) */
        reg threesixtyfive_day_death_ind treatment [pweight=weights], cformat(%5.3f)

        * Save results from Three hundred Sixty Five Day day death for excel
        matrix list r(table) /* View regression result from Three hundred Sixty Five Day day death as a table */
        matrix reg_table = r(table) /* Convert regression result from Three hundred Sixty Five Day day death to a table and save as reg_table */
        local `a'_`t'_1cp = string(_b[treatment]*100,"%9.1f") /* Save treatment coefficient as a macro. Convert number to percent and keep at the tenth decimal place. "_1cp" is shortened for 1 year death treatment coefficient for the pscore weighting */
        local `a'_`t'_1lbp = string(reg_table[5,1]*100,"%9.1f") /* Save lower bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_1lbp" is shortened for 1 year death lower bound for the pscore weighting */
        local `a'_`t'_1ubp = string(reg_table[6,1]*100,"%9.1f") /* Save upper bound of CI as a macro. Convert number to percent and keep at the tenth decimal place. "_1ubp" is shortened for 1 year death upper bound for the pscore weighting */

        restore

    }

    * Restore original file (need clear option)
    use `all_hospital_file',clear
}



*__________________ P-SCORE: CREATE EXCEL SHEETS USING MACRO'S SAVED ________________*

foreach z of local analysis_groups {

    noisily di "Creating table for `z'"

    *NOTE: NAIVE AND PSCORE RESULTS WILL BE ON ONE TABLE

    * Set file name for excel sheet with replace option
    if inlist("`z'", "all"){ /* if all hospital sample then replace */
    putexcel set "results_without_labels.xlsx", sheet("pscore-`z'") replace
    }
    else{ /* if not all hospital sample, then simply modify the existing excel sheet */
    putexcel set "results_without_labels.xlsx", sheet("pscore-`z'") modify
    }

    *--- Trauma levels 1 vs 2 ---*

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel A1 = "``z'_one_v_two_sum'"
    putexcel A2 = "lvl 1: ``z'_one_v_two_ts'"
    putexcel A3 = "lvl 2: ``z'_one_v_two_cs'"
    putexcel A6 = "``z'_one_v_two_30cn' (``z'_one_v_two_30lbn' to ``z'_one_v_two_30ubn')"
    putexcel A7 = "``z'_one_v_two_90cn' (``z'_one_v_two_90lbn' to ``z'_one_v_two_90ubn')"
    putexcel A8 = "``z'_one_v_two_18cn' (``z'_one_v_two_18lbn' to ``z'_one_v_two_18ubn')"
    putexcel A9 = "``z'_one_v_two_1cn' (``z'_one_v_two_1lbn' to ``z'_one_v_two_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel A12 = "``z'_one_v_two_30cp' (``z'_one_v_two_30lbp' to ``z'_one_v_two_30ubp')"
    putexcel A13 = "``z'_one_v_two_90cp' (``z'_one_v_two_90lbp' to ``z'_one_v_two_90ubp')"
    putexcel A14 = "``z'_one_v_two_18cp' (``z'_one_v_two_18lbp' to ``z'_one_v_two_18ubp')"
    putexcel A15 = "``z'_one_v_two_1cp' (``z'_one_v_two_1lbp' to ``z'_one_v_two_1ubp')"

    *--- Trauma levels 1 vs 3 ---*

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel B1 = "``z'_one_v_three_sum'"
    putexcel B2 = "lvl 1: ``z'_one_v_three_ts'"
    putexcel B3 = "lvl 3: ``z'_one_v_three_cs'"
    putexcel B6 = "``z'_one_v_three_30cn' (``z'_one_v_three_30lbn' to ``z'_one_v_three_30ubn')"
    putexcel B7 = "``z'_one_v_three_90cn' (``z'_one_v_three_90lbn' to ``z'_one_v_three_90ubn')"
    putexcel B8 = "``z'_one_v_three_18cn' (``z'_one_v_three_18lbn' to ``z'_one_v_three_18ubn')"
    putexcel B9 = "``z'_one_v_three_1cn' (``z'_one_v_three_1lbn' to ``z'_one_v_three_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel B12 = "``z'_one_v_three_30cp' (``z'_one_v_three_30lbp' to ``z'_one_v_three_30ubp')"
    putexcel B13 = "``z'_one_v_three_90cp' (``z'_one_v_three_90lbp' to ``z'_one_v_three_90ubp')"
    putexcel B14 = "``z'_one_v_three_18cp' (``z'_one_v_three_18lbp' to ``z'_one_v_three_18ubp')"
    putexcel B15 = "``z'_one_v_three_1cp' (``z'_one_v_three_1lbp' to ``z'_one_v_three_1ubp')"

    *--- Trauma levels 1 vs 4/5 ---*
    if inlist("`z'", "all", "amb", "fal", "mot", "m16", "m25", "m41","ipA","opA"){ /* create this column only if the sample is for everyone, amb, only falls, motor vehicle accidents or various niss bands */

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel C1 = "``z'_one_v_four_five_sum'"
    putexcel C2 = "lvl 1: ``z'_one_v_four_five_ts'"
    putexcel C3 = "lvl 4/5: ``z'_one_v_four_five_cs'"
    putexcel C6 = "``z'_one_v_four_five_30cn' (``z'_one_v_four_five_30lbn' to ``z'_one_v_four_five_30ubn')"
    putexcel C7 = "``z'_one_v_four_five_90cn' (``z'_one_v_four_five_90lbn' to ``z'_one_v_four_five_90ubn')"
    putexcel C8 = "``z'_one_v_four_five_18cn' (``z'_one_v_four_five_18lbn' to ``z'_one_v_four_five_18ubn')"
    putexcel C9 = "``z'_one_v_four_five_1cn' (``z'_one_v_four_five_1lbn' to ``z'_one_v_four_five_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel C12 = "``z'_one_v_four_five_30cp' (``z'_one_v_four_five_30lbp' to ``z'_one_v_four_five_30ubp')"
    putexcel C13 = "``z'_one_v_four_five_90cp' (``z'_one_v_four_five_90lbp' to ``z'_one_v_four_five_90ubp')"
    putexcel C14 = "``z'_one_v_four_five_18cp' (``z'_one_v_four_five_18lbp' to ``z'_one_v_four_five_18ubp')"
    putexcel C15 = "``z'_one_v_four_five_1cp' (``z'_one_v_four_five_1lbp' to ``z'_one_v_four_five_1ubp')"
    }

    *--- Trauma levels 1 vs nontrauma ---*

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel D1 = "``z'_one_v_nt_sum'"
    putexcel D2 = "lvl 1: ``z'_one_v_nt_ts'"
    putexcel D3 = "NT: ``z'_one_v_nt_cs'"
    putexcel D6 = "``z'_one_v_nt_30cn' (``z'_one_v_nt_30lbn' to ``z'_one_v_nt_30ubn')"
    putexcel D7 = "``z'_one_v_nt_90cn' (``z'_one_v_nt_90lbn' to ``z'_one_v_nt_90ubn')"
    putexcel D8 = "``z'_one_v_nt_18cn' (``z'_one_v_nt_18lbn' to ``z'_one_v_nt_18ubn')"
    putexcel D9 = "``z'_one_v_nt_1cn' (``z'_one_v_nt_1lbn' to ``z'_one_v_nt_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel D12 = "``z'_one_v_nt_30cp' (``z'_one_v_nt_30lbp' to ``z'_one_v_nt_30ubp')"
    putexcel D13 = "``z'_one_v_nt_90cp' (``z'_one_v_nt_90lbp' to ``z'_one_v_nt_90ubp')"
    putexcel D14 = "``z'_one_v_nt_18cp' (``z'_one_v_nt_18lbp' to ``z'_one_v_nt_18ubp')"
    putexcel D15 = "``z'_one_v_nt_1cp' (``z'_one_v_nt_1lbp' to ``z'_one_v_nt_1ubp')"

    *--- Trauma levels 2 vs 3 ---*

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel E1 = "``z'_two_v_three_sum'"
    putexcel E2 = "lvl 2: ``z'_two_v_three_ts'"
    putexcel E3 = "lvl 3: ``z'_two_v_three_cs'"
    putexcel E6 = "``z'_two_v_three_30cn' (``z'_two_v_three_30lbn' to ``z'_two_v_three_30ubn')"
    putexcel E7 = "``z'_two_v_three_90cn' (``z'_two_v_three_90lbn' to ``z'_two_v_three_90ubn')"
    putexcel E8 = "``z'_two_v_three_18cn' (``z'_two_v_three_18lbn' to ``z'_two_v_three_18ubn')"
    putexcel E9 = "``z'_two_v_three_1cn' (``z'_two_v_three_1lbn' to ``z'_two_v_three_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel E12 = "``z'_two_v_three_30cp' (``z'_two_v_three_30lbp' to ``z'_two_v_three_30ubp')"
    putexcel E13 = "``z'_two_v_three_90cp' (``z'_two_v_three_90lbp' to ``z'_two_v_three_90ubp')"
    putexcel E14 = "``z'_two_v_three_18cp' (``z'_two_v_three_18lbp' to ``z'_two_v_three_18ubp')"
    putexcel E15 = "``z'_two_v_three_1cp' (``z'_two_v_three_1lbp' to ``z'_two_v_three_1ubp')"

    *--- Trauma levels 2 vs 4/5 ---*
    if inlist("`z'", "all", "amb", "fal", "mot", "m16", "m25", "m41","ipA","opA"){ /* create this column only if the sample is for everyone amb, only falls, motor vehicle accidents or various niss bands */

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel F1 = "``z'_two_v_four_five_sum'"
    putexcel F2 = "lvl 2: ``z'_two_v_four_five_ts'"
    putexcel F3 = "lvl 4/5: ``z'_two_v_four_five_cs'"
    putexcel F6 = "``z'_two_v_four_five_30cn' (``z'_two_v_four_five_30lbn' to ``z'_two_v_four_five_30ubn')"
    putexcel F7 = "``z'_two_v_four_five_90cn' (``z'_two_v_four_five_90lbn' to ``z'_two_v_four_five_90ubn')"
    putexcel F8 = "``z'_two_v_four_five_18cn' (``z'_two_v_four_five_18lbn' to ``z'_two_v_four_five_18ubn')"
    putexcel F9 = "``z'_two_v_four_five_1cn' (``z'_two_v_four_five_1lbn' to ``z'_two_v_four_five_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel F12 = "``z'_two_v_four_five_30cp' (``z'_two_v_four_five_30lbp' to ``z'_two_v_four_five_30ubp')"
    putexcel F13 = "``z'_two_v_four_five_90cp' (``z'_two_v_four_five_90lbp' to ``z'_two_v_four_five_90ubp')"
    putexcel F14 = "``z'_two_v_four_five_18cp' (``z'_two_v_four_five_18lbp' to ``z'_two_v_four_five_18ubp')"
    putexcel F15 = "``z'_two_v_four_five_1cp' (``z'_two_v_four_five_1lbp' to ``z'_two_v_four_five_1ubp')"
    }

    *--- Trauma levels 2 vs nontrauma ---*

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel G1 = "``z'_two_v_nt_sum'"
    putexcel G2 = "lvl 2: ``z'_two_v_nt_ts'"
    putexcel G3 = "NT: ``z'_two_v_nt_cs'"
    putexcel G6 = "``z'_two_v_nt_30cn' (``z'_two_v_nt_30lbn' to ``z'_two_v_nt_30ubn')"
    putexcel G7 = "``z'_two_v_nt_90cn' (``z'_two_v_nt_90lbn' to ``z'_two_v_nt_90ubn')"
    putexcel G8 = "``z'_two_v_nt_18cn' (``z'_two_v_nt_18lbn' to ``z'_two_v_nt_18ubn')"
    putexcel G9 = "``z'_two_v_nt_1cn' (``z'_two_v_nt_1lbn' to ``z'_two_v_nt_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel G12 = "``z'_two_v_nt_30cp' (``z'_two_v_nt_30lbp' to ``z'_two_v_nt_30ubp')"
    putexcel G13 = "``z'_two_v_nt_90cp' (``z'_two_v_nt_90lbp' to ``z'_two_v_nt_90ubp')"
    putexcel G14 = "``z'_two_v_nt_18cp' (``z'_two_v_nt_18lbp' to ``z'_two_v_nt_18ubp')"
    putexcel G15 = "``z'_two_v_nt_1cp' (``z'_two_v_nt_1lbp' to ``z'_two_v_nt_1ubp')"

    *--- Trauma levels 3 vs 4/5 ---*
    if inlist("`z'", "all", "amb", "fal", "mot", "m16", "m25", "m41","ipA","opA"){ /* create this column only if the sample is for everyone, amb, only falls, motor vehicle accidents or various niss bands */

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel H1 = "``z'_three_v_four_five_sum'"
    putexcel H2 = "lvl 3: ``z'_three_v_four_five_ts'"
    putexcel H3 = "lvl 4/5: ``z'_three_v_four_five_cs'"
    putexcel H6 = "``z'_three_v_four_five_30cn' (``z'_three_v_four_five_30lbn' to ``z'_three_v_four_five_30ubn')"
    putexcel H7 = "``z'_three_v_four_five_90cn' (``z'_three_v_four_five_90lbn' to ``z'_three_v_four_five_90ubn')"
    putexcel H8 = "``z'_three_v_four_five_18cn' (``z'_three_v_four_five_18lbn' to ``z'_three_v_four_five_18ubn')"
    putexcel H9 = "``z'_three_v_four_five_1cn' (``z'_three_v_four_five_1lbn' to ``z'_three_v_four_five_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel H12 = "``z'_three_v_four_five_30cp' (``z'_three_v_four_five_30lbp' to ``z'_three_v_four_five_30ubp')"
    putexcel H13 = "``z'_three_v_four_five_90cp' (``z'_three_v_four_five_90lbp' to ``z'_three_v_four_five_90ubp')"
    putexcel H14 = "``z'_three_v_four_five_18cp' (``z'_three_v_four_five_18lbp' to ``z'_three_v_four_five_18ubp')"
    putexcel H15 = "``z'_three_v_four_five_1cp' (``z'_three_v_four_five_1lbp' to ``z'_three_v_four_five_1ubp')"
    }

    *--- Trauma levels 3 vs nontrauma ---*

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel I1 = "``z'_three_v_nt_sum'"
    putexcel I2 = "lvl 3: ``z'_three_v_nt_ts'"
    putexcel I3 = "NT: ``z'_three_v_nt_cs'"
    putexcel I6 = "``z'_three_v_nt_30cn' (``z'_three_v_nt_30lbn' to ``z'_three_v_nt_30ubn')"
    putexcel I7 = "``z'_three_v_nt_90cn' (``z'_three_v_nt_90lbn' to ``z'_three_v_nt_90ubn')"
    putexcel I8 = "``z'_three_v_nt_18cn' (``z'_three_v_nt_18lbn' to ``z'_three_v_nt_18ubn')"
    putexcel I9 = "``z'_three_v_nt_1cn' (``z'_three_v_nt_1lbn' to ``z'_three_v_nt_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel I12 = "``z'_three_v_nt_30cp' (``z'_three_v_nt_30lbp' to ``z'_three_v_nt_30ubp')"
    putexcel I13 = "``z'_three_v_nt_90cp' (``z'_three_v_nt_90lbp' to ``z'_three_v_nt_90ubp')"
    putexcel I14 = "``z'_three_v_nt_18cp' (``z'_three_v_nt_18lbp' to ``z'_three_v_nt_18ubp')"
    putexcel I15 = "``z'_three_v_nt_1cp' (``z'_three_v_nt_1lbp' to ``z'_three_v_nt_1ubp')"

    *--- Trauma levels 4/5 vs nontrauma ---*
    if inlist("`z'", "all", "amb", "fal", "mot", "m16", "m25", "m41","ipA","opA"){ /* create this column only if the sample is for everyone, amb, only falls, motor vehicle accidents or various niss bands */

    /* Place macro's from naive regression in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel J1 = "``z'_four_five_v_nt_sum'"
    putexcel J2 = "lvl 4/5: ``z'_four_five_v_nt_ts'"
    putexcel J3 = "NT: ``z'_four_five_v_nt_cs'"
    putexcel J6 = "``z'_four_five_v_nt_30cn' (``z'_four_five_v_nt_30lbn' to ``z'_four_five_v_nt_30ubn')"
    putexcel J7 = "``z'_four_five_v_nt_90cn' (``z'_four_five_v_nt_90lbn' to ``z'_four_five_v_nt_90ubn')"
    putexcel J8 = "``z'_four_five_v_nt_18cn' (``z'_four_five_v_nt_18lbn' to ``z'_four_five_v_nt_18ubn')"
    putexcel J9 = "``z'_four_five_v_nt_1cn' (``z'_four_five_v_nt_1lbn' to ``z'_four_five_v_nt_1ubn')"

    /* Place macro's from pscore weighting in specific cells in excel. For descriptions of macro labels, see comments above */
    putexcel J12 = "``z'_four_five_v_nt_30cp' (``z'_four_five_v_nt_30lbp' to ``z'_four_five_v_nt_30ubp')"
    putexcel J13 = "``z'_four_five_v_nt_90cp' (``z'_four_five_v_nt_90lbp' to ``z'_four_five_v_nt_90ubp')"
    putexcel J14 = "``z'_four_five_v_nt_18cp' (``z'_four_five_v_nt_18lbp' to ``z'_four_five_v_nt_18ubp')"
    putexcel J15 = "``z'_four_five_v_nt_1cp' (``z'_four_five_v_nt_1lbp' to ``z'_four_five_v_nt_1ubp')"
    }

    *---------- CREATE BALANCE TABLE FOR EXCEL (BEFORE AND AFTER WEIGHTING) ------------*

    * Create list to loop
    if inlist("`z'", "gun", "cut"){
        local treatment_list `" "one_v_two" "one_v_nt" "two_v_nt" "'

        * Comment in if you want to combine nt with lvl 2. Comment out the top
        * local treatment_list `" "one_v_nt" "'
    }
    else{
        local treatment_list `" "one_v_two" "one_v_three" "one_v_four_five" "one_v_nt" "two_v_three" "two_v_four_five" "two_v_nt" "three_v_four_five" "three_v_nt" "four_five_v_nt" "'
    }

    /* Place macro's for balance table before and after weighting in specific cells in excel. For descriptions of macro labels, see comments above when storing macro's via local function */
    foreach x of local treatment_list{

        * Set file name for excel sheet with modify option to create new table to store the balance tables
        putexcel set "results_without_labels.xlsx", sheet("bal_tab_`z'_`x'") modify

        putexcel A1 = "``z'`x'AGEtn'"
        putexcel B1 = "``z'`x'AGEcn'"
        putexcel C1 = "``z'`x'AGE_p'"
        putexcel G1 = "``z'`x'AGEtp'"
        putexcel H1 = "``z'`x'AGEcp'"

        putexcel E1 = "``z'`x'AGESDB'"  /*std diff*/
        putexcel J1 = "``z'`x'AGESDA'"  /*std diff*/

        putexcel A2 = "``z'`x'femaletn'"
        putexcel B2 = "``z'`x'femalecn'"
        putexcel C2 = "``z'`x'female_p'"
        putexcel G2 = "``z'`x'femaletp'"
        putexcel H2 = "``z'`x'femalecp'"

        putexcel E2 = "``z'`x'femaleSDB'"  /*std diff*/
        putexcel J2 = "``z'`x'femaleSDA'"  /*std diff*/

        putexcel A4 = "``z'`x'whitetn'"
        putexcel B4 = "``z'`x'whitecn'"
        putexcel C4 = "``z'`x'white_p'"
        putexcel G4 = "``z'`x'whitetp'"
        putexcel H4 = "``z'`x'whitecp'"

        putexcel E4 = "``z'`x'whiteSDB'"  /*std diff*/
        putexcel J4 = "``z'`x'whiteSDA'"  /*std diff*/

        putexcel A5 = "``z'`x'blacktn'"
        putexcel B5 = "``z'`x'blackcn'"
        putexcel C5 = "``z'`x'black_p'"
        putexcel G5 = "``z'`x'blacktp'"
        putexcel H5 = "``z'`x'blackcp'"

        putexcel E5 = "``z'`x'blackSDB'"  /*std diff*/
        putexcel J5 = "``z'`x'blackSDA'"  /*std diff*/

        putexcel A6 = "``z'`x'other_ntn'"
        putexcel B6 = "``z'`x'other_ncn'"
        putexcel C6 = "``z'`x'other_n_p'"
        putexcel G6 = "``z'`x'other_ntp'"
        putexcel H6 = "``z'`x'other_ncp'"

        putexcel E6 = "``z'`x'other_nSDB'"  /*std diff*/
        putexcel J6 = "``z'`x'other_nSDA'"  /*std diff*/

        putexcel A7 = "``z'`x'asian_pitn'"
        putexcel B7 = "``z'`x'asian_picn'"
        putexcel C7 = "``z'`x'asian_pi_p'"
        putexcel G7 = "``z'`x'asian_pitp'"
        putexcel H7 = "``z'`x'asian_picp'"

        putexcel E7 = "``z'`x'asian_piSDB'"  /*std diff*/
        putexcel J7 = "``z'`x'asian_piSDA'"  /*std diff*/

        putexcel A8 = "``z'`x'hispanictn'"
        putexcel B8 = "``z'`x'hispaniccn'"
        putexcel C8 = "``z'`x'hispanic_p'"
        putexcel G8 = "``z'`x'hispanictp'"
        putexcel H8 = "``z'`x'hispaniccp'"

        putexcel E8 = "``z'`x'hispanicSDB'"  /*std diff*/
        putexcel J8 = "``z'`x'hispanicSDA'"  /*std diff*/

        putexcel A9 = "``z'`x'cc_cnttn'"
        putexcel B9 = "``z'`x'cc_cntcn'"
        putexcel C9 = "``z'`x'cc_cnt_p'"
        putexcel G9 = "``z'`x'cc_cnttp'"
        putexcel H9 = "``z'`x'cc_cntcp'"

        putexcel E9 = "``z'`x'cc_cntSDB'"  /*std diff*/
        putexcel J9 = "``z'`x'cc_cntSDA'"  /*std diff*/

        putexcel A11 = "``z'`x'cc1_6tn'"
        putexcel B11 = "``z'`x'cc1_6cn'"
        putexcel C11 = "``z'`x'cc1_6_p'"
        putexcel G11 = "``z'`x'cc1_6tp'"
        putexcel H11 = "``z'`x'cc1_6cp'"

        putexcel E11 = "``z'`x'cc1_6SDB'"  /*std diff*/
        putexcel J11 = "``z'`x'cc1_6SDA'"  /*std diff*/

        putexcel A12 = "``z'`x'cc7tn'"
        putexcel B12 = "``z'`x'cc7cn'"
        putexcel C12 = "``z'`x'cc7_p'"
        putexcel G12 = "``z'`x'cc7tp'"
        putexcel H12 = "``z'`x'cc7cp'"

        putexcel E12 = "``z'`x'cc7SDB'"  /*std diff*/
        putexcel J12 = "``z'`x'cc7SDA'"  /*std diff*/

        putexcel A13 = "``z'`x'comorbidtn'"
        putexcel B13 = "``z'`x'comorbidcn'"
        putexcel C13 = "``z'`x'comorbid_p'"
        putexcel G13 = "``z'`x'comorbidtp'"
        putexcel H13 = "``z'`x'comorbidcp'"

        putexcel E13 = "``z'`x'comorbidSDB'"  /*std diff*/
        putexcel J13 = "``z'`x'comorbidSDA'"  /*std diff*/

        /*by category for comorbidity ("<1")*/
        putexcel A15 = "``z'`x'cs1tn'"
        putexcel B15 = "``z'`x'cs1cn'"
        putexcel C15 = "``z'`x'cs1_p'"
        putexcel G15 = "``z'`x'cs1tp'"
        putexcel H15 = "``z'`x'cs1cp'"

        putexcel E15 = "``z'`x'cs1SDB'"  /*std diff*/
        putexcel J15 = "``z'`x'cs1SDA'"  /*std diff*/

        /*by category for comorbidity ("1-3")*/
        putexcel A16 = "``z'`x'cs1_3tn'"
        putexcel B16 = "``z'`x'cs1_3cn'"
        putexcel C16 = "``z'`x'cs1_3_p'"
        putexcel G16 = "``z'`x'cs1_3tp'"
        putexcel H16 = "``z'`x'cs1_3cp'"

        putexcel E16 = "``z'`x'cs1_3SDB'"  /*std diff*/
        putexcel J16 = "``z'`x'cs1_3SDA'"  /*std diff*/

        /*by category for comorbidity (">=4")*/
        putexcel A17 = "``z'`x'cs4tn'"
        putexcel B17 = "``z'`x'cs4cn'"
        putexcel C17 = "``z'`x'cs4_p'"
        putexcel G17 = "``z'`x'cs4tp'"
        putexcel H17 = "``z'`x'cs4cp'"

        putexcel E17 = "``z'`x'cs4SDB'"  /*std diff*/
        putexcel J17 = "``z'`x'cs4SDA'"  /*std diff*/

        putexcel A18 = "``z'`x'nisstn'"
        putexcel B18 = "``z'`x'nisscn'"
        putexcel C18 = "``z'`x'niss_p'"
        putexcel G18 = "``z'`x'nisstp'"
        putexcel H18 = "``z'`x'nisscp'"

        putexcel E18 = "``z'`x'nissSDB'"  /*std diff*/
        putexcel J18 = "``z'`x'nissSDA'"  /*std diff*/

        /*(niss_bands 2 "16-24")*/
        putexcel A20 = "``z'`x'n16_24tn'"
        putexcel B20 = "``z'`x'n16_24cn'"
        putexcel C20 = "``z'`x'n16_24_p'"
        putexcel G20 = "``z'`x'n16_24tp'"
        putexcel H20 = "``z'`x'n16_24cp'"

        putexcel E20 = "``z'`x'n16_24SDB'"  /*std diff*/
        putexcel J20 = "``z'`x'n16_24SDA'"  /*std diff*/

        /*(niss_bands 3 "25-40")*/
        putexcel A21 = "``z'`x'n25_40tn'"
        putexcel B21 = "``z'`x'n25_40cn'"
        putexcel C21 = "``z'`x'n25_40_p'"
        putexcel G21 = "``z'`x'n25_40tp'"
        putexcel H21 = "``z'`x'n25_40cp'"

        putexcel E21 = "``z'`x'n25_40SDB'"  /*std diff*/
        putexcel J21 = "``z'`x'n25_40SDA'"  /*std diff*/

        /*(niss_bands 4 "41-49" 5 "50+")*/
        putexcel A22 = "``z'`x'n41tn'"
        putexcel B22 = "``z'`x'n41cn'"
        putexcel C22 = "``z'`x'n41_p'"
        putexcel G22 = "``z'`x'n41tp'"
        putexcel H22 = "``z'`x'n41cp'"

        putexcel E22 = "``z'`x'n41SDB'"  /*std diff*/
        putexcel J22 = "``z'`x'n41SDA'"  /*std diff*/

        putexcel A23 = "``z'`x'risstn'"
        putexcel B23 = "``z'`x'risscn'"
        putexcel C23 = "``z'`x'riss_p'"
        putexcel G23 = "``z'`x'risstp'"
        putexcel H23 = "``z'`x'risscp'"

        putexcel E23 = "``z'`x'rissSDB'"  /*std diff*/
        putexcel J23 = "``z'`x'rissSDA'"  /*std diff*/

        putexcel A24 = "``z'`x'CFItn'"
        putexcel B24 = "``z'`x'CFIcn'"
        putexcel C24 = "``z'`x'CFI_p'"
        putexcel G24 = "``z'`x'CFItp'"
        putexcel H24 = "``z'`x'CFIcp'"

        putexcel E24 = "``z'`x'CFISDB'"  /*std diff*/
        putexcel J24 = "``z'`x'CFISDA'"  /*std diff*/

        putexcel A25 = "``z'`x'BLOODPTtn'"
        putexcel B25 = "``z'`x'BLOODPTcn'"
        putexcel C25 = "``z'`x'BLOODPT_p'"
        putexcel G25 = "``z'`x'BLOODPTtp'"
        putexcel H25 = "``z'`x'BLOODPTcp'"

        putexcel E25 = "``z'`x'BLOODPTSDB'"  /*std diff*/
        putexcel J25 = "``z'`x'BLOODPTSDA'"  /*std diff*/

        putexcel A26 = "``z'`x'm_hh_inctn'"
        putexcel B26 = "``z'`x'm_hh_inccn'"
        putexcel C26 = "``z'`x'm_hh_inc_p'"
        putexcel G26 = "``z'`x'm_hh_inctp'"
        putexcel H26 = "``z'`x'm_hh_inccp'"

        putexcel E26 = "``z'`x'm_hh_incSDB'"  /*std diff*/
        putexcel J26 = "``z'`x'm_hh_incSDA'"  /*std diff*/

        putexcel A28 = "``z'`x'pvrtytn'"
        putexcel B28 = "``z'`x'pvrtycn'"
        putexcel C28 = "``z'`x'pvrty_p'"
        putexcel G28 = "``z'`x'pvrtytp'"
        putexcel H28 = "``z'`x'pvrtycp'"

        putexcel E28 = "``z'`x'pvrtySDB'"  /*std diff*/
        putexcel J28 = "``z'`x'pvrtySDA'"  /*std diff*/

        putexcel A29 = "``z'`x'fem_ctytn'"
        putexcel B29 = "``z'`x'fem_ctycn'"
        putexcel C29 = "``z'`x'fem_cty_p'"
        putexcel G29 = "``z'`x'fem_ctytp'"
        putexcel H29 = "``z'`x'fem_ctycp'"

        putexcel E29 = "``z'`x'fem_ctySDB'"  /*std diff*/
        putexcel J29 = "``z'`x'fem_ctySDA'"  /*std diff*/

        putexcel A30 = "``z'`x'eld_ctytn'"
        putexcel B30 = "``z'`x'eld_ctycn'"
        putexcel C30 = "``z'`x'eld_cty_p'"
        putexcel G30 = "``z'`x'eld_ctytp'"
        putexcel H30 = "``z'`x'eld_ctycp'"

        putexcel E30 = "``z'`x'eld_ctySDB'"  /*std diff*/
        putexcel J30 = "``z'`x'eld_ctySDA'"  /*std diff*/

        putexcel A31 = "``z'`x'metrotn'"
        putexcel B31 = "``z'`x'metrocn'"
        putexcel C31 = "``z'`x'metro_p'"
        putexcel G31 = "``z'`x'metrotp'"
        putexcel H31 = "``z'`x'metrocp'"

        putexcel E31 = "``z'`x'metroSDB'"  /*std diff*/
        putexcel J31 = "``z'`x'metroSDA'"  /*std diff*/

        putexcel A32 = "``z'`x'cllgetn'"
        putexcel B32 = "``z'`x'cllgecn'"
        putexcel C32 = "``z'`x'cllge_p'"
        putexcel G32 = "``z'`x'cllgetp'"
        putexcel H32 = "``z'`x'cllgecp'"

        putexcel E32 = "``z'`x'cllgeSDB'"  /*std diff*/
        putexcel J32 = "``z'`x'cllgeSDA'"  /*std diff*/

        putexcel A33 = "``z'`x'gen_mdtn'"
        putexcel B33 = "``z'`x'gen_mdcn'"
        putexcel C33 = "``z'`x'gen_md_p'"
        putexcel G33 = "``z'`x'gen_mdtp'"
        putexcel H33 = "``z'`x'gen_mdcp'"

        putexcel E33 = "``z'`x'gen_mdSDB'"  /*std diff*/
        putexcel J33 = "``z'`x'gen_mdSDA'"  /*std diff*/

        putexcel A34 = "``z'`x'med_ctytn'"
        putexcel B34 = "``z'`x'med_ctycn'"
        putexcel C34 = "``z'`x'med_cty_p'"
        putexcel G34 = "``z'`x'med_ctytp'"
        putexcel H34 = "``z'`x'med_ctycp'"

        putexcel E34 = "``z'`x'med_ctySDB'"  /*std diff*/
        putexcel J34 = "``z'`x'med_ctySDA'"  /*std diff*/

    }

}

}


/*
scp jessyjkn@phs-rs24.bsd.uchicago.edu:/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims_p1/merged_ats_claims_for_stata/results_without_labels.xlsx /Users/jessyjkn/Desktop/Job/Data/trauma_center_project/pscore_results/

*/



