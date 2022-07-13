*------------------------------------------------------------------------------*
* Project: Trauma Center Project
* Author: Jessy Nguyen
* Last Updated: July 11, 2022
* Description: This script will use GLM, with link=logit, to model mortality probability which will be used to create Hospital
* Surgical Quality Scores (i.e. the binary 30-day mortality indicator minus the modeled risk-adjusted surgical mortality probability)
*------------------------------------------------------------------------------*

* Copy the following in stata within terminal to run code.
* do /mnt/labshares/sanghavi-lab/Jessy/hpc_utils/codes/python/trauma_center_project/final_codes_python_sas_R_stata/18_hospital_quality_measures/05a_hos_qual_scores_calculations_in_stata/creating_hos_qual_measures.do

* Set working directory
cd "/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/hospital_quality_measure/data_to_run_glm_in_stata/"

*--- Read in surgical DRG data ---*

* Read in Data for 2017 first
use patid BENE_ID DRG_CD ADMSN_DT PRVDR_NUM SEX_IDENT_CD RTI_RACE_CD comorbidity_score thirty_day_death_ind AGE year_fe using 2017, clear

* Append all other years 12-16
forval i=12/16{
append using 20`i', keep(patid BENE_ID DRG_CD ADMSN_DT PRVDR_NUM SEX_IDENT_CD RTI_RACE_CD comorbidity_score thirty_day_death_ind AGE year_fe)
}

*___ Clean data ___*

* Remove observations if Race is unknown
drop if RTI_RACE_CD=="0"
drop if SEX_IDENT_CD=="0"

* Destring and Rename variables needed in the model
destring RTI_RACE_CD, generate(RACE)
destring SEX_IDENT_CD, generate(SEX)
destring DRG_CD, generate(DRG_factor)

* Label the variables that were destringed
label define RACE_label 1 "White" 2 "Black" 3 "Other" 4 "Asian/PI" 5 "Hispanic" 6 "Native Americans/Alaskan Native"
label values RACE RACE_label
label define SEX_label 1 "M" 2 "F"
label values SEX SEX_label

**************** Predict survival probability using GLM *********************

* Run GLM (logit) (this is the same as just running -logit-)
glm thirty_day_death_ind ib1.RACE ib1.SEX c.comorbidity_score##c.comorbidity_score c.AGE##c.AGE i.DRG_factor i.year_fe, family(binomial) link(logit)

* Predict mortality from model
predict prob_predict_death

* Check distribution (deciles)
noisily sumdist prob_predict_death, n(10)
noisily univar prob_predict_death

* Subtracted the modeled death probability from the binary 30-day death indicator for each observation. Since 1 is dead and 0 is not, the more negative the residual, the higher quality. We will negate this in later scripts so that more positive = higher quality
gen prob_diff_residual = thirty_day_death_ind - prob_predict_death

* Check distribution again after subtracting
noisily sumdist prob_diff_residual, n(10)
noisily univar prob_diff_residual

* Export file to merge back with
save "hos_qual_calculated_from_stata.dta", replace

