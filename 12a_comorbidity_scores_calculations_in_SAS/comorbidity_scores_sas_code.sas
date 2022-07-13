/* SAS has two main blocks of code: the data step and the procedure (proc) step. The data statement
begins the data step. The proc statement begins the procedure step. Within the data statement, 
there is a set statement that allows you to modify an existing dataset. You can save the contents
under a new name by specifying a new name after the "data" statement. */

/* Running the following script in terminal to run in phs: "/usr/local/SASHome/SASFoundation/9.4/sas" -SYSIN /mnt/labshares/sanghavi-lab/Jessy/hpc_utils/codes/python/trauma_center_project/final_codes_python_sas_R_stata/12a_comorbidity_scores_calculations_in_SAS/comorbidity_scores_sas_code.sas -LOG /mnt/labshares/sanghavi-lab/Jessy/hpc_utils/codes/sas/trauma_center_project/log/job1.log */

/* Need following code to prevent "ERROR: Insufficient authorization to access" when executing in linux */
ods listing close;

/* Read in data (CSV) */
proc import datafile="/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/comorbidity_score/2011_for_sas.csv"
	out=diag_imp replace;
guessingrows=3000000; /* This will "guess" the column type through examining 7k rows. Need this to force DX column as character. */
delimiter=',';
datarow=2;
getnames=YES;
run;

/* Convert numeric to character for code type column */
data diag_imp; /* Specify that I want to start data step. Using same name to not make a copy. */
set diag_imp; /* Specify I want to modify the existing diag_imp dataset */
charvar_cd_tp = STRIP(PUT(Dx_CodeType, z2.)); /* Convert from 9 to 09 or 10 to 10 */
drop Dx_CodeType;
rename charvar_cd_tp=Dx_CodeType;
run;

/* be careful that above doesn't convert 10's, only 9's*/

/* View datatype */
proc contents data=diag_imp;
run;

/* Calculate Comorbidity scores */
%let dx_set = diag_imp; /* Define a Macro Variable for the dataset. Macro Variables can basically store anything */
%let score_set = combined_comorb_score_set; /* Define a Macro Variable for combined_comorb_score_set*/

proc sql; /* To combine functionalities. This is a SQL Language */
  create table for_combined_comorbidity_score as /* Need AS to create a table using the following codes */
  select patid, /* SELECT names the columns that will appear on the report and the order in which they will appear */
         MAX(case when Dx_CodeType = "09" and DX = "2911" then 1
                  when Dx_CodeType = "09" and DX = "2912" then 1
                  when Dx_CodeType = "09" and DX = "2915" then 1
                  when Dx_CodeType = "09" and DX = "2918" then 1
                  when Dx_CodeType = "09" and DX = "2919" then 1
                  when Dx_CodeType = "09" and DX >= "3039" and DX <= "30393" then 1
                  when Dx_CodeType = "09" and DX >= "3050" and DX <= "30503" then 1
                  when Dx_CodeType = "09" and DX = "V113" then 1
                  when Dx_CodeType = "10" and DX = "E52" then 1
                  when Dx_CodeType = "10" and DX like "F10%" then 1
                  when Dx_CodeType = "10" and DX = "G621" then 1
                  when Dx_CodeType = "10" and DX = "I426" then 1
                  when Dx_CodeType = "10" and DX like "K292%" then 1
                  when Dx_CodeType = "10" and DX = "K700" then 1
                  when Dx_CodeType = "10" and DX like "K703%" then 1
                  when Dx_CodeType = "10" and DX = "K709" then 1
                  when Dx_CodeType = "10" and DX like "T51%" then 1
                  when Dx_CodeType = "10" and DX = "Z658" then 1
                  when Dx_CodeType = "10" and DX like "Z714%" then 1
                  else 0
             end) as Alcohol_abuse, /* MAX returns the largest value. CASE returns a single value that is conditionally evaluated */
         MAX(case when Dx_CodeType = "09" and DX >= "140" and substr(DX, 1, 3) <= "171" then 1
                  when Dx_CodeType = "09" and DX >= "174" and substr(DX, 1, 3) <= "195" then 1
                  when Dx_CodeType = "09" and DX >= "200" and substr(DX, 1, 3) <= "208" then 1
                  when Dx_CodeType = "09" and DX = "2730" then 1
                  when Dx_CodeType = "09" and DX = "2733" then 1
                  when Dx_CodeType = "09" and DX = "V1046" then 1
                  when Dx_CodeType = "10" and DX >= "C00" and substr(DX, 1, 3) <= "C26" then 1
                  when Dx_CodeType = "10" and DX >= "C30" and substr(DX, 1, 3) <= "C34" then 1
                  when Dx_CodeType = "10" and DX >= "C37" and substr(DX, 1, 3) <= "C41" then 1
                  when Dx_CodeType = "10" and DX like "C43%" then 1
                  when Dx_CodeType = "10" and DX >= "C45" and substr(DX, 1, 3) <= "C58" then 1
                  when Dx_CodeType = "10" and DX >= "C60" and substr(DX, 1, 3) <= "C75" then 1
                  when Dx_CodeType = "10" and DX like "C76%" then 1
                  when Dx_CodeType = "10" and DX >= "C81" and substr(DX, 1, 4) <= "C943" then 1
                  when Dx_CodeType = "10" and DX like "C948%" then 1
                  when Dx_CodeType = "10" and DX like "C95%" then 1
                  when Dx_CodeType = "10" and DX >= "C960" and DX <= "C964" then 1
                  when Dx_CodeType = "10" and DX = "C969" then 1
                  when Dx_CodeType = "10" and DX = "C96A" then 1
                  when Dx_CodeType = "10" and DX = "C96Z" then 1
                  when Dx_CodeType = "10" and DX = "D45" then 1
                  when Dx_CodeType = "10" and DX = "D89" then 1
                  when Dx_CodeType = "10" and DX = "Z8546" then 1
                  else 0
             end) as Any_tumor,
         MAX(case when Dx_CodeType = "09" and DX = "42610" then 1
                  when Dx_CodeType = "09" and DX = "42611" then 1
                  when Dx_CodeType = "09" and DX = "42613" then 1
                  when Dx_CodeType = "09" and DX >= "4262" and DX <= "4264" then 1
                  when Dx_CodeType = "09" and DX >= "42650" and DX <= "42653" then 1
                  when Dx_CodeType = "09" and DX >= "4266" and DX <= "4268" then 1
                  when Dx_CodeType = "09" and DX = "4270" then 1
                  when Dx_CodeType = "09" and DX = "4272" then 1
                  when Dx_CodeType = "09" and DX = "42731" then 1
                  when Dx_CodeType = "09" and DX = "4276" then 1
                  when Dx_CodeType = "09" and DX = "4279" then 1
                  when Dx_CodeType = "09" and DX = "7850" then 1
                  when Dx_CodeType = "09" and DX = "V450" then 1
                  when Dx_CodeType = "09" and DX = "V533" then 1
                  when Dx_CodeType = "10" and DX = "I440" then 1
                  when Dx_CodeType = "10" and DX = "I441" then 1
                  when Dx_CodeType = "10" and DX >= "I443" and DX <= "I452" then 1
                  when Dx_CodeType = "10" and DX >= "I454" and substr(DX, 1, 4) <= "I458" then 1
                  when Dx_CodeType = "10" and DX = "I459" then 1
                  when Dx_CodeType = "10" and DX >= "I47" and substr(DX, 1, 3) <= "I49" then 1
                  when Dx_CodeType = "10" and DX = "R000" then 1
                  when Dx_CodeType = "10" and DX = "R001" then 1
                  when Dx_CodeType = "10" and DX = "R008" then 1
                  when Dx_CodeType = "10" and DX like "T821%" then 1
                  when Dx_CodeType = "10" and DX like "Z450%" then 1
                  when Dx_CodeType = "10" and DX = "Z950" then 1
                  when Dx_CodeType = "10" and DX = "Z95810" then 1
                  when Dx_CodeType = "10" and DX = "Z95818" then 1
                  when Dx_CodeType = "10" and DX = "Z959" then 1
                  else 0
             end) as Cardiac_arrhythmias,
         MAX(case when Dx_CodeType = "09" and DX = "4150" then 1
                  when Dx_CodeType = "09" and DX = "4168" then 1
                  when Dx_CodeType = "09" and DX = "4169" then 1
                  when Dx_CodeType = "09" and DX >= "491" and substr(DX, 1, 3) <= "494" then 1
                  when Dx_CodeType = "09" and DX like "496%" then 1
                  when Dx_CodeType = "10" and DX like "I260%" then 1
                  when Dx_CodeType = "10" and DX >= "I272" and DX <= "I279" then 1
                  when Dx_CodeType = "10" and DX >= "J40" and substr(DX, 1, 3) <= "J47" then 1
                  when Dx_CodeType = "10" and DX >= "J60" and substr(DX, 1, 3) <= "J67" then 1
                  when Dx_CodeType = "10" and DX = "J684" then 1
                  when Dx_CodeType = "10" and DX = "J701" then 1
                  when Dx_CodeType = "10" and DX = "J703" then 1
                  else 0
             end) as Chronic_pulmonary_disease,
         MAX(case when Dx_CodeType = "09" and DX >= "2860" and DX <= "2869" then 1
                  when Dx_CodeType = "09" and DX = "2871" then 1
                  when Dx_CodeType = "09" and DX >= "2873" and DX <= "2875" then 1
                  when Dx_CodeType = "10" and DX >= "D65" and substr(DX, 1, 3) <= "D68" then 1
                  when Dx_CodeType = "10" and DX = "D691" then 1
                  when Dx_CodeType = "10" and DX >= "D693" and DX <= "D696" then 1
                  else 0
             end) as Coagulopathy,
         MAX(case when Dx_CodeType = "09" and DX >= "2504" and DX <= "25073" then 1
                  when Dx_CodeType = "09" and DX >= "25090" and DX <= "25093" then 1
                  when Dx_CodeType = "10" and DX >= "E102" and DX <= "E108" then 1
                  when Dx_CodeType = "10" and DX >= "E112" and DX <= "E118" then 1
                  when Dx_CodeType = "10" and DX >= "E122" and DX <= "E128" then 1
                  when Dx_CodeType = "10" and DX >= "E132" and substr(DX, 1, 4) <= "E138" then 1
                  else 0
             end) as Complicated_diabetes,
         MAX(case when Dx_CodeType = "09" and DX = "40201" then 1
                  when Dx_CodeType = "09" and DX = "40211" then 1
                  when Dx_CodeType = "09" and DX = "40291" then 1
                  when Dx_CodeType = "09" and DX like "425%" then 1
                  when Dx_CodeType = "09" and DX like "428%" then 1
                  when Dx_CodeType = "09" and DX = "4293" then 1
                  when Dx_CodeType = "10" and DX = "A1884" then 1
                  when Dx_CodeType = "10" and DX = "I099" then 1
                  when Dx_CodeType = "10" and DX = "I110" then 1
                  when Dx_CodeType = "10" and DX = "I130" then 1
                  when Dx_CodeType = "10" and DX = "I132" then 1
                  when Dx_CodeType = "10" and DX = "I255" then 1
                  when Dx_CodeType = "10" and DX like "I42%" then 1
                  when Dx_CodeType = "10" and DX like "I43%" then 1
                  when Dx_CodeType = "10" and DX like "I50%" then 1
                  when Dx_CodeType = "10" and DX = "I517" then 1
                  when Dx_CodeType = "10" and DX = "P290" then 1
                  else 0
             end) as Congestive_heart_failure,
         MAX(case when Dx_CodeType = "09" and DX >= "2801" and DX <= "2819" then 1
                  when Dx_CodeType = "09" and DX = "2859" then 1
                  when Dx_CodeType = "10" and DX >= "D501" and DX <= "D509" then 1
                  when Dx_CodeType = "10" and DX >= "D51" and substr(DX, 1, 3) <= "D53" then 1
                  when Dx_CodeType = "10" and DX = "D649" then 1
                  else 0
             end) as Deficiency_anemia,
         MAX(case when Dx_CodeType = "09" and DX like "290%" then 1
                  when Dx_CodeType = "09" and DX = "3310" then 1
                  when Dx_CodeType = "09" and DX = "3311" then 1
                  when Dx_CodeType = "09" and DX = "3312" then 1
                  when Dx_CodeType = "10" and DX >= "F01" and substr(DX, 1, 3) <= "F03" then 1
                  when Dx_CodeType = "10" and DX = "F05" then 1
                  when Dx_CodeType = "10" and DX like "G30%" then 1
                  when Dx_CodeType = "10" and DX = "G3101" then 1
                  when Dx_CodeType = "10" and DX = "G3109" then 1
                  when Dx_CodeType = "10" and DX = "G311" then 1
                  else 0
             end) as Dementia,
         MAX(case when Dx_CodeType = "09" and DX like "276%" then 1
                  when Dx_CodeType = "10" and DX = "E222" then 1
                  when Dx_CodeType = "10" and DX like "E86%" then 1
                  when Dx_CodeType = "10" and DX like "E87%" then 1
                  else 0
             end) as Fluid_and_electrolyte_disorders,
         MAX(case when Dx_CodeType = "09" and DX >= "042" and substr(DX, 1, 3) <= "044" then 1
                  when Dx_CodeType = "10" and DX like "B20%" then 1
                  else 0
             end) as HIV_AIDS,
         MAX(case when Dx_CodeType = "09" and DX like "342%" then 1
                  when Dx_CodeType = "09" and DX like "344%" then 1
                  when Dx_CodeType = "10" and DX = "G041" then 1
                  when Dx_CodeType = "10" and DX = "G114" then 1
                  when Dx_CodeType = "10" and DX = "G801" then 1
                  when Dx_CodeType = "10" and DX = "G802" then 1
                  when Dx_CodeType = "10" and DX like "G81%" then 1
                  when Dx_CodeType = "10" and DX like "G82%" then 1
                  when Dx_CodeType = "10" and DX like "G83%" then 1
                  else 0
             end) as Hemiplegia,
         MAX(case when Dx_CodeType = "09" and DX = "4011" then 1
                  when Dx_CodeType = "09" and DX = "4019" then 1
                  when Dx_CodeType = "09" and DX = "40210" then 1
                  when Dx_CodeType = "09" and DX = "40290" then 1
                  when Dx_CodeType = "09" and DX = "40410" then 1
                  when Dx_CodeType = "09" and DX = "40490" then 1
                  when Dx_CodeType = "09" and DX = "40511" then 1
                  when Dx_CodeType = "09" and DX = "40519" then 1
                  when Dx_CodeType = "09" and DX = "40591" then 1
                  when Dx_CodeType = "09" and DX = "40599" then 1
                  when Dx_CodeType = "10" and DX like "I10%" then 1
                  when Dx_CodeType = "10" and DX >= "I11" and substr(DX, 1, 3) <= "I13" then 1
                  when Dx_CodeType = "10" and DX like "I15%" then 1
                  when Dx_CodeType = "10" and DX = "N262" then 1
                  else 0
             end) as Hypertension,
         MAX(case when Dx_CodeType = "09" and DX = "07032" then 1
                  when Dx_CodeType = "09" and DX = "07033" then 1
                  when Dx_CodeType = "09" and DX = "07054" then 1
                  when Dx_CodeType = "09" and DX = "4560" then 1
                  when Dx_CodeType = "09" and DX = "4561" then 1
                  when Dx_CodeType = "09" and DX = "45620" then 1
                  when Dx_CodeType = "09" and DX = "45621" then 1
                  when Dx_CodeType = "09" and DX = "5710" then 1
                  when Dx_CodeType = "09" and DX = "5712" then 1
                  when Dx_CodeType = "09" and DX = "5713" then 1
                  when Dx_CodeType = "09" and DX >= "57140" and DX <= "57149" then 1
                  when Dx_CodeType = "09" and DX = "5715" then 1
                  when Dx_CodeType = "09" and DX = "5716" then 1
                  when Dx_CodeType = "09" and DX = "5718" then 1
                  when Dx_CodeType = "09" and DX = "5719" then 1
                  when Dx_CodeType = "09" and DX = "5723" then 1
                  when Dx_CodeType = "09" and DX = "5728" then 1
                  when Dx_CodeType = "09" and DX = "V427" then 1
                  when Dx_CodeType = "10" and DX like "B18%" then 1
                  when Dx_CodeType = "10" and DX like "I85%" then 1
                  when Dx_CodeType = "10" and DX = "I864" then 1
                  when Dx_CodeType = "10" and DX like "K70%" then 1
                  when Dx_CodeType = "10" and DX = "K711" then 1
                  when Dx_CodeType = "10" and DX >= "K713" and DX <= "K715" then 1
                  when Dx_CodeType = "10" and DX = "K717" then 1
                  when Dx_CodeType = "10" and DX like "K721%" then 1
                  when Dx_CodeType = "10" and DX like "K729%" then 1
                  when Dx_CodeType = "10" and DX >= "K73" and substr(DX, 1, 3) <= "K74" then 1
                  when Dx_CodeType = "10" and DX = "K754" then 1
                  when Dx_CodeType = "10" and DX = "K7581" then 1
                  when Dx_CodeType = "10" and DX = "K760" then 1
                  when Dx_CodeType = "10" and DX >= "K762" and DX <= "K769" then 1
                  when Dx_CodeType = "10" and DX = "Z4823" then 1
                  when Dx_CodeType = "10" and DX = "Z944" then 1
                  else 0
             end) as Liver_disease,
         MAX(case when Dx_CodeType = "09" and DX >= "196" and substr(DX, 1, 3) <= "199" then 1
                  when Dx_CodeType = "10" and DX = "C459" then 1
                  when Dx_CodeType = "10" and DX >= "C77" and substr(DX, 1, 3) <= "C80" then 1
                  else 0
             end) as Metastatic_cancer,
         MAX(case when Dx_CodeType = "09" and DX like "440%" then 1
                  when Dx_CodeType = "09" and DX = "4412" then 1
                  when Dx_CodeType = "09" and DX = "4414" then 1
                  when Dx_CodeType = "09" and DX = "4417" then 1
                  when Dx_CodeType = "09" and DX = "4419" then 1
                  when Dx_CodeType = "09" and DX >= "4431" and DX <= "4439" then 1
                  when Dx_CodeType = "09" and DX = "4471" then 1
                  when Dx_CodeType = "09" and DX = "5571" then 1
                  when Dx_CodeType = "09" and DX = "5579" then 1
                  when Dx_CodeType = "09" and DX = "V434" then 1
                  when Dx_CodeType = "10" and DX = "E0851" then 1
                  when Dx_CodeType = "10" and DX = "E0852" then 1
                  when Dx_CodeType = "10" and DX = "E0951" then 1
                  when Dx_CodeType = "10" and DX = "E0952" then 1
                  when Dx_CodeType = "10" and DX = "E1051" then 1
                  when Dx_CodeType = "10" and DX = "E1052" then 1
                  when Dx_CodeType = "10" and DX = "E1151" then 1
                  when Dx_CodeType = "10" and DX = "E1351" then 1
                  when Dx_CodeType = "10" and DX = "E1352" then 1
                  when Dx_CodeType = "10" and DX = "I7779" then 1
                  when Dx_CodeType = "10" and DX = "I670" then 1
                  when Dx_CodeType = "10" and DX like "I70%" then 1
                  when Dx_CodeType = "10" and DX like "I71%" then 1
                  when Dx_CodeType = "10" and DX = "I731" then 1
                  when Dx_CodeType = "10" and DX like "I738%" then 1
                  when Dx_CodeType = "10" and DX = "I739" then 1
                  when Dx_CodeType = "10" and DX = "I771" then 1
                  when Dx_CodeType = "10" and DX >= "I7771" and DX <= "I7774" then 1
                  when Dx_CodeType = "10" and DX like "I79%" then 1
                  when Dx_CodeType = "10" and DX = "K551" then 1
                  when Dx_CodeType = "10" and DX = "K558" then 1
                  when Dx_CodeType = "10" and DX = "K559" then 1
                  when Dx_CodeType = "10" and DX like "Z9582%" then 1
                  when Dx_CodeType = "10" and DX = "Z959" then 1
                  else 0
             end) as Peripheral_vascular_disease,
         MAX(case when Dx_CodeType = "09" and DX >= "295" and DX <= "29899" then 1
                  when Dx_CodeType = "09" and DX = "2991" then 1
                  when Dx_CodeType = "09" and DX = "29911" then 1
                  when Dx_CodeType = "10" and DX like "F20%" then 1
                  when Dx_CodeType = "10" and DX >= "F22" and substr(DX, 1, 3) <= "F25" then 1
                  when Dx_CodeType = "10" and DX like "F28%" then 1
                  when Dx_CodeType = "10" and DX like "F29%" then 1
                  when Dx_CodeType = "10" and DX >= "F30" and substr(DX, 1, 3) <= "F33" then 1
                  when Dx_CodeType = "10" and DX = "F348" then 1
                  when Dx_CodeType = "10" and DX = "F349" then 1
                  when Dx_CodeType = "10" and DX like "F39%" then 1
                  when Dx_CodeType = "10" and DX = "F4489" then 1
                  when Dx_CodeType = "10" and DX = "F843" then 1
                  else 0
             end) as Psychosis,
         MAX(case when Dx_CodeType = "09" and DX like "416%" then 1
                  when Dx_CodeType = "09" and DX = "4179" then 1
                  when Dx_CodeType = "10" and DX like "I26%" then 1
                  when Dx_CodeType = "10" and DX like "I27%" then 1
                  when Dx_CodeType = "10" and DX = "I280" then 1
                  when Dx_CodeType = "10" and DX = "I288" then 1
                  when Dx_CodeType = "10" and DX = "I289" then 1
                  else 0
             end) as Pulmonary_circulation_disorders,
         MAX(case when Dx_CodeType = "09" and DX = "40311" then 1
                  when Dx_CodeType = "09" and DX = "40391" then 1
                  when Dx_CodeType = "09" and DX = "40412" then 1
                  when Dx_CodeType = "09" and DX = "40492" then 1
                  when Dx_CodeType = "09" and DX like "585%" then 1 /*like statement with % is any value beginning with*/
                  when Dx_CodeType = "09" and DX like "586%" then 1
                  when Dx_CodeType = "09" and DX = "V420" then 1
                  when Dx_CodeType = "09" and DX = "V451" then 1
                  when Dx_CodeType = "09" and DX = "V560" then 1
                  when Dx_CodeType = "09" and DX = "V568" then 1
                  when Dx_CodeType = "10" and DX = "I120" then 1
                  when Dx_CodeType = "10" and DX like "I13%" then 1
                  when Dx_CodeType = "10" and DX >= "N032" and DX <= "N037" then 1
                  when Dx_CodeType = "10" and DX >= "N052" and DX <= "N057" then 1
                  when Dx_CodeType = "10" and DX like "N18%" then 1
                  when Dx_CodeType = "10" and DX like "N19%" then 1
                  when Dx_CodeType = "10" and DX = "N250" then 1
                  when Dx_CodeType = "10" and DX = "Z3932" then 1
                  when Dx_CodeType = "10" and DX = "Z4822" then 1
                  when Dx_CodeType = "10" and DX like "Z490%" then 1
                  when Dx_CodeType = "10" and DX = "Z4931" then 1
                  when Dx_CodeType = "10" and DX = "Z9115" then 1
                  when Dx_CodeType = "10" and DX = "Z940" then 1
                  when Dx_CodeType = "10" and DX = "Z992" then 1
                  else 0
             end) as Renal_failure,
         MAX(case when Dx_CodeType = "09" and DX >= "260" and substr(DX, 1, 3) <= "263" then 1
                  when Dx_CodeType = "10" and DX >= "E40" and substr(DX, 1, 3) <= "E46" then 1
                  when Dx_CodeType = "10" and DX = "E640" then 1
                  when Dx_CodeType = "10" and DX = "R634" then 1
                  when Dx_CodeType = "10" and DX = "R64" then 1
                  else 0
             end) as Weight_loss
  from &dx_set /* Ampersand identifies macro variables. FROM creates this table using the dataset which was assigned as a Macro Variable */
  group by patid /* Groups identical data into groups. W/o this, every row will have 1's. (https://www.geeksforgeeks.org/sql-group-by/) */
  order by patid; /* Ordering patient ID */
quit;

data &score_set;
  set for_combined_comorbidity_score; /* Modifies for_combined_comorbidity_score table by adding weights and saves to the macrovariable &score_set (which is the combined_comorb_score_set) */
  
   combinedscore = (5 * Metastatic_cancer);
   combinedscore + (Congestive_heart_failure * 2);
   combinedscore + (Dementia * 2);
   combinedscore + (Renal_failure * 2);
   combinedscore + (Weight_loss * 2);
   combinedscore + (Hemiplegia);
   combinedscore + (Alcohol_abuse);
   combinedscore + (Any_tumor);
   combinedscore + (Cardiac_arrhythmias);
   combinedscore + (Chronic_pulmonary_disease);
   combinedscore + (Coagulopathy);
   combinedscore + (Complicated_diabetes);
   combinedscore + (Deficiency_anemia);
   combinedscore + (Fluid_and_electrolyte_disorders);
   combinedscore + (Liver_disease);
   combinedscore + (Peripheral_vascular_disease);
   combinedscore + (Psychosis);
   combinedscore + (Pulmonary_circulation_disorders);
   combinedscore + (HIV_AIDS * -1);
   combinedscore + (Hypertension * -1);
 
  keep patid combinedscore;
run;

/* Export data in excel. Make sure to delete previously saved file before exporting */
proc export 
  data=combined_comorb_score_set
  dbms=xlsx 
  outfile="/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/comorbidity_score/files_output_from_sas/2011_sas_output.xlsx"
  replace;
run;
