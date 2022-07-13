#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The script will create figures 2 and 3
#----------------------------------------------------------------------------------------------------------------------#

############################################# IMPORT MODULES ###########################################################

# Read in relevant libraries
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.transforms import Affine2D # to move error bars
import matplotlib.transforms as transforms

##################################### PREPARED DATA TO CREATE FIGURES ##################################################

# Define columns
cols = ['ACS_Ver', 'State_Des', 'thirty_day_death_ind','niss','patid','prob_diff_residual','POSbeds','AHAbeds', 'IMM_3',
        'MORT_30_AMI', 'MORT_30_CABG', 'MORT_30_COPD', 'MORT_30_HF', 'MORT_30_PN','MORT_30_STK', 'READM_30_HOSP_WIDE',
        'SEP_1','amb_ind','fall_ind','motor_accident_ind','firearm_ind','PRVDR_NUM','year_fe','num_bene_served']

# Read in final analytical claims file (the matched contains the trauma levels and the unmatched are all of the nontrauma hospitals)
claims_trauma = pd.read_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_matched_claims_allyears_w_hos_qual.dta',columns=cols)
claims_nontrauma = pd.read_stata('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/final_unmatched_claims_allyears_w_hos_qual.dta',columns=cols)

# Create trauma_lvl column using state_des
claims_trauma['TRAUMA_LEVEL'] = np.where(claims_trauma['State_Des'] == '1', '1',
                                      np.where(claims_trauma['State_Des'] == '2', '2',
                                               np.where(claims_trauma['State_Des'] == '3', '3',
                                                        np.where(claims_trauma['State_Des'] == '4', '4&5',
                                                                 np.where(claims_trauma['State_Des'] == '5', '4&5',
                                                                          np.where(claims_trauma['State_Des'] == '-',
                                                                                   '-', '-'))))))

# Prioritize ACS. There is no levels 4/5 in ACS_Ver
claims_trauma.loc[claims_trauma['ACS_Ver'] == '1', 'TRAUMA_LEVEL'] = '1'
claims_trauma.loc[claims_trauma['ACS_Ver'] == '2', 'TRAUMA_LEVEL'] = '2'
claims_trauma.loc[claims_trauma['ACS_Ver'] == '3', 'TRAUMA_LEVEL'] = '3'

# Remove if TRAUMA_LEVEL == '-'
claims_trauma = claims_trauma[claims_trauma['TRAUMA_LEVEL']!='-']

# Add column to unmatched
claims_nontrauma['TRAUMA_LEVEL'] = 'NT'

# Concat all trauma and nontrauma hospitals in on dataframe
all_hos_df = pd.concat([claims_trauma,claims_nontrauma],axis=0)

# Create New Injury Severity Scores (NISS) bins
all_hos_df['niss_bins'] = np.where(((all_hos_df['niss'] > 15) & (all_hos_df['niss'] <= 20)), '16-20',
                               np.where(((all_hos_df['niss'] > 20) & (all_hos_df['niss'] <= 25)), '21-25',
                               np.where(((all_hos_df['niss'] > 25) & (all_hos_df['niss'] <= 30)),'26-30',
                               np.where(((all_hos_df['niss'] > 30) & (all_hos_df['niss'] <= 40)), '31-40',
                               np.where(((all_hos_df['niss'] > 40) & (all_hos_df['niss'] <= 50)), '41-50',
                               np.where((all_hos_df['niss'] > 50), '51+' ,0))))))

# Rename variable for hospital quality
all_hos_df = all_hos_df.rename(columns={'prob_diff_residual':'hos_qual_residual'})
all_hos_df['hos_qual_residual'] = all_hos_df['hos_qual_residual'].astype(float)

# Negate hospital quality so that positive is associated with higher quality. Currently, the more negative the score, the higher the quality but we need to negate the scores so that more positive = higher quality.
all_hos_df['hos_qual_residual'] = all_hos_df['hos_qual_residual']*-1

####################################### CREATE EXHIBIT NISS VS DEATH 30 DAYS ###########################################
# i.e. figure 2

# Define columns
columns=['_at1','_m1','_margin','_ci_lb','_ci_ub']

# Read in data
margins_df = pd.read_stata(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/merged_ats_claims_for_stata/nissvdeath_predicted_gem_min_all.dta',columns=columns)

# remove levels 345
margins_df = margins_df[margins_df['_at1'].isin(['LVL 1','LVL 2','Non-trauma'])]

# Convert to percent
margins_df['_margin'] = margins_df['_margin']*100
margins_df['_ci_lb'] = margins_df['_ci_lb']*100
margins_df['_ci_ub'] = margins_df['_ci_ub']*100

#--- Set up error bars ---#

# Calc Error bars length (need to half it since the function to create CI will double the length)
margins_df['bar_length'] = (margins_df['_ci_ub'] - margins_df['_ci_lb'])/2

# Create list from series in order to plot the error bars (half length)
error_bar_length_list_t1 = margins_df[margins_df['_at1']=='LVL 1']['bar_length'].tolist()
error_bar_length_list_t2 = margins_df[margins_df['_at1']=='LVL 2']['bar_length'].tolist()
error_bar_length_list_nt = margins_df[margins_df['_at1']=='Non-trauma']['bar_length'].tolist()

# Create list from series in order to plot the error bars (midpoint length)
error_bar_mdpt_list_t1 =margins_df[margins_df['_at1']=='LVL 1']['_margin'].tolist()
error_bar_mdpt_list_t2 =margins_df[margins_df['_at1']=='LVL 2']['_margin'].tolist()
error_bar_mdpt_list_nt =margins_df[margins_df['_at1']=='Non-trauma']['_margin'].tolist()
#-------#

# Point Plot with lines connecting the points
ax = sns.pointplot(x='_m1', y='_margin', data=margins_df,hue='_at1', zorder=0,palette=['darkorchid','orange','forestgreen'],markers=['o','^','s'],scale=0.5,ci=None, order=['16-20', '21-25', '26-30','31-40','41+'])

# Plot error bars
x = [0, 1, 2, 3, 4]  # the index starts at 0 so since there are ten categorical variables, need to start at 0 to 9
plt.errorbar(x, error_bar_mdpt_list_t1, yerr=error_bar_length_list_t1, fmt=',', color='darkorchid',capsize=3)
plt.errorbar(x, error_bar_mdpt_list_t2, yerr=error_bar_length_list_t2, fmt=',', color='orange',capsize=3)
plt.errorbar(x, error_bar_mdpt_list_nt, yerr=error_bar_length_list_nt, fmt=',', color='forestgreen',capsize=3)

plt.xlabel('New injury severity score') # x label
plt.ylabel('Adjusted 30 day mortality (percent)') # y label
plt.ylim([0, 50])  # Change dimensions of y axis if using all sample
L=plt.legend(title=None,frameon=False)
L.get_texts()[0].set_text('Level 1')
L.get_texts()[1].set_text('Level 2')
sns.despine()  # Trim off the edges that are not the axis
# plt.show()
plt.savefig(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/figures/niss_quality_after_margins_gem_min.pdf')  # Save figure
plt.close()  # Deletes top graph

# scp jessyjkn@phs-rs24.bsd.uchicago.edu:/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/figures/niss_quality_after_margins_gem_min.pdf /Users/jessyjkn/Desktop/Job/figures_from_server/trauma_center_project/

######################### CREATE DISTRIBUTION GRAPH OF HOSPITAL QUALITY BY TRAUMA LEVEL ################################
# i.e. figure 3A

# Group by hospital (hospital level data)
hos_qual_df = all_hos_df.groupby(['TRAUMA_LEVEL','PRVDR_NUM'])['hos_qual_residual'].agg(['mean']).reset_index().rename(columns={'mean':'hos_qual_residual'})

#___Perform a t test___#

# Create list for T level 1 and 2 and non-trauma centers and drop any with an nan
t1_list = hos_qual_df[(hos_qual_df['TRAUMA_LEVEL']=='1')&(~hos_qual_df['hos_qual_residual'].isna())]['hos_qual_residual'].to_list()
t2_list = hos_qual_df[(hos_qual_df['TRAUMA_LEVEL']=='2')&(~hos_qual_df['hos_qual_residual'].isna())]['hos_qual_residual'].to_list()
nt_list = hos_qual_df[(hos_qual_df['TRAUMA_LEVEL']=='NT')&(~hos_qual_df['hos_qual_residual'].isna())]['hos_qual_residual'].to_list()

# Create list for loop
comp_groups = ['1v2','1vnt','2vnt']

for c in comp_groups:

    print(f'\nTwo sample t-test results for {c}\n')

    # Test each group
    if c in ['1v2']:
        t_value,p_value=stats.ttest_ind(t1_list,t2_list)
    elif c in ['1vnt']:
        t_value,p_value=stats.ttest_ind(t1_list,nt_list)
    elif c in ['2vnt']:
        t_value,p_value=stats.ttest_ind(nt_list,t2_list)

    print('Test statistic is %f'%float("{:.6f}".format(t_value)))

    print('p-value for two tailed test is %f'%p_value)

    # Set alpha level for print out
    alpha = 0.05

    if p_value<=alpha:

        print('Since p-value(=%f)'%p_value,'<','alpha(=%.2f)'%alpha,'''We reject the null hypothesis (H0 = no difference)''')

    else:

        print('Since p-value(=%f)'%p_value,'>','alpha(=%.2f)'%alpha,'''We do not reject the null hypothesis H0.''')
#_______#

# Overlap all hospital type (trauma level 1, level 2, and nontrauma) into one graph
ax = sns.histplot(data=hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='1'],x='hos_qual_residual',stat='percent',color='darkorchid',edgecolor='indigo',zorder=1,hatch='////')
sns.histplot(data=hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='2'],x='hos_qual_residual',stat='percent',color='orange',edgecolor='darkorange',zorder=0,ax=ax,hatch='\\\\\\\\')
sns.histplot(data=hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='NT'],x='hos_qual_residual',stat='percent',color='forestgreen',edgecolor='darkgreen',zorder=2,ax=ax,hatch='oooo') #,bins=1500

# Store means in variables
tl_one_mean = hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='1']['hos_qual_residual'].mean()
tl_two_mean = hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='2']['hos_qual_residual'].mean()
nt_mean = hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='NT']['hos_qual_residual'].mean()
# notes, if you want scientific notation then do, e.g., "{:.3e}".format(tl_one_mean) where 3 is 3 decimal places

# Store standard deviations in variables
tl_one_sd = np.std(hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='1']['hos_qual_residual'])
tl_two_sd = np.std(hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='2']['hos_qual_residual'])
nt_sd = np.std(hos_qual_df[hos_qual_df['TRAUMA_LEVEL']=='NT']['hos_qual_residual'])

# Plot a line that resembles the mean. ymin and ymax options do NOT refer to the coordinates
plt.axvline(x=tl_one_mean,ymin=0,ymax=1,color='darkorchid',zorder=4)
plt.axvline(x=tl_two_mean,ymin=0,ymax=1,color='orange',zorder=3)
plt.axvline(x=nt_mean,ymin=0,ymax=1,color='forestgreen',zorder=4)

# Plot text to display mean
ax.annotate('Lvl 1 mean', xy=(tl_one_mean, 16), xytext=(tl_one_mean-0.02, 16),arrowprops=dict(facecolor='darkorchid', edgecolor='indigo', shrink=0.01))
ax.annotate('Lvl 2 mean', xy=(tl_two_mean, 13), xytext=(tl_two_mean-0.02, 13),arrowprops=dict(facecolor='orange', edgecolor='darkorange', shrink=0.01))
ax.annotate('Non-trauma mean', xy=(nt_mean, 17), xytext=(nt_mean+0.008, 17),arrowprops=dict(facecolor='forestgreen', edgecolor='darkgreen', shrink=0.01))

# #______ SD ________#
# # Create positions for 2 standard deviations (hence the times 2)
# x_values_sd = [(tl_one_mean-2*tl_one_sd),(tl_one_mean+2*tl_one_sd),(tl_two_mean-2*tl_two_sd),(tl_two_mean+2*tl_two_sd),(nt_mean-2*nt_sd),(nt_mean+2*nt_sd)]
# x_labels_for_values = ['-2sd','2sd','-2sd','2sd','-2sd','2sd']
# plt.xticks(x_values_sd,x_labels_for_values,rotation=45)
# #______#

#___ Quantiles ___#
# Create function to find 25th Percentile
def q25(x):
    return x.quantile(0.25)

# Create function to find 75th Percentile
def q75(x):
    return x.quantile(0.75)
#______#

# Find 25th and 75th percentile for lvl 1,2,nt distributions
tl1_25th = q25(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='1']['hos_qual_residual'])
tl1_75th = q75(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='1']['hos_qual_residual'])
tl2_25th = q25(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='2']['hos_qual_residual'])
tl2_75th = q75(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='2']['hos_qual_residual'])
nt_25th = q25(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='NT']['hos_qual_residual'])
nt_75th = q75(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='NT']['hos_qual_residual'])

# Create positions on x axis for 25th and 7th percentile for each distribution
x_values_sd = [tl1_25th,tl1_75th,tl2_25th,tl2_75th,nt_25th,nt_75th]
x_labels_for_values = ['25th','75th','25th','75th','25th','75th']
plt.xticks(x_values_sd,x_labels_for_values,rotation=45)
#_______#

# Change colors of x values
colors = ['darkorchid','darkorchid','orange','orange','forestgreen','forestgreen']
for ticklabel, tickcolor in zip(plt.gca().get_xticklabels(), colors):
    ticklabel.set_color(tickcolor)

# Set up legend
legend_elements = [Patch(facecolor='darkorchid', edgecolor='indigo', label='Level 1',hatch='///'),
                   Patch(facecolor='orange', edgecolor='darkorange', label='Level 2',hatch='\\\\\\'),
                   Patch(facecolor='forestgreen', edgecolor='darkgreen', label='Non-trauma',hatch='ooo')]
plt.legend(handles=legend_elements, fontsize='small', loc='center right', title=None,frameon=False)

plt.xlim([-0.05, 0.035])  # Change dimensions of x axis
plt.subplots_adjust(bottom=0.20) # Extend bottom portion of graph to see x label
# plt.ylim([0,6])  # Change dimensions of y axis
plt.xlabel('Risk-adjusted surgical quality measure')
sns.despine()  # Trim off the edges that are not the axis
# plt.show()
plt.savefig(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/figures/hospital_quality_histogram.pdf')  # Save figure
plt.close()  # Deletes top graph



######################### CREATE DISTRIBUTION GRAPH OF HOSPITAL VOLUME BY TRAUMA LEVEL ################################
# e.g. graph 3B

# Drop all provider id duplicates so that it is on the hospital level (you can also group by too. I checked and it's the same graph)
all_hos_df = all_hos_df.drop_duplicates(subset=['year_fe','PRVDR_NUM'])

#___Perform a t test___#

# Create list for T level 1 and 2 and non-trauma centers and drop any with an nan
t1_list = all_hos_df[(all_hos_df['TRAUMA_LEVEL']=='1')&(~all_hos_df['num_bene_served'].isna())]['num_bene_served'].to_list()
t2_list = all_hos_df[(all_hos_df['TRAUMA_LEVEL']=='2')&(~all_hos_df['num_bene_served'].isna())]['num_bene_served'].to_list()
nt_list = all_hos_df[(all_hos_df['TRAUMA_LEVEL']=='NT')&(~all_hos_df['num_bene_served'].isna())]['num_bene_served'].to_list()

# Create list for loop
comp_groups = ['1v2','1vnt','2vnt']

for c in comp_groups:

    print(f'\nTwo sample t-test results for {c}\n')

    # Test each group
    if c in ['1v2']:
        t_value,p_value=stats.ttest_ind(t1_list,t2_list)
    elif c in ['1vnt']:
        t_value,p_value=stats.ttest_ind(t1_list,nt_list)
    elif c in ['2vnt']:
        t_value,p_value=stats.ttest_ind(nt_list,t2_list)

    print('Test statistic is %f'%float("{:.6f}".format(t_value)))

    print('p-value for two tailed test is %f'%p_value)

    # Set alpha level for print out
    alpha = 0.05

    if p_value<=alpha:

        print('Since p-value(=%f)'%p_value,'<','alpha(=%.2f)'%alpha,'''We reject the null hypothesis (H0 = no difference)''')

    else:

        print('Since p-value(=%f)'%p_value,'>','alpha(=%.2f)'%alpha,'''We do not reject the null hypothesis H0.''')
#_______#

# Overlap all hospital type (trauma level 1, level 2, and nontrauma) into one graph
ax = sns.histplot(data=all_hos_df[all_hos_df['TRAUMA_LEVEL']=='1'],x='num_bene_served',stat='percent',color='darkorchid',edgecolor='indigo',zorder=0,hatch='////')
sns.histplot(data=all_hos_df[all_hos_df['TRAUMA_LEVEL']=='2'],x='num_bene_served',stat='percent',color='orange',edgecolor='darkorange',zorder=1,ax=ax,hatch='\\\\\\\\')
sns.histplot(data=all_hos_df[all_hos_df['TRAUMA_LEVEL']=='NT'],x='num_bene_served',stat='percent',color='forestgreen',edgecolor='darkgreen',zorder=2,ax=ax,hatch='oooo') #,bins=1500

# Store means in variables
tl_one_mean = all_hos_df[all_hos_df['TRAUMA_LEVEL']=='1']['num_bene_served'].mean()
tl_two_mean = all_hos_df[all_hos_df['TRAUMA_LEVEL']=='2']['num_bene_served'].mean()
nt_mean = all_hos_df[all_hos_df['TRAUMA_LEVEL']=='NT']['num_bene_served'].mean()
# notes, if you want scientific notation then do, e.g., "{:.3e}".format(tl_one_mean) where 3 is 3 decimal places

# Store standard deviations in variables
tl_one_sd = np.std(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='1']['num_bene_served'])
tl_two_sd = np.std(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='2']['num_bene_served'])
nt_sd = np.std(all_hos_df[all_hos_df['TRAUMA_LEVEL']=='NT']['num_bene_served'])

# Plot a line that resembles the mean. ymin and ymax options do NOT refer to the coordinates
plt.axvline(x=tl_one_mean,ymin=0,ymax=1,color='darkorchid')
plt.axvline(x=tl_two_mean,ymin=0,ymax=1,color='orange',zorder=0)
plt.axvline(x=nt_mean,ymin=0,ymax=1,color='forestgreen',zorder=3)

# Plot text to display mean
ax.annotate('Lvl 1 mean', xy=(tl_one_mean, 25), xytext=(210, 25),arrowprops=dict(facecolor='darkorchid', edgecolor='indigo', shrink=0.01))
ax.annotate('Lvl 2 mean', xy=(tl_two_mean, 20), xytext=(210, 20),arrowprops=dict(facecolor='orange', edgecolor='darkorange', shrink=0.01))
ax.annotate('Non-trauma mean', xy=(nt_mean, 30), xytext=(210, 30),arrowprops=dict(facecolor='forestgreen', edgecolor='darkgreen', shrink=0.01))

# Set up legend
legend_elements = [Patch(facecolor='darkorchid', edgecolor='indigo', label='Level 1',hatch='///'),
                   Patch(facecolor='orange', edgecolor='darkorange', label='Level 2',hatch='\\\\\\'),
                   Patch(facecolor='forestgreen', edgecolor='darkgreen', label='Non-trauma',hatch='ooo')]
plt.legend(handles=legend_elements, fontsize='small', loc='center right', title=None,frameon=False)

# plt.xlim([-0.027, 0.03])  # Change dimensions of x axis
plt.subplots_adjust(bottom=0.15) # Extend bottom portion of graph to see x label
# plt.ylim([0,40])  # Change dimensions of y axis
plt.xlabel('Number of injured patients served in a year')
sns.despine()  # Trim off the edges that are not the axis
# plt.show()
plt.savefig(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/figures/hospital_quality_histogram_num_bene.pdf')  # Save figure
plt.close()  # Deletes top graph




