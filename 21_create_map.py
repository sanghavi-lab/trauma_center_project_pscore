#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center project
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: The goal of this script is to create a map that visualizes the distribution of trauma center levels 1 and
# 2 throughout the United States (figure 1)
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT PACKAGES #######################################################

# Read in relevant libraries
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np

################################################# CREATE MAP ###########################################################

#___ Read in and prepare data ___#

# Define columns for ATS data
cols_ats = ['AHA_num', 'State', 'ACS_Ver', 'State_Des']

# Define columns for crosswalk data
cols_xwalk = ['LONG', 'LAT', 'ID', 'MSTATE']

# Read in geometry data for usa states outline
usa_state = gpd.read_file('/mnt/labshares/sanghavi-lab/data/public_data/data/shp_files/cb_2018_us_state_500k/')

# Keep only us states using fips codes
fips_list = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21',
             '22', '23', '24','25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
             '40', '41', '42','44', '45', '46','47', '48', '49', '50', '51', '53', '54', '55', '56']
usa_state = usa_state[usa_state['STATEFP'].isin(fips_list)]

# Read in trauma data from ATS and crosswalk data
trauma_ats_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/AM_TRAUMA_DATA_FIRST_TAB_UNLOCKED_2019.xlsx', usecols=cols_ats,dtype=str)
trauma_xwalk_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/NPINUM_MCRNUM_AHAID_CROSSWALK_2019.xlsx', header=3, dtype=str,usecols=cols_xwalk)

# Put indicator of 1 to observe if crosswalk and ats data matched
trauma_xwalk_df['ats_match_ind'] = 1

# Convert LONG/LAT to float
trauma_xwalk_df['LONG']=trauma_xwalk_df['LONG'].astype('float')
trauma_xwalk_df['LAT']=trauma_xwalk_df['LAT'].astype('float')

# Merge trauma data and crosswalk on AHA ID (American Hospital Association ID)
trauma_ats_xwalk_df = pd.merge(trauma_xwalk_df,trauma_ats_df, left_on=['ID'], right_on=['AHA_num'],how='right')

# Filter out those who did matched
trauma_ats_xwalk_df = trauma_ats_xwalk_df[~trauma_ats_xwalk_df['ats_match_ind'].isna()]

# Create column for Trauma Level based on state's designation
trauma_ats_xwalk_df['TRAUMA_LVL'] = 0
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "1", 'TRAUMA_LVL'] = 1
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "2", 'TRAUMA_LVL'] = 2
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "3", 'TRAUMA_LVL'] = 3
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "4", 'TRAUMA_LVL'] = 4
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "5", 'TRAUMA_LVL'] = 5

# Replace the TRAUMA_LVL based on American College of Surgeon
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "1", 'TRAUMA_LVL'] = 1
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "2", 'TRAUMA_LVL'] = 2
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "3", 'TRAUMA_LVL'] = 3
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "4", 'TRAUMA_LVL'] = 4
trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "5", 'TRAUMA_LVL'] = 5

#________________________ Create US Map broken down by state/county ________________________#

#--- Create shapely object for points ---#

# Create shapely object from long and lat
points = gpd.GeoDataFrame(trauma_ats_xwalk_df, geometry=gpd.points_from_xy(trauma_ats_xwalk_df['LONG'], trauma_ats_xwalk_df['LAT']))

#--- Create the Map ---#

# Layout
fig, ax = plt.subplots(figsize = (20,20))

# For now, remove hawaii and alaska (see script below on create pdf files for HI and AK separately)
points = points[(points['MSTATE']!='HI')&(points['MSTATE']!='AK')]
usa_state = usa_state[(usa_state['STATEFP'] != '15') & (usa_state['STATEFP'] != '02')]

# Re-projection of map to be more flat
# Source dataset (region_map) is "encoded" in geographic coordinate system (units: lats and lons). It is safe to assume
# in my case, this is WGS84 (EPSG: 4326). If I want my plot to look more like it does in e.g Google Maps (i.e. more flat),
# I will have to re-project its coordinates into one of many projected coordinate systems (e.g. units: meters). I can use
# globally acceptable WEB MERCATOR (EPSG: 3857). Geopandas makes this as easy as possible through the most used CRSes by
# their EPSG code.

# Source does not have a crs assigned to it, do this:
usa_state.crs = {"init": "epsg:4326"} # For state outline
points.crs = {"init": "epsg:4326"} # for points

# Now that Geopandas is the "encoding" of your coordinates, I can perform any coordinate re-projection
usa_state = usa_state.to_crs(epsg=3857)
points = points.to_crs(epsg=3857)

# Plot US states outline
kwarg1s_state = {'edgecolor': 'gray', 'linewidth': 2} # Edge color is black
usa_state.plot(legend=True,legend_kwds={'orientation': "horizontal"},**kwarg1s_state, color='none',zorder=2,facecolor='none',ax=ax) # color and facecolor none for transparancy

# Plot the coordinates of all five trauma centers (including undefined). Be sure to consider facecolor if needed.
points[points['TRAUMA_LVL']==1].plot(zorder=3,ax=ax,color='darkorchid',marker="o",edgecolors='black', markersize=50)
points[points['TRAUMA_LVL']==2].plot(zorder=3,ax=ax,color='orange',marker="^",edgecolors='black', markersize=50) # Need edgecolor none so square is visible

# Create the legend
LegendElement = [Line2D([0], [0], marker='o', color='white',markeredgecolor='black', label='Level 1',markerfacecolor='darkorchid', markersize=15),
                 Line2D([0], [0], marker='^', color='white',markeredgecolor='black', label='Level 2', markerfacecolor='orange', markersize=15)
                 ]
ax.legend(handles = LegendElement, loc='lower right',fontsize=13, frameon=False,title='').get_title().set_fontsize('5')

# Erase axes
plt.axis('off')

# Export it.
plt.savefig('/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/figures/map/usa_map_w_traumalvl.pdf')

# Close map
plt.close()

########################################### CREATE MAP FOR HI AND AK ###################################################

# Specify states
states=['HI','AK']

for s in states:

    #___ Read in and prepare data ___#

    # Define columns for ATS data
    cols_ats = ['AHA_num', 'State', 'ACS_Ver', 'State_Des']

    # Define columns for crosswalk data
    cols_xwalk = ['LONG', 'LAT', 'ID', 'MSTATE']

    # Read in geometry data for usa states outline
    usa_state = gpd.read_file('/mnt/labshares/sanghavi-lab/data/public_data/data/shp_files/cb_2018_us_state_500k/')

    # Keep only us states using fips codes
    fips_list = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21',
                 '22', '23', '24','25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
                 '40', '41', '42','44', '45', '46','47', '48', '49', '50', '51', '53', '54', '55', '56']
    # usa_county = usa_county[usa_county['STATEFP'].isin(fips_list)]
    usa_state = usa_state[usa_state['STATEFP'].isin(fips_list)]

    # Read in trauma data from ATS and crosswalk data
    trauma_ats_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/AM_TRAUMA_DATA_FIRST_TAB_UNLOCKED_2019.xlsx', usecols=cols_ats,dtype=str)
    trauma_xwalk_df = pd.read_excel(f'/mnt/labshares/sanghavi-lab/data/public_data/data/trauma_center_data/NPINUM_MCRNUM_AHAID_CROSSWALK_2019.xlsx', header=3, dtype=str,usecols=cols_xwalk)

    # Put indicator of 1 to observe if crosswalk and ats data matched
    trauma_xwalk_df['ats_match_ind'] = 1

    # Convert LONG/LAT to float
    trauma_xwalk_df['LONG']=trauma_xwalk_df['LONG'].astype('float')
    trauma_xwalk_df['LAT']=trauma_xwalk_df['LAT'].astype('float')

    # Merge trauma data and crosswalk on AHA ID (American Hospital Association ID)
    trauma_ats_xwalk_df = pd.merge(trauma_xwalk_df,trauma_ats_df, left_on=['ID'], right_on=['AHA_num'],how='right')

    # Filter out those who did matched
    trauma_ats_xwalk_df = trauma_ats_xwalk_df[~trauma_ats_xwalk_df['ats_match_ind'].isna()]

    # Create column for Trauma Level based on state's designation
    trauma_ats_xwalk_df['TRAUMA_LVL'] = 0
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "1", 'TRAUMA_LVL'] = 1
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "2", 'TRAUMA_LVL'] = 2
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "3", 'TRAUMA_LVL'] = 3
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "4", 'TRAUMA_LVL'] = 4
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['State_Des'] == "5", 'TRAUMA_LVL'] = 5

    # Replace the TRAUMA_LVL based on American College of Surgeon
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "1", 'TRAUMA_LVL'] = 1
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "2", 'TRAUMA_LVL'] = 2
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "3", 'TRAUMA_LVL'] = 3
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "4", 'TRAUMA_LVL'] = 4
    trauma_ats_xwalk_df.loc[trauma_ats_xwalk_df['ACS_Ver'] == "5", 'TRAUMA_LVL'] = 5

    #________________________ Create US Map broken down by state/county ________________________#

    #--- Create shapely object for points ---#

    # Create shapely object from long and lat
    points = gpd.GeoDataFrame(trauma_ats_xwalk_df, geometry=gpd.points_from_xy(trauma_ats_xwalk_df['LONG'], trauma_ats_xwalk_df['LAT']))

    #--- Create the Map ---#

    # Layout
    fig, ax = plt.subplots(figsize = (20,20))

    # Keep specific state
    points = points[(points['MSTATE']==f'{s}')]

    # Keep specific states
    if s in ['HI']:
        usa_state = usa_state[(usa_state['STATEFP'] == '15')]
    if s in ['AK']:
        usa_state = usa_state[(usa_state['STATEFP'] == '02')]

    # Re-projection of map to be more flat
    # Source dataset (region_map) is "encoded" in geographic coordinate system (units: lats and lons). It is safe to assume
    # in my case, this is WGS84 (EPSG: 4326). If I want my plot to look more like it does in e.g Google Maps (i.e. more flat),
    # I will have to re-project its coordinates into one of many projected coordinate systems (e.g. units: meters). I can use
    # globally acceptable WEB MERCATOR (EPSG: 3857). Geopandas makes this as easy as possible through the most used CRSes by
    # their EPSG code.

    # Source does not have a crs assigned to it, do this:
    usa_state.crs = {"init": "epsg:4326"} # For state outline
    points.crs = {"init": "epsg:4326"} # for points

    # Now that Geopandas is the "encoding" of your coordinates, I can perform any coordinate re-projection
    usa_state = usa_state.to_crs(epsg=3857)
    points = points.to_crs(epsg=3857)

    # Plot US states outline
    kwarg1s_state = {'edgecolor': 'gray', 'linewidth': 2}  # Edge color is black
    usa_state.plot(legend=True, legend_kwds={'orientation': "horizontal"}, **kwarg1s_state, color='none', zorder=2,
                   facecolor='none', ax=ax)  # color and facecolor none for transparancy

    # Plot the coordinates of all five trauma centers (including undefined). Be sure to consider facecolor if needed.
    points[points['TRAUMA_LVL'] == 1].plot(zorder=3, ax=ax, color='darkorchid', marker="o", edgecolors='black', markersize=50)
    points[points['TRAUMA_LVL'] == 2].plot(zorder=3, ax=ax, color='orange', marker="^", edgecolors='black',
                                           markersize=50)  # Need edgecolor none so square is visible

    # Create the legend
    LegendElement = [Line2D([0], [0], marker='o', color='white', markeredgecolor='black', label='Level 1', markerfacecolor='darkorchid',markersize=15),
                     Line2D([0], [0], marker='^', color='white', markeredgecolor='black', label='Level 2',markerfacecolor='orange', markersize=15)]

    # Erase axes
    plt.axis('off')

    # Export it.
    plt.savefig(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project/figures/map/{s}_map_w_traumalvl.pdf')

    # Close map
    plt.close()





























