#----------------------------------------------------------------------------------------------------------------------#
# Project: Trauma center analysis using Medicare data
# Author: Jessy Nguyen
# Last Updated: July 11, 2022
# Description: This script prepares the files for ICDPICR by dropping any duplicated icd codes.
#----------------------------------------------------------------------------------------------------------------------#

################################################ IMPORT MODULES ########################################################

# Read in relevant libraries
import numpy as np
import pandas as pd

######################################### DROP DUPLICATED ICD CODES ####################################################
# Here we use pandas to drop duplicated icd codes and convert to csv in preparation for ICDPICR                        #
########################################################################################################################

# Specify Years
years = [2011,2012,2013,2014,2015,2016,2017]

# Identify columns needed for ICDPICR
col = ['UNIQUE_ID'] + [f'dx{i}' for i in range(1,39)]

for y in years:

    # Read in data
    ip_op = pd.read_parquet(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/all_hos_claims/{y}',engine='fastparquet',columns=col)

    # Convert all to str
    ip_op = ip_op.astype(str)

    # Long format so that the icd codes are all in one column called DX
    ip_op_long = pd.melt(ip_op, id_vars='UNIQUE_ID', value_name='DX',var_name='column_names') # Note: UNIQUE ID is the MEDPAR_ID (from IP) or CLM_ID (from op)

    # Recover memory
    del ip_op

    # Drop Duplicated icd code
    ip_op_long = ip_op_long.drop_duplicates(subset=['UNIQUE_ID','DX'],keep='last')

    # Wide Format to input into ICDPICR
    ip_op_wide = ip_op_long.pivot(index='UNIQUE_ID', columns='column_names', values='DX')

    # Recover memory
    del ip_op_long

    # Reset the index so that UNIQUE_ID is not an index
    ip_op_wide = ip_op_wide.reset_index()

    # Replace all empty strings with NaNs
    ip_op_wide = ip_op_wide.replace('',np.nan)

    # Read out (Because I dropped duplicated icd codes, note that the number of dx columns may be different from the claims data)
    ip_op_wide.to_csv(f'/mnt/labshares/sanghavi-lab/Jessy/data/trauma_center_project_all_hos_claims/niss_calculation/for_R/{y}_after_drop_duplicates.csv',index=False,index_label=False)







