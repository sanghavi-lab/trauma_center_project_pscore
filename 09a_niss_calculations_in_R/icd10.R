# Read in packages
library(icdpicr)
library(tidyverse)

# Step 1: Read in data with all dx as characters
data.in <- read.csv('{specify/path/containing/icd10/data.csv}', stringsAsFactors = F)

# Step 2: Find AIS and ISS (for ICD10, this will use the ROCmax method by default)
data.out <- cat_trauma(data.in,'dx',icd10='cm', i10_iss_method='gem_min')

# Step 3: Read out data
write.csv(data.out,'{specify/path/to/export/icd10/data.csv}')


