# Read in packages
library(icdpicr)
library(tidyverse)

# Step 1: Read in data with all dx as characters.
data.in <- read.csv('{specify/path/containing/icd9/data.csv}', stringsAsFactors = F)

# Step 2: Find AIS, ISS, and NISS
data.out <- cat_trauma(data.in,'dx',icd10=F)

# Step 3: Read out data
write.csv(data.out,'{specify/path/to/export/icd9/data.csv}')