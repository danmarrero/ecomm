# ETL Script

ptm <- proc.time()

rm(list = ls(all.names = TRUE))

options(scipen = 999)

options(googleAuthR.scopes.selected =
          "https://www.googleapis.com/auth/cloud-platform")

project <- "ecomm-197702"
zone <- "us-east4-a"
account_key <- "gcs-key.json"
bucket <- "ecomm_lux"

Sys.setenv(
  GCS_AUTH_FILE = account_key,
  GCS_DEFAULT_PROJECT_ID = project,
  GCS_DEFAULT_ZONE = zone,
  GCS_DEFAULT_BUCKET = bucket
)

library(tidyverse)
library(readxl)
library(bigrquery)
library(googleCloudStorageR)

gcs_auth()

# Load Data ---------------------------------------------------------------

gcs_upload("gl-dashboard.html", gcs_global_bucket(bucket), type = 'text/html')
gcs_upload("dashboard.html", gcs_global_bucket(bucket), type = 'text/html')
gcs_upload("p-lc.png", gcs_global_bucket(bucket))
gcs_upload("p-to.png", gcs_global_bucket(bucket))
