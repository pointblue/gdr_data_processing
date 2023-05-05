# Script for processing dive data from 1819
# Setting up to access files directly from AWS

# Code written by AS
# modified in 2021 to re-process dive data with correct foraging dive definition

# list packages, check if installed, install if not, load
list.of.packages <-
  c(
    "aws.s3",
    "stringr",
    "data.table",
    "tidyverse",
    "diveMove",
    "fasttime",
    "lubridate",
    "TwGeos",
    "RcppRoll"
  )
# compare to existing packages
new.packages <-
  list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
# install missing packages
if (length(new.packages) > 0) {
  install.packages(new.packages)
}
# load required packages
lapply(list.of.packages, library, character.only = TRUE)

# Set creditials for aws access
Sys.setenv(
  "AWS_ACCESS_KEY_ID" = "your_key",
  "AWS_SECRET_ACCESS_KEY" = "super_secret_key",
  "AWS_DEFAULT_REGION" = "us-west-2"
)
# set working directory

# UNITS:
# Temperature is measured every 30 seconds in 1/10 degree C, converted to degree C in processing
# Pressure measured once per second in mBar, converted to meters in processing
# Light is measured per minute on the minute in lux, not converted in processing


# Step 1: Zero offset correction ####
# set file index so can run in batches if needed
index = 1:10

# read in table with ID and deploy dates
gdr_depl <-
  read.csv("data/croz_royds_gdr_depl_all_v2021-08-27.csv") %>%
  filter(!is.na(filename))
filelist <- gdr_depl$filename


# First, make sure the aws.s3 package is installed
# install.packages("aws.s3")

library(aws.s3)

# Set your AWS access key ID and secret access key.
Sys.setenv("AWS_ACCESS_KEY_ID" = "your_access_key_id",
           "AWS_SECRET_ACCESS_KEY" = "your_secret_access_key")

# Loop to process each file
for (i in index) {
  # pull out filename and paste to create path
  fn <- as.character(filelist[i])
  print(paste("Processing", fn, "...", sep = " "), quote = FALSE)
  # print(paste("Started ",format(Sys.time(),tz="PST")))
  path = paste0("s3://deju-penguinscience/AllData/gdr_datashare/GDR_",
                seas,
                fn)
  # Read in header of file and extract logger id number
  log_id <-
    substr(str_match(
      s3read_using(
        fread,
        object = path,
        sep = ";",
        nrows = 8,
        col.names = "log_id"
      )[7],
      "Logger Id=(.*?)-ARE2"
    )[2], 3, 5)
  # print message if log ID matches
  ifelse(
    log_id == substr(fn, 15, 17),
    print("log ID match", quote = FALSE),
    print(
      paste(
        "**Warning** log ID in header does not match log id in file name: ",
        "header log ID = ",
        log_id,
        ", file name log id = ",
        substr(fn, 11, 14),
        sep = ""
      ),
      quote = FALSE
    )
  )
  deploy_date <-
    as.Date(gdr_depl[gdr_depl$filename == fn, "deploy_date"], format = "%Y-%m-%d")
  retrieve_date <-
    as.Date(gdr_depl[gdr_depl$filename == fn, "retrieve_date"], format = "%Y-%m-%d")
  # Load data format date, add columns for depth and datetime and filter to deployment dates
  # Add depth column to convert pressure (in mBar) to meters depth
  # Depth (in meter) = 0.010197 x (Pressure_measured_in_mBar - Pressure_atmospheric)
  # The average atmospheric pressure in the Ross Sea should be around 980 mBar
  # reading in table takes about 1 min
  data <-
    s3read_using(
      fread,
      object = path,
      skip = 25,
      drop = 6,
      col.names = c("date", "time", "temp", "pressure", "lux")
    )
  # fread can only read dates in as character so have to modify after loading
  # modifying data takes about 9 minutes
  print("modifying data...", quote = FALSE)
  data <- data %>%
    mutate(
      depth = 0.010197 * (pressure - 980),
      date = as.Date(date, format = "%d/%m/%Y"),
      time.posixct = as.POSIXct(paste(date, time, sep = " "), format =
                                  "%Y-%m-%d %H:%M:%S", tz = "GMT")
    ) %>%
    filter(date >= deploy_date & date <= retrieve_date)
  # Format as TDR object
  print("creating TDR object...", quote = FALSE)
  tdrX <- createTDR(
    time = data$time.posixct,
    depth = data$depth,
    concurrentData = data[, c(3, 5)],
    dtime = 1,
    file = "data"
  )
  # remove data object to free up memory
  rm("data")
  gc()
  print("tdrX created, calibrating depth...", quote = FALSE)
  # print(paste("Start calibration time: ", Sys.time()))
  start_time = Sys.time()
  # used these settings after some testing indicated they performed well with this data
  dcalib <- calibrateDepth(
    tdrX,
    dive.thr = 3,
    zoc.method = "filter",
    depth.bound = c(-10, 10),
    k = c(9, 540),
    probs = c(0.5, 0.05),
    descent.crit.q = 0.01,
    ascent.crit.q = 0,
    knot.factor = 20,
    na.rm = TRUE
  )
  end_time = Sys.time()
  # print(paste("End calibration time: ", end_time))
  print(paste(
    "Total calibration time = ",
    difftime(end_time, start_time, units = "hours")
  ), quote = FALSE)
  zoc_name <- paste(substr(fn, 1, 20), "_", "zoc", ".Rda", sep = "")
  s3saveRDS(dcalib,
            object = zoc_name,
            bucket = "your_bucket_location",
            multipart = TRUE)
  # Remove large objects from workspace
  rm(list = c("dcalib", "tdrX"))
  gc()  # run garbage collector to free up memory
  # remove files from temp dir too
  tmp_dir <- tempdir()
  files <- list.files(tmp_dir, full.names = T, pattern = "^file")
  file.remove(files)
  print("****DONE****", quote = FALSE)
  # print(paste("Ended ", Sys.time()))
  gc()
}
