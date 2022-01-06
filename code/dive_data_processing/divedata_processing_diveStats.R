# Script for processing all dive data from Geolocating Dive Recorders deployed on Adelie penguins on Ross Island 2016-2018 (last recoveries in 2019)

# Code written by A. Schmidt
# modified in 2021 to correct foraging dive definition
# code now also includes: 
  # 1. adjusting clocks for drift and rebooted loggers
  # 2. trimming to dives between deploy and retrieve dates (not inclusive)


# list packages, check if installed, install if not, load
list.of.packages <- c("aws.s3","stringr","data.table","tidyverse","diveMove","fasttime","lubridate","TwGeos","RcppRoll","future.apply")
# compare to existing packages
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
# install missing packages
if(length(new.packages)>0) {install.packages(new.packages)}
# load required packages
lapply(list.of.packages, library, character.only = TRUE)

# Set creditials for aws access
Sys.setenv("AWS_ACCESS_KEY_ID" = "your_KEY_ID", "AWS_SECRET_ACCESS_KEY" = "your_secret_access_key", 
           "AWS_DEFAULT_REGION" = "your_region")


# set working directory
setwd("your_dir")

# UNITS:
# Temperature is measured every 30 seconds in 1/10 degree C, converted to degree C in processing
# Pressure measured once per second in mBar, converted to meters in processing
# Light is measured per minute on the minute in lux, not converted in processing

# Step 1: zero offset correction to adjust for sensor drift

# read in GDR deploy file
# file contains data on all individuals that were deployed on
depl_dat<- read.csv("data/croz_royds_gdr_depl_all_v2021-08-27.csv")%>%
  filter(!is.na(filename))%>% # only include records for birds that had raw files
  mutate(bird_fn = coalesce(bad_avid,bird_id))%>% # add column that has bad avid prioritized because this is how most files were named
  select(season,br_col,bird_id,bird_fn,deploy_date,retrieve_date,filename,start_rec,end_rec,rebooted)


# list files already processed in s3 bucket
s3_files <- get_bucket_df("bucket_name", prefix="bucket_prefix")
s3_str <- paste(substr(s3_files$Key[-1], 14,33),".txt",sep="")
diff <- setdiff(filelist,s3_str)


# Loop to process each file
for(i in 1:length(diff)){
  # pull out filename and paste to create path
  # fn<-as.character(filelist[i])
  fn <- diff[i]
  message(paste("Processing", fn, "...", sep=" "))

  path=paste("s3://bucket/bucket_prefix/",fn,sep="")
  # Read in header of file and extract logger id number
  log_id <- substr(str_match(s3read_using(fread, object = path, sep=";",nrows=8,col.names="log_id")[7],"Logger Id=(.*?)-ARE2")[2],3,5)
  # print message if log ID matches
  ifelse(log_id==substr(fn,15,17),message("log ID match"),
         message(paste("**Warning** log ID in header does not match log id in file name: ","header log ID = ",log_id,", file name log id = ",substr(fn,11,14), sep=""),quote=FALSE))
  deploy_date <- as.Date(depl_dat[depl_dat$filename==fn,"deploy_date"],format="%Y-%m-%d")
  retrieve_date <- as.Date(depl_dat[depl_dat$filename==fn,"retrieve_date"], format="%Y-%m-%d")
  
  # Load data format date, add columns for depth and datetime and filter to deployment dates
  # Add depth column to convert pressure (in mBar) to meters depth
  # Depth (in meter) = 0.010197 x (Pressure_measured_in_mBar - Pressure_atmospheric)
  # The average atmospheric pressure in the Ross Sea ~980 mBar

  data <-s3read_using(fread, object = path,skip=25, drop=6, col.names=c("date","time","temp","pressure","lux"))
  #format dates and filter
  message("modifying data...")
  data<-data%>%
    mutate(depth=0.010197*(pressure-980),date=as.Date(date, format="%d/%m/%Y"),
           time.posixct =as.POSIXct(paste(date, time, sep=" "), format="%Y-%m-%d %H:%M:%S", tz="GMT"))%>%
    filter(date>=deploy_date&date<=retrieve_date)
  # Format as TDR object
  message("creating TDR object...")
  tdrX <- createTDR(time=data$time.posixct,
                    depth=data$depth,
                    concurrentData=data[, c(3,5)],
                    dtime=1, file="data")
  # remove data object to free up memory
  rm("data")
  gc()
  message("tdrX created, calibrating depth...")
  
  # test file took 4.66 hours to do the ZOC in 1718
  # test file on aws took 3.68 hours for an 1819 file (32189)
  start_time= Sys.time()
  dcalib <- calibrateDepth(tdrX, dive.thr = 3,
                           zoc.method="filter",
                           depth.bound=c(-10,10),
                           k=c(9,540),
                           probs = c(0.5,0.05),
                           descent.crit.q=0.01,
                           ascent.crit.q=0,
                           knot.factor = 20,
                           na.rm=TRUE)
  end_time = Sys.time()
  message(paste("Total calibration time = ",difftime(end_time,start_time, units="hours")))
  zoc_name<- paste(substr(fn,1,20),"_", "zoc",".Rda",sep="")
  # save in case need to come back to later
  s3saveRDS(dcalib, object=zoc_name,bucket="your_bucket", multipart=TRUE)
  # Remove large objects from workspace
  rm(list=c("dcalib","tdrX"))
  # clean up temp directory
  tmp_dir <- tempdir()
  files <- list.files(tmp_dir, full.names = T, pattern = "^file")
  file.remove(files)
  message("****DONE****")
  gc()  # run garbage collector to free up memory
}




# Step 2: calculate dive stats

# read in additional data required to calculate dive stats
# read in GDR deploy file
# file contains data on all individuals that were deployed on
depl_dat<- read.csv("data/croz_royds_gdr_depl_all_v2021-08-27.csv")%>%
  filter(!is.na(filename))%>% # only include records for birds that had raw files
  mutate(bird_fn = coalesce(bad_avid,bird_id))%>% # add column that has bad avid prioritized because this is how most files were named
  select(season,br_col,bird_id,bird_fn,deploy_date,retrieve_date,filename,start_rec,end_rec,rebooted)

# read in existing clock drift files
clock_drift <- read.csv("data/gdr_estimated_clock_drift.csv")

# rebooted
rebooted <- read.csv("data/rebooted_clock_adjust.csv")%>%
  mutate(date_start=as.POSIXct(date_start,tz="GMT"),recorded_start=as.POSIXct(recorded_start,tz="GMT"),
         offset=difftime(date_start,recorded_start,units="secs"))%>% # time offset in seconds
  dplyr::select(-X)

# join tables together and calculate drift rate per file
gdr_depl <- depl_dat%>%
  left_join(clock_drift)%>%
  mutate(start_rec=as.Date(start_rec, format="%m/%d/%Y"),end_rec=as.Date(end_rec, format="%m/%d/%Y"),
  drift_rate = cdrift_av*60/as.numeric(difftime(end_rec,start_rec, units="days")))%>%
  mutate(drift_rate = ifelse(season==2016&is.na(drift_rate),mean(drift_rate,na.rm=TRUE),drift_rate))%>% # no drift calculated for 1617 so applying mean drift from other 2 years
  group_by(season)%>% # group by season to apply mean season drift rate to remaining missing drift rates
  mutate(drift_rate = ifelse(is.na(drift_rate),mean(drift_rate,na.rm=TRUE),drift_rate))%>%
  ungroup()%>%
  left_join(rebooted)

# Apply diveStats_yr function to calculate diveStats from all birds in a season
# saves individual csvs for each bird
source("code/diveStats_GDR.R")
  
# inputs to function:
# season
# bird_id
# gdr_depl=table with deployment info for all birds that season
# s3_path=path to zero offset corrected data
#____s3 path can be "bucket" for reading directly from bucket, path is coded in function (do this is running from tsinfo or local machines)
#____ or can be path where s3 data are mounted on instance if running from aws server
# out = path where individual diveStats files will be saved 

# 2016 ####

# apply function to list of bird_ids, individuals files written as output
depl16 <- gdr_depl%>%
  filter(season==2016)
s3_path="bucket"
out16 = "out_path"

# if only running in one session, might want to use setDTthreads to allow data table functions to use more threads
lapply(depl16$bird_fn,diveStats_yr,seas=2016,gdr_depl=gdr_depl,s3_path=s3_path,out=out16)

# # 2017 ####
depl17 <- gdr_depl%>%
  filter(season==2017)
s3_path="bucket"
out17 = "out_path"

lapply(depl17$bird_fn,diveStats_yr,seas=2017,gdr_depl=gdr_depl,s3_path=s3_path,out=out17)

# 2018 ####
depl18 <- gdr_depl%>%
  filter(season==2018)
s3_path="bucket"
out18 = "out_path"

lapply(depl18$bird_fn,diveStats_yr,seas=2018,gdr_depl=gdr_depl,s3_path=s3_path,out=out18)

# read in and bind diveStats by season ####
# Once done with all years, read in files for each season and bind together
# 1617####
setwd("your_wd")
file_ls <- list.files(pattern = ".csv$")
length(file_ls)
plan(multicore) # use multisession on windows
divefiles <- future_lapply(
  file_ls,
  read_csv,
  col_types=cols(
    .default = col_double(),
    begdesc = col_datetime(format = ""),
    enddesc = col_datetime(format = ""),
    begasc = col_datetime(format = ""),
    botttim_ds = col_double(),
    enddive = col_datetime(format = ""),
    divetype = col_character(),
    divetype_ds = col_character(),
    divetype_as = col_character(),
    begdesc_orig = col_datetime(format = ""),
    file_id = col_character(),
    colony = col_character(),
    date = col_date(format = "")),
  future.seed = TRUE)
plan(sequential) # turn off multicore
# bind all files together
diveStats_all_1617<-rbindlist(divefiles)%>%# 3 seconds on aws m4.10xlarge
  mutate(bird_fn=parse_number(file_id),season=2016)

# check that have all birds in combined file
unique(diveStats_all_1617$bird_fn)
# write compiled file
fwrite(diveStats_all_1617,"your_path")
#replace extreme values with NA
diveStats_all_1617_filt<-diveStats_all_1617%>%
  filter(divetim<=420,maxdep <=190)%>%
  # for columns that start with temp_, replace with NA if temp_max >50, or if temp_min <(-5)
  mutate(across(starts_with("temp_"),~replace(.x,temp_max>50|temp_min<(-5),NA)))


# summarise deployments from 1617
diveSumm_1617<-dive_summary(diveStats_all_1617,season=2016)

diveSumm_1617_filt<-dive_summary(diveStats_all_1617_filt,season=2016)



#figure to summarize 1617 deployments
p1617 <- diveSumm_1617%>%
  ungroup()%>%
  dplyr::select(bird_id,ndives,ndays,dives_perday:maxpostdur,maxlt:start_time_offset,E_dive:prop_Fdive)%>%
  mutate_all(as.numeric)%>%
  pivot_longer(cols = c(-bird_id),names_to="variable")%>%
  ggplot()+
  geom_histogram(aes(x=value),bins=20)+
  facet_wrap(~variable,scales="free")+
  ggtitle("2016 dive deployment summary")

p1617_filt <- diveSumm_1617_filt%>%
  ungroup()%>%
  dplyr::select(bird_id,ndives,ndays,dives_perday:maxpostdur,maxlt:start_time_offset,E_dive:prop_Fdive)%>%
  mutate_all(as.numeric)%>%
  pivot_longer(cols = c(-bird_id),names_to="variable")%>%
  ggplot()+
  geom_histogram(aes(x=value),bins=20)+
  facet_wrap(~variable,scales="free")+
  ggtitle("2016 filtered")


ggpubr::ggarrange(p1617,p1617_filt,nrow=1,ncol=2)


# 1718 ####
setwd("your_path")
file_ls <- list.files(pattern = ".csv$")
length(file_ls)
plan(multicore) # use multisession on windows
divefiles<- future_lapply(file_ls,read_csv, future.seed = TRUE)
plan(sequential)
# bind all files together
diveStats_all_1718<-rbindlist(divefiles)%>% # 3 seconds on aws m4.10xlarge. #Fails if read in with fread because some parsing errors with datetimes, somehow introduced by fwrite?
  mutate(bird_fn=parse_number(file_id),season=2017)
# check that have all bird in combined file
unique(diveStats_all_1718$bird_fn)

#filter to remove outliers
diveStats_all_1718_filt <-diveStats_all_1718%>%
  filter(divetim<=420,maxdep <=190)%>%
  # for columns that start with temp_, replace with NA if temp_max >50, or if temp_min <(-5)
  mutate(across(starts_with("temp_"),~replace(.x,temp_max>50|temp_min<(-5),NA)))


# summarise deployments from 1718
diveSumm_1718<-dive_summary(diveStats_all_1718,season=2017)

diveSumm_1718_filt<-dive_summary(diveStats_all_1718_filt,season=2017)


#figure to summarize deployments
p1718 <-diveSumm_1718%>%
  ungroup()%>%
  dplyr::select(bird_id,ndives,ndays,dives_perday:maxpostdur,maxlt:start_time_offset,E_dive:prop_Fdive)%>%
  mutate_all(as.numeric)%>%
  pivot_longer(cols = c(-bird_id),names_to="variable")%>%
  ggplot()+
  geom_histogram(aes(x=value),bins=20)+
  facet_wrap(~variable,scales="free")+
  ggtitle("2017 dive deployment summary")

p1718_filt <-diveSumm_1718_filt%>%
  ungroup()%>%
  dplyr::select(bird_id,ndives,ndays,dives_perday:maxpostdur,maxlt:start_time_offset,E_dive:prop_Fdive)%>%
  mutate_all(as.numeric)%>%
  pivot_longer(cols = c(-bird_id),names_to="variable")%>%
  ggplot()+
  geom_histogram(aes(x=value),bins=20)+
  facet_wrap(~variable,scales="free")+
  ggtitle("2017 filtered")

ggpubr::ggarrange(p1718,p1718_filt,nrow=1,ncol=2)


# 1819 ####
setwd("your_path")
file_ls <- list.files(pattern = ".csv$")
length(file_ls)
plan(multicore) # use multisession on windows
divefiles <- future_lapply(
  file_ls,
  read_csv,
  col_types=cols(
    .default = col_double(),
    begdesc = col_datetime(format = ""),
    enddesc = col_datetime(format = ""),
    begasc = col_datetime(format = ""),
    botttim_ds = col_double(),
    enddive = col_datetime(format = ""),
    divetype = col_character(),
    divetype_ds = col_character(),
    divetype_as = col_character(),
    begdesc_orig = col_datetime(format = ""),
    file_id = col_character(),
    colony = col_character(),
    date = col_date(format = "")),
    future.seed = TRUE
  ) # fread fails because some parsing errors with datetimes, somehow introduced by fwrite?
plan(sequential)
# bind all files together
diveStats_all_1819<-rbindlist(divefiles)%>% # 3 seconds on aws m4.10xlarge. #Fails if read in with fread because some parsing errors with datetimes, somehow introduced by fwrite?
  mutate(bird_fn=parse_number(file_id),season=2018)
  # check that have all bird in combined file
unique(diveStats_all_1819$file_id)

#filter to remove outliers
diveStats_all_1819_filt <-diveStats_all_1819%>%
  filter(divetim<=420,maxdep <=190)%>%
  # for columns that start with temp_, replace with NA if temp_max >50, or if temp_min <(-5)
  mutate(across(starts_with("temp_"),~replace(.x,temp_max>50|temp_min<(-5),NA)))

# summarise deployments from 1819
diveSumm_1819<-dive_summary(diveStats_all_1819,season=2018)

# summarise deployments from 1819 after filtering
diveSumm_1819_filt<-dive_summary(diveStats_all_1819_filt,season=2018)



#figure to summarize deployments
p1819 <- diveSumm_1819%>%
  ungroup()%>%
  dplyr::select(bird_id,ndives,ndays,dives_perday:maxpostdur,maxlt:start_time_offset,E_dive:prop_Fdive)%>%
  mutate_all(as.numeric)%>%
  pivot_longer(cols = c(-bird_id),names_to="variable")%>%
  ggplot()+
  geom_histogram(aes(x=value),bins=20)+
  facet_wrap(~variable,scales="free")+
  ggtitle("2018 dive deployment summary")


p1819_filt <- diveSumm_1819_filt%>%
  ungroup()%>%
  dplyr::select(bird_id,ndives,ndays,dives_perday:maxpostdur,maxlt:start_time_offset,E_dive:prop_Fdive)%>%
  mutate_all(as.numeric)%>%
  pivot_longer(cols = c(-bird_id),names_to="variable")%>%
  ggplot()+
  geom_histogram(aes(x=value),bins=20)+
  facet_wrap(~variable,scales="free")+
  ggtitle("2018 dive deployment summary filtered")

ggpubr::ggarrange(p1819,p1819_filt,nrow=1,ncol=2)

# Bind all diveStats from all years together
diveStats_all_yrs <- bind_rows(diveStats_all_1617,diveStats_all_1718,diveStats_all_1819)

diveStats_all_yrs_filt <- bind_rows(diveStats_all_1617_filt,diveStats_all_1718_filt,diveStats_all_1819_filt)
fwrite(diveStats_all_yrs_filt, "your_path")
