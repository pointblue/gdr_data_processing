# Script for processing all dive data from 1617-1819
# Setting up to access files directly from AWS

# Code written by AS
# modified in 2021 to re-process dive data with correct foraging dive definition
# code now also: 
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
Sys.setenv("AWS_ACCESS_KEY_ID" = "fill with your id", "AWS_SECRET_ACCESS_KEY" = "definitely don't commit your real key", 
           "AWS_DEFAULT_REGION" = "us-west-2")



# UNITS:
# Temperature is measured every 30 seconds in 1/10 degree C, converted to degree C in processing
# Pressure measured once per second in mBar, converted to meters in processing
# Light is measured per minute on the minute in lux, not converted in processing

# Step 1: zero offset correction completed previously (in 2018)

# Step 2: calculate dive stats

# read in additional data required to calculate dive stats
# read in GDR deploy file
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
  
# inputs to function:
# season
# bird_id
# gdr_depl=table with deployment info for all birds that season
# s3_path=path to zero offset corrected data
#____s3 path can be "bucket" for reading directly from bucket, path is coded in function (do this is running from tsinfo or local machines)
#____ or can be path where s3 data are mounted on instance if running from aws server
# out = path where individual diveStats files will be saved 

# 2016 ####
# source("~/Documents/code/ADPE_GDR/diveStats_GDR.R")
source('code/divesum_diveStats_GDR.R')

# apply function to list of bird_ids, individuals files written as output
depl16 <- gdr_depl%>%
  filter(season==2016)
s3_path16 = "bucket"
out16 = "data/diveStats/1617"

# if only running in one session, might want to use setDTthreads to allow data table functions to use more threads
lapply(
  depl16$bird_fn,
  diveStats_yr,
  seas = 2016,
  gdr_depl = gdr_depl,
  s3_path = "bucket",
  out = out16,
  version = "ds",
  strategy = "multisession"
)



# # 2017 ####
# plan("sequential")
# filter to get bird ids for the year
depl17 <- gdr_depl%>%
  filter(season==2017)
s3_path="bucket"
out17 = "data/diveStats/1718"
lapply(
  depl17$bird_fn,
  diveStats_yr,
  seas = 2017,
  gdr_depl = gdr_depl,
  s3_path = s3_path,
  out = out17,
  version = "ds",
  strategy = "multisession"
)



# 2018 ####
depl18 <- gdr_depl %>%
  filter(season == 2018)
s3_path = "bucket"
out18 = "data/diveStats/1819"

# plan("sequential")
# source("~/Documents/code/ADPE_GDR/diveStats_GDR.R")
lapply(
  depl18$bird_fn,
  diveStats_yr,
  seas = 2018,
  gdr_depl = gdr_depl,
  s3_path = s3_path,
  out = out18,
  version = "ds",
  strategy = "multisession"
)

# read in and bind diveStats by season ####
# Once done with all years, read in files for each season and bind together
# 1617####
# setwd("~/s3data/GDR_1617_diveStats")

file_ls <- list.files(pattern = ".csv$")
length(file_ls)
plan(multisession) # use multisession on windows
divefiles <- future_lapply(
  file_ls,
  read_csv,
  col_types=cols(
    .default = col_double(),
    begdesc = col_datetime(format = ""),
    enddesc = col_datetime(format = ""),
    begasc = col_datetime(format = ""),
    botttim = col_double(),
    enddive = col_datetime(format = ""),
    divetype = col_character(),
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
fwrite(diveStats_all_1617,"data/diveStats_ds_all_1617.csv")
#replace extreme values with NA
diveStats_all_1617_filt<-diveStats_all_1617%>%
  filter(divetim<=420,maxdep <=190)%>%
  # for columns that start with temp_, replace with NA if temp_max >50, or if temp_min <(-5)
  mutate(across(starts_with("temp_"),~replace(.x,temp_max>50|temp_min<(-5),NA)))

fwrite(diveStats_all_1617_filt,"data/diveStats_ds_all_1617_filt.csv")


# summarise deployments from 1617
diveSumm_1617<-dive_summary(diveStats_all_1617,season=2016)
write_csv(diveSumm_1617,"data/GDR_diveStats_all/gdr_divetable_1617.csv")

diveSumm_1617_filt<-dive_summary(diveStats_all_1617_filt,season=2016)
write_csv(diveSumm_1617_filt,"data/gdr_divetable_1617_filt.csv")


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


# ggpubr::ggarrange(p1617,p1617_filt,nrow=1,ncol=2)


# 1718 ####
# setwd("~/s3data/GDR_1718_diveStats")
setwd("Z:/Informatics/S031/analyses/GDR/data/diveStats/1718")
file_ls <- list.files(pattern = ".csv$")
length(file_ls)
plan(multisession) # use multisession on windows
divefiles<- future_lapply(file_ls,read_csv, future.seed = TRUE)
plan(sequential)
# bind all files together
diveStats_all_1718<-rbindlist(divefiles)%>% # 3 seconds on aws m4.10xlarge. #Fails if read in with fread because some parsing errors with datetimes, somehow introduced by fwrite?
  mutate(bird_fn=parse_number(file_id),season=2017)
# check that have all bird in combined file
unique(diveStats_all_1718$bird_fn)

fwrite(diveStats_all_1718,"data/diveStats_ds_all_1718.csv")

#filter to remove outliers
diveStats_all_1718_filt <-diveStats_all_1718%>%
  filter(divetim<=420,maxdep <=190)%>%
  # for columns that start with temp_, replace with NA if temp_max >50, or if temp_min <(-5)
  mutate(across(starts_with("temp_"),~replace(.x,temp_max>50|temp_min<(-5),NA)))

fwrite(diveStats_all_1718_filt, "data/diveStats_all_1718_filt.csv")


# summarise deployments from 1718
diveSumm_1718<-dive_summary(diveStats_all_1718,season=2017)
write_csv(diveSumm_1718,"data/gdr_divetable_1718.csv")

diveSumm_1718_filt<-dive_summary(diveStats_all_1718_filt,season=2017)
write_csv(diveSumm_1718_filt,"data/gdr_divetable_1718_filt.csv")


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

# ggpubr::ggarrange(p1718,p1718_filt,nrow=1,ncol=2)


# 1819 ####

# setwd("~/s3data/GDR_1819_diveStats")
file_ls <- list.files(pattern = ".csv$")
length(file_ls)
plan(multisession) # use multisession on windows
divefiles <- future_lapply(
  file_ls,
  read_csv,
  col_types=cols(
    .default = col_double(),
    begdesc = col_datetime(format = ""),
    enddesc = col_datetime(format = ""),
    begasc = col_datetime(format = ""),
    botttim = col_double(),
    enddive = col_datetime(format = ""),
    divetype = col_character(),
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

fwrite(diveStats_all_1819,"data/diveStats_ds_all_1819.csv")

diveStats_all_1819 <- fread("data/diveStats_ds_all_1819.csv")

#filter to remove outliers
diveStats_all_1819_filt <-diveStats_all_1819%>%
  filter(divetim<=420,maxdep <=190)%>%
  # for columns that start with temp_, replace with NA if temp_max >50, or if temp_min <(-5)
  mutate(across(starts_with("temp_"),~replace(.x,temp_max>50|temp_min<(-5),NA)))
fwrite(diveStats_all_1819_filt,"data/diveStats_ds_all_1819_filt.csv")

# summarise deployments from 1819
diveSumm_1819<-dive_summary(diveStats_all_1819,season=2018)
write_csv(diveSumm_1819,"data/GDR_diveStats_all/gdr_divetable_1819.csv")

# summarise deployments from 1819 after filtering
diveSumm_1819_filt<-dive_summary(diveStats_all_1819_filt,season=2018)
write_csv(diveSumm_1819_filt,"data/gdr_divetable_1819_filt.csv")



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

# ggpubr::ggarrange(p1819,p1819_filt,nrow=1,ncol=2)

# Bind all diveStats from all years together
diveStats_all_yrs <- bind_rows(diveStats_all_1617,diveStats_all_1718,diveStats_all_1819)
fwrite(diveStats_all_yrs, "data/diveStats_ds_all.csv")

diveStats_all_yrs_filt <- bind_rows(diveStats_all_1617_filt,diveStats_all_1718_filt,diveStats_all_1819_filt)
fwrite(diveStats_all_yrs_filt, "data/diveStats_ds_all_filt.csv")



# Some visualizing####

diveStats_all_yrs%>%
  filter(season==2016)%>%
  filter(divetim<=420,maxdep<=190,temp_max <=50,temp_min >=-5)%>%
  summarise(maxdep=max(maxdep,na.rm = TRUE))%>%
  ggplot(aes(date,maxdep))+
  geom_col()+
  facet_wrap(~bird_id,nrow=10,ncol=9)+
  ggtitle("1617 max dive depth by day by bird")


# make summary figs for all birds in a year
diveStats_all_1718_filt%>%
  group_by(bird_id,date)%>%
  summarise(maxdep=max(maxdep,na.rm = TRUE))%>%
  ggplot(aes(date,maxdep))+
  geom_col()+
  facet_wrap(~bird_id,nrow=10,ncol=9)+
  ggtitle("1718 max dive depth by day by bird")


diveStats_all_1617_filt <- diveStats_all_1617%>%
  filter(divetim<(8*60))

filter(diveStats_all_1718,divetim>480)%>%
  ggplot(aes(maxdep,divetim))+
  geom_text(aes(label = paste(bird_id, "^(", did, ")", sep = "")),
                parse = TRUE, size=2)+
  ggtitle("1718 divetime >480s (8 min)")


diveStats_all_1718_filt%>%
  # filter(divetim<500)%>%
  group_by(bird_id,date)%>%
  summarise(maxdur=max(divetim,na.rm = TRUE))%>%
  ggplot(aes(date,maxdur))+
  geom_col()+
  facet_wrap(~bird_id,nrow=10,ncol=9)+
  ggtitle("1718 max dive duration by day by bird")

diveStats_all_1617%>%
  # filter(divetim<500)%>%
  group_by(bird_id,date)%>%
  summarise(maxdur=max(divetim,na.rm = TRUE))

diveStats_all_1617%>%
  ggplot(aes(divetim))+
  geom_histogram(bins=100)+375/60
  ggtitle("1617 histogram of all dives by divetim")

sd(diveStats_all_1617$divetim)
mean(diveStats_all_1617$divetim)+2*sd(diveStats_all_1617$divetim)
  facet_wrap(~bird_id,nrow=10,ncol=7,scales="free_y")+
  ggtitle("1617 max dive duration by day by bird")
  

  
# 1617 temperature plots
diveStats_all_1718_filt%>%
    # filter(divetim<500)%>%
    # group_by(bird_id,date)%>%
    # summarise(maxtemp=max(temp_max,na.rm = TRUE))%>%
  ggplot(aes(temp_min))+
  geom_histogram(bins=100)+
  ggtitle("1718 min temp by dive")
mean(diveStats_all_1617$temp_min,na.rm=TRUE)+2*sd(diveStats_all_1617$temp_min,na.rm=TRUE)
min(diveStats_all_1617$temp_min,na.rm=TRUE) 
max(diveStats_all_1617$temp_max,na.rm=TRUE)

# 1718
diveStats_all_1718%>%
  filter(temp_min<=50&temp_min>=-5)%>%
  # summarise(max=max(temp_min, na.rm=TRUE), min=min(temp_min,na.rm=TRUE))
  # group_by(bird_id,date)%>%
  # summarise(maxtemp=max(temp_max,na.rm = TRUE))%>%
  ggplot(aes(temp_min))+
  geom_histogram(bins=100)+
  ggtitle("1718 min temp by dive (filtered to >=-5 and <= 50)")
mean(diveStats_all_1718$temp_min,na.rm=TRUE)+2*sd(diveStats_all_1718$temp_min,na.rm=TRUE)
min(diveStats_all_1718$temp_min,na.rm=TRUE) 
max(diveStats_all_1718$temp_max,na.rm=TRUE)


