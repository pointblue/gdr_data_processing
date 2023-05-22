# Script for pre-processing light-level geolocation data from 1819
# Setting up to access files directly from AWS
### Load packages
rm(list = ls())
# library(devtools)
# install_github("SLisovski/GeoLocTools", force=TRUE)
library(aws.s3)
library(tidyverse)
library(data.table)
# devtools::install_github("SLisovski/TwGeos")
library(TwGeos)
library(stringr)
library(fasttime)
library(lubridate)
library(GeoLocTools)
setupGeolocation()
library(ggplot2)
library(geosphere)
library(ggspatial)
library(maptools)
data(wrld_simpl)
library(abind)
library(rgdal)
library(sf)

# Set credentials for aws access
# Should only need if accessing private repo
# Sys.setenv(
#   "AWS_ACCESS_KEY_ID" = "your_key",
#   "AWS_SECRET_ACCESS_KEY" = "super_secret_key",
#   "AWS_DEFAULT_REGION" = "us-west-2"
# )

# UNITS:
# Temperature is measured every 30 seconds in 1/10 degree C, converted to degree C in processing
# Pressure measured once per second in mBar, converted to meters in processing
# Light is measured per minute on the minute in lux, not converted in processing

# List content of GDR_1819 folder
bucketlist()
tmp = get_bucket_df(bucket = "deju-penguinscience", prefix="AllData/gdr_datashare/GDR_1819/") # 72 files
# filelist <- tmp$Key[2:73]
# filelist <- substr(filelist, 10, 33)

## Get info on retrieved devices in "/data/croz_royds_gdr_depl_all.csv"
depl_dat <- read.csv("data/croz_royds_gdr_depl_all_v2021-08-27.csv")%>%
  filter(season==2018,!is.na(filename))
# filelist<-as.character(depl_dat$filename) # 70 files

####################
## COMPLETE FILES ##
####################

## Get clock drift data
drift <- read.csv("data/clock_drift_1819_v2.csv")
filelist<-as.character(drift$filename) # 47 complete files

## Load the raw data for interactive twilight annotation
index=56:length(filelist)
for(i in index){
  # pull out filename and paste to create path
  fn<-filelist[i]
  print(paste("Loading", fn, "...", sep=" "))
  path=paste0("s3://deju-penguinscience/AllData/gdr_datashare/GDR_1819/", fn, sep="")
  raw <-s3read_using(fread, object = path, skip=25, drop=6, col.names=c("date","time","temp","pressure","lux"))
  raw$Date  <- as.POSIXct(paste(raw$date, raw$time), format = "%d/%m/%Y %H:%M:%S", tz = "GMT")
  raw$Light  <- log(ifelse(raw$lux<1, 1, raw$lux))
  # head(raw)
  # Change start date and time of rebooted logger (50945)
  #if (i==68) { # 50945
  #  raw$Date <- seq(from=as.POSIXct("2017-09-01 05:23:00", tz="GMT"), by=1, length.out=length(raw$Light))
  #  print(filename)
  #  print(head(raw$Date))
  #} else {
  #  print("Date is fine - no reboot, no need to change")
  #}
  # Modify dtime according to clock drift
  print("Adjusting for clock drift")
  print(paste(drift$cdrift[drift$filename==filelist[i]], "min"))
  cdrift <- drift$cdrift[drift$filename==filelist[i]]
  drift_rate <- cdrift*60/(as.numeric(difftime(as.POSIXct("2020-03-02 10:33:00", tz="GMT"), raw$Date[1], units="days"))) # Atesys clock test was performed on March 2, 2020
  print(paste(drift_rate, "sec per day"))
  end <- raw$Date[length(raw$Date)] + drift_rate*as.numeric(difftime(raw$Date[length(raw$Date)], raw$Date[1], units="days")) 
  raw$Date <- driftAdjust(time=raw$Date, start=raw$Date[1], end=end)
  raw$Date <- with_tz(raw$Date, "GMT")   # lubridate function   
  print(head(raw))   
  # Remove NA values
  raw <- subset(raw, !is.na(raw$Light))
  # Subset with deploy and retrieval dates
  deploy_date <- as.POSIXct(depl_dat$deploy_date[depl_dat$filename==filelist[i]], format="%Y-%m-%d", tz="GMT")
  retrieve_date <- as.POSIXct(depl_dat$retrieve_date[depl_dat$filename==filelist[i]], format="%Y-%m-%d", tz="GMT")
  raw <- subset(raw, raw$Date>=deploy_date & raw$Date<=retrieve_date)
  # print(head(raw))
  #Release coordinates
  if (depl_dat$br_col[which(depl_dat$filename==filelist[i])]=="CROZ") {
    lon.calib <- 169.237657
    lat.calib <- -77.453600
  } else {
    lon.calib <- 166.168286
    lat.calib <- -77.552929
  }
  ## Twilight annotation
  # Choose twilight threshold and visualize raw light data
  threshold <- 1
  offset <- 0 # adjusts the y-axis to put night (dark shades) in the middle 
  # (0 to 4, or 23 depending on files - no negative values!)
  lightImage( tagdata = raw,
              offset = offset,     
              zlim = c(0, 20)) # There is twilight from March to October
  tsimageDeploymentLines(raw$Date, lon = lon.calib, lat = lat.calib,
                         offset = offset, lwd = 3, col = adjustcolor("orange", alpha.f = 0.5)) # Add sunrise and sunset times at Crozier
  print("Starting the interactive twilight annotation - Please see http://geolocationmanual.vogelwarte.ch/twilight.html for more instructions on how to proceed")
  # Define daily sunrise and sunset times
  twl <- preprocessLight(raw, 
                         threshold = threshold,
                         offset = offset, 
                         lmax = 11,         # max. light valu
                         gr.Device = "default")
  
  # Two windows will appear. Move them so they are not on top of each other and you can see
  # both. They should look like a big black blob. This identifies the "nightime" period over
  # time. The top of the blob shows all the sunrises and the bottom of blob shows all the 
  # sunsets. You can note for instance that the days get longer (and thus the nights shorter)
  # at the end of the time series, because the blob gets thinner. You may even note changes
  # in the light image that relate to changes in activity patterns or breeding behavior.
  
  # Step 1. Click on the window entitled "Select subset". With the left mouse button choose
  # where you want the start of the dataset to be, and right mouse button to choose the end.
  # You will notice that the red bar at the top moves and that the second window zooms into
  # that time period. Select when you want your time series to start and end. This allows
  # you to ignore for instance periods of nesting. Once you are happy with the start and 
  # end of the timeseries press "a" on the keyboard to accept and move to next step.
  
  # Step 2. click on the window entitled "Find twilights" and the second window will zoom in.
  # All you need to do here is click in the dark part (in the zoomed in image i.e. the one not
  # entitled "Find twilights") of the image and this will identify all the sunrises (orange)
  # and sunsets (blue) based on the threshold defined in the previous section. Press "a" on 
  # the keyboard to accept and move to next step.
  
  # Step 3. This step is for adding or deleting points. If there are no missing data points,
  # you can skip this step by pressing "a" on the keyboard. However, if you do want to add a
  # point, you can click on the "Insert twilights" window to select a region of "the blob" that
  # the second unintitled window will zoom into. In the zoomed window, use left mouse click to
  # add a sunrise, and right mouse click to add a sunset. You can use "u" on the keyboard to 
  # undo any changes, and "d" to delete any points which are extra. Press "a" to move to next 
  # step.
  
  # Step 4. This step allows you to find points which have been miss-classified (often because
  # the bird was in the shade or in a burrow) and to move the respective sunrise or sunset to
  # where it should be. Choose a point by clicking on it in the "edit twilights" window and the
  # other window will display the sunrise (or sunset) from the previous and next days (purple
  # and green) relative to the current sunrise or sunset (in black). Thus if the black line 
  # shows a much earlier sunset or later sunrise than the purple and green ones, it is likely 
  # badly classified. You can then left click at the point where you want the day to start and
  # press "a" to accept and move the sunrise or sunset. You will notice the red line then moves.
  # Do this for as many points as necessary. Then close the windows with "q".
  
  head(twl)
  twl$Twilight <- as.POSIXct(twl$Twilight, tz = "GMT")
  twl <- subset(twl, !Deleted)  
  twl <- twl[order(twl$Twilight),]
  #Additional automated cleaning/filtering 
  twl_filt <- twilightEdit(twilights = twl,
                           offset = offset,
                           window = 4,           # two days before and two days after
                           outlier.mins = 45,    # difference in mins
                           stationary.mins = 25, # are the other surrounding twilights within 25 mins of one another
                           plot = TRUE)
  # add file name to plot 
  title(main=fn)
  # Save twilight file
  id <- substr(fn,1,nchar(fn)-4)
  #id <- fn
  write.csv(twl_filt, paste0("data/geoloc/1819/twl/", id,
                             "_twl.csv"), row.names = F)
  # Add twilights (polar day) - To be able to estimate positions between the colony and the first real twilights
  twl <- subset(twl_filt, !Deleted)  
  twl <- twilightAdjust(twl, 60)
  twl$Inserted <- FALSE
  tm <- seq(min(raw$Date), max(raw$Date), by = "12 hours") # generate two twilights per day
  rise <- rep(c(TRUE, FALSE), length = length(tm))[1:length(tm)]
  tm_twl  <- twilight(tm, lon.calib, lat.calib, rise = rise, zenith = 80, iters = 5) # zenith=80, need to use solar angle about 10° above the horizon
  twl_add <- data.frame(Twilight = tm_twl, Rise = rise, Deleted = FALSE, Edited = FALSE, Twilight0 = tm_twl, Inserted = TRUE) 
  # twl_add <- data.frame(Twilight = tm_twl, Rise = rise, Deleted = FALSE, Marker=0, Inserted = TRUE, Twilight3 = tm_twl, Marker3=0)
  twl_add <- subset(twl_add, !is.na(Twilight))
  twl <- rbind(twl_add[twl_add$Twilight<min(twl$Twilight)-12*60*60,], twl, twl_add[twl_add$Twilight>max(twl$Twilight)+12*60*60,])
  write.csv(twl, paste0("data/geoloc/1819/twl/", id,
                        "_twl_long.csv"), row.names = F)
  # Clean up
  rm(raw, twl, twl_add, twl_filt)
  gc()  # run garbage collector to free up memory
  # delete temp files from C: drive
  tmp_dir <- tempdir()
  files <- list.files(tmp_dir, full.names = T, pattern = "^file")
  file.remove(files)
  #End loop
}
print("No more file to process")

######################
## INCOMPLETE FILES ##
######################

drift <- read.csv("data/clock_drift_1819_v2.csv") # 57 complete data files, including 2 ref files
filelist_c <- as.character(drift$filename) # 57 files
depl_dat <- read.csv("/data/croz_royds_gdr_depl_all.csv")%>%
  mutate(bird_id=as.numeric(bird_id))%>%
  subset(season=="2018"&retrieve_date!="NA")%>%
  mutate(id=str_pad(gsub("NA","",bird_id),9,pad="0"))
filelist_t<-as.character(depl_dat$filename[!is.na(depl_dat$filename)]) # 70 files
# list of incomplete files
filelist_i <- c(setdiff(filelist_c, filelist_t), setdiff(filelist_t, filelist_c)) # 17 files

# Load the raw data for interactive twilight annotation
index=17:length(filelist_i)
for (i in index){
  # pull out filename and paste to create path
  fn<-filelist_i[i]
  print(paste("Loading", fn, "...", sep=" "))
  path=paste0("s3://deju-penguinscience/AllData/gdr_datashare/GDR_1819/", fn, sep="")
  raw <-s3read_using(fread, object = path, skip=25, drop=6, col.names=c("date","time","temp","pressure","lux"))
  raw$Date  <- as.POSIXct(paste(raw$date, raw$time), format = "%d/%m/%Y %H:%M:%S", tz = "GMT")
  raw$Light  <- log(ifelse(raw$lux<1, 1, raw$lux))
  head(raw)
  # Change start date and time of rebooted logger (69936)
  if (i==6) { # 69936
    raw$Date <- seq(from=as.POSIXct("2018-09-25 15:34:00", tz="GMT"), by=1, length.out=length(raw$Light))
    print(filename)
    print(head(raw$Date))
  } else if (i==4) { # 46645
    print(filename)
    raw$Date <- seq(from=as.POSIXct("2018-09-25 15:27:00", tz="GMT"), by=1, length.out=length(raw$Light))
    print(head(raw$Date))
  } else {
    print("Date is fine - no reboot, no need to change")
  }
  # Modify dtime according to clock drift
  print("Adjusting for clock drift: applying mean drift rate of 1.527724 sec per day")
  drift_rate <- 1.527724
  end <- raw$Date[length(raw$Date)] + drift_rate*as.numeric(difftime(raw$Date[length(raw$Date)], raw$Date[1], units="days")) 
  raw$Date <- driftAdjust(time=raw$Date, start=raw$Date[1], end=end)
  raw$Date <- with_tz(raw$Date, "GMT")   # lubridate function   
  print(head(raw)) 
  # Remove NA values
  raw <- subset(raw, !is.na(raw$Light))
  # Subset with deploy and retrieval dates
  deploy_date <- as.POSIXct(depl_dat$deploy_date[depl_dat$filename==filelist_i[i]], format="%Y-%m-%d", tz="GMT")  
  retrieve_date <- as.POSIXct(depl_dat$retrieve_date[depl_dat$filename==filelist_i[i]], format="%Y-%m-%d", tz="GMT")
  deploy_date <- deploy_date[!is.na(deploy_date)]
  retrieve_date <- retrieve_date[!is.na(retrieve_date)]
  raw <- subset(raw, raw$Date>=deploy_date & raw$Date<=retrieve_date)
  #Release coordinates
  if (depl_dat$br_col[which(depl_dat$filename==filelist_i[i])]=="CROZ") {
    lon.calib <- 169.237657
    lat.calib <- -77.453600
  } else {
    lon.calib <- 166.168286
    lat.calib <- -77.552929
  }
  ## Twilight annotation
  # Choose twilight threshold and visualize raw light data
  threshold <- 1
  offset <- 0 # adjusts the y-axis to put night (dark shades) in the middle 
  # (0 to 4, or 23 depending on files - no negative values!)
  lightImage( tagdata = raw,
              offset = offset,     
              zlim = c(0, 20)) # There is twilight from March to October
  tsimageDeploymentLines(raw$Date, lon = lon.calib, lat = lat.calib,
                         offset = offset, lwd = 3, col = adjustcolor("orange", alpha.f = 0.5)) # Add sunrise and sunset times at Crozier
  print("Starting the interactive twilight annotation - Please see http://geolocationmanual.vogelwarte.ch/twilight.html for more instructions on how to proceed")
  # Define daily sunrise and sunset times
  twl <- preprocessLight(raw, 
                         threshold = threshold,
                         offset = offset, 
                         lmax = 11,         # max. light valu
                         gr.Device = "default")
  
  # Two windows will appear. Move them so they are not on top of each other and you can see
  # both. They should look like a big black blob. This identifies the "nightime" period over
  # time. The top of the blob shows all the sunrises and the bottom of blob shows all the 
  # sunsets. You can note for instance that the days get longer (and thus the nights shorter)
  # at the end of the time series, because the blob gets thinner. You may even note changes
  # in the light image that relate to changes in activity patterns or breeding behavior.
  
  # Step 1. Click on the window entitled "Select subset". With the left mouse button choose
  # where you want the start of the dataset to be, and right mouse button to choose the end.
  # You will notice that the red bar at the top moves and that the second window zooms into
  # that time period. Select when you want your time series to start and end. This allows
  # you to ignore for instance periods of nesting. Once you are happy with the start and 
  # end of the timeseries press "a" on the keyboard to accept and move to next step.
  
  # Step 2. click on the window entitled "Find twilights" and the second window will zoom in.
  # All you need to do here is click in the dark part (in the zoomed in image i.e. the one not
  # entitled "Find twilights") of the image and this will identify all the sunrises (orange)
  # and sunsets (blue) based on the threshold defined in the previous section. Press "a" on 
  # the keyboard to accept and move to next step.
  
  # Step 3. This step is for adding or deleting points. If there are no missing data points,
  # you can skip this step by pressing "a" on the keyboard. However, if you do want to add a
  # point, you can click on the "Insert twilights" window to select a region of "the blob" that
  # the second unintitled window will zoom into. In the zoomed window, use left mouse click to
  # add a sunrise, and right mouse click to add a sunset. You can use "u" on the keyboard to 
  # undo any changes, and "d" to delete any points which are extra. Press "a" to move to next 
  # step.
  
  # Step 4. This step allows you to find points which have been miss-classified (often because
  # the bird was in the shade or in a burrow) and to move the respective sunrise or sunset to
  # where it should be. Choose a point by clicking on it in the "edit twilights" window and the
  # other window will display the sunrise (or sunset) from the previous and next days (purple
  # and green) relative to the current sunrise or sunset (in black). Thus if the black line 
  # shows a much earlier sunset or later sunrise than the purple and green ones, it is likely 
  # badly classified. You can then left click at the point where you want the day to start and
  # press "a" to accept and move the sunrise or sunset. You will notice the red line then moves.
  # Do this for as many points as necessary. Then close the windows with "q".
  
  print(head(twl))
  id <- substr(fn,1,nchar(fn)-4)
  write.csv(twl, paste0("data/geoloc/1819/twl/", id,
                        "_twl.csv"), row.names = F)
  #Additional automated cleaning/filtering 
  #twl <- twl[!twl$Deleted,]
  #twl_filt <- twilightEdit(twilights = twl,
  #                         offset = offset,
  #                         window = 4,           # two days before and two days after
  #                         outlier.mins = 45,    # difference in mins
  #                         stationary.mins = 25, # are the other surrounding twilights within 25 mins of one another
  #                         plot = TRUE)
  #print(head(twl_filt))
  # Save twilight file
  #id <- fn
  #write.csv(twl_filt, paste0("Z:/Informatics/S031/analyses/GDR/data/geoloc/1718/twl/", id,
  #                          "_twl_filt.csv"), row.names = F)
  
  # Add twilights (polar day) - To be able to estimate positions between the colony and the first real twilights
  #twl <- subset(twl_filt, !Deleted) 
  twl <- subset(twl, !Deleted)
  twl <- twilightAdjust(twl, 60)
  twl$Inserted <- FALSE
  tm <- seq(min(raw$Date), max(raw$Date), by = "12 hours") # generate two twilights per day
  rise <- rep(c(TRUE, FALSE), length = length(tm))[1:length(tm)]
  tm_twl  <- twilight(tm, lon.calib, lat.calib, rise = rise, zenith = 80, iters = 5) # zenith=80, need to use solar angle about 10? above the horizon
  #twl_add <- data.frame(Twilight = tm_twl, Rise = rise, Deleted = FALSE, Edited = FALSE, Twilight0 = tm_twl, Inserted = TRUE) 
  twl_add <- data.frame(Twilight = tm_twl, Rise = rise, Deleted = FALSE, Marker = 0, Inserted = TRUE, Twilight3 = tm_twl, Marker3 = 0)
  twl_add <- subset(twl_add, !is.na(Twilight))
  twl <- rbind(twl_add[twl_add$Twilight<min(twl$Twilight)-12*60*60,], twl, twl_add[twl_add$Twilight>max(twl$Twilight)+12*60*60,])
  write.csv(twl, paste0("data/geoloc/1819/twl/", id,
                        "_twl_long.csv"), row.names = F)
  # Clean up
  rm(raw, twl, twl_add, id)
  gc()  # run garbage collector to free up memory
  # delete temp files from C: drive
  tmp_dir <- tempdir()
  files <- list.files(tmp_dir, full.names = T, pattern = "^file")
  file.remove(files)
  #End loop
}
print("No more file to process")