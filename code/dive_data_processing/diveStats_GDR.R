# this file started with copy of GDR_diveStats_1617.R on 4/21/2021

# Two main functions for calculating dive stats
# diveStats_GDR and OneDiveStats_GDR are modified versions (modified by AS) of the functions originally in diveMove (v 1.4.5)


# update for 1819####
# edited the way divetim was calculated because previous method didn't work when bottom time was NA
# v2.0 update 3/8/21: Discovered needed to edit dive category criteria to match divesum
#---------------adding a column called divetype_as to hold what was divetype_ds before
#---------------divetype_ds now matches divecategory in divesumlul_3.prg
#---------------there are still differences in how ascent rate and descent rate are being calculated but think they are minor
# update 3/16/21: adding enddive column to record time at end of dive
# update 4/30/21: re-writing to include functionality for all years
#-----------------changing difftime columns to numeric so can use rbindlist
# v3.0 update 10/08/21: correcting some errors found in the onediveStats function
# ----------------correcting misktake that allowed zero change in depth to be counted as a change in direction
# ----------------changing unds1m_ds to count only changes in dirction from ascent to descent
# ----------------changing unds1m_ds to count in whole dive, not just bottom
# ----------------removed unds1m (only have unds1m_ds now)
# ----------------changing how bottom_ds assigned, also affects botttim_ds
#
# v3.1 update 10/21/21: changing code approach for dive classification
# --------------- using same criteria but changing how it's written to use dplyr case_when
# --------------- hoping will be a little cleaner and faster
# --------------- also adding column to identify which case dive was qualified under


# # outer function to read in calibrated data and apply diveStats functions to each individual

diveStats_yr <-
  function(bird,
           seas,
           gdr_depl,
           s3_path,
           out = "~/diveStats") {
    require(dplyr)
    # select deploy data for that bird
    gdr_depl <- gdr_depl %>%
      filter(season == seas)
    bird_dat <- gdr_depl[gdr_depl$bird_fn == bird, ]
    file = as.character(gdr_depl$filename[gdr_depl$bird_fn == bird])
    
    # build zoc name and check if file has been zero offset corrected
    if (seas == 2016) {
      col = ifelse(substr(file, 15, 15) == "c", "croz", "royds")
      zoc_name <-
        paste0(col, parse_number(as.character(file)), "zoc", ".Rda")
      options(show.error.messages = TRUE)
    } else{
      zoc_name <- gsub(".txt", "_zoc.Rda", file)
    }
    message(paste("Loading", zoc_name, sep = " "))
    seas_code <-
      paste0(substr(seas, 3, 4), as.numeric(as.numeric(substr(seas, 3, 4))) +
               1)
    # Set up paths to files
    if (s3_path == "bucket") {
      prefix = "your_prefix"
      bucket = "your_bucket"
      try(dcalib <-
            s3readRDS(object = zoc_name,
                      bucket = bucket,
                      silent = TRUE),
          silent = TRUE)
    } else{
      path = paste0(s3_path, zoc_name)
      try(dcalib <- readRDS(path),
          silent = TRUE)
    }
    # check if was able to load a calibrated file, if file doesn't exist, print error rather than stopping
    if (!exists("dcalib")) {
      message(paste("Error:", zoc_name, "does not exist", sep = " "))
    } else{
      # Calculate dive statistics
      message(paste("calculating dive stats for", file))
      try(diveStats_tab <- diveStats_GDR(dcalib))
    }
    if (!exists("diveStats_tab")) {
      message("diveStats not calculated, moving to next file")
    } else{
      #calculate adjusted time:
      drift_rate = bird_dat$drift_rate
      if (bird_dat$rebooted == 0) {
        # calculate start and end date of device clock
        clock_start = bird_dat$start_rec
        
        # number of days between device start time and first dive
        start_diff <-
          as.numeric(difftime(first(diveStats_tab$begdesc), clock_start, units =
                                "days"))
        start <-
          first(diveStats_tab$begdesc) + seconds(drift_rate * start_diff)
        end_diff <-
          as.numeric(difftime(last(diveStats_tab$begdesc), clock_start, units = "days")) # number of days between tag start and time of last dive
        end <-
          last(diveStats_tab$begdesc) + seconds(drift_rate * end_diff) # new end time to use to apply drift rate
        
        # add column for original time to keep ability to check adjustment
        diveStats_tab$begdesc_orig <- diveStats_tab$begdesc
        
      } else{
        # Change start date and time of rebooted loggers
        message("logger rebooted, adjusting clock start and clock drift")
        # get date that logger rebooted from rebooted file
        clock_start <-
          rebooted$date_start[rebooted$filename == file]
        offset <-
          seconds(rebooted$offset[rebooted$filename == file])
        # calculate adjusted time of begdesc first to be able to tell time between logger start and first dive
        # add estimated clock offset to times before applying drift
        diveStats_tab$begdesc_orig <- diveStats_tab$begdesc
        diveStats_tab$begdesc <-
          diveStats_tab$begdesc + seconds(offset)
        diveStats_tab$enddesc <-
          diveStats_tab$enddesc + seconds(offset)
        diveStats_tab$begasc <-
          diveStats_tab$begasc + seconds(offset)
        diveStats_tab$enddive <-
          diveStats_tab$enddive + seconds(offset)
        
        #clock drift
        ## number of days between tag start time and first dive
        start_diff <-
          as.numeric(difftime(first(diveStats_tab$begdesc), clock_start, units =
                                "days"))
        # datetime of first dive plus number of seconds estimated to have drifted by then
        start <-
          first(diveStats_tab$begdesc) + seconds(drift_rate * start_diff)
        # number of days between tag start and time of last dive
        end_diff <-
          as.numeric(difftime(last(diveStats_tab$begdesc), clock_start, units = "days"))
        # new end time to use to apply drift rate
        end <-
          last(diveStats_tab$begdesc) + seconds(drift_rate * end_diff)
      }
      # adjust date.time fields for clock drift
      diveStats_tab$begdesc <-
        with_tz(driftAdjust(
          time = diveStats_tab$begdesc,
          start = start,
          end = end
        ),
        "GMT")
      diveStats_tab$enddesc <-
        with_tz(driftAdjust(
          time = diveStats_tab$enddesc,
          start = start,
          end = end
        ),
        "GMT")
      diveStats_tab$begasc <-
        with_tz(driftAdjust(
          time = diveStats_tab$begasc,
          start = start,
          end = end
        ),
        "GMT")
      # add column for time at the end of the dive (based on adjusted times)
      diveStats_tab$enddive <-
        diveStats_tab$begdesc + diveStats_tab$divetim
      
      # modify and add columns
      # filter to dates between deploy and retrieve dates
      file_id <- gsub(".txt", "_diveStats.csv", file)
      diveStats_tab <- diveStats_tab %>%
        mutate(
          bird_id = gdr_depl$bird_id[gdr_depl$filename == file],
          file_id = file_id,
          colony = gdr_depl$br_col[gdr_depl$filename == file],
          date = as.Date(begdesc)
        ) %>%
        filter(date > as.Date(bird_dat$deploy_date) &
                 date < as.Date(bird_dat$retrieve_date))
      
      file_path <- paste0(out, file_id)
      print(paste("writing:", file_path, sep = " "))
      write_csv(diveStats_tab, file = file_path) # writing individual file
      #clear objects from environment
      rm(dcalib)
      rm(diveStats_tab)
      gc()
    }
  }


# If re-run diveStats, consider adding this to inside of diveStats_yr
# inputs:
# diveStats_tab = compiled diveStats for all birds in a season
# future edits could add capability of running on single file or compiled file

dive_summary <- function(diveStats_tab, season) {
  # Calculate summary of deployment
  type_sum <- diveStats_tab %>%
    group_by(file_id, bird_id) %>%
    count(divetype_ds) %>%
    pivot_wider(names_from = divetype_ds, values_from = n) %>%
    rename(F_dive = F,
           E_dive = E,
           O_dive = O)
  
  dsum <- diveStats_tab %>%
    # calculate the number of 1 and 2 min dives?
    group_by(file_id, bird_id) %>%
    summarise(
      ndives = n(),
      ndays = length(unique(date)),
      # samp_int = getDtime(dcalib@tdr),
      # dthresh = dcalib@dive.thr,
      firstdive = first(date),
      lastdive = last(date),
      dives_perday = ndives / ndays,
      meandep = mean(maxdep, na.rm = TRUE),
      maxdep = max(maxdep, na.rm = TRUE),
      meandur = mean(divetim, na.rm = TRUE),
      maxdur = max(divetim, na.rm = TRUE),
      botttim_perday = sum(botttim, na.rm = TRUE) / ndays,
      meanpostdur = mean(postdive_dur, na.rm = TRUE),
      maxpostdur = max(postdive_dur, na.rm = TRUE),
      minlt = min(lt_min, na.rm = TRUE),
      maxlt = max(lt_max, na.rm = TRUE),
      meanlt = mean(lt_mean, na.rm = TRUE),
      mintemp = min(temp_min, na.rm = TRUE),
      maxtemp = max(temp_max, na.rm = TRUE),
      meantemp = mean(temp_mean, na.rm = TRUE),
      meanbotttemp = mean(temp_at_bott, na.rm = TRUE),
      meanunds1m_ds = mean(unds1m_ds),
      start_time_offset = difftime(first(begdesc), first(begdesc_orig)),
      end_time_offset  = difftime(last(begdesc), last(begdesc_orig))
    ) %>%
    left_join(type_sum) %>%
    mutate(prop_Fdive = F_dive / ndives,
           season = season) %>%
    dplyr::select(season, bird_id:prop_Fdive, file_id)
  return(dsum)
}


# Function to calculate statistics for all dives
diveStats_GDR <-
  function (x,
            strategy = "multisession",
            depth.deriv = FALSE,
            tz = "GMT")
  {
    require(diveMove)
    require(future.apply)
    require(dplyr)
    if (!is(x, "TDRcalibrate"))
      stop("x must be a TDRcalibrate object")
    zvtdr <- getTDR(x)
    interval <- getDtime(zvtdr)
    if (interval != 1) {
      message(
        paste0(
          "Warning: dive interval ",
          interval,
          ", foraging dive classification assumes sampling interval of 1s"
        )
      )
    } else{
      diveid <- getDAct(x, "dive.id")
      postdiveid <- getDAct(x, "postdive.id")
      ok <- which(diveid > 0 & diveid %in% postdiveid)
      dtimes <- getTime(zvtdr)[ok]
      ddepths <- getDepth(zvtdr)[ok]
      dids <- diveid[ok]
      dphases <- getDPhaseLab(x)[ok] #vector with dive phases
      if (names(zvtdr@concurrentData)[1] == "temperature") {
        dtemps <- getCCData(zvtdr)[ok, "temperature"]
      } else{
        dtemps <- getCCData(zvtdr)[ok, "temp"]
      }
      dlux <- getCCData(zvtdr)[ok, "lux"]
      okpd <- which(postdiveid %in% unique(dids))
      pdtimes <- getTime(zvtdr)[okpd]
      pddepths <- getDepth(zvtdr)[okpd]
      pdids <- postdiveid[okpd]
      postdive_dur <- tapply(pdtimes, pdids, function(k) {
        difftime(k[length(k)], k[1], units = "secs")
      })
      dtimestz <- tz
      td = data.frame(dphases, dtimes, ddepths, dtemps, dlux, dids)
      plan(strategy = strategy) #use multisession on windows
      perdive <- rbindlist(future_by(
        td,
        dids,
        oneDiveStats_GDR,
        interval = interval,
        simplify = FALSE
      ))
      plan(sequential)
      res <- data.frame(perdive, postdive_dur)
      return(res)
    }
    message('moving to next file')
  }



# This function selects one dive at a time and calculates numerous statistics
# The original implementation in diveMove included speed calculations that are not implemented in this version
# function requires a data frame with pressure data, the interval the data were recorded at, and a time zone
# use plot = TRUE to plot individual dives for verification
oneDiveStats_GDR <- function (x,
                              interval,
                              tz = "GMT",
                              plot = FALSE) {
  require(dplyr)
  # pull out dive ID
  did = x$dids
  # print(did[1])
  # add column for difference in depth by dive
  x <- x %>%
    mutate(
      ddepth_delta = c(NA, diff(ddepths)),
      # Add column for direction and identify change points 1= down, 2=up
      dir = ifelse(ddepth_delta > 0, 1, NA),
      dir = ifelse(ddepth_delta < 0, 2, dir),
      dir = ifelse(ddepth_delta == 0, lag(dir), dir)
    ) %>%
    #find where direction changes and tag with with 8=change from down to up, 9= change from up to down
    mutate(
      dir_change = ifelse((dir - lead(dir)) == -1, 8, NA),
      dir_change = ifelse(dir - lead(dir) == 1, 9, dir_change)
    )
  
  # identify where change points are
  # rows where direction changes from down to up
  du_row <- which(x$dir_change == 8)
  # rows where direction changes from up to down
  ud_row <- which(x$dir_change == 9)
  
  # unds 1m calcs:
  # Count all direction changes
  unducount <- length(du_row) + length(ud_row)
  # identify direction changes from up to down
  unds1m_rows <-
    ud_row[which(abs(x[du_row[-length(du_row)], "ddepths"] - x[ud_row, "ddepths"]) >
                   1)]
  # calculate depth differences
  unds1m_diffs <-
    abs(x[du_row[-length(du_row)], "ddepths"] - x[ud_row, "ddepths"])
  # count number where depth change >1m
  unds1m_ds <- length(which(unds1m_diffs > 1))
  
  
  # Chunk from original oneDiveStats function:
  desc <- x[grep("D", as.character(x[, 1])), 2:ncol(x)]
  bott <- x[grep("B", as.character(x[, 1])), 2:ncol(x)]
  asc <- x[grep("A", as.character(x[, 1])), 2:ncol(x)]
  begdesc <- desc[1, 1]
  enddesc <- desc[nrow(desc), 1]
  desctim <-
    as.numeric(difftime(enddesc, begdesc, units = "secs") + interval / 2)
  descdist <- max(desc[, 2], na.rm = TRUE)
  maxdep <- max(x[, 3], na.rm = TRUE)
  if (nrow(bott) > 0) {
    botttim <- as.numeric(difftime(bott[nrow(bott), 1], bott[1, 1],
                                   units = "secs"))
    bottdist <- sum(abs(diff(bott[!is.na(bott[, 2]), 2])))
    bottdep_mean <- mean(bott[, 2], na.rm = TRUE)
    bottdep_median <- median(bott[, 2], na.rm = TRUE)
    bottdep_sd <- sd(bott[, 2], na.rm = TRUE)
  } else{
    botttim <- NA
    bottdist <- NA
    bottdep_mean <- NA
    bottdep_median <- NA
    bottdep_sd <- NA
  }
  
  # Calculate bottom stats like divesum
  # Bottom is 60% of max dive depth and <0.5m/s change in depth
  # speed is calculated as difference in depth between record at time t and time t+2 divided by 2
  # so should be average depth change over 3 seconds
  # if meets criteria, assigned "B", otherwise assigned "NB" (not bottom)
  x$bott_ds <-
    ifelse(x$ddepths > 0.6 * maxdep &
             (abs(x$ddepths - lead(x$ddepths, 2)) / 2) < 0.5, "B", "NB")
  # select bottom data
  bott_row_ds <- which(x$bott_ds == "B")
  
  # if there is a bottom, calculate statistics
  if (length(bott_row_ds) > 0) {
    # as of v3.0: this is now selecting all data from the first "B" to the last "B"
    bott_ds <- x[c(first(bott_row_ds):last(bott_row_ds)), 2:ncol(x)]
    # calculate bottom time
    # difference between first and last time in bottom
    botttim_ds <-
      as.numeric(difftime(bott_ds[nrow(bott_ds), 1], bott_ds[1, 1],
                          units = "secs"))
    # calculate bottom distance
    bottdist_ds <- sum(abs(diff(bott_ds[!is.na(bott_ds[, 2]), 2])))
    # calculate average depth of bottom
    bottdep_mean_ds <- mean(bott_ds[, 2], na.rm = TRUE)
    # calculate median depth of bottom
    bottdep_median_ds <- median(bott_ds[, 2], na.rm = TRUE)
    # calculate sd of bottom
    bottdep_sd_ds <- sd(bott_ds[, 2], na.rm = TRUE)
    # calculate average temperature at the bottom
    temp_at_bott_ds <-
      suppressWarnings(mean(bott_ds$dtemps, na.rm = TRUE) / 10)
  } else{
    # if no bottom fill in all columns with NA
    botttim_ds = bottdist_ds = bottdep_mean_ds = bottdep_median_ds = bottdep_sd_ds =
      temp_at_bott_ds = NA
  }
  
  # ascent statistics:
  # these are based on ascent period identified by diveMove criteria rather than divesum
  # identify when ascent begins
  begasc <- asc[1, 1]
  # calculate time of ascent
  asctim <-
    as.numeric(difftime(asc[nrow(asc), 1], begasc, units = "secs") +
                 interval / 2)
  # calculate ascent distance
  ascdist <- max(asc[, 2], na.rm = TRUE)
  # sum all periods to calculate total dive time
  divetim <-
    ifelse(!is.na(botttim), desctim + botttim + asctim, desctim + asctim)
  enddive <- begdesc + divetim
  
  # Average ascent and descent rate
  # note: this is using the start and end times of ascent as descent phase as identified by diveMove
  # numbers would be slightly different if identifyin descent and ascent phases using bottom_ds
  ascrate <- ascdist / as.numeric(asctim)
  descrate <- descdist / as.numeric(desctim)
  
  ## count number of 5s intervals with average rate of change <=1m/s
  #calculate mean of previous 5 values (inclusive, n=5), every 5 values (by=5)
  #assign bin numbers to rows
  x$bin <- rep(1:ceiling(nrow(x) / 5), each = 5)[1:nrow(x)]
  dc <-  x %>%
    group_by(bin) %>%
    # remove first record of each bin because contains depth delta from previous bin to current bin
    slice(2:5) %>%
    # calculate average of absolute values of depth changes in each bin
    # output is average change in meters per second because sample interval is 1s
    summarise(dc = mean(abs(ddepth_delta), na.rm = T), .groups = "drop")
  # Count number of bins with rate of change <= 1 m/s
  dc01 <- length(dc$dc[dc$dc <= 1])
  # count number of bins with rate of change >1.5 m/s
  dc15 <- length(dc$dc[dc$dc > 1.5])
  
  # Calculate light variables
  # light measured once per minute on the minute
  # take average of light reading at bottom to get est of bottom light (most likely will be same as minimum light)
  # Surpressing warnings because a lot of missing data so a lot of warnings generated
  lt_min <-  suppressWarnings(min(x$dlux, na.rm = TRUE))
  lt_max <-   suppressWarnings(max(x$dlux, na.rm = TRUE))
  lt_mean <-   suppressWarnings(mean(x$dlux, na.rm = TRUE))
  lt_at_bott <-   suppressWarnings(mean(bott$dlux, na.rm = TRUE))
  lt_depmin <-    suppressWarnings(x$ddepths[which.min(x$dlux)])
  lt_depmax <-    suppressWarnings(x$ddepths[which.max(x$dlux)])
  
  # Calculate temperature variables
  # Temperature measured every 30 seconds
  # Divide by 10 to get to degrees C
  temp_min <-   suppressWarnings(min(x$dtemps, na.rm = TRUE) / 10)
  temp_depmin <-   suppressWarnings(x$ddepths[which.min(x$dtemps)])
  temp_max <-   suppressWarnings(max(x$dtemps, na.rm = TRUE) / 10)
  temp_depmax <-   suppressWarnings(x$ddepths[which.max(x$dtemps)])
  temp_mean <-   suppressWarnings(mean(x$dtemps, na.rm = TRUE) / 10)
  temp_maxdiff <-   suppressWarnings(temp_max - temp_min)
  temp_at_bott <-
    suppressWarnings(mean(bott$dtemps, na.rm = TRUE) / 10)
  
  # Classify dive type using bottom time determined by diveMove
  # calculate how much time in each dive phase
  # 1. Foraging dive
  # case 1: btime>=20 .and. maxdepth>=15 .and. unducount>=4
  if (!is.na(botttim)) {
    divecase = case_when(
      maxdep >= 15 && botttim >= 20 && unducount >= 4 ~ 1,
      #or case 2:
      # *slow ascent/descent and fast ascent/descent periods comprise at least 30% each of total dive
      # case maxdepth>=10 .and. btime>=15 .and. divedur>=30 .and. unducount>=4 .and. ;
      # (((dc0+dc1)*5/divedur>.3 .and. (dc15)*5/divedur>.3)
      # *or lots of undulations with very fast ascent/descent
      # .or.(unducount>=6 .and. ar>1.0 .and. dr>1.0))
      maxdep >= 10 && botttim >= 15 &&
        divetim >= 30 && unducount >= 4 &&
        (
          dc01 * 5 / divetim > 0.3 &&  #number o
            dc15 * 5 > 0.3 ||
            unducount >= 6 &&
            ascrate > 1.0 &&
            descrate > 1.0
        ) ~ 2,
      maxdep >= 15 && (botttim < 20 || unducount < 4) ~ 3,
      #or
      # case maxdepth>=10 .and. btime<15 .and. unducount<6 .and. ar>.8 .and. dr>.8
      # *relatively deep dive with not much undulating, fast ascent/descent
      maxdep >= 10 &&
        botttim < 15 && unducount < 6 & ascrate > 0.8 & descrate > 0.8 ~ 4,
      # none of the above
      TRUE ~ 5
    )
    # add divetype based on cases
    divetype <- case_when(divecase %in% c(1, 2) ~ "F",
                          divecase %in% c(3, 4) ~ "E",
                          divecase == 5 ~ "O")
  } else{
    divecase = 6
    divetype = "O"
  }
  
  # Classify dive type using bottom time like divesum
  # calculate how much time in each dive phase
  if (!is.na(botttim_ds)) {
    divecase_ds = case_when(
      maxdep >= 15 && botttim_ds >= 20 && unducount >= 4 ~ 1,
      #or case 2:
      # *slow ascent/descent and fast ascent/descent periods comprise at least 30% each of total dive
      # case maxdepth>=10 .and. btime>=15 .and. divedur>=30 .and. unducount>=4 .and. ;
      # (((dc0+dc1)*5/divedur>.3 .and. (dc15)*5/divedur>.3)
      # *or lots of undulations with very fast ascent/descent
      # .or.(unducount>=6 .and. ar>1.0 .and. dr>1.0))
      maxdep >= 10 && botttim_ds >= 15 &&
        divetim >= 30 && unducount >= 4 &&
        (
          dc01 * 5 / divetim > 0.3 &&  #number of
            dc15 * 5 > 0.3 ||
            unducount >= 6 &&
            ascrate > 1.0 &&
            descrate > 1.0
        ) ~ 2,
      maxdep >= 15 && (botttim_ds < 20 || unducount < 4) ~ 3,
      #or
      # case maxdepth>=10 .and. btime<15 .and. unducount<6 .and. ar>.8 .and. dr>.8
      # *relatively deep dive with not much undulating, fast ascent/descent
      maxdep >= 10 &&
        botttim_ds < 15 &&
        unducount < 6 & ascrate > 0.8 & descrate > 0.8 ~ 4,
      # none of the above
      TRUE ~ 5
    )
    # add dive type based on dive case
    divetype_ds <- case_when(divecase_ds %in% c(1, 2) ~ "F",
                             divecase_ds %in% c(3, 4) ~ "E",
                             divecase_ds == 5 ~ "O")
  } else{
    divecase_ds = 6
    divetype_ds = "O"
  }
  
  
  if (plot) {
    plot(
      x$dtimes,
      -x$ddepths,
      type = "l",
      main = paste(
        did[1],
        divetype_ds,
        "\n",
        "unds =",
        unducount,
        "unds1m =",
        unds1m_ds
      ),
      xlab = "Time",
      ylab = "Depth (m)"
    )
    if (length(bott_row_ds) > 0) {
      lines(bott_ds$dtimes,
            -bott_ds$ddepths,
            col = "green",
            lwd = 2)
      points(x$dtimes[du_row], -x$ddepths[du_row], pch = 19, col = "blue")
      points(x$dtimes[ud_row], -x$ddepths[ud_row], pch = 19, col = "red")
      points(x$dtimes[unds1m_rows],-x$ddepths[unds1m_rows], col = "orange", cex =
               2)
      legend(
        "topleft",
        c("descent-ascent", "ascent-descent", "unds1m", "bottom"),
        col = c("blue", "red", "orange", "green"),
        pch = c(19, 19, 1, 19)
      )
    } else{
      points(x$dtimes[du_row], -x$ddepths[du_row], pch = 19, col = "blue")
      points(x$dtimes[ud_row], -x$ddepths[ud_row], pch = 19, col = "red")
      points(x$dtimes[unds1m_rows],-x$ddepths[unds1m_rows], col = "orange", cex =
               2)
      legend(
        "topleft",
        c("descent-ascent", "ascent-descent", "unds1m", "bottom"),
        col = c("blue", "red", "orange", "green"),
        pch = c(19, 19, 1, 19)
      )
    }
  }
  
  # create data frame with all calculated vars
  data.frame(
    did = did[1],
    begdesc,
    enddesc,
    begasc,
    desctim,
    botttim,
    botttim_ds,
    asctim,
    enddive,
    divetim,
    descdist,
    bottdist,
    bottdist_ds,
    ascdist,
    bottdep_mean,
    bottdep_mean_ds,
    bottdep_median,
    bottdep_median_ds,
    bottdep_sd,
    bottdep_sd_ds,
    maxdep,
    ascr = ascrate,
    descr = descrate,
    unds1m_ds,
    unds = unducount,
    lt_min = ifelse(lt_min == Inf, NA, lt_min),
    lt_max = ifelse(lt_max == -Inf, NA, lt_max),
    lt_mean = ifelse(is.nan(lt_mean), NA, lt_mean),
    lt_at_bott = ifelse(exists("lt_at_bott"), ifelse(is.nan(lt_at_bott), NA, lt_at_bott), NA),
    lt_depmin = ifelse(exists("lt_depmin"), lt_depmin, NA),
    lt_depmax = ifelse(exists("lt_depmax"), lt_depmax, NA),
    temp_min = ifelse(temp_min == Inf, NA, temp_min),
    temp_max = ifelse(temp_max == -Inf, NA, temp_max),
    temp_mean = ifelse(is.nan(temp_mean), NA, temp_mean),
    temp_maxdiff = ifelse(temp_maxdiff == -Inf, NA, temp_maxdiff),
    temp_at_bott = ifelse(
      exists("temp_at_bott"),
      ifelse(is.nan(temp_at_bott), NA, temp_at_bott),
      NA
    ),
    temp_at_bott_ds = ifelse(
      exists("temp_at_bott_ds"),
      ifelse(is.nan(temp_at_bott_ds), NA, temp_at_bott_ds),
      NA
    ),
    temp_depmin = ifelse(exists("temp_depmin"), temp_depmin, NA),
    temp_depmax = ifelse(exists("temp_depmax"), temp_depmax, NA),
    divetype,
    divecase,
    divetype_ds,
    divecase_ds
  )
}


