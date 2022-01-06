library(GeoLocTools)
suppressMessages(suppressWarnings(setupGeolocation()))
require(parallel)
require(sf)
require(rnaturalearth)
require(rnaturalearthdata)

if(!dir.exists("data") & !dir.exists("data/mask")) {
  dir.create("data")
  dir.create("data/mask")
}
##### create sstArray
# source("code/mask/makeSSTarray.R")
# sstArray <- makeSSTarray(xlim = c(145, 230), ylim = c(-80, -55), fact = 5)
source("code/mask/landMask.R")

is.sea <- landMask(xlim = c(145, 250), ylim = c(-90, -50))

log.prior <- function(p) {
  f <- is.sea(p)
  ifelse(f | is.na(f), -1000, 0)
}

##### get metadata
mdat    <- drive_get("croz_royds_1718_retr_withdd.csv")
tmpPath <- tempdir()
drive_download(mdat$path, paste0(tmpPath, "/metadata.csv"), overwrite = T, verbose = F)

mTab    <- read.csv(paste0(tmpPath, "/metadata.csv"))

mTab$date_deployed <- as.POSIXct(mTab$date_deployed, format = "%m/%d/%Y", tz = "GMT")
mTab$date_retrieved <- as.POSIXct(mTab$date_retrieved, format = "%m/%d/%Y", tz = "GMT")
mTab$last_seen_bandtal <- as.POSIXct(mTab$last_seen_bandtal, format = "%m/%d/%Y", tz = "GMT")
mTab$last_dry_day <- as.POSIXct(mTab$last_dry_day, format = "%m/%d/%Y", tz = "GMT")
mTab$first_dry_day <- as.POSIXct(mTab$first_dry_day, format = "%m/%d/%Y", tz = "GMT")
mTab$start <- as.POSIXct(mTab$start, format = "%m/%d/%Y", tz = "GMT")
mTab$end <- as.POSIXct(mTab$end, format = "%m/%d/%Y", tz = "GMT")

# mTab$date_deployed <- as.POSIXct(mTab$date_deployed, format = "%d.%m.%y", tz = "GMT")
# mTab$date_retrieved <- as.POSIXct(mTab$date_retrieved, format = "%d.%m.%y", tz = "GMT")
# mTab$last_seen_bandtal <- as.POSIXct(mTab$last_seen_bandtal, format = "%d.%m.%y", tz = "GMT")
# mTab$last_dry_day <- as.POSIXct(mTab$last_dry_day, format = "%d.%m.%y", tz = "GMT")
# mTab$first_dry_day <- as.POSIXct(mTab$first_dry_day, format = "%d.%m.%y", tz = "GMT")
# mTab$start <- as.POSIXct(mTab$start, format = "%d.%m.%y", tz = "GMT")
# mTab$end <- as.POSIXct(mTab$end, format = "%d.%m.%y", tz = "GMT")

mTab <- subset(mTab, !is.na(final_run))

mTab$id <- as.numeric(lapply(strsplit(sapply(strsplit(as.character(mTab$filename), "_"), function(x) x[[1]]), ".txt"), function(y) y[[1]]))
unlink(tmpPath)

### Functions

toDateline <- function(p) {
  p[,1] <- ifelse(p[,1]<0, 180 + (180 + p[,1]), p[,1])
  p  
}

initialPath <- function (twilight, rise, time = twilight, zenith = 96, tol = 0.08, unfold = TRUE) 
{
  ls <- thresholdLocation(twilight, rise, zenith = zenith, tol = tol)
  if (!is.null(time)) {
    keep <- !is.na(ls$x[, 1L])
    ts <- ls$time[keep]
    lon <- ls$x[keep, 1L]
    if (unfold) lon <- unwrapLon(lon)
    lon <- approx(x = ts, y = lon, xout = time, rule = 2)$y
    eq  <- is.na(ls$x[, 2L])
    keep <- !is.na(ls$x[, 2L])
    ts <- ls$time[keep]
    lat <- ls$x[keep, 2L]
    lat <- approx(x = ts, y = lat, xout = time, rule = 2)$y
    lat[eq] <- NA
    ls <- list(time = time, x = cbind(lon, lat))
  }
  ls
}



### Map
xlim = c(140, 250)
ylim = c(-87, -50)

bbox1 <- st_polygon(list(matrix(c(-180, -180, 0, 0, -180, -90, 90, 90, -90, -90), ncol = 2, byrow = F))) %>% st_geometry() %>% st_set_crs(4326)
bbox2 <- st_polygon(list(matrix(c(180, 180, 0, 0, 180, -90, 90, 90, -90, -90), ncol = 2, byrow = F))) %>% st_geometry() %>% st_set_crs(4326)

bbox  <- st_polygon(list(matrix(c(xlim, rev(xlim), xlim[1], ylim[1], ylim, rev(ylim)), ncol = 2, byrow = F))) %>% st_geometry() %>% st_set_crs(4326)

world <- rnaturalearth::ne_countries(50, type = "countries", returnclass = "sf")
wr1 <- suppressMessages(suppressWarnings(elide(as(world %>% st_intersection(bbox1), "Spatial"), shift = c(360, 0)) %>% st_as_sf() %>% st_set_crs(4326)))
wr2 <- suppressMessages(suppressWarnings(world %>% st_buffer(0) %>% st_intersection(bbox2)))

mp    <- suppressMessages(suppressWarnings(rbind(wr1, wr2) %>% st_intersection(bbox)))
