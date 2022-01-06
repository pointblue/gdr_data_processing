#### Spatial mask (ocean/land) with sst (sea surface temperature, from NASA OISST v2.0)

require(sf)
require(raster)
require(ncdf4)
require(gdalUtils)
require(RCurl)

xlim = c(120, 270)
ylim = c(-85, -40)


url       <- "ftp://ftp2.psl.noaa.gov/pub/Datasets/noaa.oisst.v2.highres/"
filenames <- getURL(url, ftp.use.epsv = FALSE, dirlistonly = TRUE)
filenames <- unlist(strsplit(filenames, "\n"))

### lsmask
tmpDir <- tempdir()

flname  <- "lsmask.oisst.nc"
lsmask  <- download.file(paste(url, flname, sep = ""), paste(tmpDir, "/", flname, sep = ""))
nc      <- nc_open(paste(tmpDir, "/", flname, sep = ""))

lons <- nc$dim$lon$vals
lats <- nc$dim$lat$vals

lonWindx = which.min(abs(min(lons) - lons))
lonEindx = which.min(abs(max(lons) - lons))
latSindx = which.min(abs(min(lats) - lats))
latNindx = which.min(abs(max(lats) - lats))

nlon = (lonEindx - lonWindx) + 1
nlat = (latNindx - latSindx) + 1

nc_close(nc)

#### 2017-2019
for(i in 2017:2019) {
  
  flname  <- paste0("sst.day.mean.", i, ".nc")
  # nc_fl   <- download.file(paste(url, flname, sep = ""), paste(tmpDir, "/", flname, sep = ""))

  nc      <- nc_open(paste(tmpDir, "/", flname, sep = ""))
  ncdates = as.POSIXct("1800-01-01 00:00:00", tz = "GMT") + nc$dim$time$vals*24*60*60
  
  sstout  = array(data = NA, dim = c(nlon, nlat, length(ncdates)))
  sstout[, , ] = ncvar_get(nc, varid = "sst", 
                           start = c(lonWindx, latSindx, 1), count = c(nlon, nlat, length(ncdates)))
  
  br  <- brick(nrow = nlat, ncol = nlon,
               xmn = min(lons), xmx = max(lons), ymn = min(lats), ymx = max(lats))
  
  sstStack <- setValues(br, aperm(sstout[,dim(sstout)[2]:1,], c(2,1,3)))
  names(sstStack) <- paste0("date_", format(ncdates, "%Y-%m-%d")) 
  nc_close(nc)

  sstStack <- crop(sstStack, as(extent(c(xlim, ylim)), "SpatialPolygons"))
  
  if(i == 2017) sst <- sstStack else sst <- stack(sst, sstStack)
      
}


## uploaded to googleDrive 14.09.2020
writeRaster(sst, filename='sstStack.grd', format="raster", overwrite=TRUE, options=c("INTERLEAVE=BAND","COMPRESS=LZW"))

