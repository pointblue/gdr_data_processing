makeSSTarray <- function(xlim = c(140, 250), ylim = c(-80, -50), fact = 5) {
  
  require(abind)
  require(raster)
  require(googledrive)
  
  stack_grd <- drive_get("sstStack.grd")
  stack_gri <- drive_get("sstStack.gri")
  
  if(!dir.exists("data/mask")) {
    dir.create("data")
    dir.create("data/mask")
  }
  
  if(!file.exists("data/mask/sstStack.grd")) drive_download(stack_grd$path, "data/mask/sstStack.grd", overwrite = T)
  if(!file.exists("data/mask/sstStack.gri")) drive_download(stack_gri$path, "data/mask/sstStack.gri", overwrite = T)
  
  sstStack <- brick("data/mask/sstStack.grd")
  sstStack <- crop(sstStack, as(extent(c(xlim, ylim)), "SpatialPolygons"))
  sstStack <- aggregate(sstStack, fact = fact)

  sst_dates <- as.POSIXct(substr(names(sstStack), 6, nchar(names(sstStack))), format = "%Y.%m.%d", tz = "GMT")

  sstArray <- abind(lapply(1:length(sst_dates), function(x) {
    SST <- sstStack[[x]]
    seaIceInd <- which(SST[]==-10)
    SST[seaIceInd] <- -1.8
    as.matrix(SST)[nrow(SST):1,]
  }), along = 3)
  
  xbin = seq(xlim[1], xlim[2], length=dim(sstArray)[2])
  ybin = seq(ylim[1], ylim[2], length=dim(sstArray)[1])
  
  
  sstList <- list(sstArray = sstArray, xbin = xbin, ybin = ybin, dates = sst_dates)
  
  save(sstList, file = "data/mask/sstArray.RData")
  return(sstList)
    
}
