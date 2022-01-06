landMask <- function(xlim, ylim) {
  
  require(raster)
  require(sf)
  require(rnaturalearth)
  require(maptools)
  
  if(!all(file.exists(c("data/mask/landMask.grd", "data/mask/landMask.gri")))) {
    
      n = 4
      xlim_large <- c(0, 360)
      ylim_large <- c(-90, 0)
    
      pls <- read_sf("data/mask/add_coastline_high_res_polygon_v7.2/add_coastline_high_res_polygon_v7.2.shp")
      
      r    <- raster(res = 2500, xmn = st_bbox(pls)[1], xmx = st_bbox(pls)[3], ymn = st_bbox(pls)[2], ymx = st_bbox(pls)[4], crs = proj4string(as(pls, "Spatial")))
      rOut <- rasterize(as(pls, "Spatial"), r, 1, silent = FALSE)
      # plot(rOut)

      rProj = projectRaster(rOut, crs = CRS("+proj=longlat +lon_0=0"), method = "ngb")
      
      centr <- raster(cbind(
                  as.matrix(crop(rProj, as(extent(c(0,180,-90,0)), "SpatialPolygons"))),
                  as.matrix(crop(rProj, as(extent(c(-180,0,-90,0)), "SpatialPolygons")))), crs = CRS("+proj=longlat +lon_0=0"))
      extent(centr) <- c(0, 360, -89.99983, -50.06953)
      # plot(pls$geometry[pls$surface=="ice shelf",], add = T)
      
      r <- raster(nrows = n * diff(ylim_large), ncols = n * diff(xlim_large), xmn = xlim_large[1], 
                  xmx = xlim_large[2], ymn = ylim_large[1], ymx = ylim_large[2], crs = proj4string(centr))
      
      lMask   <- resample(crop(merge(e1, e2, tolerance = 1), r), r)
      lMask[] <- ifelse(!is.na(r[]) | lMask[]>0, 1, NA)

      writeRaster(lMask, filename='data/mask/landMask.grd', format="raster", overwrite=TRUE, options=c("INTERLEAVE=BAND","COMPRESS=LZW"))
      
  } else {
    lMask <- raster('data/mask/landMask.grd')
    lMask <- crop(lMask, extent(c(xlim, ylim)))
  }
  
  m <- as.matrix(is.na(lMask))[nrow(lMask):1, ]
  m <- !m
  xbin <- seq(xlim[1], xlim[2], length = ncol(m) + 1)
  ybin <- seq(ylim[1], ylim[2], length = nrow(m) + 1)
  
  function(p) {
    m[cbind(.bincode(p[, 2], ybin), .bincode(p[, 1], xbin))]
  }
}
