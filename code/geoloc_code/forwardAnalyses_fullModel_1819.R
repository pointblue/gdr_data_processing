require("googledrive")
drive_auth(email = "simeon.lisovski@gmail.com")
source("code/functions/setupSession_1819.R")

tmpDir <- tempdir()

fls <- drive_ls("geoloc/1819/twl/", n_max = 500)
  fls$ind_id <- as.numeric(sapply(strsplit(fls$name, "_"), function(x) as.character(x[[1]])))
  fls <- subset(fls, ind_id%in%as.numeric(mTab$id))

  ### Creation date
  
  paths <- dir(R.home(), full.names=TRUE)
  tail(file.info(paths)$ctime)  
  
  fitFls <- data.frame(path = as.character(list.files("~/Google Drive/Shared/geoloc/results_1819/fit/", pattern = "*.RData", full.names=TRUE)))
  fitFls$ids <- as.numeric(sapply(strsplit(list.files("~/Google Drive/Shared/geoloc/results_1819/fit/", pattern = "*.RData"), "_"), function(x) as.character(x[[1]])))
  fitFls$dts <- abs(as.POSIXct(Sys.Date()) - file.info(as.character(fitFls$path))$ctime)
  
  # fls <- fls[fls$ind_id%in%fitFls$ids[fitFls$dts>45],]
  fls <- fls[fls$ind_id%in%c(5138, 1819),]
  
for(ind in 1:nrow(fls)) {
    
    mdatInd      <- which(mTab$id==as.numeric(fls$ind_id[ind]))
    id_tag       <- mTab$id[mdatInd]
    
    ### load twl ----
      drive_download(as_id(fls$id[ind]), paste0(tmpDir, "/twl.csv"), overwrite = T, verbose = F)
      twl          <- read.csv(paste0(tmpDir, "/twl.csv"))  
      twl$Twilight <- as.POSIXct(twl$Twilight, tz = "GMT")
      twl          <- subset(twl, !Deleted)  
      twl          <- twilightAdjust(twl, 60)
    
      twl <- twl[order(twl$Twilight),]
      
      st.twl  <- mTab$start[mdatInd] - c(2*24*60*60)
      end     <- mTab$end[mdatInd]
      retr    <- mTab$date_retrieved[mdatInd]
      if (end>=retr) {
        end.twl <- end
      } else if (difftime(retr, end, units="days")==1) {
        end.twl <- end + c(1*24*60*60)
      } else {
        end.twl = end + c(2*24*60*60)
      }
      twl <- subset(twl, twl$Twilight>=st.twl & twl$Twilight<=end.twl & !duplicated(twl$Twilight))
  
      
    ### calibration ----
      if (mTab$colony[mdatInd]=="croz") {
        lon.calib <- 169.23
        lat.calib <- -77.45
        calib <- c(96.2498399, 97.5389312, 4.3962314, 0.1710442)
      } else {
        lon.calib <- 166.16
        lat.calib <- -77.55
        calib <- c(95.93327559, 97.29943174, 2.71897057, 0.09853875)
      }
    
    ### initial path ----   
      
    bestZenith <- t(sapply(seq(-1, 1, length = 20), function(x) {
      init      <- initialPath(twl$Twilight, twl$Rise, zenith = calib[1]+x, tol = 0.15)$x
      init2     <- thresholdPath(twl$Twilight, twl$Rise, zenith = calib[1]+x, tol = 0.15)$x[is.na(init[,2]),2]
      cbind(x, sum(log.prior(init)==0)/nrow(init), sd(init2))
    }))
    
    bZ <- bestZenith[order(bestZenith[,2], bestZenith[,3], decreasing = T)[1],1]  
      
    fixedx <- rep(FALSE, nrow(twl))
      fixedInd <- which(twl$Twilight<=mTab$start[mdatInd]|twl$Twilight>=(end.twl - c(1*24*60*60)))
    fixedx[unique(c(1,2,fixedInd))] <- TRUE 
      
    init <- initialPath(twl$Twilight, twl$Rise, zenith = calib[1]+bZ, tol = 0.15)$x
    init[twl$Inserted & !fixedx,1:2] <- NA
      
    init[fixedx,1] <- lon.calib
    init[fixedx,2] <- lat.calib
      
    acceptLoc <- log.prior(init)==0
    
      
     # plot(mp$geometry)
     # points(init, type = "o", pch = 16, col = adjustcolor("cornflowerblue", alpha.f = 0.5))
     # points(init[!acceptLoc,], type = "o", pch = 16, col = adjustcolor("firebrick", alpha.f = 0.5))
  
    prop <- abind::abind(lapply(1:1000, function(z) {
            dev    <- rlnorm(nrow(twl), calib[3], calib[4])*60
            twlDev <- twl$Twilight + dev * ifelse(twl$Rise, 1, -1) - median(dev) * ifelse(twl$Rise, 1, -1)
            crds   <- thresholdPath(twlDev, twl$Rise, zenith = calib[1]+bZ, tol=0.3)$x
            crds[twl$Inserted & !fixedx,1:2] <- NA
            crds
          }), along = 3)
    x0 <- init
    
    for(i in which(!twl$Inserted & !fixedx & !acceptLoc)) {
            
            bef    <- max(which(!is.na(x0[1:(i-1),1])))
            pos     <- matrix(prop[i,,], ncol = 2, byrow = T)
            pos[,2] <- rnorm(nrow(pos), x0[bef,2], 4)  
            accept <- log.prior(pos)==0
          
            if(sum(accept)<2) {
              pos <- matrix(c(prop[i,1,], runif(length(prop[i,1,]), -85, -50)), ncol = 2)
              accept <- log.prior(pos)==0
            }
            
            pos     <- pos[accept,]
            posW    <- pos; posW[,1] <- wrapLon(pos[,1])
            speed   <- (suppressWarnings(geosphere::distVincentySphere(matrix(c(wrapLon(x0[bef,1]),x0[bef,2]), ncol = 2), posW))/1000)/
                          as.numeric(difftime(twl$Twilight[i], twl$Twilight[bef], units = "hours"))
            x0[i,]  <- pos[which.max(dgamma(speed, calib[3], calib[4])),]
            
          }
    
    x0 <- toDateline(x0)
          
      x0[,1] <- zoo::na.approx(x0[,1],rule = 3)
      x0[,2] <- zoo::na.approx(x0[,2],rule = 3)
          
    z0 <- trackMidpts(x0)
      
    # plot(mp$geometry)
    # points(x0, type = "o", pch = 16, col = adjustcolor("grey10", alpha.f = 0.5))
    
    ### pre vs final run ----
    
    if((!is.na(mTab$best_zenith) & mTab$final_run)[mdatInd]) {
    
    ### params to run ----
    parms <- data.frame(zenith = seq(-0.75, 0.75, by = 0.25))
      
    ### parallelized run ----
    cl <- parallel::makeCluster(nrow(parms), setup_strategy = "sequential")
    clusterExport(cl, c("twl", "fixedx", "x0", "z0", "calib"))
    clusterEvalQ(cl, {
      
      library(GeoLocTools)
      suppressWarnings(setupGeolocation())
      source("code/mask/landMask.R")
      
      is.sea <- landMask(xlim = c(130, 250), ylim = c(-90, -50))
      
      log.prior <- function(p) {
        f <- is.sea(p)
        ifelse(f | is.na(f), -1000, 0)
      }
      
    })
      
    parOut <- parLapply(cl, split(parms, 1:nrow(parms)), function(p) {
    
          model <- thresholdModel(twl$Twilight,
                                  twl$Rise,
                                  twilight.model = "ModifiedGamma",
                                  alpha = calib[3:4],
                                  beta =  c(0.2, 0.25),
                                  x0 = x0, 
                                  z0 = z0, 
                                  zenith = calib[2]+p[[1]],
                                  logp.x = log.prior,
                                  logp.z = log.prior,
                                  missing = ifelse(twl$Inserted, 3, 0),
                                  fixedx = fixedx)
            
          x.proposal <- mvnorm(S = diag(c(0.005, 0.005)), n = nrow(twl))
          z.proposal <- mvnorm(S = diag(c(0.005, 0.005)), n = nrow(twl) - 1)
            
          fit <- estelleMetropolis(model, x.proposal, z.proposal, iters = 10, thin = 2000, verbose = F)
    
    
          for (k in 1:3) {
            x.proposal <- mvnorm(chainCov(fit$x), s = 0.2)
            z.proposal <- mvnorm(chainCov(fit$z), s = 0.2)
            
            fit <- estelleMetropolis(model, x.proposal, z.proposal, x0 = chainLast(fit$x),
                                     z0 = chainLast(fit$z), iters = 300, thin = 20, verbose = F)
          }
          
          ## Final model ----
          x.proposal <- mvnorm(chainCov(fit$x), s = 0.25)
          z.proposal <- mvnorm(chainCov(fit$z), s = 0.25)
          
          fit <- estelleMetropolis(model, x.proposal, z.proposal, x0 = chainLast(fit$x), 
                                   z0 = chainLast(fit$z), iters = 2000, thin = 20, verbose = F)
    
          
          fit
          
    })
    
    stopCluster(cl)
    gc(verbose = F)
    
    ### plot ----
    pdf(paste0(tmpDir, "/", mTab$id[mdatInd], "_maps_iter1.pdf"), width = 15, height = 8)
    opar <- par(mfrow = c(3,3), mar = c(0,0,0,0))
    
    plot(mp$geometry)
    points(x0, type = "o", col = adjustcolor("grey80", alpha.f = 0.75))
    
    for(p in 1:length(parOut)) {
      
      fit <- parOut[[p]]
      if(is.null(fit)) {
        plot(1,1, xaxt = "n", yaxt = "n", xlab = "", ylab = "")
      } else {
      sm  <- locationSummary(fit$z, time=fit$model$time)
      
      r   <- raster(nrows = 2 * diff(ylim), ncols = 2 * diff(xlim), xmn = xlim[1]-5,
                    xmx = xlim[2]+5, ymn = ylim[1]-5, ymx = ylim[2]+5, crs = "+proj=longlat")
      
      s  <- slices(type = "intermediate", mcmc = fit, grid = r)
      sk <- SGAT:::slice(s, sliceIndices(s))
      
      plot(mp$geometry, border = "transparent")
      plot(bbox, add = T)
      plot(sk, col = rev(viridis::viridis(100)), add = T, legend = FALSE)
      plot(mp$geometry, border = "grey40", col = adjustcolor("grey70", alpha.f = 0.8), add = T)
      
      lines(sm[,"Lon.50%"], sm[,"Lat.50%"], col = "black", type = "l", pch = 16, cex = 0.75)
      lines(sm[,"Lon.50%"], sm[,"Lat.50%"], col = heat.colors(nrow(sm)), type = "o", pch = 16, cex = 0.75)
      points(lon.calib, lat.calib, pch = 21, bg = "white", cex = 2)
      }
      
      mtext(paste0(mTab$id[mdatInd], " - zenithAdjust = ", parms[p,1]), 3, line = -1.5, cex = 1)
    }
    par(opar)  
    dev.off()
    
    drive_upload(paste0(tmpDir, "/", mTab$id[mdatInd], "_maps_iter1.pdf"), 
                 path = paste0("geoloc/results_1819/preRun/", mTab$id[mdatInd], "_maps_iter1.pdf"), overwrite = T, verbose = F)
    unlink(paste0(tmpDir, mTab$id[mdatInd], "_maps_iter1.pdf"))
    
    
    } else {
    
      ### maks ----
      is.sea <- landMask(xlim = c(130, 250), ylim = c(-90, -50))
      
      log.prior <- function(p) {
        f <- is.sea(p)
        ifelse(f | is.na(f), -1000, 0)
      }
      
      ### model ----
      model <- thresholdModel(twl$Twilight,
                              twl$Rise,
                              twilight.model = "ModifiedGamma",
                              alpha = calib[3:4],
                              beta =  c(0.2, 0.25),
                              x0 = x0, 
                              z0 = z0, 
                              zenith = calib[2]+mTab[mdatInd,"best_zenith"],
                              logp.x = log.prior,
                              logp.z = log.prior,
                              missing = ifelse(twl$Inserted, 3, 0),
                              fixedx = fixedx)
      
      x.proposal <- mvnorm(S = diag(c(0.005, 0.005)), n = nrow(twl))
      z.proposal <- mvnorm(S = diag(c(0.005, 0.005)), n = nrow(twl) - 1)
      
      fit <- estelleMetropolis(model, x.proposal, z.proposal, iters = 10, thin = 2000, verbose = F)
      
      for (k in 1:3) {
        fit <- estelleMetropolis(model, x.proposal, z.proposal, x0 = chainLast(fit$x),
                                 z0 = chainLast(fit$z), iters = 300, thin = 20, verbose = F)
    
        x.proposal <- mvnorm(S = diag(c(0.05, 0.05)), n = nrow(twl))
        z.proposal <- mvnorm(S = diag(c(0.05, 0.05)), n = nrow(twl) - 1)
      }
    
    
      x.proposal <- mvnorm(chainCov(fit$x), s = 0.25)
      z.proposal <- mvnorm(chainCov(fit$z), s = 0.25)
      
      fit <- estelleMetropolis(model, x.proposal, z.proposal, x0 = chainLast(fit$x), 
                               z0 = chainLast(fit$z), iters = 2000, thin = 20, verbose = F)
      
      save(fit, file = paste0(tmpDir, "/fit.RData"))
      drive_upload(paste0(tmpDir, "/fit.RData"), 
                   path = paste0("geoloc/results_1819/fit/", mTab$id[mdatInd], "_fit.RData"), overwrite = T, verbose = F)
      
      ### summary ----
      
      sm     <- locationSummary(fit$z, time=fit$model$time)
        trk  <- st_sfc(lapply(1:(nrow(sm)-1), function(x) st_linestring(as.matrix(sm[x:(x+1),c("Lon.50%", "Lat.50%")]))), crs = 4326)
        indD <- sapply(c(st_intersects(trk, mp)), length)>0
        indD[c(1:5, (length(indD)-4):length(indD))] <- FALSE
        # plot(trk, col = ifelse(indD, "blue", "red"))
        # plot(mp$geometry, add = T)
        mov    <- cbind(SGAT2Movebank(fit$z, time = fit$model$time), inserted = twl$Inserted[-nrow(twl)], filterLand = c(indD, FALSE))
      
      write.csv(mov, paste0(tmpDir, "/movSum.csv"), row.names = FALSE)
      drive_upload(paste0(tmpDir, "/movSum.csv"), 
                   path = paste0("geoloc/results_1819/movementSummary/", mTab$id[mdatInd], "_movementSummary.csv"), overwrite = T, verbose = F)
      
      
      ### plot ----
      r   <- raster(nrows = 2 * diff(ylim), ncols = 2 * diff(xlim), xmn = xlim[1]-5,
                    xmx = xlim[2]+5, ymn = ylim[1]-5, ymx = ylim[2]+5, crs = "+proj=longlat")
      
      s  <- slices(type = "intermediate", mcmc = fit, grid = r)
      sk <- SGAT:::slice(s, sliceIndices(s)[!indD])
      
      png(paste0(tmpDir, "/track.png"), width = 800, height = 800)
      opar <- par(mar = c(1,1,1,1))
      plot(mp$geometry, border = "transparent")
      plot(bbox, add = T)
      plot(sk, col = rev(viridis::viridis(100)), add = T, legend = FALSE)
      plot(mp$geometry, border = "grey40", col = adjustcolor("grey70", alpha.f = 0.8), add = T)
      
      sm$`Lon.50%`[indD] <- NA
      
      lines(sm[,"Lon.50%"], sm[,"Lat.50%"], col = "black", type = "l", pch = 16, cex = 1)
      lines(sm[,"Lon.50%"], sm[,"Lat.50%"], col = heat.colors(nrow(sm)), type = "o", pch = 16, cex = 1)
      points(sm[twl$Inserted,"Lon.50%"], sm[twl$Inserted,"Lat.50%"], pch = "x", cex = 0.5)
      points(lon.calib, lat.calib, pch = 21, bg = "white", cex = 2)
      
      mtext(mTab$id[mdatInd], 3, line = -5, cex = 1.8)
      par(opar)
      dev.off()
      
      drive_upload(paste0(tmpDir, "/track.png"), 
                   path = paste0("geoloc/results_1819/movementSummary/trackMaps/", mTab$id[mdatInd], "_track.csv"), overwrite = T, verbose = F)
      
    }
    
    }
    