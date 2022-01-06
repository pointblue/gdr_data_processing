initialProbPath <- function(twl, fixedx, calib, beta, tol = 0.12, log.prior = log.prior, start.crds = NULL, particles = 1000, verbose = FALSE) {

  x0 <- data.frame(toDateline(with(twl, thresholdFirst(Twilight, Rise, zenith = calib[1], tol=tol)$x)), fixedx = fixedx, insterted = twl$Inserted)
  x0[x0$insterted,1] <- NA
  x0[x0$insterted,2] <- NA
  x0$tol <- ifelse(is.na(x0[,1]), TRUE, FALSE)
  x0[fixedx,1] <- start.crds[1] 
  x0[fixedx,2] <- start.crds[2]
  x0[,1] <- zoo::na.approx(x0[,1], rule = 3)
  x0[,2] <- zoo::na.approx(x0[,2], rule = 3)
  
  x0$accept <- log.prior(x0[,1:2])==0
   plot(mp$geometry)
   points(x0[,1:2], type = "o")
  
  for(i in which(!x0$accept & !x0$fixedx & !x0$insterted)) {
    
    cat("\b\b\b\b\b\b")
    cat(sprintf("%6d", i))

    # plot(mp$geometry)
    # points(x0[i-1,1], x0[i-1,2], pch = 16, col = "blue")
    # points(x0[i,1], x0[i,2], pch = 16, col = "orange")

    accept <- rep(FALSE, particles)
    x0_new <- matrix(NA, ncol = 2, nrow = particles)
    
    n = 0
    repeat{
      
        x0_test <- do.call("rbind", lapply(1:(particles - sum(accept)), function(x) {
          dev    <- rlnorm(nrow(twl), calib[3], calib[4])*60
          twlDev <- twl$Twilight + dev * ifelse(twl$Rise, 1, -1) - median(dev) * ifelse(twl$Rise, 1, -1)
            
          thresholdLocation(twlDev[i:(i+1)], twl$Rise[i:(i+1)], zenith = calib[1], tol=tol)$x
        }))
        
        x0_test[is.nan(x0_test[,2]),2] <- rnorm(sum(is.nan(x0_test[,2])), x0[i-1,2], 1.5)
                  
        accept_run <- log.prior(toDateline(x0_test))==0
        x0_new[which(!accept)[accept_run],] <- x0_test[accept_run,]
              
        accept[which(!accept)[accept_run]] <- TRUE
        
        n = n+1
        
        if(n > 5) {
          repeat{
            
            x0_test <- matrix(c(rnorm((particles - sum(accept)), x0[i,1], 3), 
                                rnorm((particles - sum(accept)), x0[i,2], 3)), ncol = 2, byrow = F)
            
            x0_test[is.nan(x0_test[,2]),2] <- rnorm(sum(is.nan(x0_test[,2])), x0[i-1,2], 1.5)
            
            accept_run <- log.prior(toDateline(x0_test))==0
            x0_new[which(!accept)[accept_run],] <- x0_test[accept_run,]
            
            accept[which(!accept)[accept_run]] <- TRUE
            
            if(sum(accept)>(length(accept)*0.1)) break
            
          }
          break
        }
        
        if(sum(accept)>(length(accept)*0.1)) break
          
    }
    
    x0_new <- x0_new[accept,]

      
    # plot(mp$geometry)
    # points(x0_new, pch = 16, col = adjustcolor("grey20", alpha.f = 0.4))
    # points(x0[i-1,1],  x0[i-1,2], pch = 16, col = "orange")
                
      diffTm  <- difftime(twl$Twilight[i], twl$Twilight[i-1], units = "hours")
      kmH     <- (distVincentySphere(x0_new, x0[i-1,])/1000)/as.numeric(diffTm)
      indM    <- which.max(dnorm(kmH, beta[1], beta[2]))
      # points(x0[indM,1],  x0[indM,2], pch = 16, col = "blue")
      
    x0[i,1:2] <- x0_new[indM,]
       
    # if(i>1) lines(toDateline(x0[1:i,]), type = "o", pch = 16, cex = 0.5, col = ifelse(x0[1:i,3], "cornflowerblue", "firebrick"))
          
  }
    
  plot(mp$geometry)
  lines(x0[,1:2], type = "o")
  
  x0
  
}
  
    
  