
library(forecast)
library(season)
library(zoo)
library(xts)
library(MASS)
library(ggplot2)
library(survival)
library(lubridate)
library(matrixStats)
library(openxlsx)


#dados <- read.xlsx ('Castelo.xlsx', 1)

# Evaluate dma(x) function to set global variables for DMAx
dma <- function (mat) {
  res <- 4*24
  inter <<- gsub (".", "h", substring (names (mat) [-1], 2), fixed = T)
  datas <<- as.Date (mat [, 1])
  ats <<- format (datas, '%d') == '01'
  datas.g <<- paste (paste (format (as.Date (mat [, 1]), "%b"), format (as.Date (mat [, 1]), "%d")), ",", sep = "")
  dias <<- merge (inter, datas) [, 2]
  tempo <<- paste (merge (inter, datas) [, 2], merge (inter, datas) [, 1])
  tempo.g <<- paste (merge (inter, datas.g) [, 2], merge (inter, datas.g) [, 1])
  dias.da.semana <<- c ()
  for (i in 1:length (weekdays (datas))) dias.da.semana <<- c (dias.da.semana, rep (weekdays (datas) [i], res))
  horas <<- c (paste (0:23, "h", sep = ""), "0h")
  meses.ano <<- paste ( format (as.Date( mat[,1]),"%b"), format (as.Date (mat [, 1]), "%Y"))
  meses.do.ano <<- c ()
  for (i in 1:length (meses.ano)) meses.do.ano <<- c (meses.do.ano, rep (meses.ano [i], res))
  anos <<- paste ("(", first (unique (meses.ano)), " - ", last (unique (meses.ano)), ")", sep = "")

  res <<- length (inter)
  y.t <<- ts (as.vector (t (as.matrix (mat [, -1])))) #Coloca os valores ordenados cronologicamente em função da hora e dia
  y.m <<- msts (as.vector (t (as.matrix (mat [, -1]))), seasonal.periods = c (res, (7*res)), ts.frequency = res)

  daily.medians <<- sapply (lapply (split (y.t, f = dias), function (x) {median (x, na.rm = F)}), function (x) as.numeric (x))
  daily.means <<- sapply (lapply (split (y.t, f = dias), function (x) {mean (x, na.rm = F)}), function (x) as.numeric (x))
  daily.agg <<- sapply (split (y.t, dias), function (x) sum (x/4, na.rm = F)) #soma das médias de cada hora por cada dia
}

# Reconstruct using JQ approach (Level 1: Weekly TBATS; Level 2: median):
JQ.reconstruct <- function () {
  # Compute daily aggregate values
  daily.agg <- sapply(split(y.t, dias), function(x) sum(x/4, na.rm = FALSE))

  # Aggregated daily model (forward/backward)
  daily.tbats.model <- function(agg.y.t) {
    for (i in 15:length(agg.y.t)) {
      if (is.na(agg.y.t[i]) && sum(is.na(agg.y.t[(i-14):(i-1)])) == 0) {
        mod <- tbats(ts(agg.y.t[(i-14):(i-1)]), seasonal.periods = 7)
        pred <- forecast(mod, h = 1)$mean
        agg.y.t[i] <- pred
      }
    }
    agg.y.t
  }

  daily.flow.fc <- daily.tbats.model(daily.agg)           # Forward forecast
  daily.flow.bc <- rev(daily.tbats.model(rev(daily.agg)))     # Backward forecast
  daily.flow.TBATS <- colMeans(rbind(daily.flow.fc, daily.flow.bc), na.rm = TRUE)

  # Create aggregated model data frame
  agg.model <- data.frame(month = month.name[month(datas)],
                           weekday = weekdays(datas),
                           estimate = daily.flow.TBATS)

  # Dynamically determine the number of days in the subset
  num_days <- length(y.t) / res   # Each day has 'res' observations

  # Reconstruct the time series as a matrix using dynamic number of columns
  matriz <- t(matrix(y.t, nrow = res, ncol = num_days))

  # Create a data frame (DF) with monthly and weekday info
  DF <- data.frame(month = month.name[month(datas)],
                   weekday = weekdays(datas),
                   matriz)

  # Define pattern function based on measure

    pat.func <- function(a) {
      if (nrow(a) != 0) {
        lapply(split(a[, -(1:2)], a$weekday), function(x) colMedians(as.matrix(x), na.rm = TRUE))
      } else {
        return(NULL)
      }
    }


  # Compute patterns for each month
  aux <- DF[which(DF$month == "January"),]
  Jan <- pat.func(aux)
  aux <- DF[which(DF$month == "February"),]
  Feb <- pat.func(aux)
  aux <- DF[which(DF$month == "March"),]
  Mar <- pat.func(aux)
  aux <- DF[which(DF$month == "April"),]
  Apr <- pat.func(aux)
  aux <- DF[which(DF$month == "May"),]
  May <- pat.func(aux)
  aux <- DF[which(DF$month == "June"),]
  Jun <- pat.func(aux)
  aux <- DF[which(DF$month == "July"),]
  Jul <- pat.func(aux)
  aux <- DF[which(DF$month == "August"),]
  Aug <- pat.func(aux)
  aux <- DF[which(DF$month == "September"),]
  Sep <- pat.func(aux)
  aux <- DF[which(DF$month == "October"),]
  Oct <- pat.func(aux)
  aux <- DF[which(DF$month == "November"),]
  Nov <- pat.func(aux)
  aux <- DF[which(DF$month == "December"),]
  Dec <- pat.func(aux)

  patterns <- list(January = Jan, February = Feb, March = Mar, April = Apr,
                   May = May, June = Jun, July = Jul, August = Aug,
                   September = Sep, October = Oct, November = Nov, December = Dec)

  # Reconstruct the time series by filling missing values
  y.p <- rep(NA, length(y.t))
  for (i in 1:length(y.t)) {
    if (is.na(y.t[i])) {
      ds <- dias.da.semana[i]                    # Day of the week for missing observation
      me <- month.name[month(tempo[i])]           # Month for the missing observation
      # Get the aggregated prediction for the day from agg.model:
      ypk <- agg.model[which(rownames(agg.model) == dias[i]),]$estimate
      # Get the pattern for the specific month and weekday
      ypat <- patterns[[me]][[ds]]
      # Determine the index within the day (respective 15-minute interval)
      ypatki <- ypat[if (i %% res == 0) res else i %% res]
      # Compute the reconstructed value
      yp15 <- (ypatki / sum(ypat/4)) * ypk
      y.p[i] <- yp15
    }
  }
   # Return the reconstructed time series
  y.complete <- y.t
  na.indices <- which(is.na(y.complete))
  y.complete[na.indices] <- y.p[na.indices]
  y.complete
}

# Reconstruct using TBATS approach (Combined Method)
forecast.reconstruct <- function () {
  yp <- rep (NA, length (y.t))
  
  pos.na <- which (is.na (y.t))
  spl.na <- split (pos.na, cumsum (c (0, diff (pos.na) != 1)))
  run.na <- list (lengths = unname (sapply (spl.na, length)), values = unname (spl.na))
  
  # List of sequences of missing values (positions)
  for (i in 1:length (run.na $ values)) {
    s <- run.na $ values [[i]]
    fs <- first (s)
    n <- 0
    m <- 0
    if (fs > 3*res*7) n <- 3 else if (fs > 2*res*7) n <- 2 else if (fs > res*7) n <- 1
    if (n > 0) {
      for (j in 1:n) {
        fc.set <- y.t [(fs-j*res*7):(fs-1)]
        if (sum (is.na (fc.set)) == 0) m <- j
      }
    }
    if (m > 0) {
      fc.set <- y.t [(fs-m*res*7):(fs-1)]
      mod <- tbats (ts (fc.set), seasonal.periods = c (res, res*7))
      fcast <- forecast (mod, h = run.na $ lengths [i]) $ mean
      yp [s] <- fcast
    }
  }
  yp
}
backcast.reconstruct <- function () {
  y <- rev (y.t)
  yp <- rep (NA, length (y))
  
  pos.na <- which (is.na (y))
  spl.na <- split (pos.na, cumsum (c (0, diff (pos.na) != 1)))
  run.na <- list (lengths = unname (sapply (spl.na, length)), values = unname (spl.na))
  
  # List of sequences of missing values (positions)
  for (i in 1:length (run.na $ values)) {
    s <- run.na $ values [[i]]
    fs <- first (s)
    n <- 0
    m <- 0
    if (fs > 3*res*7) n <- 3 else if (fs > 2*res*7) n <- 2 else if (fs > res*7) n <- 1
    if (n > 0) {
      for (j in 1:n) {
        fc.set <- y [(fs-j*res*7):(fs-1)]
        if (sum (is.na (fc.set)) == 0) m <- j
      }
    }
    if (m > 0) {
      fc.set <- y [(fs-m*res*7):(fs-1)]
      mod <- tbats (ts (fc.set), seasonal.periods = c (res, res*7))
      fcast <- forecast (mod, h = run.na $ lengths [i]) $ mean
      yp [s] <- fcast
    }
  }
  rev (yp)
}
combine.pred <- function (y, fc, bc) {
  combine.vec <- function (fore, back) {
    l <- length (fore)
    vec <- rep (NA, l)
    ai <- function (i) {
      if (l == 1) 0.5
      else (l-i)/(l-1)
    }
    for (j in 1:l) {
      vec [j] <- ai(j)*fore[j]+(1-ai(j))*back[j]
    }
    vec
  }

  yp <- rep (NA, length (y))

  pos.na <- which (is.na (y))
  spl.na <- split (pos.na, cumsum (c (0, diff (pos.na) != 1)))
  run.na <- list (lengths = unname (sapply (spl.na, length)), values = unname (spl.na))

  for (i in 1:length (run.na$values)) {
    s <- run.na$values[[i]]
    if (sum (is.na (fc [s])) > 0) yp [s] <- bc [s]
    if (sum (is.na (bc [s])) > 0) yp [s] <- fc [s]
    if (sum (is.na (fc [s])) == 0 && sum (is.na (bc [s])) == 0) {
      yp [s] <- combine.vec (fc [s], bc [s])
    }
  }
  yp
}
# Final method (Combined Method):
TBATS.reconstruct <- function () {
  forecast.estimates <- forecast.reconstruct ()
  backcast.estimates <- backcast.reconstruct ()
  combination.estimates <- combine.pred (y.t,forecast.estimates, backcast.estimates)
  combination.estimates
  # Combine the original time series with reconstructed values
  y.complete <- y.t
  na.indices <- which(is.na(y.complete))
  y.complete[na.indices] <- combination.estimates[na.indices]
  y.complete
}

reset_globals <- function() {
  inter <<- NULL
  datas <<- NULL
  ats <<- NULL
  datas.g <<- NULL
  dias <<- NULL
  tempo <<- NULL
  tempo.g <<- NULL
  dias.da.semana <<- NULL
  horas <<- NULL
  meses.ano <<- NULL
  meses.do.ano <<- NULL
  anos <<- NULL
  y.t <<- NULL
  y.m <<- NULL
  daily.medians <<- NULL
  daily.means <<- NULL
  daily.agg <<- NULL
}




# Loop through each year, apply the dma and JQ.reconstruct functions, and then combine the results
JQ.function <- function () {
# Get the unique years from the Date column
unique_years <- sort(unique(format(as.Date(matrix_pronta$Date), "%Y")))

# Prepare empty vectors to store the original time series and reconstructed values
reconstructed_all <- c()
for (yr in unique_years) {
  reset_globals()

  # Subset the data for the current year based on the Date column
  df_year <- subset(matrix_pronta, format(as.Date(Date), "%Y") == yr)

  # Call dma on the subset (this will set global variables like y.t for the current year's data)
  dma(df_year)

  # Now call your reconstruction function (for example, using 'median')
  rec_values <- JQ.reconstruct()

  # Append the current year's original and reconstructed values to the overall vectors
  reconstructed_all <- c(reconstructed_all, rec_values)
 

}
 reconstructed_all
}
TBATS.function <- function () {
# Get the unique years from the Date column
unique_years <- sort(unique(format(as.Date(matrix_pronta$Date), "%Y")))

# Prepare empty vectors to store the original time series and reconstructed values
reconstructed_all <- c()
for (yr in unique_years) {
  reset_globals()

  # Subset the data for the current year based on the Date column
  df_year <- subset(matrix_pronta, format(as.Date(Date), "%Y") == yr)

  # Call dma on the subset (this will set global variables like y.t for the current year's data)
  dma(df_year)

  # Now call your reconstruction function (for example, using 'median')
  rec_values <- TBATS.reconstruct()

  # Append the current year's original and reconstructed values to the overall vectors
  reconstructed_all <- c(reconstructed_all, rec_values)
  

}

reconstructed_all
}


