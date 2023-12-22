# Extract data for the Moon from the JPL ASCII development ephemeris (DE)
# files and store in parquet file format.

ProcessDE441Moon <- function(filename)
{
  logger::log_info('Reading filename {filename}')
  
  ascii_data <- readLines(here::here("data", "raw", filename))
  
  # Create vector to store the ascii data sequentially
  vect <- rep(0, 11643300)
  
  # k indexes the result vector - 3892517 rows
  # i indexes the data blocks in the file - 11415 data blocks
  # j indexes each data element inside each data block - 341 data elements in each block
  # 3892517 = (11415 * 341) + 2
  k <- as.integer(1)
  for (i in seq(from = 2, to = 3892517, by = 341)){
    for (j in seq(from = 0, to = 339, by = 1)){
      
      # First data element in the row
      tmpstr1 <- as.character(substr(ascii_data[j+i], 4, 26))
      tmpstr1 <- chartr(old = "D", new = "E", tmpstr1)
      tmpstr1 <- as.numeric(tmpstr1)
      vect[k] <- tmpstr1
      k <- k + 1L
      
      # Second data element in the row
      tmpstr1 <- as.character(substr(ascii_data[j+i], 30, 52))
      tmpstr1 <- chartr(old = "D", new = "E", tmpstr1)
      tmpstr1 <- as.numeric(tmpstr1)
      vect[k] <- tmpstr1
      k <- k + 1L
      
      # Third data element in the row
      tmpstr1 <- as.character(substr(ascii_data[j+i], 56, 78))
      tmpstr1 <- chartr(old = "D", new = "E", tmpstr1)
      tmpstr1 <- as.numeric(tmpstr1)
      vect[k] <- tmpstr1
      k <- k + 1L
    }
  }
  
  # Column names for Moon, which has 13 coefficients for X, Y, and Z
  moon_col_names <- c("Julian_Day_Start", "Julian_Day_End", "INTERVAL",
                      "X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "X9",
                      "X10", "X11", "X12", "X13", "Y1", "Y2", "Y3", "Y4",
                      "Y5", "Y6", "Y7", "Y8", "Y9", "Y10", "Y11", "Y12", "Y13",
                      "Z1", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7", "Z8",
                      "Z9", "Z10", "Z11", "Z12", "Z13")
  moon_data = matrix(0.0,nrow=91312, DE441NUMCOEFFMOON*3+3)
  colnames(moon_data) <- moon_col_names
  
  # Populate the intervals for the Moon
  for (i in seq(from = 1L, to = 91312, by = DE441NUMSUBINTMOON)){
    moon_data[i, "INTERVAL"] <- 1
    moon_data[i+1, "INTERVAL"] <- 2
    moon_data[i+2, "INTERVAL"] <- 3
    moon_data[i+3, "INTERVAL"] <- 4
    moon_data[i+4, "INTERVAL"] <- 5
    moon_data[i+5, "INTERVAL"] <- 6
    moon_data[i+6, "INTERVAL"] <- 7
    moon_data[i+7, "INTERVAL"] <- 8
  }
  
  # Populate the Julian Days for the Moon
  j <- 1L
  for (i in seq(from = 1L, to = 91312, by = DE441NUMSUBINTMOON)){
    moon_data[i, "Julian_Day_Start"] <- vect[j]
    moon_data[i, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+1, "Julian_Day_Start"] <- vect[j]
    moon_data[i+1, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+2, "Julian_Day_Start"] <- vect[j]
    moon_data[i+2, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+3, "Julian_Day_Start"] <- vect[j]
    moon_data[i+3, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+4, "Julian_Day_Start"] <- vect[j]
    moon_data[i+4, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+5, "Julian_Day_Start"] <- vect[j]
    moon_data[i+5, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+6, "Julian_Day_Start"] <- vect[j]
    moon_data[i+6, "Julian_Day_End"] <- vect[j+1]
    moon_data[i+7, "Julian_Day_Start"] <- vect[j]
    moon_data[i+7, "Julian_Day_End"] <- vect[j+1]
    j <- j + 1020
  }
  
  # Populate the Moon subinterval 1
  k <- 438L
  for (i in seq(from = 1L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 2
  k <- 477L
  for (i in seq(from = 2L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 3
  k <- 516L
  for (i in seq(from = 3L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 4
  k <- 555L
  for (i in seq(from = 4L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 5
  k <- 594L
  for (i in seq(from = 5L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 6
  k <- 633L
  for (i in seq(from = 6L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 7
  k <- 672L
  for (i in seq(from = 7L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Moon subinterval 8
  k <- 711L
  for (i in seq(from = 8L, to = 91312L, by = DE441NUMSUBINTMOON)){
    for (j in seq(from = 3L, to = 41L, by = 1L)){
      moon_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Create file name to save
  fn <- stringr::str_sub(filename, 1, 9)
  fnn <- paste(sep = "", "moon_", fn, "_441", ".parquet")
  
  logger::log_info('Saving filename {fnn}')
  
  #Save Moon data
  df <- as.data.frame(moon_data)
  arrow::write_parquet(df, here::here("data", "processed", 
                         "moon", fnn))
}