# Extract data for Mercury from the JPL ASCII development ephemeris (DE)
# files and store in parquet file format.

ProcessDE441Mercury <- function(filename)
{
  log_info('Reading filename {filename}')
  
  ascii_data <- readLines(here("data", "raw", filename))
  
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
  
  # Column names for Mercury, which has 4 intervals and 14 coefficients for X, Y, and Z
  mercury_col_names <- c("Julian_Day_Start", "Julian_Day_End", "INTERVAL",
                         "X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "X9",
                         "X10", "X11", "X12", "X13", "X14", "Y1", "Y2", "Y3", "Y4",
                         "Y5", "Y6", "Y7", "Y8", "Y9", "Y10", "Y11", "Y12", "Y13",
                         "Y14", "Z1", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7", "Z8",
                         "Z9", "Z10", "Z11", "Z12", "Z13", "Z14")
  mercury_data = matrix(0.0,nrow=45656,ncol=DE441NUMCOEFFMERCURY*3+3)
  colnames(mercury_data) <- mercury_col_names
  
  # Populate the intervals for Mercury
  for (i in seq(from = 1L, to = 45656L, by = DE441NUMSUBINTMERCURY)){
    mercury_data[i, "INTERVAL"] <- 1
    mercury_data[i+1L, "INTERVAL"] <- 2
    mercury_data[i+2L, "INTERVAL"] <- 3
    mercury_data[i+3L, "INTERVAL"] <- 4
  }
  
  # Populate the Julian Days for Mercury
  j <- 1L
  for (i in seq(from = 1L, to = 45656L, by = DE441NUMSUBINTMERCURY)){
    mercury_data[i, "Julian_Day_Start"] <- vect[j]
    mercury_data[i, "Julian_Day_End"] <- vect[j+1]
    mercury_data[i+1, "Julian_Day_Start"] <- vect[j]
    mercury_data[i+1, "Julian_Day_End"] <- vect[j+1]
    mercury_data[i+2, "Julian_Day_Start"] <- vect[j]
    mercury_data[i+2, "Julian_Day_End"] <- vect[j+1]
    mercury_data[i+3, "Julian_Day_Start"] <- vect[j]
    mercury_data[i+3, "Julian_Day_End"] <- vect[j+1]
    j <- j + 1020
  }
  
  # Populate Mercury subinterval 1
  k <- 0L
  for (i in seq(from = 1L, to = 45656L, by = DE441NUMSUBINTMERCURY)){
    for (j in seq(from = 3L, to = 44L, by = 1L)){
      mercury_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate Mercury subinterval 2
  k <- 42L
  for (i in seq(from = 2L, to = 45656L, by = DE441NUMSUBINTMERCURY)){
    for (j in seq(from = 3L, to = 44L, by = 1L)){
      mercury_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate Mercury subinterval 3
  k <- 84L
  for (i in seq(from = 3L, to = 45656L, by = DE441NUMSUBINTMERCURY)){
    for (j in seq(from = 3L, to = 44L, by = 1L)){
      mercury_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate Mercury subinterval 4
  k <- 126L
  for (i in seq(from = 4L, to = 45656L, by = DE441NUMSUBINTMERCURY)){
    for (j in seq(from = 3L, to = 44L, by = 1L)){
      mercury_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Create file name to save
  fn <- str_sub(filename, 1, 9)
  fnn <- paste(sep = "", "mercury_", fn, "_441", ".parquet")
  
  log_info('Saving filename {fnn}')
  
  #Save Mercury data
  df <- as.data.frame(mercury_data)
  write_parquet(df, here("data", "processed", 
                         "mercury", fnn))
}