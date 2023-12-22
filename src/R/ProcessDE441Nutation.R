# Extract data for Nutation from the JPL ASCII development ephemeris (DE)
# files and store in parquet file format.

ProcessDE441Nutation <- function(filename)
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
  
  # Column names for Nutations, which has 10 Coefficients for Longitude and Obliquity
  nutation_col_names <- c("Julian_Day_Start", "Julian_Day_End", "INTERVAL",
                          "Longitude1", "Longitude2", "Longitude3", "Longitude4",
                          "Longitude5", "Longitude6", "Longitude7", "Longitude8",
                          "Longitude9", "Longitude10", "Obliquity1", "Obliquity2",
                          "Obliquity3", "Obliquity4","Obliquity5", "Obliquity6",
                          "Obliquity7", "Obliquity8", "Obliquity9", "Obliquity10")
  nutation_data = matrix(0.0,nrow=45656,ncol=DE441NUMCOEFFNUTATION*2+3)
  colnames(nutation_data) <- nutation_col_names
  
  # Populate the intervals for the Nutations
  for (i in seq(from = 1L, to = 45656, by = DE441NUMSUBINTNUTATION)){
    nutation_data[i, "INTERVAL"] <- 1
    nutation_data[i+1, "INTERVAL"] <- 2
    nutation_data[i+2, "INTERVAL"] <- 3
    nutation_data[i+3, "INTERVAL"] <- 4
  }
  
  # Populate the Julian Days for the Nutations
  j <- 1L
  for (i in seq(from = 1L, to = 45656, by = DE441NUMSUBINTNUTATION)){
    nutation_data[i, "Julian_Day_Start"] <- vect[j]
    nutation_data[i, "Julian_Day_End"] <- vect[j+1]
    nutation_data[i+1, "Julian_Day_Start"] <- vect[j]
    nutation_data[i+1, "Julian_Day_End"] <- vect[j+1]
    nutation_data[i+2, "Julian_Day_Start"] <- vect[j]
    nutation_data[i+2, "Julian_Day_End"] <- vect[j+1]
    nutation_data[i+3, "Julian_Day_Start"] <- vect[j]
    nutation_data[i+3, "Julian_Day_End"] <- vect[j+1]
    j <- j + 1020
  }
  
  # Populate the Nutations subinterval 1
  k <- 816L
  for (i in seq(from = 1L, to = 45656, by = DE441NUMSUBINTNUTATION)){
    for (j in seq(from = 3L, to = 22L, by = 1L)){
      nutation_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Nutations subinterval 2
  k <- 836L
  for (i in seq(from = 2L, to = 45656, by = DE441NUMSUBINTNUTATION)){
    for (j in seq(from = 3L, to = 22L, by = 1L)){
      nutation_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Nutations subinterval 3
  k <- 856L
  for (i in seq(from = 3L, to = 45656, by = DE441NUMSUBINTNUTATION)){
    for (j in seq(from = 3L, to = 22L, by = 1L)){
      nutation_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate the Nutations subinterval 4
  k <- 876L
  for (i in seq(from = 4L, to = 45656, by = DE441NUMSUBINTNUTATION)){
    for (j in seq(from = 3L, to = 22L, by = 1L)){
      nutation_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Create file name to save
  fn <- str_sub(filename, 1, 9)
  fnn <- paste(sep = "", "nutation", fn, "_441", ".parquet")
  
  log_info('Saving filename {fnn}')
  
  #Save Nutation data
  df <- as.data.frame(nutation_data)
  write_parquet(df, here("data", "processed", 
                         "nutation", fnn))
}