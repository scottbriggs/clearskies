# Extract data for Venus from the JPL ASCII development ephemeris (DE)
# files and store in parquet file format.

ProcessDE441Venus <- function(filename)
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
  
  # Column names for Venus, which has 10 coefficients for X, Y, and Z
  venus_col_names <- c("Julian_Day_Start", "Julian_Day_End", "INTERVAL",
                       "X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "X9",
                       "X10", "Y1", "Y2", "Y3", "Y4", "Y5", "Y6", "Y7", "Y8",
                       "Y9", "Y10", "Z1", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7",
                       "Z8", "Z9", "Z10")
  venus_data = matrix(0.0,nrow=22828,ncol=DE441NUMCOEFFVENUS*3+3)
  colnames(venus_data) <- venus_col_names
  
  # Populate the intervals for Venus
  for (i in seq(from = 1L, to = 22828L, by = DE441NUMSUBINTVENUS)){
    venus_data[i, "INTERVAL"] <- 1
    venus_data[i+1L, "INTERVAL"] <- 2
  }
  
  # Populate the Julian Days for Venus
  j <- 1L
  for (i in seq(from = 1L, to = 22828L, by = DE441NUMSUBINTVENUS)){
    venus_data[i, "Julian_Day_Start"] <- vect[j]
    venus_data[i, "Julian_Day_End"] <- vect[j+1]
    venus_data[i+1, "Julian_Day_Start"] <- vect[j]
    venus_data[i+1, "Julian_Day_End"] <- vect[j+1]
    j <- j + 1020
  }
  
  # Populate Venus subinterval 1
  k <- 168L
  for (i in seq(from = 1L, to = 22828L, by = DE441NUMSUBINTVENUS)){
    for (j in seq(from = 3L, to = 32L, by = 1L)){
      venus_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Populate Venus subinterval 2
  k <- 198L
  for (i in seq(from = 2L, to = 22828L, by = DE441NUMSUBINTVENUS)){
    for (j in seq(from = 3L, to = 32L, by = 1L)){
      venus_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Create file name to save
  fn <- stringr::str_sub(filename, 1, 9)
  fnn <- paste(sep = "", "venus_", fn, "_441", ".parquet")
  
  logger::log_info('Saving filename {fnn}')
  
  #Save Venus data
  df <- as.data.frame(venus_data)
  arrow::write_parquet(df, here::here("data", "processed", 
                         "venus", fnn))
}