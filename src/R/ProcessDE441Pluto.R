# Extract data for Pluto from the JPL ASCII development ephemeris (DE)
# files and store in parquet file format.

ProcessDE441Pluto <- function(filename)
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
  
  # Column names for Pluto, which has 6 coefficients for X, Y, and Z
  pluto_col_names <- c("Julian_Day_Start", "Julian_Day_End", "INTERVAL",
                       "X1", "X2", "X3", "X4", "X5", "X6",
                       "Y1", "Y2", "Y3", "Y4", "Y5", "Y6",
                       "Z1", "Z2", "Z3", "Z4", "Z5", "Z6")
  pluto_data = matrix(0.0,nrow=11414,ncol=DE441NUMCOEFFPLUTO*3+3)
  colnames(pluto_data) <- pluto_col_names
  
  # Populate the intervals for Pluto
  for (i in seq(from = 1L, to = 11414, by = DE441NUMSUBINTPLUTO)){
    pluto_data[i, "INTERVAL"] <- 1
  }
  
  # Populate the Julian Days for Pluto
  j <- 1L
  for (i in seq(from = 1L, to = 11414, by = DE441NUMSUBINTPLUTO)){
    pluto_data[i, "Julian_Day_Start"] <- vect[j]
    pluto_data[i, "Julian_Day_End"] <- vect[j+1]
    j <- j + 1020
  }
  
  # Populate Pluto subinterval 1
  k <- 420L
  for (i in seq(from = 1L, to = 11414, by = DE441NUMSUBINTPLUTO)){
    for (j in seq(from = 3L, to = 20L, by = 1L)){
      pluto_data[i, j+1] <- vect[j+k]
    }
    k <- k + 1020L
  }
  
  # Create file name to save
  fn <- str_sub(filename, 1, 9)
  fnn <- paste(sep = "", "pluto", fn, "_441", ".parquet")
  
  log_info('Saving filename {fnn}')
  
  #Save Pluto data
  df <- as.data.frame(pluto_data)
  write_parquet(df, here("data", "processed", 
                         "pluto", fnn))
}