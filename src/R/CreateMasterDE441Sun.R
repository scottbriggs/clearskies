
# Create master parquet file for the Sun for all time periods in the
# DE441 emphemeris

CreateMasterDE441Sun <- function()
{
  # Get list of raw files to process
  f <- list.files(here("data", "raw"))
  
  # Create parquet file for the Sun for each raw file
  lapply(f, ProcessDE441Sun)
  
  # Get list of all parquet files for the sun
  fp <- list.files(here("data", "processed", "sun"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- read_parquet(here("data", "processed", "sun", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileSun <- dplyr::bind_rows(df_list)
  
  log_info('Save aggregated parquet file for the Sun')
  
  # Save aggregated data for the Sun
  write_parquet(masterFileSun, here("data", "processed", 
                                    "sun", "SunDE441.parquet"))
}