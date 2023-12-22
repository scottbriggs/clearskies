
# Create master parquet file Mars for all time periods in the
# DE441 emphemeris

CreateMasterDE441Mars <- function()
{
  # Get list of raw files to process
  f <- list.files(here("data", "raw"))
  
  # Create parquet file for Mars for each raw file
  lapply(f, ProcessDE441Mars)
  
  # Get list of all parquet files for Mars
  fp <- list.files(here("data", "processed", "mars"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- read_parquet(here("data", "processed", "mars", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileMars <- dplyr::bind_rows(df_list)
  
  log_info('Save aggregated parquet file for Mars')
  
  # Save aggregated data for Mars
  write_parquet(masterFileVenus, here("data", "processed", 
                                    "Mars", "MarsDE441.parquet"))
}