
# Create master parquet file Neptune for all time periods in the
# DE441 emphemeris

CreateMasterDE441Neptune <- function()
{
  # Get list of raw files to process
  f <- list.files(here("data", "raw"))
  
  # Create parquet file for Neptune for each raw file
  lapply(f, ProcessDE441Neptune)
  
  # Get list of all parquet files for Neptune
  fp <- list.files(here("data", "processed", "neptune"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- read_parquet(here("data", "processed", "neptune", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileNeptune <- dplyr::bind_rows(df_list)
  
  log_info('Save aggregated parquet file for Neptune')
  
  # Save aggregated data for Uranus
  write_parquet(masterFileVenus, here("data", "processed", 
                                    "Neptune", "NeptuneDE441.parquet"))
}