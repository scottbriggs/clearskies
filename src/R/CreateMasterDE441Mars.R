
# Create master parquet file Mars for all time periods in the
# DE441 emphemeris

CreateMasterDE441Mars <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Mars for each raw file
  lapply(f, ProcessDE441Mars)
  
  # Get list of all parquet files for Mars
  fp <- list.files(here::here("data", "processed", "mars"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "mars", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileMars <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Mars')
  
  # Save aggregated data for Mars
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "Mars", "MarsDE441.parquet"))
}