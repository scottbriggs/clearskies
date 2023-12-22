
# Create master parquet file Saturn for all time periods in the
# DE441 emphemeris

CreateMasterDE441Saturn <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Saturn for each raw file
  lapply(f, ProcessDE441Saturn)
  
  # Get list of all parquet files for Saturn
  fp <- list.files(here::here("data", "processed", "saturn"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "saturn", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileSaturn <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Saturn')
  
  # Save aggregated data for Saturn
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "Saturn", "SaturnDE441.parquet"))
}