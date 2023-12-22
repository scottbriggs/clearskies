
# Create master parquet file for the Moon for all time periods in the
# DE441 emphemeris

CreateMasterDE441Moon <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for the Moon for each raw file
  lapply(f, ProcessDE441Moon)
  
  # Get list of all parquet files for the Moon
  fp <- list.files(here::here("data", "processed", "moon"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "moon", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileMoon <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for the Moon')
  
  # Save aggregated data for the Moon
  arrow::write_parquet(masterFileMoon, here::here("data", "processed", 
                                    "moon", "MoonDE441.parquet"))
}