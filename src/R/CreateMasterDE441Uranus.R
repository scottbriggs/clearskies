
# Create master parquet file Uranus for all time periods in the
# DE441 emphemeris

CreateMasterDE441Uranus <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Uranus for each raw file
  lapply(f, ProcessDE441Uranus)
  
  # Get list of all parquet files for Uranus
  fp <- list.files(here::here("data", "processed", "uranus"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "uranus", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileUranus <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Uranus')
  
  # Save aggregated data for Uranus
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "Uranus", "UranusDE441.parquet"))
}