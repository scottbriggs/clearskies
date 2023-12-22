
# Create master parquet file Pluto for all time periods in the
# DE441 emphemeris

CreateMasterDE441Pluto <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Pluto for each raw file
  lapply(f, ProcessDE441Pluto)
  
  # Get list of all parquet files for Pluto
  fp <- list.files(here::here("data", "processed", "pluto"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "pluto", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFilePluto <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Pluto')
  
  # Save aggregated data for Pluto
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "Pluto", "PlutoDE441.parquet"))
}