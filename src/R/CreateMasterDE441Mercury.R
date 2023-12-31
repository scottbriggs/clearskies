
# Create master parquet file for Mercury for all time periods in the
# DE441 emphemeris

CreateMasterDE441Mercury <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Mercury for each raw file
  lapply(f, ProcessDE441Mercury)
  
  # Get list of all parquet files for mercury
  fp <- list.files(here::here("data", "processed", "mercury"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "mercury", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileMercury <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Mercury')
  
  # Save aggregated data for Mercury
  arrow::write_parquet(masterFileMercury, here::here("data", "processed", 
                                    "mercury", "MercuryDE441.parquet"))
}