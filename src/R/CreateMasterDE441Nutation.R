
# Create master parquet file Nutation for all time periods in the
# DE441 emphemeris

CreateMasterDE441Nutation <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Nutation for each raw file
  lapply(f, ProcessDE441Nutation)
  
  # Get list of all parquet files for Nutation
  fp <- list.files(here::here("data", "processed", "nutation"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "nutation", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileNutation <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Nutation')
  
  # Save aggregated data for Nutation
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "Nutation", "NutationDE441.parquet"))
}