
# Create master parquet file for Venus for all time periods in the
# DE441 emphemeris

CreateMasterDE441Venus <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Venus for each raw file
  lapply(f, ProcessDE441Venus)
  
  # Get list of all parquet files for Venus
  fp <- list.files(here::here("data", "processed", "venus"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow::read_parquet(
      here::here("data", "processed", "venus", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileVenus <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Venus')
  
  # Save aggregated data for Venus
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "venus", "VenusDE441.parquet"))
}