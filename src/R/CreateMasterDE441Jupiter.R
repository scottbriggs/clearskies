
# Create master parquet file Jupiter for all time periods in the
# DE441 emphemeris

CreateMasterDE441Jupiter <- function()
{
  # Get list of raw files to process
  f <- list.files(here::here("data", "raw"))
  
  # Create parquet file for Jupiter for each raw file
  lapply(f, ProcessDE441Jupiter)
  
  # Get list of all parquet files for Jupiter
  fp <- list.files(here::here("data", "processed", "jupiter"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- arrow:read_parquet(
      here::here("data", "processed", "jupiter", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileJupiter <- dplyr::bind_rows(df_list)
  
  logger::log_info('Save aggregated parquet file for Jupiter')
  
  # Save aggregated data for Jupiter
  arrow::write_parquet(masterFileVenus, here::here("data", "processed", 
                                    "Jupiter", "JupiterDE441.parquet"))
}