
# Create master parquet file for EMB for all time periods in the
# DE441 emphemeris

CreateMasterDE441EMB <- function()
{
  # Get list of raw files to process
  f <- list.files(here("data", "raw"))
  
  # Create parquet file for EMB for each raw file
  lapply(f, ProcessDE441EMB)
  
  # Get list of all parquet files for EMB
  fp <- list.files(here("data", "processed", "emb"))
  
  # Create data frames for each parquet file
  numFiles <- length(fp)
  df_list <- vector(mode = "list", numFiles)
  for (i in 1:numFiles) {
    df_list[[i]] <- read_parquet(here("data", "processed", "emb", fp[[i]]))
  }
  
  # Combine data frames into a single data frame
  masterFileEMB <- dplyr::bind_rows(df_list)
  
  log_info('Save aggregated parquet file for EMB')
  
  # Save aggregated data for EMB
  write_parquet(masterFileVenus, here("data", "processed", 
                                    "EMB", "EMBDE441.parquet"))
}