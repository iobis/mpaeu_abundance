# Process ICES data

# Load packages
library(reticulate)
library(arrow)
library(dplyr)
library(h3jsr)
# Source Python functions
source_python("Python/ices_biotic_data_processing.py")
# Create dir for processed data (optional)
fs::dir_create("data/ices-processed")

# Process
biotic_dict <- map_csv("data-raw/ICES_Acoustic")
aggregated_dict <- aggregate_ices_biotic_by_year(biotic_dict)
full_df <- merge_year_dfs(aggregated_dict)

# Add H3 index
cells <- point_to_cell(sf::st_as_sf(
    data.frame(geometry = full_df[,c("HaulCenter")]),
    wkt = "geometry", crs = "EPSG:4326"
), res = 7)
full_df$h3_7 <- cells

# Save as parquet files (optional)
full_df |>
    mutate(date_year = lubridate::year(full_df$HaulStartTime)) |>
    group_by(date_year) |>
    write_dataset("data/ices-processed")