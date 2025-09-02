# Get available data for abundance on OBIS

library(dplyr)
library(ggplot2)

get_data <- function(taxonid, datatype = "occurrence") {
    results <- switch(datatype,
                      occurrence = .get_occurrence(taxonid),
                      gridded = .get_gridded(taxonid),
                      abundance = .get_abundance(taxonid))
    return(results)
}

.get_occurrence <- function(taxonid) {
    robis::occurrence(taxonid = taxonid)
}

.get_gridded <- function(taxonid) {
    #TODO
}

.get_abundance <- function(taxonid) {
    occ_data <- robis::occurrence(taxonid = taxonid,
                                  extensions = "MeasurementOrFact")

    emof <- robis::unnest_extension(occ_data, extension = "MeasurementOrFact")

    measures <- length(unique(emof$measurementType))
    ids <- length(unique(emof$occurrenceID))
    cli::cli_alert_info("There are {.val {measures}} measurement type{?s} across {.val {ids}} record{?s}.")

    return(list(
        occurrences = occ_data[,which(colnames(occ_data) != "mof")],
        measurements = emof
    ))
}

#Engraulis encrasicolus
enen <- get_data(126426, datatype = "abundance")

unique(enen$measurements$measurementType)

all_abund <- enen$measurements |>
    filter(!is.na(measurementTypeID)) |>
    group_by(measurementType, measurementTypeID) |>
    count() |>
    mutate(mt = tolower(measurementType)) |>
    #filter(grepl("abundance", mt))
    filter(grepl("count|abundance|biomass", mt))

enen_abundance <- enen$measurements |>
    filter(measurementType == "Abundance") |>
    filter(measurementUnit == "#ind/km²") |>
    mutate(measurementValue = as.numeric(measurementValue))

enen_abundance <- enen_abundance |>
    left_join(enen$occurrences[,c("id", "decimalLongitude", "decimalLatitude")])

wrld <- rnaturalearth::ne_countries(returnclass = "sf")

ggplot() +
    geom_sf(data = wrld, fill = "grey80", color = "grey80") +
    geom_point(data = enen_abundance, aes(x = decimalLongitude, y = decimalLatitude,
                                          size = measurementValue)) +
    theme_light() +
    coord_sf(xlim = c(1, 4), ylim = c(48, 52))


abund_m <- enen$measurements |>
    #filter(!is.na(measurementTypeID)) |>
    mutate(mt = tolower(measurementType)) |>
    filter(grepl("count|abundance|biomass|number of individuals|number of specimens|density of biological|weight", mt)) |>
    left_join(enen$occurrences[,c("id", "decimalLongitude", "decimalLatitude")])

ggplot() +
    geom_sf(data = wrld, fill = "grey80", color = "grey80") +
    geom_point(data = enen$occurrences, aes(x = decimalLongitude, y = decimalLatitude)) +
    geom_point(data = abund_m, aes(x = decimalLongitude, y = decimalLatitude), color = "blue") +
    theme_light() +
    coord_sf(ylim = c(45, 35),  xlim = c(-10, 10))
ggsave("~/Downloads/enen_records.png")
abund_m <- abund_m |>
    group_by(measurementType, measurementTypeID, measurementUnit) |>
    summarise(n_records = n(), 
              range_long = paste(round(range(decimalLongitude), 2), collapse = ", "),
              range_lat = paste(round(range(decimalLatitude), 2), collapse = ", "))
View(abund_m)
write.csv(abund_m, "~/Downloads/teste_abudance.csv")

outros <- enen$measurements |>
    #filter(!is.na(measurementTypeID)) |>
    mutate(mt = tolower(measurementType)) |>
    filter(!grepl("count|abundance|biomass", mt))
unique(outros$measurementType)
