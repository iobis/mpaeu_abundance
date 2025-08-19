get_data <- function(taxonid, datatype = "occurrence") {
    results <- lapply(datatype, \(dt) {
        switch(dt,
               occurrence = .get_occurrence(taxonid),
               gridded = .get_gridded(taxonid),
               abundance = .get_abundance(taxonid))
    })
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

    return(measures)
}

#126426