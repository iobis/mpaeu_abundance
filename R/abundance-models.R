library(arrow)
library(dplyr)
library(ggplot2)
library(terra)

# DATA EXPLORATION ------
ices_data <- open_dataset("data/ices-processed")

enen_aggregated <- ices_data |>
    filter(CatchSpeciesCode == 126426) |>
    group_by(h3_7) |>
    summarise(weight = median(CatchSpeciesCategoryWeight),
              abundance = median(Abundance)) |>
    collect()

# Plot to check
sf::sf_use_s2(FALSE)
enen_points <- h3jsr::cell_to_point(enen_aggregated$h3_7, simple = FALSE)
enen_points <- enen_points |>
    rename(h3_7 = h3_address) |>
    left_join(enen_aggregated)
wrld <- rnaturalearth::ne_countries(returnclass = "sf")
# wrld <- rnaturalearth::ne_countries(returnclass = "sf") |>
#     sf::st_crop(enen_points)
ggplot() +
    geom_sf(data = wrld, fill = "grey80", color = "grey80") +
    geom_sf(data = enen_points, aes(size = abundance)) +
    theme_light() +
    coord_sf(xlim = c(-9, 0), ylim = c(42, 50))

# Plot by year
enen_aggregated_year <- ices_data |>
    filter(CatchSpeciesCode == 126426) |>
    group_by(h3_7, date_year) |>
    summarise(weight = median(CatchSpeciesCategoryWeight),
              abundance = median(Abundance)) |>
    collect()

enen_points_year <- h3jsr::cell_to_point(enen_aggregated_year$h3_7, simple = FALSE)
enen_points_year <- bind_cols(enen_points_year, enen_aggregated_year)
ggplot() +
    geom_sf(data = wrld, fill = "grey80", color = "grey80") +
    geom_sf(data = enen_points_year, aes(size = abundance, color = as.factor(date_year))) +
    theme_light() +
    coord_sf(xlim = c(-9, 0), ylim = c(42, 50)) +
    facet_wrap(~date_year)

# Attach environmental data
env_layers <- rast(list.files(
    "data-raw/env", full.names = T
))
plot(env_layers)

env_layers <- scale(env_layers)
enen_data <- extract(env_layers, enen_points, ID = FALSE)
enen_data <- enen_data[,-length(enen_data)]
#enen_data_st <- scale(enen_data)

enen_pt_env <- bind_cols(enen_points, enen_data_st)

enen_pt_env <- enen_pt_env[which(
    enen_pt_env$abundance < quantile(enen_pt_env$abundance, .99)
),]

par(mfrow = c(3,3))
for (i in colnames(enen_data)) {
    plot(enen_pt_env[["abundance"]] ~ enen_pt_env[[i]],
          ylab = "Abundance", xlab = i, main = i, pch = 20, col = "blue")
}
par(mfrow = c(3,3))
for (i in colnames(enen_data)) {
    plot(density(enen_pt_env[[i]]),
          ylab = "Abundance", xlab = i, main = i, pch = 20, col = "blue")
}
par(mfrow = c(1,1))

# MODELLING --------

# Poisson GLM
m1 <- glm(as.integer(abundance) ~ thetao_mean + I(thetao_mean^2) + bathymetry_mean + so_min + o2_min,
          family = poisson, data = enen_pt_env)
summary(m1)

get_part_curves <- function(model) {
    coef <- names(m1$coefficients)
    coef <- coef[!grepl("\\(", coef)]
    m <- data.frame(matrix(ncol = length(coef), nrow = 100))
    dat <- m1$data |>
        sf::st_drop_geometry() |>
        select(all_of(coef))
    means <- rep(unname(apply(dat, 2, mean)), each = 100)    
    m[,] <- means
    names(m) <- coef
    par(mfrow = c(ceiling(length(coef)/2), ceiling(length(coef)/2)))
    for (i in seq_along(coef)) {
        m[[coef[i]]] <- seq(min(dat[[coef[i]]]), max(dat[[coef[i]]]), length.out = 100)
        p <- predict(model, m, type = "response")
        plot(p ~ m[[coef[i]]], type = "l", xlab = coef[i], ylab = "Abudance")
    }
    par(mfrow = c(1,1))
    return(invisible())
}

get_part_curves(m1)

env_pred <- crop(env_layers, enen_points)
pred_m1 <- predict(subset(
    env_pred,
    c("thetao_mean", "bathymetry_mean", "so_min", "o2_min")
), m1, type = "response")
plot(pred_m1)
points(enen_pt_env, cex = enen_pt_env$abundance/100)
