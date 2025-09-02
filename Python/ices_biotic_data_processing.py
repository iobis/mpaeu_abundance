# Code to preprocess ices biotic data from https://acoustic.ices.dk/submissions.
from pathlib import Path
from typing import Dict, List, Union
import pandas as pd
from pyproj import Geod

def map_csv(
        folder_path: Union[str, Path],
        absolute_paths: bool = True
) -> Dict[int, List[str]]:
    """
     Iterates through a folder structure with the following shape:
        folder_path/
            2020/
                .../file.csv
            2021/
                subfolder1/...
                subfolder2/...
            2022/
                ...

    and returns { year(int): [paths(str) to files .csv found under that year] }.

    Arguments:
        folder_path: path to the parent folder where all .csv are.
        absolute_paths (optional): returns absolute paths (True) or relatives to folder_path (False). Defuault: True

    Returns:
        Dict[int, List[str]]
    """

    # Parent folder 
    base = Path(folder_path)
    if not base.exists() or not base.is_dir():
        raise NotADirectoryError(f"{base} does not exist or is not a directory")

    result: Dict[int, List[str]] = {}

    for entry in base.iterdir():
        if not entry.is_dir():
            continue
        nombre = entry.name
        if len(nombre) == 4 and nombre.isdigit():  # detect folders with year names e.g. "2021"
            year = int(nombre)

            # Iterate and filter with .csv  extension 
            csv_files = []
            for f in entry.rglob("*"):
                if f.is_file() and f.suffix.lower() == ".csv":  #case-insensitive
                    ruta = f.resolve() if absolute_paths else f.relative_to(base)
                    csv_files.append(str(ruta))

            # Orden estable para reproducibilidad
            csv_files.sort()

            if csv_files:
                result[year] = csv_files

    # Ordenar por año ascendente antes de devolver
    return dict(sorted(result.items()))

# Function to preprocess single ICES acoustic (biotic) csv and keep the needed info:
def preprocess_ices_biotic_csv (
        csv: str
) -> pd.DataFrame:
    """
    Function that picks an ICES acoustic (biotic) .csv and processes and cleans to return a Pandas Dataframe with the following fields:

    Fields:
        HaulNumber: number of the haul
        HaulStationName: name of the haul station
        HaulStartTime: time of the haul
        Distance: distance (in meters) covered by the haul
        CatchSpeciesCode: WoRMS AphialID
        CatchWeightUnit: unit of CatchSpeciesCategoryWeight
        CatchSpeciesCategoryWeight: catched total weight
        HaulCenter: WKT with haul center latitude and longitude (middle of haul start and stop)

    """
    # Read the .csv skipping the first two rows
    df = pd.read_csv(csv, skiprows=2)

    # Separate Haul info and Catch info:
        # Haul:
    df_haul = df.loc[df["Haul"].astype(str).str.strip().eq("Haul")].reset_index(drop=True)  
        # Catch: 
    catch_blk = df.loc[df["Haul"].astype(str).str.strip().eq("Catch")].reset_index(drop=True)   # Isolate catch block
    header = catch_blk.iloc[0, 2:].astype(str).str.strip()
    df_catch = catch_blk.iloc[1:, 2:].copy()
    df_catch.columns = header.values
    df_catch = df_catch.reset_index(drop=True)


    # Haul Lat-Long column names:
    lat_long_cols = ["HaulStartLatitude","HaulStartLongitude","HaulStopLatitude","HaulStopLongitude"]

    # Secure that lat-long columns are numeric:
    for c in lat_long_cols:
        df_haul[c] = pd.to_numeric(df_haul[c], errors="coerce")

    # Compute geodesic distance between HaulStart and End points (straight line):
    geod = Geod(ellps="WGS84")
    _, _, dist = geod.inv(
        df_haul["HaulStartLongitude"].to_numpy(),
        df_haul["HaulStartLatitude"].to_numpy(),
        df_haul["HaulStopLongitude"].to_numpy(),
        df_haul["HaulStopLatitude"].to_numpy(),
    )

    # Add a new column 'Distance' to the dataframe (in meters):
    df_haul["Distance"] = dist

    # Secure column type:
    df_haul["HaulNumber"]  = pd.to_numeric(df_haul["HaulNumber"], errors="coerce").astype("Int64")
    df_catch["HaulNumber"] = pd.to_numeric(df_catch["HaulNumber"], errors="coerce").astype("Int64")

    # Columns to join:
    cols_catch = ["HaulNumber", "CatchSpeciesCode", "CatchWeightUnit", "CatchSpeciesCategoryWeight"]

    # Merge between df_haul and df_catch (relation one to many) to pass the catch data into the haul data.
    df_complete = df_haul.merge(df_catch[cols_catch], on="HaulNumber", how="left")

    # Add haul center info:
    df_complete["HaulCenterLongitude"] = (df_complete["HaulStartLongitude"] + df_complete["HaulStopLongitude"]) / 2
    df_complete["HaulCenterLatitude"]  = (df_complete["HaulStartLatitude"]  + df_complete["HaulStopLatitude"])  / 2
    df_complete["HaulCenter"] = "POINT (" + df_complete["HaulCenterLongitude"].astype(str) + " " + df_complete["HaulCenterLatitude"].astype(str) + ")"
    df_complete["HaulStart"] = "POINT (" + df_complete["HaulStartLongitude"].astype(str) + " " + df_complete["HaulStartLatitude"].astype(str) + ")"
    df_complete["HaulStop"] = "POINT (" + df_complete["HaulStopLongitude"].astype(str) + " " + df_complete["HaulStopLatitude"].astype(str) + ")"

    # Key to unify dataset (each specie in each haul)
    keys = ["HaulNumber","CatchSpeciesCode"]

    # Columns to keep:
    keep_cols = ["HaulNumber", "HaulStationName", "HaulStartTime", "Distance", "CatchSpeciesCode", "CatchWeightUnit", "CatchSpeciesCategoryWeight", "HaulStart", "HaulStop", "HaulCenter"]

    # Filter and drop duplicates
    df_per_species = (
        df_complete.loc[df_complete["CatchSpeciesCode"].notna(), keep_cols]
        .drop_duplicates(subset=keys, keep="first")
        .reset_index(drop=True)
    )

    # Delete rows with Distance = 0 as they are wrong:
    df_per_species = df_per_species[df_per_species["Distance"].ne(0)].reset_index(drop=True)

    return df_per_species

# Function to generate a single-full dataset with all the csv files of each year in the parent folder:
def aggregate_ices_biotic_by_year(
    mapeo: Dict[int, List[str]]
) -> Dict[int, pd.DataFrame]:
    """
    Executes the function preprocess_ices_biotic_csv for each csv in each year based on the dictionary files and merges by year.
    Returns a dictionary wich {year: pd.Dataframe}

    Params:
        mapeo: {año: [rutas_csv, ...]}

    Fields:
        HaulStationName: haul station name.
        HaulStartTime: time of the haul
        Distance: distance (in meters) covered by the haul
        CatchSpeciesCode: WoRMS AphialID
        CatchWeightUnit: unit of CatchSpeciesCategoryWeight
        CatchSpeciesCategoryWeight: catched total weight
        HaulCenter: WKT with haul center latitude and longitude (middle of haul start and stop)
        Abundance: total weight per meter haul

    Then, to get the pd.Dataframe of a year you can use: aggregated_dict = aggregate_ices_biotic_by_year(biotic_dict); df_2020 = aggregated_dict.get(2020) 
    """
    drop_duplicates = True

    keys = ["HaulStationName", "CatchSpeciesCode"]

    result: Dict[int, pd.DataFrame] = {}

    for year in sorted(mapeo.keys()):
        frames = []
        for path in mapeo[year]:
            try:
                df_clean = preprocess_ices_biotic_csv(path)
                frames.append(df_clean)
            except Exception:
                continue

        if frames:
            df_year = pd.concat(frames, ignore_index=True)
            if drop_duplicates and keys:
                existing = [k for k in keys if k in df_year.columns]
                if existing:
                    df_year = df_year.drop_duplicates(subset=existing, keep="first", ignore_index=True)
                    # Secure numeric values
                    df_year["CatchSpeciesCategoryWeight"] = pd.to_numeric(df_year["CatchSpeciesCategoryWeight"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
                    df_year["Distance"] = pd.to_numeric(df_year["Distance"], errors="coerce")
                    df_year["Abundance"] = df_year["CatchSpeciesCategoryWeight"]/df_year["Distance"]
                    df_year.drop(columns="HaulNumber", inplace=True)
            result[year] = df_year


    return result


# Function to merge pd.Datafraes from the entire dictionary to obtain a single Dataframe:
def merge_year_dfs(dfs_by_year: Dict[int, pd.DataFrame]) -> pd.DataFrame:
    """
    Concats all the pd.Dataframes that are in the dictionary retrieved by the function aggregate_ices_biotic_by_year() and returns a complete pd.Dataframe.
    """
    frames = []
    for year, df in dfs_by_year.items():
        if df is None or df.empty:
            continue
        if "Year" not in df.columns:
            df = df.copy()
        frames.append(df)
    return pd.concat(frames, ignore_index=True)

# Use in Python - commenting to be able to import the function in R
#biotic_dict = map_csv(r"C:\Users\beñat.egidazu\Desktop\Tests\ICES_Acoustic")
#aggregated_dict = aggregate_ices_biotic_by_year(biotic_dict)
#full_df = merge_year_dfs(aggregated_dict)
#full_df.to_csv(r"C:\Users\beñat.egidazu\Desktop\Tests\ICES_Acoustic\full.csv")





