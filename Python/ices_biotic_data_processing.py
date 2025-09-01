# Code to preprocess ices biotic data from https://acoustic.ices.dk/submissions.
import os
from pathlib import Path
from typing import Dict, List, Union
import geopandas as gpd
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

biotic_dict = map_csv(r"C:\Users\beñat.egidazu\Desktop\Tests\ICES_Acoustic")

# Read the .csv skipping the first two rows
df = pd.read_csv(biotic_dict[2020][0], skiprows=2)

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

df_complete.to_csv(r"C:\Users\beñat.egidazu\Desktop\Tests\ICES_Acoustic\merged_dataset.csv")

