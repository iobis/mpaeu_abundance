# Code to preprocess ices biotic data from https://acoustic.ices.dk/submissions.
import os
from pathlib import Path
from typing import Dict, List, Union

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

