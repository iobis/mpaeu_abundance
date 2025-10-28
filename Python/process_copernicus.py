# Code to process copernicus marine data:
import os
import xarray as xr
import pandas as pd
import numpy as np
from time import time

# Ensure PROJ_LIB is set for rioxarray/rasterio
try:
    import pyproj
    proj_dir = pyproj.datadir.get_data_dir()
    if proj_dir and os.path.isdir(proj_dir):
        os.environ["PROJ_LIB"] = proj_dir
    else:
        proj_dir = None
except Exception:
    proj_dir = None

if proj_dir is None:
    print("Warning: PROJ data dir not detected by pyproj. If rioxarray/rasterio falla, set PROJ_LIB to your pyproj data dir.")

import rioxarray



# Copernicus Environmental Variables Dictionary:
var_map = {
    'thetao': 'sea_temperature',
    'so': 'salinity',
    'o2': 'oxygen',
    'V': "current_speed"}

# Class with functions to process Copernicus data:
class CopernicusProcessor:
    def __init__(self, dataset: xr.Dataset, epsg: int, var_map: dict = None):
        self.dataset = dataset
        self.epsg = epsg
        self.var_map = var_map or {}

    def get_variable_data(self, variable_name: str) -> xr.DataArray:
        if variable_name in self.dataset:
            return self.dataset[variable_name]
        else:
            raise ValueError(f"Variable {variable_name} not found in dataset.")

    def get_cell_size(self, da: xr.DataArray) -> str:
        """
        Get cell size from DataArray coordinates.
        
        Args:
            da (xr.DataArray): Input DataArray
            
        Returns:
            str: Cell size formatted as '0_083' for 0.083 degrees
        """
        # Get x dimension name
        x_dim = next((n for n in ['x', 'lon', 'longitude'] if n in da.dims), None)
        if x_dim is None:
            raise ValueError("No x/longitude dimension found")
        
        # Calculate resolution from coordinates
        x_coords = da[x_dim].values
        if len(x_coords) < 2:
            raise ValueError("Need at least 2 coordinate values to calculate resolution")
        
        resolution = abs(x_coords[1] - x_coords[0])
        
        # Format as string: 0.083 -> '0_083'
        resolution_str = f"{resolution:.3f}".replace('.', '_')
        return resolution_str

    def subset_by_time(self, start_time: str, end_time: str) -> xr.Dataset:
        return self.dataset.sel(time=slice(start_time, end_time))

    def to_dataframe(self) -> pd.DataFrame:
        return self.dataset.to_dataframe().reset_index()
    
    def get_range(self, variable_name: str) -> xr.DataArray:
        """
        Calculate average absolute difference between the maximum and minimum records per year (range). Bio-Oracle logic.      
        Args:
            variable_name (str): Name of the variable in dataset
        
        Returns:
            xr.DataArray: Raster with average absolute difference (Bio-Oracle Range).
        """

        # Get the variable data
        data = self.get_variable_data(variable_name)
        
        # Convert time to datetime if not already
        data['time'] = pd.to_datetime(data.time)
        
        # Initialize list to store annual ranges
        years = data.time.dt.year.values
        unique_years = np.unique(years)
        
        # Calculate range for each year
        annual_ranges = []
        for year in unique_years:
            year_data = data.sel(time=data.time.dt.year == year)
            year_range = np.abs(year_data.max(dim='time') - year_data.min(dim='time'))
            annual_ranges.append(year_range)
        
        # Stack all yearly ranges and calculate mean
        all_ranges = xr.concat(annual_ranges, dim='year')
        mean_range = all_ranges.mean(dim='year')
        
        return mean_range
    
    def get_max(self, variable_name: str) -> xr.DataArray:

        """
        Calculates the maximum value from the netcdf (decade). Bio-Oracle logic.

        Args:
            variable_name (str): Name of the variable in dataset
        
        Returns:
            xr.DataArray: Raster with the maximum value in each cell (Bio-Oracle Maximum).
        """

        # Get the variable data:
        data = self.get_variable_data(variable_name)

        # Return the maximum value across the time dimension:
        return data.max(dim='time')
    
    def get_min(self, variable_name: str) -> xr.DataArray:

        """
        Calculates the minimum value from the netcdf (decade). Bio-Oracle logic.

        Args:
            variable_name (str): Name of the variable in dataset
        
        Returns:
            xr.DataArray: Raster with the minimum value in each cell (Bio-Oracle Maximum).
        """

        # Get the variable data:
        data = self.get_variable_data(variable_name)

        # Return the minimum value across the time dimension:
        return data.min(dim='time')
    
    def dataarray_to_geotiff(self, da: xr.DataArray, variable_key: str, out_dir: str,
                                    nodata: float = None, dtype: str = None,
                                    compress: str = "DEFLATE") -> str:
        """
        Writes and xr.DataArray 2D to GeoTIFF.

        Args:
            da (xr.DataArray): DataArray to write. 
            out_path (str): Output file path.
            nodata (float, optional): NoData value to set in the GeoTIFF.
            dtype (str, optional): Data type for the output GeoTIFF.
            compress (str, optional): Compression method for the GeoTIFF. Defaults to "DEFLATE".

        Returns:
            GeoTIFF file in the out_path.
        """

        mapped = self.var_map.get(variable_key, variable_key)

        # Ensure variable name:
        try:
            da = da.copy()
            da.name = mapped
        except Exception:
            pass

        # Colapse time dimension if exists
        if 'time' in da.dims:
            da = da.squeeze('time', drop=True)

        # Detect spatial dimensions
        x_dim = next((n for n in ['x', 'lon', 'longitude'] if n in da.dims), None)
        y_dim = next((n for n in ['y', 'lat', 'latitude'] if n in da.dims), None)
        if x_dim is None or y_dim is None:
            raise ValueError("No se encontraron dimensiones espaciales ('lon'/'lat' o 'x'/'y').")

        # Rename to x/y if needed:
        if (x_dim, y_dim) != ('x', 'y'):
            da = da.rename({x_dim: 'x', y_dim: 'y'})

        # Ensure rioxarray 'knows' spatial dimensions
        da = da.rio.set_spatial_dims(x_dim='x', y_dim='y', inplace=False)

        # If it has no CRS write it from self.epsg
        try:
            has_crs = da.rio.crs is not None
        except Exception:
            has_crs = False

        if not has_crs:
            da = da.rio.write_crs(f"EPSG:{self.epsg}", inplace=False)

        # Prepare kargs and write the GeoTIFF
        write_kwargs = {"driver": "GTiff", "compress": compress}
        if dtype is not None:
            write_kwargs["dtype"] = dtype
        if nodata is not None:
            write_kwargs["nodata"] = nodata

        # Map output path:
        out_path = os.path.join(out_dir, f"{mapped}.tif")
        da.rio.to_raster(out_path, **write_kwargs)
        return out_path
    
    def speed_from_components(self, u_component: str, v_component: str) -> xr.DataArray:
        """
        Calculate speed from u and v components.

        Args:
            u_component (str): Name of the u component variable in dataset.
            v_component (str): Name of the v component variable in dataset.

        Returns: 
            xr.DataArray: DataArray with calculated speed.
        """

        # Get components and compute product speed:
        self_u = self.get_variable_data(u_component)
        self_v = self.get_variable_data(v_component) 
        speed = xr.ufuncs.sqrt(self_u**2 + self_v**2)  
        
        # Give a name to the product:
        speed.name = 'V'

        # Add to dataset:
        self.dataset['V'] = speed

        return self.dataset

# Function to get varget predictor variables from Copernicus NetCDFs:
def batch_process_netcdfs(input_root: str, output_root: str, epsg: int, 
                         processing_config: dict, var_map: dict = None):
    """
    Process multiple NetCDF files across folders and generate GeoTIFFs.
    Gets max, min and or range (Bio.Oracle logic) for specified variables from the NetCDFs.
    Args:
        input_root (str): Root folder containing subfolders with NetCDFs
        output_root (str): Root folder where output folders/files will be created
        epsg (int): EPSG code for output files
        processing_config (dict): Dictionary defining which variables to process and how
            Example:
            {
                'thetao': ['max', 'range'],  # Apply get_max and get_range
                'so': ['max'],               # Apply only get_max
                'o2': ['min', 'range'],      # Apply get_min and get_range
                'current': {                 # Special case for current speed
                    'components': ['uo', 'vo'],
                    'operations': ['max']
                }
            }
        var_map (dict, optional): Variable name mapping for output files

    Returns:
        GeoTIFF files by folder, in the output_root. 
    """
    
    # Create output root if it doesn't exist
    os.makedirs(output_root, exist_ok=True)
    
    # Walk through input folders
    for folder_path, _, files in os.walk(input_root):
        # Skip root folder itself
        if folder_path == input_root:
            continue
            
        # Get relative path to create matching output folder
        rel_path = os.path.relpath(folder_path, input_root)
        output_folder = os.path.join(output_root, rel_path)
        os.makedirs(output_folder, exist_ok=True)
        
        print(f"\nProcessing folder: {rel_path}")
        
        # Collect all NetCDF files in current folder
        nc_files = [f for f in files if f.endswith('.nc')]
        if not nc_files:
            print(f"No NetCDF files found in {folder_path}")
            continue
            
        # Load all NetCDFs in folder
        datasets = []
        for nc_file in nc_files:
            try:
                ds = xr.open_dataset(os.path.join(folder_path, nc_file))
                datasets.append(ds)
            except Exception as e:
                print(f"Error loading {nc_file}: {e}")
                continue
        
        # Merge datasets if more than one
        if len(datasets) > 1:
            try:
                ds = xr.merge(datasets)
            except Exception as e:
                print(f"Error merging datasets: {e}")
                continue
        else:
            ds = datasets[0]
            
        # Create processor instance
        processor = CopernicusProcessor(ds, epsg, var_map)
        
        # Process each variable according to config
        for var_name, operations in processing_config.items():
            if var_name == 'V':
                # Special handling for current speed
                try:
                    u, v = operations['components']
                    ds = processor.speed_from_components(u, v)
                    var_name = 'V'  # Reset var_name to process current speed
                except Exception as e:
                    print(f"Error calculating current speed: {e}")
                    continue
            
            # Apply requested operations
            ops = operations['operations'] if isinstance(operations, dict) else operations
            for op in ops:
                try:
                    if op == 'max':
                        result = processor.get_max(var_name)
                    elif op == 'min':
                        result = processor.get_min(var_name)
                    elif op == 'range':
                        result = processor.get_range(var_name)
                    else:
                        print(f"Unknown operation: {op}")
                        continue
                        
                    # Save result
                    outname = f"{var_name}_{op}"

                    # Get cellsize for naming:
                    cellsize_str = processor.get_cell_size(result)

                    processor.dataarray_to_geotiff(
                        da=result,
                        variable_key=f"{var_name}_{op}_{cellsize_str}_{epsg}",
                        out_dir=output_folder,
                        nodata=-9999,
                        dtype='float32'
                    )
                    print(f"Saved {outname}")
                    
                except Exception as e:
                    print(f"Error processing {var_name} with {op}: {e}")
                    continue
        
        # Close datasets
        for ds in datasets:
            ds.close()


# Example usage:
if __name__ == "__main__":
    input_root = r"C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Raw\Daily"
    output_root = r"C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\predictors_0_083_deg"
    
    # Define processing configuration
    processing_config = {
        'thetao': ['max', 'range'],
        'so': ['min'],
        'o2': ['min'],
        'V': {
            'components': ['uo', 'vo'],
            'operations': ['max']
        }
    }
    
    # Process all folders
    batch_process_netcdfs(
        input_root=input_root,
        output_root=output_root,
        epsg=4326,
        processing_config=processing_config,
        var_map=var_map
    )