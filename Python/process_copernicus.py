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

# Copernicus Environmental Variables Dictionary:
var_map = {
    'thetao': 'sea_temperature',
    'so': 'salinity',
    'o2': 'oxygen',
    'V': "current_speed"}




# ---------------------------- TESTING -----------------------------
# Inputs:
input_file = r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Raw\Daily\1993_2000\Physical_1993_1_1_to_1999_12_31.nc'
env_epsg = 4326  # WGS84

# Load dataset
ds = xr.open_dataset(input_file)
print(ds)

# # Example usage:
processor = CopernicusProcessor(ds, env_epsg, var_map=var_map)
# # thetao_range = processor.get_range('thetao')  # Example for Sea Surface Temperature
# # print(thetao_range)

current_netcdf = processor.speed_from_components('uo', 'vo')
current_netcdf.to_netcdf(r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\current_speed_test.nc')

current_max = processor.get_max("V")
# thethao_min = processor.get_min("thetao")

# print("Max and Min calculated.")

processor.dataarray_to_geotiff(da= current_max,  variable_key="V", out_dir= r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests', nodata=-9999, dtype='float32')

# print("Max GeoTIFF saved.")

# Optional: Save as netCDF
# thethao_max.to_netcdf(r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\thetao_max_test.nc')
# thethao_min.to_netcdf(r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\thetao_min_test.nc')