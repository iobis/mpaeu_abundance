# Code to process copernicus marine data:
import os
import xarray as xr
import pandas as pd
import numpy as np
from time import time
import rioxarray

# Class with functions to process Copernicus data:
class CopernicusProcessor:
    def __init__(self, dataset: xr.Dataset, epsg: int):
        self.dataset = dataset
        self.epsg = epsg

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
    
    def dataarray_to_geotiff(self, da: xr.DataArray, out_path: str,
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

        da.rio.to_raster(out_path, **write_kwargs)
        return out_path


# Inputs:
input_file = r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Raw\Daily\1993_2000\Physical_1993_1_1_to_1999_12_31.nc'
env_epsg = 4326  # WGS84

# Load dataset
ds = xr.open_dataset(input_file)

# Example usage:
processor = CopernicusProcessor(ds, env_epsg)
# thetao_range = processor.get_range('thetao')  # Example for Sea Surface Temperature
# print(thetao_range)

thethao_max = processor.get_max("thetao")
thethao_min = processor.get_min("thetao")

print("Max and Min calculated.")

processor.dataarray_to_geotiff(thethao_max, r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\thetao_max_test.tif', nodata=-9999, dtype='float32')

print("Max GeoTIFF saved.")

# Optional: Save as netCDF
# thethao_max.to_netcdf(r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\thetao_max_test.nc')
# thethao_min.to_netcdf(r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\thetao_min_test.nc')