# Code to process copernicus marine data:
import os
import xarray as xr
import pandas as pd
import numpy as np
from time import time

# Inputs:
input_file = r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Raw\Daily\Physical_1993_1_1_to_1999_12_31.nc'
env_epsg = 4326  # WGS84

# Load dataset
ds = xr.open_dataset(input_file)
print(ds)

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
    
# Example usage:
processor = CopernicusProcessor(ds, env_epsg)
thetao_range = processor.get_range('thetao')  # Example for Sea Surface Temperature
print(thetao_range)

# Optional: Save as netCDF
thetao_range.to_netcdf(r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Tests\thetao_mean_range.nc')