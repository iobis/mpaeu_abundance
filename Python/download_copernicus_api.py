import os
from datetime import datetime
import getpass
import copernicusmarine

# Work directory
out_dir = r'C:\Users\beñat.egidazu\Desktop\PhD\Papers\Fisheries_2\Data_nca\Environmental_data_copernicus\Raw\Daily'
os.chdir(out_dir)

# Copernicus Marine Credentials 
USERNAME = input("Enter your username: ")
PASSWORD = getpass.getpass("Enter your password: ")
copernicusmarine.login( username = USERNAME, password = PASSWORD)

# Uncomment this to download Sea Surface Temperature, Salinity & Sea Water Velocity in the study area:
productID = "cmems_mod_ibi_phy_my_0.083deg-3D_P1D-m" 
lon = (-19.08, 5.08)
lat = (26 , 56.08)
variables = ["uo", "vo", "so", "thetao"]
variable_name = "Physical"
start_date = datetime(1993, 1, 1, 0)
end_date = datetime (1999, 12, 31, 23)
# Adapt to the SDM environmental variable depth:
min_depth = 0
max_depth = 1


# # Uncomment this to download Dissolved Oxygen in the study area:
# productID = "cmems_mod_ibi_bgc_my_0.083deg-3D_P1D-m" 
# lon = (-19.08, 5.08)
# lat = (26 , 56.08)
# variables = ["o2"]
# variable_name = "Biogeochemichal"
# start_date = datetime(1993, 1, 1, 0)
# end_date = datetime (1999, 12, 31, 23)
# # Adapt to the SDM environmental variable depth:
# min_depth = 0
# max_depth = 1

# Retrieve:
copernicusmarine.subset(
  dataset_id = productID,
  variables = variables,
  minimum_longitude = lon[0],
  maximum_longitude = lon[1],
  minimum_latitude = lat[0],
  maximum_latitude = lat[1],
  start_datetime = start_date,
  end_datetime = end_date,
  minimum_depth = min_depth,
  maximum_depth = max_depth,
  output_filename = f"{variable_name}_{start_date.year}_{start_date.month}_{start_date.day}_to_{end_date.year}_{end_date.month}_{end_date.day}"
)