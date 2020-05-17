#To run this file you NEED
#pip install Rtree AND
#sudo apt-get update && apt-get install -y libspatialindex-dev OR brew install spatialindex

import geopandas.tools
from shapely.geometry import Point
import pandas as pd

countries = geopandas.GeoDataFrame.from_file(
  "https://raw.githubusercontent.com/AliceWi/TopoJSON-Germany/master/germany.json",
  layer=1,
  driver="TopoJSON")
countries = countries[["id", "geometry"]]
countries.columns = ["ags", "geometry"]


"""
Converts all lon lat columns in a dataframe into a Series of AGS (Landkreis) Data

:param request: the pandas dataframe
:return: a series of AGS Data
"""

def coords_convert(df):
    df[["lat","lon"]] = df[["lat","lon"]].apply(pd.to_numeric, errors='coerce')
    geometry=geopandas.points_from_xy(df["lon"], df["lat"])
    gdf = geopandas.GeoDataFrame(df, geometry=geometry)
    gdf.crs = countries.crs # supresses warning
    gdf = geopandas.sjoin(gdf, countries, how="left", op='intersects')
    return gdf["ags"]
    

# Example Usage:

# df["ags"] = coords_convert(pd.DataFrame([{"value": 1,"lat": 48.366512, "lon": 10.894446}])) returns Series Augsburg
