#To run this file you NEED
#pip install Rtree AND
#sudo apt-get update && apt-get install -y libspatialindex-dev OR brew install spatialindex

import geopandas.tools
# from shapely.geometry import Point
import pandas as pd

countries = geopandas.GeoDataFrame.from_file(
  "https://raw.githubusercontent.com/AliceWi/TopoJSON-Germany/master/germany.json",
  layer=1,
  driver="TopoJSON")

countries = countries[["id", "geometry", "state", "name", "districtType"]]
countries.columns = ["ags", "geometry", "state", "landkreis", "districtType"]


"""
Converts all lon lat columns in a dataframe into a Series of AGS (Landkreis) Data

:param request: the pandas dataframe
:return: a series of AGS Data
"""

def coords_convert(df):
    df[["lat","lon"]] = df[["lat","lon"]].apply(pd.to_numeric, errors='coerce')
    geometry=geopandas.points_from_xy(df["lon"], df["lat"])
    gdf = geopandas.GeoDataFrame(df, geometry=geometry)
    gdf = gdf.dropna(subset=["geometry"]).reset_index(drop=True)
    gdf.crs = countries.crs # supresses warning
    gdf = geopandas.sjoin(gdf, countries, how="left", op='intersects')

    return gdf["ags"]


def get_ags(df):
    df[["lat", "lon"]] = df[["lat", "lon"]].apply(pd.to_numeric, errors='coerce')
    geometry = geopandas.points_from_xy(df["lon"], df["lat"])
    gdf = geopandas.GeoDataFrame(df, geometry=geometry)
    gdf = gdf.dropna(subset=["geometry"]).reset_index(drop=True)
    gdf.crs = countries.crs  # supresses warning
    gdf = geopandas.sjoin(gdf, countries, how="left", op='intersects')
    
    # in case there was a "ags" column in the original dataframe:
    gdf = gdf.rename(columns={"ags_left":"ags"})

    for col in ["ags_right", "index_right"]:
        try:
            gdf = gdf.drop(columns=col)
        except:
            pass

def attach_to_ags(df):
    # df = pd.DataFrame()
    df = df.merge(countries, on="ags", how="left")
    return df
    # import matplotlib.pyplot as plt
    # plt.figure()
    # ax = gdf.plot(color="green")
    # gdf.loc[gdf["ags"].isna()].plot(color="red", ax = ax)
    # plt.show()


    return gdf

# Example Usage:

# df["ags"] = coords_convert(pd.DataFrame([{"value": 1,"lat": 48.366512, "lon": 10.894446}])) returns Series Augsburg
