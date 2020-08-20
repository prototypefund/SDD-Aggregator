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
    gdf_joined = geopandas.sjoin(gdf, countries, how="left", op='intersects')
    gdf_joined = gdf_joined.drop(columns=["ags_right", "index_right"], errors="ignore")

    nan_indices = gdf_joined["ags"].isna()
    if not gdf_joined.loc[nan_indices].empty:
        # If there are locations just slightly outside of the country polygon, they
        # will result in NaN values. Try to fix this by repeating the sjoin
        # with slighty enlarged countries for those locations
        countries_with_buffer = countries.copy()
        countries_with_buffer["geometry"] = countries.buffer(0.004)
        gdf_joined.loc[nan_indices] = geopandas.sjoin(gdf.loc[nan_indices], countries_with_buffer, how="left", op='intersects')
        gdf_joined = gdf_joined.drop(columns=["ags_right", "index_right"], errors="ignore")
        if gdf_joined.loc[gdf_joined['ags'].isna()].empty:
            try:
                print(f"coords_convert: Successfully applied buffer fix to {len(gdf_joined.loc[nan_indices]['Name'].unique())} locations!")
            except Exception as e:
                print(f"coords_convert: Successfully applied buffer fix to {len(gdf_joined.loc[nan_indices]['name'].unique())} locations!")

        else:
            try:
                loc_err = gdf_joined.loc[gdf_joined['ags'].isna()]['Name'].unique()
            except:
                loc_err = gdf_joined.loc[gdf_joined['ags'].isna()]['name'].unique()
            print(f"Warning: coords_convert: {len(loc_err)} locatations could not be found {loc_err}")
            gdf_joined = gdf_joined.dropna(subset=["ags"])

    # in case there was a "ags" column in the original dataframe:
    gdf_joined = gdf_joined.rename(columns={"ags_left": "ags"})
    gdf_joined["ags"] = gdf_joined["ags"].astype(int)
    return gdf_joined


def attach_to_ags(df):
    # df = pd.DataFrame()
    df = df.merge(countries, on="ags", how="left")
    return df
    # import matplotlib.pyplot as plt
    # plt.figure()
    # ax = gdf.plot(color="green")
    # gdf.loc[gdf["ags"].isna()].plot(color="red", ax = ax)
    # plt.show()


    # return gdf

# Example Usage:

# df["ags"] = coords_convert(pd.DataFrame([{"value": 1,"lat": 48.366512, "lon": 10.894446}])) returns Series Augsburg
