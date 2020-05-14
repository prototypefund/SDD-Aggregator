'''
This script is used to create the stations_with_ags.csv
for the hystreet aggregator. Needs to be run only when
new stations are added.
'''

import os
import requests
import pandas as pd
import geopandas
from scipy.spatial import cKDTree
from geopy import distance


# 1 Get Hystreet Locations
#-----------------------
headers = {'Content-Type': 'application/json',
           'X-API-Token': os.getenv('HYSTREET_TOKEN')}
res = requests.get('https://hystreet.com/api/locations/', headers=headers)
locations = res.json()

# 2 Search Lat/Lon for Hystreet Locations
#-----------------------
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="everyonecounts")
for location in locations:
    query = location["city"]+" "+location["name"].split("(")[0]
    geoloc = geolocator.geocode(query,exactly_one=True)
    if geoloc==None and "seite" in query:
        # Berlin Kurfürstendamm Nordseite --> Berlin Kurfürstendamm
        query2 = query.replace("Nordseite","").replace("Ostseite","").replace("Südseite","").replace("Westseite","").split("(")[0]
        geoloc = geolocator.geocode(query2,exactly_one=True)
    if geoloc==None:
        print("!!! NOT FOUND: ",query,query2)
        location["lat"]=None
        location["lon"]=None
        location["address"]=None
    else:
        print(query,"==>",geoloc.address)
        location["lat"]=geoloc.latitude
        location["lon"]=geoloc.longitude
        location["address"]=geoloc.address

# 3 Find AGS by measuring distance from location lat/lon to gemeinde lat/lon
#-----------------------
df_ags = pd.read_csv("data-gemeindeschluessel.csv", sep="\t")
df_ags = df_ags[~df_ags["lat"].isna()]
df_ags = df_ags[~df_ags["ags"].isna()]
df_ags = df_ags[df_ags["invalid"]!=1]
df_ags = df_ags.rename(columns={'lat':'lat_ags', 'lon':'lon_ags','name':'name_ags'})

def ckdnearest(gdA, gdB):
    # input two geopandas dataframes
    # for each entry in gdA concatenate the spatially closest element in gdB
    # uses a cKDTree for very efficient lookup
    # https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)) )
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)) )
    btree = cKDTree(nB) # build spatial index for nB
    dist, idx = btree.query(nA, k=1) # k nearest neighbours of nA in nB
    gdf = pd.concat(
        [gdA.reset_index(drop=True), gdB.iloc[idx, gdB.columns != 'geometry'].reset_index(drop=True),
         pd.Series(dist, name='cKD_dist')], axis=1)
    return gdf

df_locations = pd.DataFrame(locations)
gdf_hy  = geopandas.GeoDataFrame(df_locations, geometry=geopandas.points_from_xy(df_locations["lon"], df_locations["lat"]))
gdf_ags = geopandas.GeoDataFrame(df_ags, geometry=geopandas.points_from_xy(df_ags["lon_ags"], df_ags["lat_ags"]))
merged  = ckdnearest(gdf_hy,gdf_ags)

# validity check: calculate distances between hystreet points and their ags counterparts
merged["distance_m"] = merged.apply(lambda x: int(distance.distance( (x.lon,x.lat),(x.lon_ags,x.lat_ags)).m),axis=1)
print("Please check distance for outliers:")
merged["distance_m"].describe()


# 4 Write output to file
#-----------------------
outfile = "stations_with_ags.csv"
outdf = merged[["id","name","city","ags","lat","lon","address","distance_m"]]
outdf = outdf.sort_values(by="id")
outdf = outdf.rename(columns={'id':'stationid'})
outdf.to_csv(outfile, index=False)
print("Done!")