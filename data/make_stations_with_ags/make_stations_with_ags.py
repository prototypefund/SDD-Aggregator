"""
This script is used to create the stations_with_ags.csv
for the hystreet aggregator. Needs to be run only when
new stations are added.
"""

import os
import requests
from shutil import copyfile
import pandas as pd
from geopy.geocoders import Nominatim

import sys
sys.path.append('../../')
from coords_to_kreis import coords_convert

UPDATE_ONLY = True
# False: Completly recreate stations_with_ags.csv, look up all stations
# True: Only update with new (unknown) stations
#
# Lookup of existing stations can sometimes result in slighlty different lat/lon
# values, which could lead to issues down the line (e.g. with influxdb tags)
# Therefore, UPDATE_ONLY = True   is recommended

oldfile = "stations_with_ags_old.csv"  # path to old file
outfile = "stations_with_ags.csv"  # where to (over-)write


# 1 Read old locations
# -----------------------
if UPDATE_ONLY:
    df_old_locations = pd.read_csv(oldfile, dtype='str')

# 2 Get Hystreet Locations
# -----------------------
headers = {'Content-Type': 'application/json',
           'X-API-Token': os.getenv('HYSTREET_TOKEN')}
res = requests.get('https://hystreet.com/api/locations/', headers=headers)
locations = res.json()

# 3 Search Lat/Lon for Hystreet Locations
# -----------------------
geolocator = Nominatim(user_agent="everyonecounts")
for location in locations:
    # check if location is already in list
    if UPDATE_ONLY and str(location["id"]) in df_old_locations["stationid"].values:
        continue
    print(f"New station: {location}")
    query = location["city"] + " " + location["name"].split("(")[0]
    geoloc = geolocator.geocode(query, exactly_one=True)
    if geoloc is None and "seite" in query:
        # Berlin Kurfürstendamm Nordseite --> Berlin Kurfürstendamm
        query2 = \
        query.replace("Nordseite", "").replace("Ostseite", "").replace("Südseite", "").replace("Westseite", "").split(
            "(")[0]
        geoloc = geolocator.geocode(query2, exactly_one=True)
    if geoloc is None:
        print("!!! NOT FOUND: ", query, query2)
        location["lat"] = None
        location["lon"] = None
        location["address"] = None
    else:
        # print(query,"==>",geoloc.address)
        location["lat"] = geoloc.latitude
        location["lon"] = geoloc.longitude
        location["address"] = geoloc.address

# 4 Find AGS by coords_convert
# -----------------------
df_new_locations = pd.DataFrame(locations, dtype='str').dropna()
df_new_locations['landkreis'] = list(coords_convert(df_new_locations))
df_new_locations = df_new_locations[["id", "name", "city", "landkreis", "lat", "lon", "address"]]
df_new_locations = df_new_locations.rename(columns={'id': 'stationid'})

# 5 Write output to file
# -----------------------
df_new_locations = df_new_locations.drop_duplicates()
df_new_locations = df_new_locations.sort_values(by="stationid")
if UPDATE_ONLY:
    # append new stations to old file
    copyfile(oldfile, outfile)
    df_new_locations.to_csv(outfile, index=False, mode='a', header=False)
else:
    # rewrite
    df_new_locations.to_csv(outfile, index=False)
print("Done!")
