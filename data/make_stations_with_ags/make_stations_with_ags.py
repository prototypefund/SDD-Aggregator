'''
This script is used to create the stations_with_ags.csv
for the hystreet aggregator. Needs to be run only when
new stations are added.
'''

import os
import requests
import pandas as pd
import geopandas
from geopy import distance
from geopy.geocoders import Nominatim
from tqdm import tqdm

import sys
sys.path.append('../../')
from coords_to_kreis import coords_convert

# 1 Get Hystreet Locations
#-----------------------
headers = {'Content-Type': 'application/json',
           'X-API-Token': os.getenv('HYSTREET_TOKEN')}
res = requests.get('https://hystreet.com/api/locations/', headers=headers)
locations = res.json()

# 2 Search Lat/Lon for Hystreet Locations
#-----------------------

geolocator = Nominatim(user_agent="everyonecounts")
for location in tqdm(locations):
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
        #print(query,"==>",geoloc.address)
        location["lat"]=geoloc.latitude
        location["lon"]=geoloc.longitude
        location["address"]=geoloc.address

# 3 Find AGS by coords_convert
#-----------------------

df_locations = pd.DataFrame(locations)
df_locations['landkreis'] = coords_convert(df_locations)

# 4 Write output to file
#-----------------------

outfile = "stations_with_ags.csv"
df_locations = df_locations[["id","name","city","landkreis","lat","lon","address"]]
df_locations = df_locations.sort_values(by="id")
df_locations = df_locations.rename(columns={'id':'stationid'})
df_locations.to_csv(outfile, index=False)
print("Done!")
