from coords_to_kreis import coords_convert
import boto3
import json
import time
from datetime import date, timedelta
import pandas as pd
import csv
import ast
import settings
import numpy as np
import ast

date = date.today() - timedelta(days = 2)
print(date)
s3_client = boto3.client('s3')
data = pd.DataFrame()
for x in range(9,19):
    try:
        response = s3_client.get_object(Bucket=settings.BUCKET, Key='googleplaces_supermarket/{}/{}/{}/{}'.format(str(date.year).zfill(4), str(date.month).zfill(2), str(date.day).zfill(2), str(x).zfill(2)))
        result = pd.DataFrame(json.loads(response["Body"].read()))
        result["date"] = date
        result["hour"] = x
        data = data.append(result)
    except Exception as e:
        print(e)
        pass
        #return
lat = []
lon = []
data.to_csv("super.csv")
data["coordinates"] = data["coordinates"].astype(str)
for index, row in data.iterrows():
    lat.append(ast.literal_eval(row["coordinates"])["lat"])
    lon.append(ast.literal_eval(row["coordinates"])["lng"])

def normal_popularity(row):
    return row["populartimes"][row["date"].weekday()]["data"][row["hour"]]

data["normal_popularity"] = data.apply(normal_popularity, axis = 1, result_type = "reduce")
data["relative_popularity"] = data["current_popularity"] / data["normal_popularity"]

data["lat"] = lat
data["lon"] = lon
data["ags"] = coords_convert(data)

pd.DataFrame(data["ags"].unique()).to_csv("super_ags.csv")
