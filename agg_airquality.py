import sys
import os
sys.path.insert(0,"./1_classes")

import S3
import Aggregation
import datetime
from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import convert_df_to_influxdb
import pandas as pd
import json

date_obj = datetime.date.today().replace(day=12)
def aggregate(date_obj):
    s3Handler = S3.S3_Handler()
    listOfFile = s3Handler.listFromAWS("airquality", date_obj)
    fullData = []
    for pathItem in listOfFile:
        jsonItem = s3Handler.readFromAWS(pathItem)
        if jsonItem != False:
            fullData = fullData + jsonItem

    # Aggregation.Aggregator.aggregateDf(fullData, "airquality", "aqi", "airquality_score")
    # pd.DataFrame.from_records(fullData)
    list_fields = ["airquality_score", "lat", "lon"]
    list_tags = ["state", "landkreis", "districtType", "ags", "name", "_id", "origin"]

    df = Aggregation.Aggregator.aggregateJson(fullData,"airquality","aqi","airquality_score")
    df["measurement"] = "airquality"
    # df["measurement"] = "airquality"
    # df["time"] = pd.to_datetime(date)
    # df["time"] = pd.to_datetime(date).timestamp()
    df["time"] = datetime.datetime(year=date_obj.year, month=date_obj.month, day=date_obj.day, hour=12).isoformat()
    # df["time"] = date.hour
    # df["time"] = datetime.datetime.timestamp(year=date.year, month=date.month, day=date.day)

    list_jsons = convert_df_to_influxdb(df, list_tags=list_tags, list_fields=list_fields)
    push_to_influxdb(list_jsons)
    return json.loads(df.to_json(orient='records'))
    # return json.loads(df.to_dict(orient="records"))
