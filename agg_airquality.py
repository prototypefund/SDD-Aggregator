import sys
import os
sys.path.insert(0,"./1_classes")

import S3
import Aggregation
import datetime
from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import transfer_df_to_influxdb
import pandas as pd

date = datetime.date.today().replace(day=12)
def aggregate(date):
    s3Handler = S3.S3_Handler()
    listOfFile = s3Handler.listFromAWS("airquality", date)
    fullData = []
    for pathItem in listOfFile:
        jsonItem = s3Handler.readFromAWS(pathItem)
        if jsonItem != False:
            fullData = fullData + jsonItem

    # Aggregation.Aggregator.aggregateDf(fullData, "airquality", "aqi", "airquality_score")
    # pd.DataFrame.from_records(fullData)
    list_fields = ["airquality_score"]
    list_tags = ["state", "landkreis", "districtType", "ags"]

    df = Aggregation.Aggregator.aggregateJson(fullData,"airquality","aqi","airquality_score")
    df["measurement"] = "airquality"
    # df["time"] = pd.to_datetime(date)
    # df["time"] = pd.to_datetime(date).timestamp()
    df["time"] = date.isoformat() + "T12:00:00.000000"
    # df["time"] = date.hour
    # df["time"] = datetime.datetime.timestamp(year=date.year, month=date.month, day=date.day)

    list_jsons = transfer_df_to_influxdb(df, list_tags=list_tags, list_fields=list_fields)
    push_to_influxdb(list_jsons)
