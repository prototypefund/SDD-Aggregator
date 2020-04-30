import sys
import os
sys.path.insert(0,"./1_classes")

import S3
import Aggregation

def aggregate(date):
    s3Handler = S3.S3_Handler()
    listOfFile = s3Handler.listFromAWS("airquality", date)
    fullData = []
    for pathItem in listOfFile:
        jsonItem = s3Handler.readFromAWS(pathItem)
        if jsonItem != False:
            fullData = fullData + jsonItem
    return Aggregation.Aggregator.aggregateJson(fullData,"airquality","aqi","airquality_score")
