import requests
import pandas as pd
import datetime
import json
import numpy as np

#Specify Timeframe
min_date = datetime.datetime.now().date() - datetime.timedelta(days=2)
max_date = datetime.datetime.now().date()
params = {"min_date": str(min_date), "max_date": str(max_date), "data_sources":"0,1,2"}
response = requests.get('https://f3fp7p5z00.execute-api.eu-central-1.amazonaws.com/test/sdd-lambda-request',params = params)
response.json()
data = pd.DataFrame()
for day, data_day in json.loads(response.json()["body"])["body"].items():
    # if day == "2020-03-23":
    #     continue
    daily_data = pd.DataFrame.from_dict(data_day, orient='index')
    daily_data = daily_data.reset_index()
    daily_data["date"] = day
    data = data.append(daily_data)


data.columns
# 'index': AGS-ID of Landkreis
# 'date': Date of measurement
# 'gmap_score': How many people are at transit stations compared to normal day?
# 'hystreet_score': How many people are walking by hystreet sensors compared to normal day?
# 'nationalExpress_score': 'national_score', 'regional_score','suburban_score', 'bus_score', 'zug_score': How many connections got cancelled?
# "bike_score": How many people were travelling on bikes that day compared to a normal day? Here someone used fancy machine learning to cancel out the effect of weather.
# 'webcam_score': How many people are visible on webcams in public places divided by 2.4 (->we dont have a "normal" value here so we use 1/highscore median)

data.replace(np.inf, np.nan, inplace=True)
data["gmap_score"].max()
data.loc[data["date"]=="2020-03-28"][["gmap_score", "index"]]
aggregate = data.groupby("date")[['bike_score', 'bus_score', 'gmap_score', 'hystreet_score', 'nationalExpress_score', 'national_score', 'regional_score','suburban_score', 'webcam_score', 'zug_score']].mean()
pd.DataFrame(aggregate).to_csv("aggregate.csv")
data.to_csv("export.csv")
