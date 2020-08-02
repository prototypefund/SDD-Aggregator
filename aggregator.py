import boto3
import pandas as pd
from datetime import date, timedelta
from agg_webcam import aggregate as agg_webcam
from agg_webcam_customvision import aggregate as agg_webcam_customvision
from agg_hystreet import aggregate as agg_hystreet
from agg_gmap_transit_score import aggregate as agg_gmap_transit_score
from agg_fahrrad import aggregate as agg_fahrrad
from agg_airquality import aggregate as agg_airquality
from agg_lemgo_digital import aggregate as agg_lemgo_digital
#from agg_tomtom import aggregate as agg_tomtom
import json
import settings
import os


if __name__ == "__main__":
    sources = "lemgo;webcam;webcam-customvision;hystreet;fahrrad;airquality"
    dict_environ = {"TIMERANGE": 2, "SOURCE_SELECTOR": sources, "OFFSET": 0}
    for key, value in dict_environ.items():
        if key in list(os.environ):
            dict_environ[key] = os.environ[key]
    list_sources = dict_environ["SOURCE_SELECTOR"].split(";")
    # How far back do you want to aggregate data?
    days = int(dict_environ["TIMERANGE"])
    dict_environ["OFFSET"] = int(dict_environ["OFFSET"])
    
    s3_client = boto3.client('s3')
    print(f"\nAggregate the last {days} days.")

    for x in range(dict_environ["OFFSET"], days + dict_environ["OFFSET"]):
        date_obj = date.today() - timedelta(days=x)
        print("\n##########################")
        print('###   START ',date_obj,"\n")
        list_result = pd.DataFrame(columns = ['landkreis'])
        list_result = list_result.set_index("landkreis")

        if 'lemgo' in list_sources:
            print("start lemgo...")
            try:
                lemgo_digital_list = pd.DataFrame(agg_lemgo_digital(date_obj))
                lemgo_digital_list = lemgo_digital_list.set_index('landkreis')
                list_result = list_result.join(lemgo_digital_list, how="outer")
            except Exception as e:
                print("Error Lemgo:")
                print(e)

        # print("--------------")
        # print("start gmap...")
        # try:
        #     gmapscore_list = pd.DataFrame(agg_gmap_transit_score(date_obj))
        #     gmapscore_list = gmapscore_list.set_index('landkreis')
        #     list_result = list_result.join(gmapscore_list, how="outer")
        # except Exception as e:
        #     print("Error GMAP:")
        #     print(e)

        if 'webcam' in list_sources:
            print("--------------")
            print("start webcams...")
            try:
                webcam_list = pd.DataFrame(agg_webcam(date_obj))
                webcam_list = webcam_list.set_index('landkreis')
                list_result = list_result.join(webcam_list, how="outer")
            except Exception as e:
                print("Error Webcam")
                print(e)

        if 'webcam-customvision' in list_sources:
            print("--------------")
            print("start webcams customvision...")
            try:
                webcam_list_customvision = pd.DataFrame(agg_webcam_customvision(date_obj))
                webcam_list_customvision = webcam_list_customvision.set_index('landkreis')
                list_result = list_result.join(webcam_list_customvision, how="outer")
            except Exception as e:
                print("Error Webcam customvision")
                print(e)

        if 'hystreet' in list_sources:
            print("--------------")
            print("start hystreet...")
            try:
                hystreet_list = pd.DataFrame(agg_hystreet(date_obj))
                hystreet_list = hystreet_list.set_index('landkreis')
                list_result = list_result.join(hystreet_list, how = "outer")
            except Exception as e:
                print("Error Hystreet")
                print(e)

        if 'fahrrad' in list_sources:
            print("--------------")
            print("start fahrrad...")
            try:
                fahrrad_list = pd.DataFrame(agg_fahrrad(date_obj))
                fahrrad_list = fahrrad_list.set_index('landkreis')
                list_result = list_result.join(fahrrad_list, how="outer")
            except Exception as e:
                print("Error Fahrrad")
                print(e)

        if 'airquality' in list_sources:
            print("--------------")
            print("start airquality...")
            try:
                airquality_list = agg_airquality(date_obj)
                if airquality_list == []:
                    print("airquality: No data")
                else:
                    airquality_df = pd.DataFrame(airquality_list)
                    airquality_df = airquality_df.set_index('ags')
                    list_result = list_result.join(airquality_df, how="outer")
            except Exception as e:
                print("Error Airquality")
                print(e)

        # print("--------------")
        # print("start tomtom...")
        # try:
            # tomtom_list = pd.DataFrame(agg_tomtom(date_obj))
            # tomtom_list = tomtom_list.set_index('landkreis')
            # list_result = list_result.join(tomtom_list, how="outer")
        # except Exception as e:
            # print("Error Tomtom")
            # print(e)

        print("--------------")
        print("write output...")
        list_result["date"] = str(date_obj)
        list_result.index = list_result.index.astype(int).astype(str).str.zfill(5)
        #list_result.to_csv("test.csv")

        #list_result
        dict_results = list_result.T.to_dict()
        #dict
        # s3_client.put_object(Bucket='sdd-s3-basebucket', Key="aggdata/live", Body=json.dumps(dict))
        response = s3_client.put_object(Bucket=settings.BUCKET, Key='aggdata/{}/{}/{}'.format(str(date_obj.year).zfill(4), str(date_obj.month).zfill(2),str(date_obj.day).zfill(2)), Body=json.dumps(dict_results))
        print("s3_client.put_object response:", response)
        print('\n###     END ',date_obj,"")
        print("##########################\n")

