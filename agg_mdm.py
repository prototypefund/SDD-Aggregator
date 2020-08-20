import pandas as pd
from awsthreading import get_mdm_data, _init, get_client, get_mdm_prefix
import settings
from xml.dom import minidom
import datetime
from coords_to_kreis import get_ags
from push_to_influxdb import push_to_influxdb
from convert_df_to_influxdb import convert_df_to_influxdb

def get_basicdatalist(document):
    return document.getElementsByTagName("basicData")

def get_data_from_basicdata(basicdata):
    xsi_type = basicdata.getAttributeNode("xsi:type").nodeValue
    if xsi_type == "TrafficStatus":
        return get_traffic_status(basicdata)
    else:
        return get_traffic_speed(basicdata)

def get_traffic_status(basicdata):
    dict_basicdata = {}
    # dict_basicdata["location_type"] = basicdata
    # dict_basicdata["vehicleType"] = basicdata.getElementsByTagName("vehicleType")[0].childNodes[0].nodeValue
    for key, value in basicdata.getElementsByTagName("pertinentLocation")[0].childNodes[1].attributes.items():
        dict_basicdata[key] = value


    for child in basicdata.childNodes:
        nodename = child.nodeName
        # print(nodename)
        if nodename == "trafficStatus":
            dict_basicdata[nodename] = child.childNodes[1].childNodes[0].nodeValue
        elif nodename != "#text" and nodename != "pertinentLocation":
            dict_basicdata[nodename] = child.childNodes[0].nodeValue
            # print(child.childNodes)
            # child.childNodes[1].childNodes[0].nodeValue
            # print(nodename , child.childNodes[1].childNodes[0].nodeValue)
    # print(dict_basicdata)
    return dict_basicdata

def get_traffic_speed(basicdata):
    dict_basicdata = {}
    # dict_basicdata["location_type"] = basicdata
    # dict_basicdata["vehicleType"] = basicdata.getElementsByTagName("vehicleType")[0].childNodes[0].nodeValue
    for key, value in basicdata.getElementsByTagName("pertinentLocation")[0].childNodes[1].attributes.items():
        dict_basicdata[key] = value
        # dict_basicdata["key"] = dict_basicdata["vehicleType"] + key
        # dict_basicdata["value"] = value
        # print(dict_basicdata)

    for child in basicdata.childNodes:
        nodename = child.nodeName
        # print(nodename)
        if nodename != "#text" and nodename != "pertinentLocation":
            dict_basicdata[nodename] = child.childNodes[1].childNodes[0].nodeValue
            # print(child.childNodes)
            # child.childNodes[1].childNodes[0].nodeValue
            # print(nodename , child.childNodes[1].childNodes[0].nodeValue)
    # print(dict_basicdata)
    return dict_basicdata

def get_location_data(mdm_file):
    list_basicdata = []
    list_nodes = mdm_file.getElementsByTagName("predefinedLocation")
    for node in list_nodes:
        # print(node.toprettyxml())
        dict_basicdata = {}
        for key, value in node.attributes.items():
            dict_basicdata[key] = value
        for key in ["latitude", "longitude"]:
            dict_basicdata[key] = float(node.getElementsByTagName(key)[0].childNodes[0].nodeValue)

        list_basicdata.append(dict_basicdata)
    # df_locations = pd.DataFrame().from_records(list_basicdata)
    # return df_locations
    return list_basicdata



def aggregate(date_obj=datetime.date.today()):
    mdm_data = get_mdm_data(date_obj)

    list_dict_basicdata = []
    list_locations = []
    for mdm_file in mdm_data:
        payload_pub = mdm_file.getElementsByTagName("payloadPublication")[0]
        mdm_file_type = payload_pub.getAttributeNode("xsi:type").value
        timestamp = payload_pub.getElementsByTagName("publicationTime")[0].childNodes[0].nodeValue
        if mdm_file_type == "ElaboratedDataPublication":
            list_basicdata = get_basicdatalist(mdm_file)
            for basicdata in list_basicdata:
                # print(basicdata.childNodes)
                dict_basicdata = get_data_from_basicdata(basicdata)
                dict_basicdata["time"] = timestamp
                list_dict_basicdata.append(dict_basicdata)
            # Lösung 2 ohne xsi_type und targetClass

        elif mdm_file_type == "PredefinedLocationsPublication":
            list_locations += get_location_data(mdm_file)
    df_data = pd.DataFrame().from_records(list_dict_basicdata)

    df_data = df_data.astype({'averageVehicleSpeed': 'float', 'percentageLongVehicles': 'float', "vehicleFlow": "float",
         'measurementOrCalculationPeriod' : "float", 'trafficStatus' : "str"}).drop(
        columns=['version', "targetClass"], errors="ignore").sort_values(by="id")

    # df_data = df_data.groupby(['id', 'forVehiclesWithCharacteristicsOf']).agg(
    #     {"vehicleFlow": "max", 'averageVehicleSpeed': 'max',
    #      'percentageLongVehicles': 'max'}).reset_index().sort_values(by="id")

    df_locations = pd.DataFrame().from_records(list_locations)
    df_locations = df_locations.drop_duplicates(subset=["id"], keep="last").drop(columns="version")

    df_data = df_data.merge(df_locations, on=["id"], how="left")
    df_data["name"] = df_data["id"].copy()
    df_data = df_data.rename(columns={"latitude" : "lon", "longitude" : "lat", "id" : "_id"}) # LAT LON VERTAUSCHT IN ROHDATEN
    df_data = df_data.astype({"lat" : "float", "lon" : "float"})
    df_data = get_ags(df_data.copy())
    df_data = df_data.rename(columns={"state" : "bundesland"})

    df_data["measurement"] = "mdm"

    list_mdm_fields = ["vehicleFlow", "averageVehicleSpeed", "percentageLongVehicles", "trafficStatus", "lat", "lon"]
    list_mdm_tags = [
        '_id',
        'name',
        'ags',
        'bundesland',
        'landkreis',
        'districtType'
        # ,'origin'
    ]
    json_out = convert_df_to_influxdb(df_data, list_mdm_fields, list_mdm_tags)
    push_to_influxdb(json_out)
    print(date_obj)

    # Lösung wenn xsi_type geschrieben wird:
    # df1 = df.loc[df["xsi_type"] == "TrafficSpeed"][["id", "averageVehicleSpeed", "vehicleType"]]
    # df2 = df.loc[df["xsi_type"] == "TrafficFlow"][["id", 'vehicleFlow', 'percentageLongVehicles', "vehicleType"]]
    # df3 = pd.merge(df1, df2, on=["id", "vehicleType"])
    # df4 = df.drop(columns=["averageVehicleSpeed", "vehicleFlow", "percentageLongVehicles", "xsi_type"]).drop_duplicates()
    # df5 = pd.merge(df3, df4, on=["id", "vehicleType"], how="left")
    #
    #
    # # Lösung ohne xsi_type:
    # df7 = df[['targetClass', 'id', 'version', 'forVehiclesWithCharacteristicsOf']].drop_duplicates().drop(columns="targetClass")
    # df8 = df[["id", 'vehicleFlow', "averageVehicleSpeed", 'percentageLongVehicles', "forVehiclesWithCharacteristicsOf"]]
    # df6 = pd.merge(df7, df8, how="left", on=["id", "forVehiclesWithCharacteristicsOf", "version"])

# if __name__ == "__main__":
    # debug
    # list_att = []
    # node = mdm_data[1]
    # f = open("z.xml", "w")
    # node.writexml(f)
    # f.close()
    # x, y = rec2(node, list_att, 0)
    # df = pd.DataFrame(y)
    # df.to_csv("z.csv", sep=";")
    # date_obj = _init()
    # date_obj = datetime.datetime.now()
    # # dict_objects = get_client().list_objects(Bucket=settings.BUCKET, Prefix=get_mdm_prefix(date_obj))
    # date_obj = date_obj.replace(minute=int(date_obj.minute/15) *15)
    # data = pd.DataFrame()
    # date_obj = datetime.date.today() - datetime.timedelta(1)

# import boto3
# import io
# import sys

# def get_data(date_obj):
#     s3_client = boto3.client('s3')
#     dict_objects = s3_client.list_objects(Bucket=settings.BUCKET, Prefix=get_prefix(date_obj))
#     list_doc = []
#     try:
#         for element in dict_objects["Contents"]:
#             response = s3_client.get_object(Bucket=settings.BUCKET, Key=element["Key"])
#
#             body = response["Body"].read()
#             buffer = io.StringIO(body.decode())
#             list_doc.append(minidom.parse(buffer))
#     except Exception as e:
#         pass
#         # return None, e
#     return list_doc, dict_objects



# def recursive(df, el, recursion_lvl):
#     spaces = recursion_lvl * "   "
#     for node in el.childNodes:
#         print(f"{spaces}VALUE: ", node.nodeValue)
#         print(f"{spaces}PARENT: ", node.parentNode.nodeName)
#         print(f"{spaces}CHILDS: ", node.hasChildNodes())
#         print("- - -")
#         if node.parentNode.nodeName == "basicData":
#             pass
#             # return node
#         if node.nodeName == "predefinedLocationReference":
#             node.getAttributeNode("id").value
#             node.getAttributeNode("targetClass").value
#             node.parentNode.getAttributeNode("xsi:type").value
#             return node
#
#
#
#         # if node.parentNode.nodeName == "basicData" and node.parentNode.hasChildNodes():
#         #     return node
#
#         rec = recursive(node, recursion_lvl + 1)
#         if rec:
#             return rec


# def rec(df, el, recursion_lvl):
#     spaces = recursion_lvl * "   "
#     for node in el.childNodes:
#         if node.hasChildNodes():
#             rec(df, node, recursion_lvl)
#         else:
#             print(f"{spaces}NAME: ", node.nodeName)
#             print(f"{spaces}NAME: ", node.nodeName)
#             print(f"{spaces}VALUE: ", node.nodeValue)
#             print(f"{spaces}PARENT: ", node.parentNode.nodeName)
#             print(f"{spaces}CHILDS: ", node.hasChildNodes())
#             print("- - -")
#         df.append()
#         if node.parentNode.nodeName == "basicData":
#             pass
#             # return node
#         if node.nodeName == "predefinedLocationReference":
#             node.getAttributeNode("id").value
#             node.getAttributeNode("targetClass").value
#             node.parentNode.getAttributeNode("xsi:type").value
#             return node
#     # df.join(rec())
#     # df = pd.DataFrame()
#     # for i in range(5):
#     #     rec(el, )


# def aggregate():
#     pass


# def rec2(node, list_att, rec_lvl):
#     print("Node\t",node)
#     # [rec_lvl, node.nodeName, "" ,"" ,node.nodeValue, "" , "" , ""]
#
#     att = node.attributes
#     dict_att = {"Rang": rec_lvl, "key": node.nodeName, "isnode": "", "nodeType": node.nodeType, "value": node.nodeValue,
#                        "isinnode": node.parentNode.nodeName, "isrelevant": "", "document": ""}
#     if dict_att["key"] != "#text":
#         dict_att["childvalue"] = node.childNodes
#     if dict_att["key"] == "#text" and dict_att["value"] != None:
#         dict_att["isinnode"]
#
#
#     if att:
#         print("Attributes:", att)
#         dict_att.update(dict(att.items()))
#     list_att.append(dict_att)
#     try:
#         childs = node.childNodes
#         print(childs[0])
#         if childs:
#             print("haschilds")
#             print(childs)
#             for child in childs:
#                 # dict_att = {}
#                 dict_att, list_att = rec2(child, list_att, rec_lvl + 1)
#             # print(node)
#             pass
#     except Exception as e:
#         print(e)
#
#     return dict_att, list_att

# def create_df(mdm_data):
#     columns = ["Rang","key","isnode","value_dtype","value","isinnode","isrelevant", "document"]
#     df = pd.DataFrame(columns=columns)
#     list_att = []
#     for el in mdm_data:
#         df = rec2(el, list_att)
#     df = rec2(mdm_data[0], list_att)

