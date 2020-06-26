
def transfer_df_to_influxdb(data, list_fields=[], list_tags=[]):
    json_out = []
    for index, row in data.iterrows():
        j = {}
        j["measurement"] = row["measurement"]
        j["fields"] = {x: row[x] for x in list_fields}
        j["time"] = row["timestamp"]
        j["tags"] = {}
        for tag in list_tags:
            j["tags"][tag] = row[tag]

        json_out.append(j)
    return json_out
