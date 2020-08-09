

if not el.hasChildNodes():

def recursive(el, recursion_lvl):
    spaces = recursion_lvl * "   "
    for node in el.childNodes:
        print(f"{spaces}NAME: ", node.nodeName)
        print(f"{spaces}VALUE: ", node.nodeValue)
        print(f"{spaces}PARENT: ", node.parentNode.nodeName)
        print(f"{spaces}CHILDS: ", node.hasChildNodes())
        print("- - -")
        if node.parentNode.nodeName == "basicData":
            pass
            # return node
        if node.nodeName == "predefinedLocationReference":
            node.getAttributeNode("id").value
            node.getAttributeNode("targetClass").value
            node.parentNode.getAttributeNode("xsi:type").value
            return node


        # if node.parentNode.nodeName == "basicData" and node.parentNode.hasChildNodes():
        #     return node

        rec = recursive(node, recursion_lvl + 1)
        if rec:
            return rec

node = recursive(el, 0)


    ElaboratedDataPublication


            # list_childnodes = mydoc.childNodes
            # for i in list_childnodes:
            #     print(i.nodeName)
            #     list_childnodes2 = i.childNodes
            #     for j in list_childnodes2:
            #         print(" ", j.nodeName)
            #         list_childnodes3 = j.childNodes
            #         for k in list_childnodes3:
            #             list_childnodes4 = k.childNodes
            #             print("   ", k.nodeName)
            #             for l in list_childnodes4:
            #                 # print("   ", l.nodeName)
            #                 pass




            payloadPublication = mydoc.getElementsByTagName('payloadPublication')
            for e2 in e.childNodes:
                print(e2.hasChildNodes())
            ex = mydoc.getElementsByTagName('exchange')
            for e in ex:
                print(e)
            payloadPublication = mydoc.getElementsByTagName('payloadPublication')
            for item in payloadPublication:
                item.hasChildNodes()
                value = item.getElementsByTagName('publicationTime')
                for val in value:
                    # print(val.childNodes)
                    childnodes = val.childNodes
                    for childnode in childnodes:
                        print(childnode.parentNode.nodeName)
                        print(childnode.data)

    import pandas as pd
    import xml.etree.ElementTree as et
    import xml


    def parse_XML(xml_file, df_cols):
        """Parse the input XML file and store the result in a pandas
        DataFrame with the given columns.

        The first element of df_cols is supposed to be the identifier
        variable, which is an attribute of each node element in the
        XML data; other features will be parsed from the text content
        of each sub-element.
        """

        xtree = et.parse(xml_file)
        xroot = xtree.getroot()
        rows = []

        for node in xroot:
            res = []
            res.append(node.attrib.get(df_cols[0]))
            for el in df_cols[1:]:
                if node is not None and node.find(el) is not None:
                    res.append(node.find(el).text)
                else:
                    res.append(None)
            rows.append({df_cols[i]: res[i]
                         for i, _ in enumerate(df_cols)})

        out_df = pd.DataFrame(rows, columns=df_cols)

        return out_df


            body
    except Exception as e:
        print(e)

        print(body)
        # df = pd.DataFrame(json.loads(body))
        # df["date_check"] = date_obj
        # df["hour_check"] = hour
        # df["timestamp_check"] = str(
        #     datetime.datetime(year=date_obj.year, month=date_obj.month, day=date_obj.day, hour=hour))
        # data = data.append(df)
    # except Exception as e:
    #     print(e, key)
    #     pass
    # if data.empty:
    #     print(f"WARNING: No data returned from S3 for {str(date_obj)}!")
    #     return []



    # one specific item attribute
    print('Item #2 attribute:')
    print(items[1].attributes['name'].value)

    # all item attributes
    print('\nAll attributes:')
    for elem in items:
        print(elem.attributes['name'].value)

    # one specific item's data
    print('\nItem #2 data:')
    print(items[1].firstChild.data)
    print(items[1].childNodes[0].data)

    # all items data
    print('\nAll item data:')
    for elem in items:
        print(elem.firstChild.data)


if __name__ == '__main__':
    # for testing
    for i in range(0,4):
        date_obj = date.today() - timedelta(days = i)
        list_results = aggregate(date_obj)
    # print(list_results)
