# 这是一个示例 Python 脚本。

import json
import random
import time

import pandas
# 按 ⌃R 执行或将其替换为您的代码。
# 按 双击 ⇧ 在所有地方搜索类、文件、工具窗口、操作和设置。
import requests
import xlrd
from geojson import Feature, LineString, FeatureCollection
from tqdm import tqdm


def fileType(filename):
    return filename.split(".")[1]


def geojsonPropSetting(obj, keyName, value):
    obj["prop"][keyName] = value


def geojsonMerge(featureList, addPropDict, features, start_lon_column_name="O_X", start_lat_column_name="O_Y",
                 end_lon_column_name="D_X", end_lat_column_name="D_Y"):
    for j in featureList:
        geojsonPropSetting(j, start_lon_column_name, addPropDict[start_lon_column_name])
        geojsonPropSetting(j, start_lat_column_name, addPropDict[start_lat_column_name])
        geojsonPropSetting(j, end_lon_column_name, addPropDict[end_lon_column_name])
        geojsonPropSetting(j, end_lat_column_name, addPropDict[end_lat_column_name])
        features.append(Feature(geometry=LineString(j["geom"]), properties=j["prop"]))


def getColumnIndex(table, columnName):
    columnIndex = None
    for i in range(table.ncols):
        if table.cell_value(0, i) == columnName:
            columnIndex = i
            break
    return columnIndex


def readlocfromexcel(path, sheet_index=0, column_lon='x', column_lat='y'):
    workbook = xlrd.open_workbook(path, formatting_info=True)
    sheet = workbook.sheet_by_index(sheet_index)
    column_lon_index = getColumnIndex(sheet, column_lon)
    column_lat_index = getColumnIndex(sheet, column_lat)
    result = []
    for i in range(1, sheet.nrows):
        lon = str(sheet.cell_value(rowx=i, colx=column_lon_index))
        lat = str(sheet.cell_value(rowx=i, colx=column_lat_index))
        result.append(lon + ',' + lat)
    return result


def spider(in_filename, start_lon_column_name, start_lat_column_name, end_lon_column_name, end_lat_column_name, keyword,
           direction, city_name=None, sheet_index=0):
    """
    :param city_name: bus爬取时需要城市名称
    :param direction: 方式，分为'bus','bike','walk','car'
    :param in_filename:excel文件路径
    :param start_lon_column_name:开始经度列名
    :param start_lat_column_name:开始纬度列名
    :param end_lon_column_name:
    :param end_lat_column_name:
    :param keyword:高德key
    :param sheet_index:表序列
    :return:null
    """
    if direction == 'bike':
        url_name = "bicycling"
        e_name = "errcode"
        suc_value = 0
        data_name = "data"
        version = "v4"
    elif direction == "walk":
        url_name = "walking"
        e_name = "status"
        suc_value = "1"
        data_name = "route"
        version = "v3"
    elif direction == "car":
        url_name = "driving"
        e_name = "status"
        suc_value = "1"
        data_name = "route"
        version = "v3"
    elif direction == "bus":
        url_name = "transit/integrated"
        e_name = "status"
        suc_value = "1"
        data_name = "route"
        version = "v3"

    start_locs = readlocfromexcel(in_filename, sheet_index, start_lon_column_name,
                                  start_lat_column_name)
    end_locs = readlocfromexcel(in_filename, sheet_index, end_lon_column_name,
                                end_lat_column_name)
    workbook = xlrd.open_workbook(in_filename, formatting_info=True)
    columnNum = workbook.sheet_by_index(0).ncols
    df = pandas.read_excel(in_filename)
    col_name = df.columns.tolist()
    col_name.insert(columnNum, 'distance')
    if direction != 'bus':
        col_name.insert(columnNum + 1, "duration")
        col_name.insert(columnNum + 2, "steps")
        df = df.reindex(columns=col_name)
        df['steps'] = df['steps'].astype(str)
    else:
        col_name.insert(columnNum + 1, "duration")
        col_name.insert(columnNum + 2, "transits")
        df = df.reindex(columns=col_name)
        df['transits'] = df['transits'].astype(str)
    rowindex = 0
    with tqdm(total=len(start_locs)) as bar:
        for start_loc, end_loc in zip(start_locs, end_locs):
            url = 'https://restapi.amap.com/' + version + '/direction/' + url_name
            if direction != 'bus':
                p = {"key": keyword, "origin": start_loc, "destination": end_loc}
            else:
                p = {"key": keyword, "origin": start_loc, "destination": end_loc, "city": city_name, "strategy": 0}
            response = requests.get(url, params=p)
            text = response.text
            res = json.loads(text)
            if res[e_name] == suc_value:
                if direction != 'bus':
                    df.loc[rowindex, 'distance'] = res[data_name]["paths"][0]["distance"]
                    df.loc[rowindex, 'duration'] = res[data_name]["paths"][0]["duration"]
                    df.loc[rowindex, 'steps'] = json.dumps(res[data_name]["paths"][0]["steps"], ensure_ascii=False)
                elif direction == 'bus':
                    print(res[data_name]["distance"])
                    if res[data_name]["distance"]:
                        df.at[rowindex, 'distance'] = res[data_name]["distance"]
                    if res["count"] != "0":
                        df.at[rowindex, 'duration'] = res[data_name]["transits"][0]["duration"]
                        df.at[rowindex, 'transits'] = json.dumps(res[data_name]["transits"][0], ensure_ascii=False)
                rowindex = rowindex + 1
                time.sleep(random.randint(100, 200) / 1000)
            else:
                rowindex = rowindex + 1
            bar.update(1)
        out_filename = in_filename.split('.')[0] + '_' + direction + '_result.csv'
        df.to_csv(out_filename, encoding='utf-8')


def steps2geojson(filename, path_column_name, start_lon_column_name="O_X", start_lat_column_name="O_Y",
                  end_lon_column_name="D_X", end_lat_column_name="D_Y", direction=""):
    """
    路径转geojson格式
    :param direction:used way
    :param start_lat_column_name:
    :param start_lon_column_name:
    :param end_lon_column_name:
    :param end_lat_column_name:
    :param filename:爬取的excel文件路径
    :param path_column_name:
    :return:
    """
    index = 0
    if fileType(filename) == 'csv':
        df = pandas.read_csv(filename)
    elif fileType(filename) == 'xls' or fileType(filename) == 'xlsx':
        df = pandas.read_excel(filename)
    df[path_column_name] = df[path_column_name].astype(str)
    features = []
    if direction != "bus":
        for row in df.iterrows():
            # print(row[1])
            # 整个路径的信息
            steps = json.loads(row[1][path_column_name])
            polyline_list = parse_polyline(steps, index)
            index = index + 1
            geojsonMerge(polyline_list, row[1], features, start_lon_column_name, start_lat_column_name,
                         end_lon_column_name, end_lat_column_name)
    else:
        for row in df.iterrows():
            rawResult = row[1][path_column_name]
            i = row[0]
            if rawResult != 'nan':
                transits = json.loads(rawResult)
                segments = transits["segments"]
                print(segments)
                for segment in segments:
                    if segment['walking']:
                        _part = parse_polyline(segment['walking']['steps'], index=i)
                        geojsonMerge(_part, row[1], features, start_lon_column_name, start_lat_column_name,
                                     end_lon_column_name, end_lat_column_name)
                    if segment['bus']:
                        _part = parse_polyline(segment['bus']['buslines'], index=i)
                        geojsonMerge(_part, row[1], features, start_lon_column_name, start_lat_column_name,
                                     end_lon_column_name, end_lat_column_name)
    feature_collection = FeatureCollection(features)
    geojson_filepath = filename.split('.')[0] + ".geojson"
    f = open(geojson_filepath, 'a')
    f.write(str(feature_collection))


def parse_polyline(_list, index):
    result = []
    for i in _list:
        # print(i)
        locs = []
        # 一个小路段的信息
        line_str = i['polyline']
        split_list = line_str.split(';')
        for loc in split_list:
            lon = loc.split(',')[0]
            lat = loc.split(',')[1]
            new_loc = (float(lon), float(lat))
            locs.append(new_loc)
        del i['polyline']
        i['index'] = index
        result.append({"geom": locs, "prop": i})
    return result


if __name__ == '__main__':
    key = '4ce7c6c0f8875bb031433631e4ed71f4'
    # a6c666fc4c889085b57df0ad47df51ef
    spider("数据/smalldis1.xls", "O_X", "O_Y", "D_X", "D_Y", key,
           city_name="杭州", direction="walk")
    steps2geojson("test/bus_work95.xls", "transits", direction="bus")
