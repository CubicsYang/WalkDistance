# 这是一个示例 Python 脚本。

import json
import random
import time

# 按 ⌃R 执行或将其替换为您的代码。
# 按 双击 ⇧ 在所有地方搜索类、文件、工具窗口、操作和设置。
import requests
import xlrd
from tqdm import tqdm
from xlutils.copy import copy


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
        result.append(str(sheet.cell_value(rowx=i, colx=column_lon_index)) + ',' + str(
            sheet.cell_value(rowx=i, colx=column_lat_index)))
    return result


def init(filename, start_lon_column_name, start_lat_column_name, end_lon_column_name, end_lat_column_name, keyword,
         outfilename, sheet_index=0):
    start_locs = readlocfromexcel(filename, sheet_index, column_lon=start_lon_column_name,
                                  column_lat=start_lat_column_name)
    end_locs = readlocfromexcel(filename, sheet_index, column_lon=end_lon_column_name,
                                column_lat=end_lat_column_name)
    workbook = xlrd.open_workbook(filename, formatting_info=True)
    columnNum = workbook.sheet_by_index(0).ncols
    xlsc = copy(workbook)
    sheetc = xlsc.get_sheet(0)
    rowindex = 1
    with tqdm(total=len(start_locs)) as pbar:
        for start_loc, end_loc in zip(start_locs, end_locs):
            # for end_loc in tqdm(end_locs):
            url = 'https://restapi.amap.com/v3/direction/walking?origin=' \
                  + start_loc + '&destination=' + end_loc + '&key=' + keyword
            response = requests.get(url)
            text = response.text
            res = json.loads(text)
            if res["status"] == '1':
                sheetc.write(rowindex, columnNum + 1, res["route"]["paths"][0]["distance"])
                sheetc.write(rowindex, columnNum + 2, res["route"]["paths"][0]["duration"])
                sheetc.write(rowindex, columnNum + 3, json.dumps(res["route"]["paths"][0]["steps"], ensure_ascii=False))
                rowindex = rowindex + 1
                time.sleep(random.randint(1000, 2000) / 1000)
            else:
                sheetc.write(rowindex, columnNum + 1, 9999)
                rowindex = rowindex + 1
                continue
            pbar.update(1)
        xlsc.save(outfilename)


# 按间距中的绿色按钮以运行脚本。
if __name__ == '__main__':
    init("数据/metroExcel.xls", "fishnetlon", "fishnetlat", "metrolon", "metrolat", '',
         "数据/metroresult.xls")
