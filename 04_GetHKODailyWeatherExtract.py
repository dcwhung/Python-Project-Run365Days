import os, sys
CONST_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(CONST_ROOT_PATH)

import io, requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import json
from com.lib.fileIO import _checkFileExist
from com.lib.fn_dataframe import _getDataFromDataFrameByPrefix

print('\n' * 5)

CFG_BOOL_DEBUG = False
CFG_TARGET_MONTH = '2021-03'

CONST_HKO_DAILY_WEATHER_EXTRACT_JSON_FILE = './Weather/HKODailyWeatherExtract.json'
curYear = str(datetime.today().year)

# -- Get Daily Extract Data (Begin) -- #
if CFG_BOOL_DEBUG:
    print('*' * 5, ' 01: Getting Weather Daily Extract (Begin) ', '*' * 5)

dfCols = ['Date', 'Max. Temp', 'Avg. Temp', 'Min. Temp', 'Humidity (%)', 'Total Rainfall (mm)', 'Avg. Wind Speed (km/h)']
df = pd.DataFrame(columns=dfCols, dtype=str)

jsonContent = requests.get('https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_' + curYear + '.xml').text
res = json.loads(jsonContent)
for monthData in res['stn']['data']:
    tarMonth = str(monthData['month']) if monthData['month'] >= 10 else '0' + str(monthData['month'])
    dayData = monthData['dayData']
    if len(dayData) > 0:
        for data in dayData:
            data = list(map(str.strip, data))
            if data[0].isdigit():
                tarDateStr = (curYear + '-' + tarMonth + '-' + data[0])

                if CFG_BOOL_DEBUG:
                    print('--> ' + tarDateStr + '...')
                df = df.append({
                    'Date' : tarDateStr,
                    'Max. Temp' : data[2],
                    'Avg. Temp' : data[3],
                    'Min. Temp' : data[4],
                    'Humidity (%)' : data[6],
                    'Total Rainfall (mm)' : data[8],
                    'Avg. Wind Speed (km/h)' : data[11],
                }, ignore_index=True)
    else:
        jsonContent = requests.get('https://www.weather.gov.hk/cis/dailyExtract/dailyExtract_' + curYear + tarMonth + '.xml').text
        try:
            res2 = json.loads(jsonContent)
            for data in res2['stn']['data'][0]['dayData']:
                data = list(map(str.strip, data))
                if data[0].isdigit():
                    tarDateStr = (curYear + '-' + tarMonth + '-' + data[0])

                    if CFG_BOOL_DEBUG:
                        print('--> ' + tarDateStr + '...')
                    df = df.append({
                        'Date' : tarDateStr,
                        'Max. Temp' : data[2],
                        'Avg. Temp' : data[3],
                        'Min. Temp' : data[4],
                        'Humidity (%)' : data[6],
                        'Total Rainfall (mm)' : data[8],
                    }, ignore_index=True)
        except:
            pass

df_dailyExtract = df.set_index('Date').fillna('/')
if CFG_BOOL_DEBUG:
    print(df_dailyExtract)

if CFG_BOOL_DEBUG:
    _getDataFromDataFrameByPrefix(df_dailyExtract, 'Date', CFG_TARGET_MONTH)

# -- Clean up dataframe -- #
if CFG_BOOL_DEBUG:
    print('\n' * 5)
df = df[0:0]
if CFG_BOOL_DEBUG:
    print(df)
    print('\n' * 5)

if CFG_BOOL_DEBUG:
    print('*' * 5, ' 01: Getting Weather Daily Extract (End) ', '*' * 5)
    print('\n' * 5)
# -- Get Daily Extract Data (End) -- #

# -- Get Meteorological Observations Data (Begin) -- #
if CFG_BOOL_DEBUG:
    print('*' * 5, ' 02: Getting Meteorological Observations Data (Begin) ', '*' * 5)

monthRange = pd.period_range(curYear + '-01-01', periods=12, freq='M')
for tarMonth in monthRange:
    tarMonthStr = str(tarMonth)
    htmlContent = requests.get('https://www.weather.gov.hk/wxinfo/pastwx/metob' + tarMonthStr.replace('-', '') + '.htm').text

    bs = BeautifulSoup(htmlContent, "html.parser")
    count = 0
    for tableContent in bs.find_all('table'):
        tableContent
        if count == 0:
            for trContent in tableContent.find_all('tr'):
                tdContent = trContent.find_all('td')
                if len(tdContent) > 0 and tdContent[0].text.isdigit():
                    tarDateStr = tarMonthStr + '-' + (tdContent[0].text.strip() if len(tdContent[0].text.strip()) == 2 else '0' + tdContent[0].text.strip())

                    if CFG_BOOL_DEBUG:
                        print('--> ' + tarDateStr + '...')
                    df = df.append({
                        'Date' : tarDateStr,
                        'Max. Temp' : tdContent[2].text.strip(),
                        'Avg. Temp' : tdContent[3].text.strip(),
                        'Min. Temp' : tdContent[4].text.strip(),
                        'Humidity (%)' : tdContent[6].text.strip(),
                        'Total Rainfall (mm)' : tdContent[8].text.strip(),
                    }, ignore_index=True)
        else:
            for trContent in tableContent.find_all('tr'):
                tdContent = trContent.find_all('td')
                if len(tdContent) > 0 and tdContent[0].text.isdigit():
                    tarDateStr = tarMonthStr + '-' + (tdContent[0].text.strip() if len(tdContent[0].text.strip()) == 2 else '0' + tdContent[0].text.strip())

                    if CFG_BOOL_DEBUG:
                        print('--> ' + tarDateStr + '...')
                    tarIdx = df.index[df['Date'] == tarDateStr].tolist()[0]
                    df.at[tarIdx, 'Avg. Wind Speed (km/h)'] = tdContent[-1].text.strip() # Wind Speed
        count += 1

df_metOb = df.set_index('Date').fillna('/')
if CFG_BOOL_DEBUG:
    print(df_metOb)

if CFG_BOOL_DEBUG:
    _getDataFromDataFrameByPrefix(df_metOb, 'Date', CFG_TARGET_MONTH)

# -- Clean up dataframe -- #
if CFG_BOOL_DEBUG:
    print('\n' * 5)
df = df[0:0]
if CFG_BOOL_DEBUG:
    print(df)
    print('\n' * 5)

if CFG_BOOL_DEBUG:
    print('*' * 5, ' 02: Getting Meteorological Observations Data (End) ', '*' * 5)
    print('\n' * 5)
# -- Get Meteorological Observations Data (End) -- #

# -- Get Daily Extract Data by Kai Tak Station (Begin) -- #
if CFG_BOOL_DEBUG:
    print('*' * 5, ' 03: Getting Kai Tak Station Daily Extract (Begin) ', '*' * 5)

jsonContent = requests.get('https://www.weather.gov.hk/cis/aws/dailyExtract/dailyExtract_SE1_' + curYear + '.xml').text
res = json.loads(jsonContent)
for monthData in res['stn']['data']:
    dayData = monthData['dayData']
    for data in dayData:
        data = list(map(str.strip, data))
        tarDateStr = (curYear
                     + '-' + (str(monthData['month']) if monthData['month'] >= 10 else '0' + str(monthData['month']))
                     + '-' + data[0])

        if CFG_BOOL_DEBUG:
            print('--> ' + tarDateStr + '...')
        df = df.append({
            'Date' : tarDateStr,
            'Max. Temp' : data[2],
            'Avg. Temp' : data[3],
            'Min. Temp' : data[4],
        }, ignore_index=True)

df = df.set_index('Date')

jsonContent = requests.get('https://www.weather.gov.hk/cis/aws/dailyExtract/dailyExtract_SE_' + curYear + '.xml').text

res = json.loads(jsonContent)
for monthData in res['stn']['data']:
    dayData = monthData['dayData']
    for data in dayData:
        data = list(map(str.strip, data))
        tarDateStr = (curYear
                     + '-' + (str(monthData['month']) if monthData['month'] >= 10 else '0' + str(monthData['month']))
                     + '-' + data[0])
        df.at[tarDateStr, 'Total Rainfall (mm)'] = data[-3] # Rainfall
        df.at[tarDateStr, 'Avg. Wind Speed (km/h)'] = round(float(data[-1]) * ((60*60)/1000), 1) # Wind Speed

df_dailyExtract_SE = df.fillna('/')
if CFG_BOOL_DEBUG:
    print(df_dailyExtract_SE)

if CFG_BOOL_DEBUG:
    _getDataFromDataFrameByPrefix(df_dailyExtract_SE, 'Date', CFG_TARGET_MONTH)

# -- Clean up dataframe -- #
if CFG_BOOL_DEBUG:
    print('\n' * 5)
df = df[0:0]
if CFG_BOOL_DEBUG:
    print(df)
    print('\n' * 5)

# -- Get Wind Speed Data (Begin) -- #
# retContent = requests.get('https://data.weather.gov.hk/cis/csvfile/SE/' + curYear + '/daily_SE_WSPD_' + curYear + '.csv').text
# df_wspd = pd.read_csv(io.StringIO(retContent), skiprows=2, skipfooter=4, engine='python')
# dfCols_wspd = df_wspd.columns.tolist()
#
# df_wspd.insert(0, 'Date',
#     df_wspd[dfCols_wspd[0]].astype(str)
#     + '-' +
#     df_wspd[dfCols_wspd[1]].astype(str).apply(lambda row: row if len(row) == 2 else '0' + row)
#     + '-' +
#     df_wspd[dfCols_wspd[2]].astype(str).apply(lambda row: row if len(row) == 2 else '0' + row)
# )
#
# df_wspd = df_wspd.rename(columns={dfCols[3] : 'Mean Wind Speed (m/s)'})
# df_wspd = df_wspd.drop([dfCols[0], dfCols[1], dfCols[2], dfCols[-1]], axis=1)
# print(df_wspd)
# -- Get Wind Speed Data (End) -- #

if CFG_BOOL_DEBUG:
    print('*' * 5, ' 03: Getting Kai Tak Station Daily Extract (End) ', '*' * 5)
    print('\n' * 5)
# -- Get Daily Extract Data by Kai Tak Station (End) -- #

# -- Get Sun & Moon Rise & Set Data (Begin) -- #
if CFG_BOOL_DEBUG:
    print('*' * 5, ' 04: Sun & Moon Rise & Set Data (Begin)', '*' * 5)

CONST_HKO_WEATHER_API = 'https://data.weather.gov.hk/weatherAPI/opendata/opendata.php'

retContent = requests.get(CONST_HKO_WEATHER_API, params=dict(
    dataType = 'SRS',
    year = curYear,
    rformat = 'json',
)).text.replace('fields', 'columns')

df_sunData = pd.read_json(retContent, orient='split')
dfCols = df_sunData.columns.tolist() # -- 'YYYY-MM-DD', 'RISE', 'TRAN.', 'SET' --
df_sunData = df_sunData.rename(columns={
    dfCols[0] : 'Date',
    dfCols[1] : 'Sunrise',
    dfCols[2] : 'Solar Noon',
    dfCols[3] : 'Sunset',
})
# print(df_sunData)

retContent = requests.get(CONST_HKO_WEATHER_API, params=dict(
    dataType = 'MRS',
    year = curYear,
    rformat = 'json',
)).text.replace('fields', 'columns')

df_moonData = pd.read_json(retContent, orient='split')
dfCols = df_moonData.columns.tolist() # -- 'YYYY-MM-DD', 'RISE', 'TRAN.', 'SET' --
df_moonData = df_moonData.rename(columns={
    dfCols[0] : 'Date',
    dfCols[1] : 'Moonrise',
    dfCols[2] : 'Moon Transit',
    dfCols[3] : 'Moonset',
})
# print(df_moonData)

df_sunMoonRiseSet = pd.merge(df_sunData, df_moonData, on=['Date'])
if CFG_BOOL_DEBUG:
    print(df_sunMoonRiseSet)

if CFG_BOOL_DEBUG:
    print('*' * 5, ' 04: Sun & Moon Rise & Set Data (End) ', '*' * 5)
    print('\n' * 5)
# -- Get Sun & Moon Rise & Set Data (End) -- #

# -- Combine Daily Data (Begin) -- #
if CFG_BOOL_DEBUG:
    print('*' * 5, ' 05: Combine Daily Data (Begin) ', '*' * 5)

df = pd.merge(df_dailyExtract.reset_index(), df_sunMoonRiseSet.reset_index(), how='left', on=['Date'])
df = df.drop(['index'], axis=1)
print(df)

if CFG_BOOL_DEBUG:
    print('*' * 5, ' 05: Combine Daily Data (End) ', '*' * 5)
    print('\n' * 5)
# -- Combine Daily Data (End) -- #

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing HKO Daily Weather Extrat to Data File (Begin) ' + '-' * 5)
df.to_json(CONST_HKO_DAILY_WEATHER_EXTRACT_JSON_FILE, orient='records', lines=True)
if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing HKO Daily Weather Extrat to Data File (End) ' + '-' * 5)
