import os, sys
CONST_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(CONST_ROOT_PATH)

import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from com.lib.fn_dataframe import _getDataFromDataFrameByPrefix

print('\n' * 5)

CFG_BOOL_DEBUG = False

CONST_SUNMOON_RISESET_HISTORY_JSON_FILE = './Weather/SunMoonRiseSetHistory.json'

curYear = datetime.today().year
# numOfDays = (datetime(day=1, month=1, year=curYear+1) - datetime(day=1, month=1, year=curYear)).days
# dateRange = pd.date_range(str(curYear) + '-01-01', periods=numOfDays, freq='D')
monthRange = pd.period_range(str(curYear) + '-01-01', periods=12, freq='M')

# -- Collecting Sun Rise & Set History from Online (Begin) -- #//
if CFG_BOOL_DEBUG:
    print('\n' * 5)
    print('-' * 5 + ' Collecting Sun Rise & Set History from Online (Begin) ' + '-' * 5)

dfCols_sunData = ['Date', 'Sunrise', 'Solar Noon', 'Sunset', 'Daylength']
df_sunData = pd.DataFrame(columns=dfCols_sunData, dtype=str)

for tarMonth in monthRange:
    tarMonthStr = str(tarMonth)
    if CFG_BOOL_DEBUG:
        print('--> ' + tarMonthStr + '...')

    htmlContent = requests.get('https://timeanddate.com/sun/hong-kong/hong-kong', params=dict(
        year = tarMonthStr[:4],
        month = tarMonthStr[-2:],
    )).text

    bs = BeautifulSoup(htmlContent, "html.parser")
    tableContent = bs.find('table', {'id' : 'as-monthsun'})
    # print(tableContent)
    for trContent in tableContent.find_all('tr'):
        thContent = trContent.find_all('th')
        if len(thContent) == 1:
            thContentStr = thContent[0].text.strip()
            tarDateStr = tarMonthStr + '-' + (thContentStr if len(thContentStr) == 2 else '0' + thContentStr)

            tdContent = trContent.find_all('td')

            if CFG_BOOL_DEBUG:
                print('--> ' + tarDateStr + '...')
            df_sunData = df_sunData.append({
                dfCols_sunData[0] : tarDateStr,
                dfCols_sunData[1] : tdContent[0].text.strip()[:5],
                dfCols_sunData[2] : tdContent[10].text.strip()[:5],
                dfCols_sunData[3] : tdContent[1].text.strip()[:5],
                dfCols_sunData[4] : tdContent[2].text.strip(),
            }, ignore_index=True)

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Collecting Sun Rise & Set History from Online (End) ' + '-' * 5)
# -- Collecting Sun Rise & Set History from Online (End) -- #//

# -- Collecting Moon Rise & Set History from Online (Begin) -- #//
if CFG_BOOL_DEBUG:
    print('\n' * 5)
    print('-' * 5 + ' Collecting Moon Rise & Set History from Online (Begin) ' + '-' * 5)

dfCols_moonData = ['Date', 'Moonrise', 'Moon Transit', 'Moonset', 'Illumination']
df_moonData = pd.DataFrame(columns=dfCols_moonData, dtype=str)

for tarMonth in monthRange:
    tarMonthStr = str(tarMonth)
    if CFG_BOOL_DEBUG:
        print('--> ' + tarMonthStr + '...')

    htmlContent = requests.get('https://timeanddate.com/moon/hong-kong/hong-kong', params=dict(
        year = tarMonthStr[:4],
        month = tarMonthStr[-2:],
    )).text

    bs = BeautifulSoup(htmlContent, "html.parser")
    tableContent = bs.find('table', {'id' : 'tb-7dmn'})
    # print(tableContent)
    for trContent in tableContent.find_all('tr'):
        thContent = trContent.find_all('th')
        if len(thContent) == 1:
            thContentStr = thContent[0].text.strip()
            tarDateStr = tarMonthStr + '-' + (thContentStr if len(thContentStr) == 2 else '0' + thContentStr)

            tdContent = trContent.find_all('td')

            riseTime, transitTime, setTime = '/', '/', '/'

            tarIdx = 0
            if tdContent[tarIdx].has_attr('colspan') and int(tdContent[tarIdx]['colspan']) == 2:
                tarIdx +=1
            else:
                riseTime = tdContent[tarIdx].text.strip()
                tarIdx += 2

            if tdContent[tarIdx].has_attr('colspan') and int(tdContent[tarIdx]['colspan']) == 2:
                tarIdx +=1
            else:
                setTime = tdContent[tarIdx].text.strip()
                tarIdx += 2

            if tdContent[tarIdx].has_attr('colspan') and int(tdContent[tarIdx]['colspan']) == 2:
                tarIdx +=1
            else:
                riseTime = tdContent[tarIdx].text.strip()
                tarIdx += 2

            if tdContent[-1].has_attr('colspan') and int(tdContent[-1]['colspan']) == 4:
                illumination = '100.0%'
            else:
                transitTime = tdContent[-4].text.strip()
                illumination = tdContent[-1].text.strip()

            if CFG_BOOL_DEBUG:
                print('--> ' + tarDateStr + '...')
            df_moonData = df_moonData.append({
                dfCols_moonData[0] : tarDateStr,
                dfCols_moonData[1] : riseTime,
                dfCols_moonData[2] : transitTime,
                dfCols_moonData[3] : setTime,
                dfCols_moonData[4] : illumination,
            }, ignore_index=True)

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Collecting Moon Rise & Set History from Online (End) ' + '-' * 5)
# -- Collecting Moon Rise & Set History from Online (End) -- #//

df = pd.merge(df_sunData, df_moonData, on='Date')
print('\n' * 5)
print(df)
print('\n' * 5)

_getDataFromDataFrameByPrefix(df, 'Date', '2021-11')

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing Sun/Moon Rise & Set History to Data File (Begin) ' + '-' * 5)
df.to_json(CONST_SUNMOON_RISESET_HISTORY_JSON_FILE, orient='records', lines=True)
if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing Sun/Moon Rise & Set History to Data File (End) ' + '-' * 5)
