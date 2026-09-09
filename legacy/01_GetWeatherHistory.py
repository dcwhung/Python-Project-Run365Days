import os, sys
CONST_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(CONST_ROOT_PATH)

import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from com.lib.fileIO import _checkFileExist
from com.lib.fn_dataframe import _getDataFromDataFrameByPrefix

print('\n' * 5)

CFG_BOOL_DEBUG = False

CONST_WEATHER_DESCRIPTION = {
    '1': 'Clear weather',
    '2': 'Few clouds',
    '3': 'Partly cloudy skies',
    '4': 'Cloudy skies',
    '7': 'Rain',
    '10': 'Thunderstorm',
    '26': 'Snow',
    '28': 'Snowstorm'
}

CONST_WEATHER_HISTORY_JSON_FILE = './Weather/WeatherHistory.json'

dfCols = ['Date', 'Time', 'Temperature (°C)', 'Wind (Km/h)', 'Humidity (%)', 'Description']

start_date = str(datetime.today().year) + '-01-01'
df = pd.DataFrame(columns=dfCols, dtype=str)
if _checkFileExist(CONST_WEATHER_HISTORY_JSON_FILE):
    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Loading Weather History Data File (Begin) ' + '-' * 5)

    df = pd.read_json(CONST_WEATHER_HISTORY_JSON_FILE, orient='records', lines=True)
    if df.shape[0] > 0:
        df['Date'] = df['Date'].astype('str').str[0:10]
        start_date = df['Date'].iloc[-1]

    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Loading Weather History Data File (End) ' + '-' * 5)
        print('\n' * 5)

end_date = datetime.today().strftime('%Y-%m-%d')
dateRange = pd.date_range(start_date, end_date)

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Collecting Weather History Data Online (Begin) ' + '-' * 5)

for tarDate in dateRange:
    tarDateStr = tarDate.strftime('%Y-%m-%d')
    if CFG_BOOL_DEBUG:
        print('--> ' + tarDateStr + '...')

    htmlContent = requests.get('https://freemeteo.hk/weather/hong-kong/history/daily-history/', params={
        'gid' : '1819729',
        'station' : '10400',
        'language' : 'english',
        'country' : 'hong-kong',
        'date' : tarDateStr,
    }).text

    soup = BeautifulSoup(htmlContent, 'html.parser')
    dailyHistoryTbl = soup.find_all('table', {'class': 'daily-history'})

    if len(dailyHistoryTbl) == 1:
        dailyHistoryTbl = dailyHistoryTbl[0]

        numOfItems = 0
        for trContent in dailyHistoryTbl.find_all('tr'):
            thContent = trContent.find_all('th')
            if len(thContent) > 0:
                numOfItems = len(thContent)
                # print('----Header----')
                # for header in thContent:
                #     print(header.text)
            else:
                # print('----Data----')
                tdContent = trContent.find_all('td')
                if len(tdContent) > 0 and len(tdContent) == numOfItems:
                    deScript = str(tdContent[9].find('script').string)

                    tarDateTime = [tarDateStr, tdContent[0].text.strip()]
                    if df.query('@tarDateTime[0] == Date and @tarDateTime[1] == Time').empty:
                        df = df.append({
                            'Date' : tarDateTime[0],
                            'Time' : tarDateTime[1],
                            'Temperature (°C)' : tdContent[1].text.strip()[:-2],
                            'Wind (Km/h)' : tdContent[3].text[tdContent[3].text.find('°')+1:].replace('Variable at ', '')[:-5],
                            'Humidity (%)' : tdContent[5].text.strip()[:-1],
                            'Description' : CONST_WEATHER_DESCRIPTION[deScript[deScript.find('n(', )+2:deScript.find(', \'CurrentWeather')]],
                        }, ignore_index=True)

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Collecting Weather History Data Online (End) ' + '-' * 5)

print('\n' * 5)
print(df)
print('\n' * 5)

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing Weather History to Data File (Begin) ' + '-' * 5)
df.to_json(CONST_WEATHER_HISTORY_JSON_FILE, orient='records', lines=True)
if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing Weather History to Data File (End) ' + '-' * 5)
