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

# -- Loading Weather Warning & Signal from HKO Online (Begin) -- #//
CONST_WARNING_AND_SIGNAL = {}

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Loading Weather Warning & Signal from HKO Online (Begin) ' + '-' * 5)

bs = BeautifulSoup(requests.get('https://www.hko.gov.hk/en/wxinfo/climat/warndb/warndba.shtml').text, "html.parser")
for tableContent in bs.find_all(class_='self_row2_table'):
    tdContent = tableContent.find_all('td')
    for img in tdContent[0].find_all('img'):
        imgSrc = img.get('src')
        CONST_WARNING_AND_SIGNAL[img.get('alt').lower().title()] = {
            'Idx' : imgSrc[imgSrc.rfind('/')+1:imgSrc.rfind('.')],
            'Type' : tdContent[1].text.strip(),
        }

if CFG_BOOL_DEBUG:
    print(CONST_WARNING_AND_SIGNAL)

    print('-' * 5 + ' Loading Weather Warning & Signal from HKO Online (End) ' + '-' * 5)
    print('\n' * 5)
# -- Loading Weather Warning & Signal from HKO Online (End) -- #//

# -- Preparing Weather Warning History dataframe from data file if any (Begin) -- #
CONST_WEATHER_WARNING_HISTORY_JSON_FILE = './Weather/WeatherWarningHistory.json'

dfCols = ['Date', 'Type', 'Warning_Signal', 'Start_Time', 'End_Time', 'Ico']

start_date = str(datetime.today().year) + '-01-01'
df = pd.DataFrame(columns=dfCols, dtype=str)
if _checkFileExist(CONST_WEATHER_WARNING_HISTORY_JSON_FILE):
    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Loading Weather Warning History Data File (Begin) ' + '-' * 5)

    df = pd.read_json(CONST_WEATHER_WARNING_HISTORY_JSON_FILE, orient='records', lines=True)
    if df.shape[0] > 0:
        df['Date'] = df['Date'].astype('str').str[0:10]
        start_date = df['Date'].iloc[-1]
    else:
        df = pd.DataFrame(columns=dfCols, dtype=str)

    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Loading Weather Warning History Data File (End) ' + '-' * 5)
        print('\n' * 5)

end_date = datetime.today().strftime('%Y-%m-%d')
dateRange = pd.date_range(start_date, end_date)
# -- Preparing Weather Warning History dataframe from data file if any (End) -- #

# -- Collecting Weather Warning History Data Online (Begin) -- #
if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Collecting Weather Warning History Data Online (Begin) ' + '-' * 5)

for tarDate in dateRange:
    tarDateStr = tarDate.strftime('%Y-%m-%d')
    if CFG_BOOL_DEBUG:
        print('--> ' + tarDateStr + '...')

    htmlContent = requests.get('https://www.hko.gov.hk//cgi-bin/climat/warndb_ea.pl', params=dict(
        start_ym = tarDate.strftime('%Y%m%d')
    )).text

    begStr = 'Tropical Cyclone Warning_Signals'
    bs = BeautifulSoup(htmlContent[htmlContent.find(begStr) + len(begStr):], "html.parser")
    for tableContent in bs.find_all('table'):
        # print(tableContent)
        for trContent in tableContent.find_all('tr'):
            tdContent = trContent.find_all('td')
            if len(tdContent) > 0 and len(tdContent) == 6:
                # print('-----Data-----')
                tarWarning = tdContent[1].text.strip().upper()
                tarPeriod = [
                    str(datetime.strptime(tdContent[3].text.strip() + ' ' + tdContent[2].text.strip(), '%d/%b/%Y %H:%M')),
                    str(datetime.strptime(tdContent[5].text.strip() + ' ' + tdContent[4].text.strip(), '%d/%b/%Y %H:%M'))
                ]
                # print(tarPeriod)

                if df.query('@tarDateStr == Date and @tarWarning == Warning_Signal and @tarPeriod[0] == Start_Time and @tarPeriod[1] == End_Time').empty:
                    df = df.append({
                        'Date' : tarDateStr,
                        'Type' : CONST_WARNING_AND_SIGNAL[tdContent[1].text.strip().lower().title()]['Type'],
                        'Warning_Signal' : tarWarning,
                        'Start_Time' : str(tarPeriod[0]),
                        'End_Time' : str(tarPeriod[1]),
                        'Ico' : tdContent[0].find('img').get('src'),
                    }, ignore_index=True)
    # exit()
if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Collecting Weather Warning History Data Online (End) ' + '-' * 5)

print('\n' * 5)
print(df)
print('\n' * 5)
# -- Collecting Weather Warning History Data Online (End) -- #

if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing Weather Warning History to Data File (Begin) ' + '-' * 5)
df.to_json(CONST_WEATHER_WARNING_HISTORY_JSON_FILE, orient='records', lines=True)
if CFG_BOOL_DEBUG:
    print('-' * 5 + ' Writing Weather Warning History to Data File (End) ' + '-' * 5)
