import os, sys
CONST_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(CONST_ROOT_PATH)

import numpy as np
import pandas as pd
from datetime import datetime, date
from com.lib.fileIO import _checkFileExist
from com.lib.fn_dataframe import _getDataFromDataFrameByPrefix

from matplotlib import pyplot as plt

print('\n' * 10)

CFG_BOOL_DEBUG = False

CONST_DAILY_WEIGHT_EXTRACT_JSON_FILE = './DailyWeightExtract.json'
curYear = str(datetime.today().year)
dateRange = pd.date_range(curYear + '-01-01', datetime.today().strftime('%Y-%m-%d'))

CONST_HEIGHT = 170
dataWeekday, dataWeight = [], []

count = 0

CONST_DAILY_WEIGHT_DATAFILE = './' + curYear + '_DailyWeight.txt'
if _checkFileExist(CONST_DAILY_WEIGHT_DATAFILE):
    with open(CONST_DAILY_WEIGHT_DATAFILE) as f:
        if CFG_BOOL_DEBUG:
            print('\n' * 5)
            print('-' * 5 + ' Collecting Weight Data (Begin) ' + '-' * 5)

        for line in f:
            content = line.replace('\n', '')
            tarIdx = content.find('lbs')
            if tarIdx > 0:
                dataWeight.append(float(content[:tarIdx].strip()))

                tarDateObj = None
                tarIdx = content.find('(')
                if tarIdx > 0:
                    tarDate = content[tarIdx+1:content.find(')')].split('/')
                    if len(tarDate[0]) == 1:
                        tarDate[0] = '0' + tarDate[0]
                    if len(tarDate[1]) == 1:
                        tarDate[1] = '0' + tarDate[1]
                    tarDateObj = datetime.strptime(curYear + tarDate[1] + tarDate[0], '%Y%m%d')
                print(dateRange[count], dataWeight[count], tarDateObj)
                count += 1

    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Collecting Weight Data (End) ' + '-' * 5)
        print('\n' * 5)

    # -- Daily Weight Records (Begin) -- #
    # -- Fill up those date without any record yet --
    dataWeight.extend([None] * (len(dateRange) - len(dataWeight)))
    dataSet = {'Date' : dateRange, 'Weight_(lbs)' : dataWeight}

    df = pd.DataFrame(dataSet)
    df['Date'] = pd.to_datetime(df['Date'])

    df.insert(0, 'Day', df.index.map(lambda x: x+1))
    df.insert(2, 'Month', df['Date'].dt.month)
    df.insert(3, 'Weekday', df.apply(lambda row: str(row['Date'].weekday()) + ' - ' + row['Date'].strftime('%A').upper()[:3], axis=1))
    df['Weight_(kg)'] = df.apply(lambda row: row['Weight_(lbs)'] * 0.454, axis=1)
    df['BMI'] = df.apply(lambda row: row['Weight_(kg)'] / ((CONST_HEIGHT/100)**2), axis=1)
    df['+/-'] = df['Weight_(lbs)'].diff(periods=1).map(lambda x: '+' if x > 0 else '-' if x < 0 else '/')
    df['%'] = df['Weight_(lbs)'].pct_change().mul(100)
    df = df.fillna('/')

    print('\n' * 5)
    print(df)
    print('\n' * 5)

    # plt.plot(df['Date'], df['Weight_(lbs)'])
    # plt.xticks(rotation=90)
    # plt.show()
    # -- Daily Weight Records (End) -- #

    df_weight = df['Weight_(lbs)'].apply(lambda x: np.nan if x == '/' else x).dropna().describe()
    print(df_weight[['min', 'max', 'mean']])
    print('Weight drop Max. %: {}'.format(round(pd.Series([df_weight['max'], df_weight['min']]).pct_change().mul(100)[1], 2)))
    print('Weight drop Cur. %: {}'.format(round(pd.Series([df_weight['max'], df['Weight_(lbs)'].iloc[-1]]).pct_change().mul(100)[1], 2)))
    print('\n' * 5)

    # -- Weekday Weight Summary (Begin) -- #
    df_wkdaySummary = df.groupby(['Weekday', '+/-'])
    df_wkdaySummary = df_wkdaySummary[['Weight_(lbs)']].count()
    df_wkdaySummary = df_wkdaySummary.reset_index()
    df_wkdaySummary = df_wkdaySummary.pivot(index='+/-', columns='Weekday', values='Weight_(lbs)')
    print(df_wkdaySummary)
    print('\n' * 5)
    # -- Weekday Weight Summary (End) -- #

    # -- Monthly Weight Summary (Begin) -- #
    df_mthSummary = df.groupby(['Month', '+/-'])
    df_mthSummary = df_mthSummary[['Weight_(lbs)']].count()
    df_mthSummary = df_mthSummary.reset_index()
    df_mthSummary = df_mthSummary.pivot(index='+/-', columns='Month', values='Weight_(lbs)')
    print(df_mthSummary)
    print('\n' * 5)

    labels = [date(1900, x, 1).strftime('%b') for x in df_mthSummary.columns.values.tolist()]
    plus_total = np.array(df_mthSummary.loc['+'].tolist())
    minus_total = np.array(df_mthSummary.loc['-'].tolist())
    nochge_total = np.array(df_mthSummary.loc['/'].tolist())
    width = 0.35       # the width of the bars: can also be len(x) sequence

    fig, ax = plt.subplots()
    ax.bar(labels, plus_total, width, color='r', label='+')
    ax.bar(labels, minus_total, width, color='g', bottom=plus_total, label='-')
    ax.bar(labels, nochge_total, width, color='tab:orange', bottom=plus_total+minus_total, label='/')
    ax.set_ylabel('Days')
    ax.legend()
    plt.show()


    df_monthlySummary = df.groupby(df['Month'])['Weight_(lbs)'].mean().reset_index()
    df_monthlySummary['Month'] = df_monthlySummary.apply(lambda row: date(1900, int(row['Month']), 1).strftime('%b'), axis=1)
    print(df_monthlySummary)
    print('\n' * 5)

    plt.plot(df_monthlySummary['Month'], df_monthlySummary['Weight_(lbs)'])
    plt.rcParams['figure.facecolor'] = 'black'
    plt.xticks(rotation=90)
    # -- Monthly Weight Summary (End) -- #

    # fig, ax = plt.subplots(2)
    # ax[0].plot(df['Date'], df['Weight_(lbs)'])
    # # ax[0].xticks(rotation=90)
    #
    # ax[1].plot(df_monthlySummary['Month'], df_monthlySummary['Weight_(lbs)'])
    # ax[1].scatter(['Jan', 'Dec'], [154.8, 125.6])
    # # ax[1].xticks(rotation=90)
    plt.show()
    exit()

    labels = [x[4:] for x in df_wkdaySummary.columns.values.tolist()]
    plus_total = np.array(df_wkdaySummary.loc['+'].tolist())
    minus_total = np.array(df_wkdaySummary.loc['-'].tolist())
    nochge_total = np.array(df_wkdaySummary.loc['/'].tolist())
    width = 0.35       # the width of the bars: can also be len(x) sequence

    fig, ax = plt.subplots()
    ax.bar(labels, plus_total, width, color='r', label='+')
    ax.bar(labels, minus_total, width, color='g', bottom=plus_total, label='-')
    ax.bar(labels, nochge_total, width, color='tab:orange', bottom=plus_total+minus_total, label='/')
    ax.set_ylabel('Days')
    ax.legend()
    plt.show()

    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Writing Daily Weight Records to Data File (Begin) ' + '-' * 5)
    df.to_json(CONST_DAILY_WEIGHT_EXTRACT_JSON_FILE, orient='records', lines=True)
    if CFG_BOOL_DEBUG:
        print('-' * 5 + ' Writing Daily Weight Records to Data File (End) ' + '-' * 5)
