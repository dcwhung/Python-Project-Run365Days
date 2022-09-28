import os, sys
CONST_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(CONST_ROOT_PATH)

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import dateutil.parser, pytz, calendar
import com.lib.fileIO as fio
from com.lib.fn_dataframe import _getDataFromDataFrameByPrefix

import math
from matplotlib import pyplot as plt

import xml.etree.ElementTree as et
from bs4 import BeautifulSoup

print('\n' * 10)

CFG_BOOL_DEBUG = False
CFG_BOOL_SHOW_OVERALL_SUMMARY = False
CFG_GARMIN_DIR = './Garmin/'
CFG_DATA_DIR = CFG_GARMIN_DIR + '::DATA_SRC::/'
CFG_DATA_JSON_FILE = CFG_GARMIN_DIR + 'run365_::DATA_SRC::_data.json'

today = datetime.today()
curYear = today.year

# -- Function for getting datetime under provided time zone & specific time string format --
def getTimeUnderTimeZone(recTime, timezone='Asia/Hong_Kong'):
    # -- Target to return datetime object in format 2021-10-17 06:10:56+08:00 -- #
    # dateStr = '2021-10-16T22:10:56.000Z'
    # print(dateStr, ' ==> ', getTimeUnderTimeZone(dateStr))
    # dateStr = '2021-10-17T06:10:56+08:00'
    # print(dateStr, ' ==> ', getTimeUnderTimeZone(dateStr))
    # dateStr = '1634451056000'
    # print(dateStr, ' ==> ', getTimeUnderTimeZone(dateStr))
    # dateStr = '2021-10-17 06:10:56'
    # print(dateStr, ' ==> ', getTimeUnderTimeZone(dateStr))

    recTime = recTime.strip()
    if 'T' in recTime:
        if '.' in recTime and recTime[-1] == 'Z': # -- Format like: 2021-10-16T22:10:56.000Z --
            return dateutil.parser.parse(recTime).replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(timezone))
        elif '+' in recTime: # -- Format like: 2021-10-17T06:10:56+08:00 --
            return dateutil.parser.parse(recTime)
    elif recTime.isdigit() and len(recTime) == 13: # -- Format like: 1634451056000 --
        return datetime.utcfromtimestamp(round(int(recTime)/1000, 1)).replace(tzinfo=pytz.timezone(timezone))
    else: # -- Format like: 2021-10-17 06:10:56 --
        return datetime.strptime(recTime, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.timezone(timezone))

# -- Function for getting Haversine Distance by Latitude & Longitude of 2 points --
def getHaversineDistance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination

    if None in origin or None in destination:
        return np.nan

    # -- Radius of the Earth (km) --
    r = 6371

    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)

    # -- Change in coordinates --
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    # -- Haversine formula --
    a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2)**2
    distance = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))

    return distance

while True:
    print('Please select data type (1: GPX, 2: KML, 3: TCX): ')
    checkDataTypeIdx = input()
    if checkDataTypeIdx in ['c', 1, 2, 3]:
        break
    else:
        print('!! Please select again !!')

# -- Prepare date info dataframe (Begin) -- #
df = pd.DataFrame({'Date' : pd.date_range(str(curYear) + '-01-01', today.strftime('%Y-%m-%d'))})
df['Month'] = df['Date'].dt.month_name() #strftime('%B')
df['Weekday'] = df['Date'].dt.day_name() #strftime('%A')
df['Quarter'] = df['Date'].dt.quarter
# df['WkOfYr'] = df['Date'].dt.isocalendar().week
dateInfo = {'Basic' : df}

for item in ['Month', 'Weekday', 'Quarter']:
    dateInfo[item] = df.groupby(df[item])['Date'].count().reset_index(name='No. of Days')
# print(dateInfo)
# exit()
df_trackSummary = {'GPX' : None, 'KML' : None, 'TCX' : None}
# -- Prepare date info dataframe (End) -- #

# -- Getting Running Data from GPX files (Begin) -- #
if checkDataTypeIdx in ['c', 1]:
    if CFG_BOOL_DEBUG:
        print('\n' * 5)
    print('-' * 5 + ' Collecting GPX Data (Begin) ' + '-' * 5)

    tarJSONfile = CFG_DATA_JSON_FILE.replace('::DATA_SRC::', 'gpx')
    fldrInfo = fio._getFolderInfo(CFG_DATA_DIR.replace('::DATA_SRC::', 'gpx'))
    if len(fldrInfo['files']) > 0:
        print('=' * 50)
        ns = {'ns' : 'http://www.topografix.com/GPX/1/1',
            'ns3' : 'http://www.garmin.com/xmlschemas/TrackPointExtension/v1'}

        dfCols = ['Date', 'Activity ID', 'Total Time', 'Distance By Coord.',
                    'Min. Ele', 'Max. Ele',
                    'Avg. Cad', 'Max. Cad',
                    'Avg. Temp', 'Min. Temp', 'Max. Temp',
                    'No. of Track Points', 'Track Info']
        df = pd.DataFrame(columns=dfCols)
        for filePath in fldrInfo['files']:
            tree = et.parse(filePath)
            root = tree.getroot()

            fileId = filePath[(filePath.rfind('_')+1):filePath.rfind('.')]

            metadata = root.find('ns:metadata', ns)
            actTime = getTimeUnderTimeZone(metadata.find('ns:time', ns).text)

            if actTime.year < curYear:
                fio._removeFile(filePath, boolTakeLog=False)
            else:
                trk = root.find('ns:trk', ns)
                if trk.find('ns:type', ns).text == 'running':
                    print(fileId, actTime)
                    trkpts = trk.findall('.//ns:trkpt', ns)

                    # -- Collect track information from file --
                    trkInfo = {'coord' : [], 'ele' : [], 'time' : [], 'temp' : [], 'cad' : []}
                    totalTime, begTime, endTime = None, None, None
                    if len(trkpts) > 0:
                        for trkpt in trkpts:
                            endTime = getTimeUnderTimeZone(trkpt.find('ns:time', ns).text)
                            if begTime == None:
                                begTime = endTime

                            trkInfo['coord'].append((float(trkpt.get('lat')), float(trkpt.get('lon'))))
                            trkInfo['ele'].append(float(trkpt.find('ns:ele', ns).text))
                            trkInfo['time'].append(endTime.strftime('%Y-%m-%d %H:%M:%S'))
                            trkInfo['temp'].append(float(trkpt.find('.//ns3:atemp', ns).text))
                            trkInfo['cad'].append(int(trkpt.find('.//ns3:cad', ns).text))

                    if begTime != None and endTime != None:
                        totalTime = str(timedelta(seconds=(endTime - begTime).total_seconds())).split('.')[0]

                    # -- Get total distance of whole track --
                    numOfTrkPts = 0
                    totalDistanceByCoord = 0
                    if len(trkInfo['coord']) >= 2:
                        df_tmp = pd.DataFrame({'Cur Coord' : trkInfo['coord']})
                        df_tmp['Nxt Coord'] = df_tmp['Cur Coord'].shift(-1)
                        df_tmp = df_tmp.dropna()
                        df_tmp['Distance'] = df_tmp.apply(lambda row: getHaversineDistance(row['Cur Coord'], row['Nxt Coord']), axis=1)
                        numOfTrkPts = df_tmp['Nxt Coord'].count()
                        totalDistanceByCoord = df_tmp['Distance'].sum()

                    # -- Get elevation summary --
                    elevation = pd.Series(trkInfo['ele'], dtype='float64').describe()[['min', 'max']].to_dict()

                    # -- Get cadence summary --
                    cadence = {'mean' : np.nan, 'max': np.nan}
                    df_tmp = pd.DataFrame({'Clock Time' : pd.to_datetime(trkInfo['time'])})
                    if not df_tmp.empty:
                        df_tmp['Race Time'] = df_tmp['Clock Time'] - df_tmp['Clock Time'][0]
                        df_tmp['Cadence'] = pd.Series(trkInfo['cad'])*2
                        cadence = pd.Series(df_tmp['Cadence']).describe()[['mean', 'max']].to_dict()

                    # -- Get Temperature summary --
                    temperature = pd.Series(trkInfo['temp'], dtype='float64').describe()[['mean', 'min', 'max']].to_dict()

                    df = df.append({
                        'Date' : actTime.strftime('%Y-%m-%d %H:%M:%S'),
                        'Activity ID' : fileId,
                        'Total Time' : totalTime,
                        'Distance By Coord.' : totalDistanceByCoord,
                        'Min. Ele' : elevation['min'],
                        'Max. Ele' : elevation['max'],
                        'Avg. Cad' : cadence['mean'],
                        'Max. Cad' : cadence['max'],
                        'Avg. Temp' : temperature['mean'],
                        'Min. Temp' : temperature['min'],
                        'Max. Temp' : temperature['max'],
                        'No. of Track Points' : numOfTrkPts,
                        'Track Info' : trkInfo,
                    }, ignore_index=True)

                    # -- Move file to backup folder once completed reading the file --
                    backupFldrPath = fldrInfo['parent'] + 'bak/'
                    fio._createFolder(backupFldrPath)
                    fio._moveFile(filePath, filePath.replace(fldrInfo['parent'], backupFldrPath))
                else:
                    # -- Remove non running record file --
                    fio._removeFile(filePath, boolTakeLog=False)

        df.insert(0, 'Day', df.index.map(lambda x: x+1))
        df.to_json(tarJSONfile, orient='records', lines=True)

        if CFG_BOOL_DEBUG:
            print('=' * 50)
            print(df)

    if fio._checkFileExist(tarJSONfile):
        df = pd.read_json(tarJSONfile, orient='records', lines=True)
        if CFG_BOOL_DEBUG:
            dfCols = df.columns.tolist()
            print(dfCols)
            print('=' * 50)
            print(df)

    if CFG_BOOL_DEBUG:
        print('=' * 50)
        print(df.dtypes)
        print('\n' * 5)

    # df['Month'] = df['Date'].dt.month_name()
    # df['Weekday'] = df['Date'].dt.day_name()
    df['TotalSec'] = df.apply(lambda row: (datetime.strptime(row['Total Time'], '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds() if row['Total Time'] != None else 0, axis=1)
    df['Pacing'] = df.apply(lambda row: str(timedelta(seconds=row['TotalSec']/row['Distance By Coord.'])).split('.')[0] if row['Distance By Coord.'] > 0 else np.nan, axis=1)

    df = df[['Date', 'Day', 'Activity ID',
                # 'Month', 'Weekday',
                'Total Time', 'TotalSec', 'Distance By Coord.', 'Pacing',
                'Avg. Cad', 'Max. Cad',
                'Min. Ele', 'Max. Ele',
                'Avg. Temp', 'Min. Temp', 'Max. Temp',
                'No. of Track Points']]

    # -- Track Summary (Begin) -- #
    # df_trackSummary['GPX'] = df.set_index('Date')
    df_trackSummary['GPX'] = df
    if CFG_BOOL_DEBUG:
        print('=' * 50)
        print(df_trackSummary['GPX'])
    # -- Track Summary (End) -- #

    # -- Overall Summary (Begin) -- #
    if CFG_BOOL_SHOW_OVERALL_SUMMARY:
        diff = np.nan
        if df['Date'][df['Date'] == '/'].count() == 0:
            # -- Make sure just use the date part ONLY instead of together with time part --
            begDate = pd.to_datetime(str(df['Date'].head(1).values[0])[:10])
            endDate = pd.to_datetime(str(df['Date'].tail(1).values[0])[:10])
            diff = (endDate - begDate).days + 1

        print('=' * 50)
        print('No. of Days: ', diff)
        print('No. of Race: ', df['Date'].count())
        print('Total Time: ', str(timedelta(seconds=df['TotalSec'].sum())).split('.')[0])

        df_errRec = df[['Date', 'Activity ID', 'Total Time']][df['Total Time'].isnull()]
        print('No. of Err Data: ', df_errRec['Activity ID'].count())
        print(df_errRec)
    # -- Overall Summary (End) -- #

    print('-' * 5 + ' Collecting GPX Data (End) ' + '-' * 5)
    if CFG_BOOL_DEBUG:
        print('\n' * 5)
# -- Getting Running Data from GPX files (End) -- #

# -- Getting Running Data from KML files (Begin) -- #
if checkDataTypeIdx in ['c', 2]:
    if CFG_BOOL_DEBUG:
        print('\n' * 5)
    print('-' * 5 + ' Collecting KML Data (Begin) ' + '-' * 5)

    tarJSONfile = CFG_DATA_JSON_FILE.replace('::DATA_SRC::', 'kml')
    fldrInfo = fio._getFolderInfo(CFG_DATA_DIR.replace('::DATA_SRC::', 'kml'))
    if len(fldrInfo['files']) > 0:
        print('=' * 50)
        ns = {'ns' : 'http://earth.google.com/kml/2.1'}

        dfCols = ['Date', 'Activity ID', 'Total Time', 'Distance', 'Distance By Coord.',
                    # 'Elevation Gain', 'Elevation Loss',
                    'No. of Track Points', 'Track Info']
        df = pd.DataFrame(columns=dfCols)
        for filePath in fldrInfo['files']:
            tree = et.parse(filePath)
            root = tree.getroot()

            fileId = filePath[(filePath.rfind('_')+1):filePath.rfind('.')]

            folder = root.find('ns:Folder', ns)

            if 'Running' in folder.find('ns:name', ns).text:
                coordinates = (None, None)
                placemark = folder.find('ns:Placemark', ns)
                if placemark != None:
                    coordinates = placemark.find('ns:LineString', ns).find('ns:coordinates', ns).text.split(' ')
                    coordinates = [tuple(map(float, coord.split(','))) for coord in coordinates] if not 'null' in coordinates else (None, None)

                actTime = None
                trkInfo = {'coord' : [], 'begTime' : [], 'endTime' : []}

                _dfCols = ['Lap', 'Time', 'Distance', 'Elevation Gain', 'Elevation Loss', 'Max Speed', 'Coordinates']
                _df = pd.DataFrame(columns=_dfCols)
                for subfolder in folder.findall('ns:Folder', ns):
                    name = subfolder.find('ns:name', ns).text
                    if name == 'Laps':
                        for placemark in subfolder.findall('ns:Placemark', ns):
                            name = placemark.find('ns:name', ns).text.split(' ')
                            if 'Lap' in name[0]:
                                dataSet = {'Lap' : name[1]}

                                bs = BeautifulSoup(placemark.find('ns:description', ns).text.strip(), "html.parser")
                                for trContent in bs.find_all('tr'):
                                    tdContent = trContent.find_all('td')
                                    if not (tdContent[0].has_attr('colspan') and int(tdContent[0]['colspan']) == 2):
                                        dataSet[tdContent[0].text.replace(':', '')] = tdContent[1].text.strip()

                                coord = placemark.find('ns:Point', ns).find('ns:coordinates', ns).text.strip().split(',')
                                dataSet['Coordinates'] = tuple(map(float, coord)) if not 'null' in coord else (None, None)

                                _df = _df.append(dataSet, ignore_index=True)
                    elif name == 'Track Points':
                        for placemark in subfolder.findall('ns:Placemark', ns):
                            begTime = getTimeUnderTimeZone(placemark.find('ns:TimeSpan', ns).find('ns:begin', ns).text)
                            endTime = getTimeUnderTimeZone(placemark.find('ns:TimeSpan', ns).find('ns:end', ns).text)

                            if actTime == None:
                                actTime = begTime

                            coord = placemark.find('ns:Point', ns).find('ns:coordinates', ns).text.split(', ')[0].split(',')
                            trkInfo['coord'].append(tuple(map(float, [coord[1], coord[0]])))
                            trkInfo['begTime'].append(begTime.strftime('%Y-%m-%d %H:%M:%S'))
                            trkInfo['endTime'].append(endTime.strftime('%Y-%m-%d %H:%M:%S'))

                if actTime != None and actTime.year < curYear:
                    fio._removeFile(filePath, boolTakeLog=False)
                else:
                    print(fileId, actTime)

                    _df['Time'] = _df.apply(lambda row: (datetime.strptime(row['Time'], '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds(), axis=1)
                    _df['Distance'] = _df.apply(lambda row: float(row['Distance'].split(' ')[0]), axis=1)
                    _df['Elevation Gain'] = _df.apply(lambda row: float(row['Elevation Gain'].split(' ')[0]), axis=1)
                    _df['Elevation Loss'] = _df.apply(lambda row: float(row['Elevation Loss'].split(' ')[0]), axis=1)
                    _df['Max Speed'] = _df.apply(lambda row: float(row['Max Speed'].split(' ')[0]), axis=1)

                    totalTime = _df['Time'].sum()
                    totalDistance = _df['Distance'].sum()

                    # -- Get total distance of whole track --
                    numOfTrkPts = 0
                    totalDistanceByCoord = np.nan
                    if len(trkInfo['coord']) >= 2:
                        df_tmp = pd.DataFrame({'Cur Coord' : trkInfo['coord']})
                        df_tmp['Nxt Coord'] = df_tmp['Cur Coord'].shift(-1)
                        df_tmp = df_tmp.dropna()
                        df_tmp['Distance'] = df_tmp.apply(lambda row: getHaversineDistance(row['Cur Coord'], row['Nxt Coord']), axis=1)
                        numOfTrkPts = df_tmp['Nxt Coord'].count()
                        totalDistanceByCoord = df_tmp['Distance'].sum()

                    df = df.append({
                        'Date' : actTime.strftime('%Y-%m-%d %H:%M:%S') if actTime != None else '/',
                        'Activity ID' : fileId,
                        'Total Time' : totalTime,
                        'Distance' : totalDistance,
                        'Distance By Coord.' : totalDistanceByCoord,
                        # 'Elevation Gain' : totalElevationGain,
                        # 'Elevation Loss' : totalElevationLoss,
                        'No. of Track Points' : numOfTrkPts,
                        'Track Info' : trkInfo,
                    }, ignore_index=True)

                    # -- Move file to backup folder once completed reading the file --
                    backupFldrPath = fldrInfo['parent'] + 'bak/'
                    fio._createFolder(backupFldrPath)
                    fio._moveFile(filePath, filePath.replace(fldrInfo['parent'], backupFldrPath))
            else:
                # -- Remove non running record file --
                fio._removeFile(filePath, boolTakeLog=False)

        df.insert(0, 'Day', df.index.map(lambda x: x+1))
        df['Total Time'] = df.apply(lambda row: str(timedelta(seconds=row['Total Time'])).split('.')[0], axis=1)
        df.to_json(tarJSONfile, orient='records', lines=True)

        if CFG_BOOL_DEBUG:
            print('=' * 50)
            print(df)

    if fio._checkFileExist(tarJSONfile):
        df = pd.read_json(tarJSONfile, orient='records', lines=True)
        if CFG_BOOL_DEBUG:
            dfCols = df.columns.tolist()
            print(dfCols)
            print('=' * 50)
            print(df)

    if CFG_BOOL_DEBUG:
        print('=' * 50)
        print(df.dtypes)
        print('\n' * 5)

    df['Date'] = df['Date'].replace('/', np.nan)
    df['Date'] = pd.to_datetime(df['Date'])

    # df['Month'] = df['Date'].dt.month_name()
    # df['Weekday'] = df['Date'].dt.day_name()
    df['TotalSec'] = df.apply(lambda row: (datetime.strptime(row['Total Time'], '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds() if row['Total Time'] != None else 0, axis=1)
    df['Pacing'] = df.apply(lambda row: str(timedelta(seconds=row['TotalSec']/row['Distance'])).split('.')[0] if row['Distance'] > 0 else np.nan, axis=1)

    df = df[['Date', 'Day', 'Activity ID',
                # 'Month', 'Weekday',
                'Total Time', 'TotalSec', 'Distance', 'Distance By Coord.', 'Pacing',
                'No. of Track Points']]

    # -- Track Summary (Begin) -- #
    # df_trackSummary['KML'] = df.set_index('Date')
    df_trackSummary['KML'] = df
    if CFG_BOOL_DEBUG:
        print('=' * 50)
        print(df_trackSummary['KML'])
    # -- Track Summary (End) -- #

    # -- Overall Summary (Begin) -- #
    if CFG_BOOL_SHOW_OVERALL_SUMMARY:
        diff = np.nan
        if df['Date'][df['Date'].isnull()].count() == 0:
            # -- Make sure just use the date part ONLY instead of together with time part --
            begDate = pd.to_datetime(str(df['Date'].head(1).values[0])[:10])
            endDate = pd.to_datetime(str(df['Date'].tail(1).values[0])[:10])
            diff = (endDate - begDate).days + 1

        print('=' * 50)
        print('No. of Days: ', diff)
        print('No. of Race: ', df['Date'].count())
        print('Total Time: ', str(timedelta(seconds=df['TotalSec'].sum())).split('.')[0])
        print('Total Distance: ', round(df['Distance'].sum(), 2))
        print('Pacing: ', str(timedelta(seconds=df['TotalSec'].sum()/df['Distance'].sum())).split('.')[0])

        df_errRec = df[['Date', 'Activity ID', 'Total Time']][df['Date'].isnull()]
        df_errRec = df_errRec.replace(np.nan, '/')
        print('No. of Err Data: ', df_errRec['Activity ID'].count())
        print(df_errRec)
    # -- Overall Summary (End) -- #

    print('-' * 5 + ' Collecting KML Data (End) ' + '-' * 5)
    if CFG_BOOL_DEBUG:
        print('\n' * 5)
# -- Getting Running Data from KML files (End) -- #

# -- Getting Running Data from TCX files (Begin) -- #
if checkDataTypeIdx in ['c', 3]:
    if CFG_BOOL_DEBUG:
        print('\n' * 5)
    print('-' * 5 + ' Collecing TCX Data (Begin) ' + '-' * 5)

    tarJSONfile = CFG_DATA_JSON_FILE.replace('::DATA_SRC::', 'tcx')
    fldrInfo = fio._getFolderInfo(CFG_DATA_DIR.replace('::DATA_SRC::', 'tcx'))
    if len(fldrInfo['files']) > 0:
        print('=' * 50)
        ns = {'ns' : 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2',
            'ns2' : 'http://www.garmin.com/xmlschemas/UserProfile/v2',
            'ns3' : 'http://www.garmin.com/xmlschemas/ActivityExtension/v2',
            'ns4' : 'http://www.garmin.com/xmlschemas/ProfileExtension/v1',
            'ns5' : 'http://www.garmin.com/xmlschemas/ActivityGoals/v1'}

        dfCols = ['Date', 'Activity ID', 'Total Time', 'Distance', 'Distance By Coord.', 'Calories', 'No. of Track Points', 'Track Info']
        df = pd.DataFrame(columns=dfCols)
        for filePath in fldrInfo['files']:
            tree = et.parse(filePath)
            root = tree.getroot()

            fileId = filePath[(filePath.rfind('_')+1):filePath.rfind('.')]
            activity = root.find('ns:Activities', ns).find('ns:Activity', ns)
            actTime = getTimeUnderTimeZone(activity.find('ns:Id', ns).text)

            if actTime.year < curYear:
                fio._removeFile(filePath, boolTakeLog=False)
            else:
                if 'Running' in activity.get('Sport'):
                    print(fileId, actTime)
                    trkInfo = {'coord' : [], 'altM' : [], 'disM' : [], 'time' : [], 'spd' : [], 'cad' : []}
                    _df = pd.DataFrame(columns=['Time', 'Distance', 'MaxSpeed', 'Calories'])
                    for lap in activity.findall('ns:Lap', ns):
                        startTime = getTimeUnderTimeZone(lap.get('StartTime'))

                        _df = _df.append({
                            'Time' : float(lap.find('ns:TotalTimeSeconds', ns).text),
                            'Distance' : float(lap.find('ns:DistanceMeters', ns).text),
                            'MaxSpeed' : float(lap.find('ns:MaximumSpeed', ns).text),
                            'Calories' : int(lap.find('ns:Calories', ns).text)
                        }, ignore_index=True)

                        trk = lap.find('ns:Track', ns)
                        for trkpt in trk.findall('.//ns:Trackpoint', ns):
                            if trkpt.find('.//ns:Position', ns) != None:
                                trkInfo['coord'].append((float(trkpt.find('.//ns:LatitudeDegrees', ns).text), float(trkpt.find('.//ns:LongitudeDegrees', ns).text)))
                            else:
                                trkInfo['coord'].append((None, None))
                            trkInfo['altM'].append(float(trkpt.find('ns:AltitudeMeters', ns).text))
                            trkInfo['disM'].append(float(trkpt.find('ns:DistanceMeters', ns).text))
                            trkInfo['time'].append(getTimeUnderTimeZone(trkpt.find('ns:Time', ns).text).strftime('%Y-%m-%d %H:%M:%S'))
                            trkInfo['spd'].append(float(trkpt.find('.//ns3:Speed', ns).text) if trkpt.find('.//ns3:Speed', ns) != None else '/')
                            trkInfo['cad'].append(int(trkpt.find('.//ns3:RunCadence', ns).text))

                    totalTime = _df['Time'].sum()
                    totalDistance = _df['Distance'].sum()/1000
                    totalCalories = _df['Calories'].sum()

                    # -- Get total distance of whole track --
                    numOfTrkPts = 0
                    totalDistanceByCoord = 0
                    if len(trkInfo['coord']) >= 2:
                        df_tmp = pd.DataFrame({'Cur Coord' : trkInfo['coord']})
                        df_tmp['Nxt Coord'] = df_tmp['Cur Coord'].shift(-1)
                        df_tmp = df_tmp.dropna()
                        df_tmp['Distance'] = df_tmp.apply(lambda row: getHaversineDistance(row['Cur Coord'], row['Nxt Coord']), axis=1)
                        numOfTrkPts = df_tmp['Nxt Coord'].count()
                        totalDistanceByCoord = df_tmp['Distance'].sum()

                    df = df.append({
                        'Date' : actTime.strftime('%Y-%m-%d %H:%M:%S'),
                        'Activity ID' : fileId,
                        'Total Time' : totalTime,
                        'Distance' : totalDistance,
                        'Distance By Coord.' : totalDistanceByCoord,
                        'Calories' : totalCalories,
                        'No. of Track Points' : numOfTrkPts,
                        'Track Info' : trkInfo,
                    }, ignore_index=True)

                    # -- Move file to backup folder once completed reading the file --
                    backupFldrPath = fldrInfo['parent'] + 'bak/'
                    fio._createFolder(backupFldrPath)
                    fio._moveFile(filePath, filePath.replace(fldrInfo['parent'], backupFldrPath))
                else:
                    # -- Remove non running record file --
                    fio._removeFile(filePath, boolTakeLog=False)

        df.insert(0, 'Day', df.index.map(lambda x: x+1))
        df['Total Time'] = df.apply(lambda row: str(timedelta(seconds=row['Total Time'])).split('.')[0], axis=1)
        df['Distance'] = df.apply(lambda row: round(row['Distance'], 2), axis=1)
        df['Calories'] = df.apply(lambda row: round(row['Calories']), axis=1)
        df.to_json(tarJSONfile, orient='records', lines=True)

        if CFG_BOOL_DEBUG:
            print('=' * 50)
            print(df)

    if fio._checkFileExist(tarJSONfile):
        df = pd.read_json(tarJSONfile, orient='records', lines=True)
        if CFG_BOOL_DEBUG:
            dfCols = df.columns.tolist()
            print(dfCols)
            print('=' * 50)
            print(df)

    if CFG_BOOL_DEBUG:
        print('=' * 50)
        print(df.dtypes)
        print('\n' * 5)

    # df['Month'] = df['Date'].dt.month_name()
    # df['Weekday'] = df['Date'].dt.day_name()
    df['TotalErrPts'] = df.apply(lambda row: row['Track Info']['coord'].count([None, None]), axis=1)
    df['TotalSec'] = df.apply(lambda row: (datetime.strptime(row['Total Time'], '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds() if row['Total Time'] != None else 0, axis=1)
    df['Pacing'] = df.apply(lambda row: str(timedelta(seconds=row['TotalSec']/row['Distance'])).split('.')[0] if row['Distance'] > 0 else np.nan, axis=1)

    df = df[['Date', 'Day', 'Activity ID',
                # 'Month', 'Weekday',
                'Total Time', 'TotalSec', 'Distance', 'Distance By Coord.', 'Pacing',
                'Calories',
                'TotalErrPts', 'No. of Track Points']]

    # -- Track Summary (Begin) -- #
    # df_trackSummary['TCX'] = df.set_index('Date')
    df_trackSummary['TCX'] = df
    if CFG_BOOL_DEBUG:
        print('=' * 50)
        print(df_trackSummary['TCX'])
    # -- Track Summary (End) -- #

    # -- Overall Summary (Begin) -- #
    if CFG_BOOL_SHOW_OVERALL_SUMMARY:
        diff = np.nan
        if df['Date'][df['Date'] == '/'].count() == 0:
            # -- Make sure just use the date part ONLY instead of together with time part --
            begDate = pd.to_datetime(str(df['Date'].head(1).values[0])[:10])
            endDate = pd.to_datetime(str(df['Date'].tail(1).values[0])[:10])
            diff = (endDate - begDate).days + 1

        print('=' * 50)
        print('No. of Days: ', diff)
        print('No. of Race: ', df['Date'].count())
        print('Total Time: ', str(timedelta(seconds=df['TotalSec'].sum())).split('.')[0])
        print('Total Distance: ', round(df['Distance'].sum(), 2))
        print('Pacing: ', str(timedelta(seconds=df['TotalSec'].sum()/df['Distance'].sum())).split('.')[0])

        df_errRec = df[['Date', 'Activity ID', 'Total Time']][df['TotalErrPts'] == df['No. of Track Points']]
        print('No. of Err Data: ', df_errRec['Activity ID'].count())
        print(df_errRec)

        df_CompleteRec = df[['Date', 'Activity ID', 'Total Time']][df['TotalErrPts'] == 0]
        print('No. of Complete Data: ', df_CompleteRec['Activity ID'].count())
        print(df_CompleteRec)
    # -- Overall Summary (End) -- #

    # # -- Monthly Summary (Begin) -- #
    # print('*' * 50)
    # df_monthRec = df.dropna().groupby(df['Month'])['Activity ID'].count().reset_index(name='No. of Race')
    # df_monthRec['No. of Days'] = [calendar.monthrange(curYear, datetime.strptime(df_monthRec['Month'].loc[idx], '%B').month)[1] for idx in df_monthRec.index]
    # df_monthRec['No. of Missing Race'] = (df_monthRec['No. of Days'] - df_monthRec['No. of Race']).replace(0, '/')
    #
    # df_tmpRec = df.dropna().groupby(df['Month']).sum().reset_index()[['Month', 'Distance', 'TotalSec', 'Calories']]
    #
    # df_monthRec = pd.merge(df_monthRec, df_tmpRec, on='Month').sort_values(by=['TotalSec'], ascending=False)
    # df_monthRec['Avg. Distance'] = df_monthRec.apply(lambda row: round(row['Distance'] / row['No. of Days'], 2), axis=1)
    # df_monthRec['Avg. Time'] = df_monthRec.apply(lambda row: str(timedelta(seconds=(row['TotalSec'] / row['No. of Days']))).split('.')[0], axis=1)
    # df_monthRec['Pacing'] = df_monthRec.apply(lambda row: str(timedelta(seconds=row['TotalSec']/(row['Distance']))).split('.')[0], axis=1)
    # df_monthRec['Avg. Calories'] = df_monthRec.apply(lambda row: round(row['Calories']/row['No. of Days']), axis=1)
    # df_monthRec['TotalSec'] = df_monthRec.apply(lambda row: str(timedelta(seconds=row['TotalSec'])).split('.')[0], axis=1)
    #
    # df_monthRec = df_monthRec.rename(columns={'TotalSec' : 'Total Time'})
    # print(df_monthRec)
    # # -- Monthly Summary (End) -- #
    #
    # # -- Weekday Summary (Begin) -- #
    # print('*' * 50)
    # df_wkdayRec = df.dropna().groupby(df['Weekday'])['Activity ID'].count().reset_index(name='No. of Race')
    # df_wkdayRec['No. of Days'] = [df['Weekday'][df['Weekday'] == e].count() for e in df_wkdayRec['Weekday']]
    # df_wkdayRec['No. of Missing Race'] = (df_wkdayRec['No. of Days'] - df_wkdayRec['No. of Race']).replace(0, '/')
    #
    # df_tmpRec = df.dropna().groupby(df['Weekday']).sum().reset_index()[['Weekday', 'Distance', 'TotalSec']]
    #
    # df_wkdayRec = pd.merge(df_wkdayRec, df_tmpRec, on='Weekday').sort_values(by=['TotalSec'], ascending=False)
    # df_wkdayRec['Avg. Time'] = df_wkdayRec.apply(lambda row: str(timedelta(seconds=(row['TotalSec'] / row['No. of Days']))).split('.')[0], axis=1)
    # df_wkdayRec['Pacing'] = df_wkdayRec.apply(lambda row: str(timedelta(seconds=row['TotalSec']/(row['Distance']))).split('.')[0], axis=1)
    # df_wkdayRec['TotalSec'] = df_wkdayRec.apply(lambda row: str(timedelta(seconds=row['TotalSec'])).split('.')[0], axis=1)
    #
    # df_wkdayRec = df_wkdayRec.rename(columns={'TotalSec' : 'Total Time'})
    # print(df_wkdayRec)
    # # -- Weekday Summary (End) -- #

    print('-' * 5 + ' Collecting TCX Data (End) ' + '-' * 5)
    if CFG_BOOL_DEBUG:
        print('\n' * 5)
# -- Getting Running Data from TCX files (End) -- #

# print(df_trackSummary)
"""
GPX:
    - As it provides inaccurate info on total time & track points coordinates, so running time, pacing & distance will be incorrect, so skip it in final dataset
    - GPX is the ONLY one provided Cadence, Elevation & Temperature, so keep them

KML:
    - Compare with TCX, TCX provides more accurate information of Total Time, so skip it in final dataset
    - Compare with TCX, the coordinates

TCX:
    - It provides the MOST details in all 3 files type
    - TCX is the ONLY one provided Calories, so keep it
"""
print(df_trackSummary['GPX'].columns.tolist())
print(df_trackSummary['KML'].columns.tolist())
print(df_trackSummary['TCX'].columns.tolist())

df_trackSummary['GPX'].drop(columns=['Date', 'Day', 'Total Time', 'TotalSec', 'Distance By Coord.', 'Pacing', 'No. of Track Points'], axis=1, inplace=True)
df_trackSummary['KML'].drop(columns=['Date', 'Day', 'Total Time', 'TotalSec', 'Distance', 'Pacing', 'No. of Track Points'], axis=1, inplace=True)
df_trackSummary['TCX'].drop(columns=['Day', 'TotalErrPts', 'No. of Track Points'], axis=1, inplace=True)

# print(df_trackSummary['GPX'].tail(20))
# print(df_trackSummary['KML'].tail(20))
# print(df_trackSummary['TCX'].tail(20))

def _getDatetimeRange(dtArr, period=30):
    floor, ceiling = None, None
    for dt in dtArr:
        if floor == None:
            floor = dt.replace(minute=0, second=0) + timedelta(minutes=(dt.minute//period)*period)
        ceiling = dt.replace(minute=0, second=0) + timedelta(minutes=(dt.minute//period+1)*period)

    return (floor, ceiling)

# startDT = datetime(2012, 10, 25, 17, 12, 16)
# endDT = datetime(2012, 12, 25, 19, 44, 16)
# print(_getDatetimeRange((startDT, endDT)))
# exit()

df_combineSummary = pd.merge(df_trackSummary['TCX'], df_trackSummary['GPX'], on=['Activity ID'])
df_combineSummary = pd.merge(df_combineSummary, df_trackSummary['KML'], on=['Activity ID'])

df_combineSummary.insert(1, 'End Time', np.nan)
df_combineSummary['End Time'] = df_combineSummary.apply(lambda row: row['Date'] + timedelta(0, row['TotalSec']), axis=1)

df_combineSummary.insert(2, 'Month', df_combineSummary['Date'].dt.month)
# df_combineSummary.insert(3, 'Weekday', df_combineSummary['Date'].dt.day_name())
# df_combineSummary.drop(columns=['TotalSec'], axis=1, inplace=True)

print(df_combineSummary)
print(df_combineSummary.columns.tolist())

# plt.bar(df_combineSummary['Date'], df_combineSummary['Distance'])
# plt.xticks(rotation=90)
# plt.show()


begDate = pd.to_datetime(str(df_combineSummary['Date'].head(1).values[0])[:10])
endDate = pd.to_datetime(str(df_combineSummary['Date'].tail(1).values[0])[:10])
diff = (endDate - begDate).days + 1

print('\n' * 5)
print('=' * 50)
print('No. of Days: ', diff)
print('No. of Race: ', df_combineSummary['Date'].count())

print('Total Time: ', str(timedelta(seconds=df_combineSummary['TotalSec'].sum())).split('.')[0])
print('Avg. Daily: ', str(timedelta(seconds=df_combineSummary['TotalSec'].sum()/diff)).split('.')[0])

print('Total Distance: ', round(df_combineSummary['Distance'].sum(), 2))
print('Avg. Daily: ', round(df_combineSummary['Distance'].sum()/diff, 2))
print('Avg. Weekly: ', round(df_combineSummary['Distance'].sum()/(diff/7), 2))
print('Pacing: ', str(timedelta(seconds=df_combineSummary['TotalSec'].sum()/df_combineSummary['Distance'].sum())).split('.')[0])

print('Total Calories: ', round(df_combineSummary['Calories'].sum(), 2))
print('Avg. Daily: ', round(df_combineSummary['Calories'].sum()/diff, 2))

df_tmp = df_combineSummary.groupby(['Month'])

df_mthSummary = pd.merge(df_tmp[['Activity ID']].count().reset_index(), df_tmp[['TotalSec']].sum().reset_index(), on=['Month'])
df_mthSummary = pd.merge(df_mthSummary, df_tmp[['TotalSec']].mean().reset_index(), on=['Month'])
df_mthSummary = pd.merge(df_mthSummary, df_tmp[['Distance']].sum().reset_index(), on=['Month'])
df_mthSummary = pd.merge(df_mthSummary, df_tmp[['Distance']].mean().reset_index(), on=['Month'])
df_mthSummary = pd.merge(df_mthSummary, df_tmp[['Calories']].sum().reset_index(), on=['Month'])
df_mthSummary = pd.merge(df_mthSummary, df_tmp[['Calories']].mean().reset_index(), on=['Month'])

df_mthSummary = df_mthSummary.rename(columns={'TotalSec_x' : 'TotalSec',
                                                'TotalSec_y' : 'Avg. TotalSec',
                                                'Distance_x' : 'Total Distance',
                                                'Distance_y' : 'Avg. Distance',
                                                'Calories_x' : 'Total Calories',
                                                'Calories_y' : 'Avg. Calories'
                                                })
df_mthSummary['Total Time'] = df_mthSummary.apply(lambda row: str(timedelta(seconds=row['TotalSec'])).split('.')[0], axis=1)
df_mthSummary['Avg. Total Time'] = df_mthSummary.apply(lambda row: str(timedelta(seconds=row['Avg. TotalSec'])).split('.')[0], axis=1)

df_mthSummary['Month'] = df_mthSummary.apply(lambda row: date(1900, int(row['Month']), 1).strftime('%b'), axis=1)
df_mthSummary = df_mthSummary.round(2)
print(df_mthSummary)
print('\n' * 5)

print(df_mthSummary['Total Distance'].describe())
print('\n' * 5)

df_begEndSummary = df_combineSummary[['Date', 'End Time']]
df_begEndSummary = df_begEndSummary.rename(columns={'Date' : 'Start Time'})
df_begEndSummary['Time Range'] = df_begEndSummary.apply(lambda row: _getDatetimeRange((row['Start Time'], row['End Time'])), axis=1)

df_begEndSummary.insert(0, 'Date', df_begEndSummary.apply(lambda row: row['Start Time'].strftime('%Y-%m-%d'), axis=1))
df_begEndSummary['Date'] = pd.to_datetime(df_begEndSummary['Date'])
df_begEndSummary['Weather'] = ''
df_begEndSummary['Temperature (°C)'] = np.nan
df_begEndSummary['Wind (Km/h)'] = np.nan
df_begEndSummary['Humidity (%)'] = np.nan

# print(df_begEndSummary)

CONST_WEATHER_HISTORY_JSON_FILE = './Weather/WeatherHistory.json'
df_weatherHistory = pd.read_json(CONST_WEATHER_HISTORY_JSON_FILE, orient='records', lines=True)

# plt.plot(df_weatherHistory['Date'], df_weatherHistory['Temperature (°C)'])
# plt.xticks(rotation=90)
# plt.show()
#

CONST_WEATHER_WARNING_HISTORY_JSON_FILE = './Weather/WeatherWarningHistory.json'
df_weatherWarningHistory = pd.read_json(CONST_WEATHER_WARNING_HISTORY_JSON_FILE, orient='records', lines=True)
df_weatherWarningHistory = df_weatherWarningHistory[(df_weatherWarningHistory['Type'] != 'Fire Danger Warnings')]
df_weatherWarningHistory = df_weatherWarningHistory[(df_weatherWarningHistory['Type'] != 'Frost Warning')]
df_weatherWarningHistory = df_weatherWarningHistory[(df_weatherWarningHistory['Type'] != 'Landslip Warning')]
df_weatherWarningHistory = df_weatherWarningHistory[(df_weatherWarningHistory['Type'] != 'Special Announcement on Flooding in the northern New Territories')]
# print(df_weatherWarningHistory)

CONST_HKO_DAILY_WEATHER_EXTRACT_JSON_FILE = './Weather/HKODailyWeatherExtract.json'
df_hkoDailyExtract = pd.read_json(CONST_HKO_DAILY_WEATHER_EXTRACT_JSON_FILE, orient='records', lines=True)

# print(df_hkoDailyExtract)


df_newWeatherWarningHistory = pd.DataFrame()

df_begEndSummary = df_begEndSummary.set_index('Date')
for date, values in df_begEndSummary.iterrows():
    dtArr = values['Time Range']

    df_tmp = df_weatherHistory.loc[(df_weatherHistory['Date'] == date) &
        (df_weatherHistory['Time'] >= dtArr[0].strftime('%H:%M')) &
        (df_weatherHistory['Time'] <= dtArr[1].strftime('%H:%M'))]
    df_tmp = df_tmp.astype({'Wind (Km/h)' : int})

    df_begEndSummary.loc[date, 'Weather'] = df_tmp[['Description']].describe().loc['top'].values[0]
    df_begEndSummary.loc[date, 'Temperature (°C)'] = df_tmp[['Temperature (°C)']].mean().values[0]
    df_begEndSummary.loc[date, 'Wind (Km/h)'] = df_tmp[['Wind (Km/h)']].mean().values[0]
    df_begEndSummary.loc[date, 'Humidity (%)'] = df_tmp[['Humidity (%)']].mean().values[0]

    df_tmp = df_weatherWarningHistory.loc[(df_weatherWarningHistory['Date'] == date) &
        (df_weatherWarningHistory['Start_Time'] <= values['Start Time']) &
        (df_weatherWarningHistory['End_Time'] >= values['Start Time'])]

    df_newWeatherWarningHistory = pd.concat([df_newWeatherWarningHistory, df_tmp], ignore_index=True)

df_begEndSummary = pd.merge(df_begEndSummary, df_hkoDailyExtract[['Date', 'Total Rainfall (mm)']], on=['Date'])
df_begEndSummary = df_begEndSummary.round(2)
# print(df_begEndSummary)

df_begEndSummary.loc[(df_begEndSummary['Total Rainfall (mm)'] == 'Trace'), 'Total Rainfall (mm)'] = 0.0
df_begEndSummary = df_begEndSummary.astype({'Total Rainfall (mm)' : float})

print(df_begEndSummary)
print(df_begEndSummary.loc[(df_begEndSummary['Total Rainfall (mm)'] > 300)])

# print(df_begEndSummary['Total Rainfall (mm)'].describe())
df_rainfallSummary = df_begEndSummary[['Date', 'Total Rainfall (mm)']]
tarIdx = df_rainfallSummary[df_rainfallSummary['Total Rainfall (mm)'] == 0].index
print(df_rainfallSummary)
df_rainfallSummary.drop(tarIdx, inplace=True)
print(df_rainfallSummary)
print(df_rainfallSummary.groupby(pd.cut(df_rainfallSummary['Total Rainfall (mm)'], np.arange(0, df_rainfallSummary['Total Rainfall (mm)'].describe().max(), 10))).count())
print('\n' * 2)

print(df_begEndSummary['Weather'].value_counts())
print('\n' * 2)
print(df_newWeatherWarningHistory['Warning_Signal'].value_counts())
print('\n' * 2)
