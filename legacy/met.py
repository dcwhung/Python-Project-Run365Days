"""
https://www.healthline.com/health/what-are-mets#calorie-connection

METs = metabolic equivalents.
One MET is defined as the energy you use when you’re resting or sitting still.

What you may not know is that you have to burn about 3,500 calories to lose 1 pound of body weight.

That means if you reduce your daily calorie intake by 500 calories or burn 500 more calories each day than you consume, you may be able to lose a pound a week.

The formula to use is: METs x 3.5 x (your body weight in kilograms) / 200 = calories burned per minute.

For example, say you weigh 160 pounds (approximately 73 kg) and you play singles tennis, which has a MET value of 8.

The formula would work as follows: 8 x 3.5 x 73 / 200 = 10.2 calories per minute. If you play tennis for an hour, you’ll burn about 613 calories.

Running | 5 mph (12 min/mile) | METS: 8.0
Running | 7 mph (8.5 min/mile) | METS: 11.5
Running | 10 mph (6 min/mile) | METS: 16.0

To determine calories expended by your favorite activity use the following equation:

METS X 3.5 X BW (KG) / 200 = KCAL/MIN.

For example, if a 30-year-old man weighing 170lbs (77.3kg) performs 45 minutes of running at 7mph, the amount of calories he would burn per minute would be:

11.5(3.5)(77.3kg)/200 = 15.6 kcals/min

So in 45 minutes, this man would burn 700 calories running at 7mph (mile per hour).

1 mile = 1.609344 km

5:22 min/km = 8.6373492 min/mile = 6.946575692459 mph
"""
from datetime import date, datetime, timedelta

def _calMinPerKm(mins, km):
    totalSec = (datetime.strptime(mins, '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds()
    result = str(timedelta(seconds=totalSec/km)).split('.')[0]

    return result

def _calMinPerMile(mins, km):
    totalSec = (datetime.strptime(mins, '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds()

    totalMin = totalSec/60
    mile = km/1.609344

    result = totalMin/mile
    return result

def _calMilePerHour(mins, km):
    totalSec = (datetime.strptime(mins, '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds()

    totalHour = totalSec/(60 * 60)
    mile = km/1.609344

    result = mile/totalHour
    return result

import pandas as pd

def _getRunningMETS(minPerMile):
    dataMapStr = """
    6.00 | Running, 4 mph (13 min/mile)
    8.30 | Running, 5 mph (12 min/mile)
    9.00 | Running, 5.2 mph (11.5 min/mile)
    9.80 | Running, 6 mph (10 min/mile)
    10.50 | Running, 6.7 mph (9 min/mile)
    11.00 | Running, 7 mph (8.5 min/mile)
    11.50 | Running, 7.5 mph (8 min/mile)
    11.80 | Running, 8 mph (7.5 min/mile)
    12.30 | Running, 8.6 mph (7 min/mile)
    12.80 | Running, 9 mph (6.5 min/mile)
    14.50 | Running, 10 mph (6 min/mile)
    16.00 | Running, 11 mph (5.5 min/mile)
    19.00 | Running, 12 mph (5 min/mile)
    19.80 | Running, 13 mph (4.6 min/mile)
    23.00 | Running, 14 mph (4.3 min/mile)
    """

    metsArr = []
    minPerMileArr = []
    for line in dataMapStr.splitlines():
        line = line.strip()
        begIdx = 0
        endIdx = line.find(' | ')-1

        mets = -1
        if begIdx > -1 and endIdx > -1:
            metsArr.append(float(line[begIdx:endIdx]))

        begIdx = line.rfind('(')+1
        endIdx = line.rfind('min/mile')-1

        if begIdx > -1 and endIdx > -1:
            minPerMileArr.append(float(line[begIdx:endIdx]))

    df = pd.DataFrame({'METS' : metsArr, 'min/mile' : minPerMileArr})
    # .set_index('min/mile')
    print(df)

    result = df.iloc[(df['min/mile']-minPerMile).abs().argsort()[:2]]
    print(result)

    result_index = df['min/mile'].sub(minPerMile).abs().idxmin()
    print(result_index)

    return df.iloc[result_index]['METS']

def _calKcalBurn(mets, weight, intensity):
    totalSec = (datetime.strptime(intensity, '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds()
    totalMin = totalSec/60

    # METS X 3.5 X BW (KG) / 200 = KCAL/MIN.
    #
    # For example, if a 30-year-old man weighing 170lbs (77.3kg) performs 45 minutes of running at 7mph, the amount of calories he would burn per minute would be:
    #
    # 11.5(3.5)(77.3kg)/200 = 15.6 kcals/min
    return (mets * 3.5 * weight / 200) * totalMin

print('Min Per KM (Pace - min/km): {}'.format(_calMinPerKm('00:32:39', 6.09)))
print('Min Per Mile (Pace - min/mile): {}'.format(_calMinPerMile('00:32:39', 6.09)))
print('Mile Per Hour (mph) : {}'.format(_calMilePerHour('00:32:39', 6.09)))
mets = _getRunningMETS(_calMinPerMile('00:32:39', 6.09))
print(mets)
print(_calKcalBurn(mets, 65, '00:32:39'))
