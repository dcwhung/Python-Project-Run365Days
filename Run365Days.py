# -- Update the root path information to system path in order to import the other library from parent directory --
import os, sys
CONST_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(CONST_ROOT_PATH)

import pandas as pd, numpy as np
from pandas import ExcelWriter

from com.lib.fn_basic import _print
from com.lib.fn_string import _trim

from datetime import datetime, timedelta
from dateutil.parser import parse

# np.seterr(all='print')
from sklearn.linear_model import SGDClassifier, LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score, cross_val_predict
from sklearn.metrics import precision_recall_fscore_support, confusion_matrix

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import matplotlib.ticker as ticker
from matplotlib.patches import ConnectionPatch
from matplotlib.animation import FuncAnimation
import time

plt.style.use('seaborn-dark')

from matplotlib import rc
# rc('text', usetex=True)

# -- Preload matching summary -- #
dict = pd.read_excel('Activities.xlsx', sheet_name=None, index_col=1, parse_dates=['Date'], squeeze=True)
df = dict['Activities']
#
# fig = plt.figure(figsize=(10, 5))
# fig.subplots_adjust(left=0.05, right=0.95)
#
# ax1 = fig.add_subplot(1, 2, 1)
#
# fig, ax1 = plt.subplots(nrows=1, ncols=1, sharex=True,figsize=(15,5))
#
# ax1.set_xlim(datetime(2021, 1, 1), datetime(2021, 4, 30))
# ax1.set_ylim(130, 160)
# ax1.set_xlabel('2021', fontsize=10)
# ax1.set_ylabel('Weight (lbs)', fontsize=10)
#
# ax1.xaxis.set_major_locator(dates.MonthLocator())
# ax1.xaxis.set_major_formatter(ticker.NullFormatter())
# ax1.xaxis.set_minor_locator(dates.MonthLocator(bymonthday=15))
# ax1.xaxis.set_minor_formatter(dates.DateFormatter('%b'))
#
# ax1.plot(df.index, df['Weight'].values, color='tab:blue', linewidth=1)
#
# ax2 = ax1.twinx()
# ax2.set_ylim(0, 10)
# ax2.set_ylabel('Distance (KM)', fontsize=10)
#
# ax2.xaxis.set_major_locator(dates.MonthLocator())
# ax2.xaxis.set_major_formatter(ticker.NullFormatter())
# ax2.xaxis.set_minor_locator(dates.MonthLocator(bymonthday=15))
# ax2.xaxis.set_minor_formatter(dates.DateFormatter('%b'))
#
#
# # ax2.plot(df.index, df['Duration'].values, color='tab:red', linewidth=1)
# ax2.plot(df.index, df['Distance'].values, color='tab:red', linewidth=1)
#
#
# plt.show()
#
#
#

segtime = [1000, 2000, 3000, 3500, 7000]
segStrength = [10000, 30000, 15000, 20000, 22000]

fig, ax = plt.subplots()
plt.plot(segtime, segStrength)

formatter = matplotlib.ticker.FuncFormatter(lambda ms, x: time.strftime('%M:%S', time.gmtime(ms // 1000)))
ax.xaxis.set_major_formatter(formatter)

plt.show()
