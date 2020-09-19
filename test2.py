import pandas as pd
import moex_data_funct as sdk
import datetime as dtt
from dateutil.tz import tzlocal
# import numpy as np


data=pd.date_range(start=dtt.datetime.now(), periods=12, freq='M')
print(data)