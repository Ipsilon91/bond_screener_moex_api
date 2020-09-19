import pandas as pd
import datetime as dtt
import moex_data_funct as sdk

# data2=pd.DataFrame([[7,8],[9,10],[11,12]], columns=['A','B'])
# # data2.reset_index(inplace=True)


# print(data2[['A','B']])
date=pd.date_range(start='2020-06-01', periods=12, freq='M')[1]
print(date)

data=sdk.bond_cash_flows(security='RU000A101483')
data=data['coupons']
print(data)
data=data[data['coupondate']<=str(date)]['value_rub'].sum()

print(data)


# def CF_schedule(isin='RU000A101483'):

#     return