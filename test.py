import pandas as pd
import datetime as dtt
import moex_data_funct as sdk
import numpy as np
import matplotlib.pyplot as plt

# Функция возвращает df с датами и суммами денежных потоков (купонов и амортизаций в рублях), которые приходят по указанной облигации
# Считает с текущего момента на следующие 12 месяцев
# Сумма считает ден.потоки с даты, указанной в предыдущей строке до даты в текущей строке
# Для первой строки с текущего момента до указанной в 0 строке даты
# Intervals в передаваемых переменных - df со столбцом дат и нулевой столбец для добавления туда сумм ден.потоков
def cash_flows(security,funct_intervals):
	coupons=sdk.get_coupons(security=security)
	# print('coupons:\n',coupons)
	amortizations=sdk.get_amortizations(security=security)
	interval_sums_list=[]
	for index,interval in funct_intervals.iterrows():
		if index==0:
			date1=now
			date2=interval['dates']
			coupon_sum=coupons[(coupons['coupondate']>=str(date1)) & (coupons['coupondate']<=str(date2))]['value_rub'].sum()
			amortization_sum=amortizations[(amortizations['amortdate']>=str(date1)) & (amortizations['amortdate']<=str(date2))]['value_rub'].sum()
			interval_sums_list.append(coupon_sum+amortization_sum)
			
		else:
			date1=funct_intervals.iloc[index-1,0]
			date2=interval['dates']
			coupon_sum=coupons[(coupons['coupondate']>=str(date1)) & (coupons['coupondate']<=str(date2))]['value_rub'].sum()
			amortization_sum=amortizations[(amortizations['amortdate']>=str(date1)) & (amortizations['amortdate']<=str(date2))]['value_rub'].sum()
			interval_sums_list.append(coupon_sum+amortization_sum)
			
	interval_sums_list=pd.DataFrame(interval_sums_list,columns=['interval_sums'])
	funct_intervals['interval_sums']=funct_intervals['interval_sums']+interval_sums_list['interval_sums']

	return funct_intervals


isins=[
'RU000A101483',
'RU000A100D89',
# 'RU000A1005T9',
# 'RU000A100D30',
# 'RU000A102044',
# 'RU000A102051',
# 'RU000A101QF7',
# 'RU000A101H43',
# 'RU000A101HU5',
# 'RU000A100M47',
# 'RU000A100D63',
]

quantities=[1,1] #,3,4,5,6,7,8,9,10,11

securities=pd.DataFrame(isins,columns=['isins'])
quantities=pd.DataFrame(quantities,columns=['quantities'])
securities=pd.concat([securities,quantities],axis=1)
now=dtt.datetime.now()
intervals=pd.date_range(start=now, periods=12, freq='M')

intervals=intervals.to_frame(name='dates', index=False)
interval_sums=pd.DataFrame(0,index=np.arange(len(intervals.index)),columns=['interval_sums'])
intervals=pd.concat([intervals, interval_sums], axis=1)


cash_flows_all=pd.DataFrame()

for index,security in securities.iterrows():
	cash_flows_sec=cash_flows(security=security['isins'],funct_intervals=intervals.copy())
	cash_flows_sec['interval_sums']*=security['quantities']
	cash_flows_all=pd.concat([cash_flows_all,cash_flows_sec]).groupby(['dates'],as_index=False).sum()

cash_flows_all['dates']=cash_flows_all['dates'].dt.strftime("%b-%y")
print('cash_flows_all at all\n',cash_flows_all)

cash_flows_all.plot.bar(x='dates', y='interval_sums', rot=0)
plt.show()