import pandas as pd
import moex_data_funct as sdk
import datetime as dtt
from dateutil.tz import tzlocal
import numpy as np
import time

# Техническая функция для определения времени работы кода
def timer_start():
	return time.time()

def timer_end(start):
	print("--- %s seconds ---" % round((time.time() - start),3))


## ПОЛУЧЕНИЕ ДАННЫХ ОБО ВСЕХ БУМАГАХ НА ВЫБРАННУЮ ДАТУ
# Выбираем какие столбцы хотим получить и на какую дату нужны данные
# Дата в формате ГГГГ-ММ-ДД
tms_all=timer_start()
needed_securities=0 #0 - все бумаги
date=str(dtt.date.today()-dtt.timedelta(days=10))
# print(date)

needed_isins=data=pd.read_excel('INTERNET_Auction_Results_rus_2020_20200708.xlsx',names=['ISIN'])

needed_columns=[
'ISIN',
'SECNAME', #Наименование ценной бумаги
# 'MATDATE', #Дата погашения
# 'YIELDDATETYPE', #Тип даты, к которой рассчитывается доходность параметра (offer, maturity, MBS)
# 'COUPONPERCENT',#Ставка купона, %
# 'OFFERDATE',#Дата Оферты
# # 'LAST',#Цена последней сделки, %
# # 'WAPRICE',#Средневзвешенная цена, % к номиналу
# # 'EFFECTIVEYIELD',#Эффективная доходность
# # 'YIELDATWAPRICE',#Доходность по средневзвешенной цене, % годовых
# # 'YIELDLASTCOUPON',#Доходность к погашению (оферте) для купонных облигаций с одним (последним) купонным периодом до погашения
# # 'DURATIONWAPRICE',#Дюрация по средневзвешенной цене, дней
# # 'ZSPREADBP',#Z-spread
# 'GSPREADBP',#G-spread
# # 'PREVPRICE',#Цена последней сделки пред. дня, % к номиналу
# 'PREVLEGALCLOSEPRICE',#Официальная цена закрытия предыдущего дня
# 'YIELD',#Доходность по последней сделке
# # 'CLOSEYIELD',#Доходность по закрытию
# # 'CLOSEPRICE',#Цена закрытия
# # 'VOLTODAY',#Объем заключенных сегодня сделок в единицах ценных бумаг, штук
# 'DURATION',#Дюрация, дней
# # 'TIME',#Время последней сделки
# 'PRICE',#Цена по которой была рассчитана доходность
# # 'COUPONPERIOD',#Длительность купона
# # 'SETTLEDATE',#Дата расчетов сделки
# # 'UPDATETIME',#Время последнего обновления
# # 'LCLOSEPRICE',#Цена закрытия
# # 'LCURRENTPRICE',#Текущая цена
# # 'SYSTIME',#Время загрузки данных системой
# # 'VALTODAY_RUR',#Объем совершенных сделок, рублей
# # 'YIELDTOOFFER',#Доходность к оферте
# # 'ZCYCMOMENT',#Дата-временной маркер использованной в расчетах КБД
# # 'EFFECTIVEYIELD',#Эффективная доходность
# # 'WAPRICE',#Средневзвешенная цена
# # 'EFFECTIVEYIELDWAPRICE',#Эффективная доходность по средневзвешенной цене
# 'TRADEMOMENT',#Время последней (видимо имеется в виду последней сделки)
# # 'YIELDDATE',#Дата, к которой рассчитывается доходность (!не пойми что показывает)
# 'LISTLEVEL',#Уровень листинга
# 'BUYBACKDATE',#Дата, к которой рассчитывается доходность (если данное поле не заполнено, то "Доходность посл.сделки" рассчитывается к Дате погашения) (!Нужна, чтобы смотреть к какой дате считаются бессрочные бонды)
]

# Из каких режимов торгов нужно брать данные
needed_boards={
'bonds':[
	'TQCB', #Т+: Облигации - безадрес.
	# 'TQDB', #Крупные пакеты: Облигации - безадрес
	'TQRD', #Т+: Облигации Д - безадрес.
	'TQIR', #Т+ Облигации ПИР - безадрес.
]
}

# Получаем три таблицы с данными и присваиваем вместо имён строк значения SECID - уник. номер бумаги
# Это нужно, чтобы потом сделать конкатенацию таблиц - там проверка соответствия идёт по индексу
all_boards_securities=pd.DataFrame()
for market,boards in needed_boards.items():
	for board in boards:
		tables=sdk.get_board_history_all(date=date,market=market,board=board)

		# print('market: ',market,'board: ',board)
		# print(tables)
		if len(tables['securities'])!=0:
			for ind,table in tables.items():
				try:
					table.set_index('SECID',inplace=True)
				except KeyError:
					pass

			# Объединяем три таблицы в одну
			all_securities = pd.concat(tables, axis=1, sort=False)

			all_securities.columns = all_securities.columns.droplevel() #pandas по-умолчанию делает multyindex с названием таблицы в индексе сверху названия колонок. Убираем его
			
			# print(all_securities.head())

			df_columns=all_securities.columns.tolist()
			available_columns=list(set(df_columns).intersection(set(needed_columns)))

			all_securities=all_securities[available_columns]
			all_securities=all_securities.loc[:,~all_securities.columns.duplicated()] #подсмотрел на stack overflow крутой кусок кода, который убирает дупликаты колонок

			all_boards_securities=pd.concat([all_boards_securities,all_securities],axis=0,sort=False)

# print(all_boards_securities.head())
# all_boards_securities.to_excel('qqqq.xlsx')
needed_isins=[
	'RU000A0ZZMQ9',
	'RU000A0ZZY42',
	'RU000A1005N2',
	'RU000A0ZZ1F6',
	'RU000A0ZYAQ7',
	'RU000A0ZZCV0',
	'RU000A100AA4',
	'RU000A100GK0',
	'RU000A0ZZEA0',
	'RU000A0JWWG0',
	'RU000A1015S0',
	'RU000A101TS4',
	'RU000A101483',
]

all_securities=all_boards_securities[all_boards_securities['ISIN'].isin(needed_isins)]

# all_securities=pd.DataFrame(needed_isins,columns=['ISIN'])


## ПОЛУЧЕНИЕ ДАННЫХ ОБ ИСТОРИЧЕСКИХ ОБЪЁМАХ ТОРГОВ ПО БУМАГАМ
# Выбираем какие столбцы хотим получить и на какую дату нужны данные
vol_needed_columnes=[
'TRADEDATE',#дата торгов (для проверки)
'VALUE', #объём сделок в деньгах
'VOLUME', #объём сделок в штуках
'NUMTRADES' #число сделок
]

needed_boards_vols={
'bonds':[
	'TQCB', #Т+: Облигации - безадрес.
	# 'TQDB', #Крупные пакеты: Облигации - безадрес
	'TQRD', #Т+: Облигации Д - безадрес.
	'TQIR', #Т+ Облигации ПИР - безадрес.
	'EQOB', #Т0 Облигации - безадрес.
],
'ndm':[
	'PSOB', #РПС: Облигации - адрес.
	'PSDB', #РПС: Облигации Д - адрес.
	# 'PSAU', #Размещение - адрес.
	'OTCB', #Анонимный РПС - адрес.
	'PSIR', #РПС: Облигации ПИР - адрес.
]
}
# Задаём интервалы, за которые нужно определить объёмы и определяем за сколько календ.дней нужно запросить данные
needed_intervals=[5,25]
needed_period=max(needed_intervals)*2 #количество календарных дней, данные по которым нужно запросить

needed_end_date=dtt.date.today()
needed_start_date=needed_end_date-dtt.timedelta(days=needed_period)
needed_end_date=str(needed_end_date)
needed_start_date=str(needed_start_date)

## Находим суммы объёмов за указанный интервал дат на всех выбранных рынках и режимах торгов

# Формируем список торговых дней, чтобы потом по ним определять объёмы торгов за последние
# N дней, а не за последние 7 точек. Это нужно, т.к. по РПС в дни без торгов даты не пишутся вообще и суммы M последних точек получаются не равны 7 последним дням
dates_benchmark_security='SBER'
all_boards_vols_ts=sdk.get_board_history(
	security=dates_benchmark_security,
	start=needed_start_date,
	end=needed_end_date,
	engine='stock',
	market='shares',
	columns=['TRADEDATE'],
	board='TQBR'
	)

def volumes(
	security='RU000A101LJ0',
	boards=needed_boards_vols,
	benchmark_dates=all_boards_vols_ts,
	columns=vol_needed_columnes,
	start_date=needed_start_date,
	end_date=needed_end_date,
	needed_intervals=needed_intervals
	):
	
	# Формируем временные ряды данных с суммами на соответствующие даты по всем режимам торгов для выбранной бумаги
	# Суммируем все режимы (так изначально надо было)
	# for market,boards in boards.items():
	# 	for board in boards:
	# 		data=sdk.get_board_history(
	# 			security=security,
	# 			start=start_date,
	# 			end=end_date,
	# 			engine='stock',
	# 			market=market,
	# 			columns=columns,
	# 			board=board
	# 			)

	# 		benchmark_dates=pd.concat([benchmark_dates,data]).groupby(['TRADEDATE'],as_index=False).sum()
	
	# benchmark_dates.sort_values(by=['TRADEDATE'], inplace=True, ascending=False, ignore_index=True)	
	
	market_bonds=needed_boards_vols['bonds']
	market_ndm=needed_boards_vols['ndm']

	benchmark_dates_bonds=benchmark_dates
	# Считаем временные ряды для основного режима торгов
	for board in market_bonds:
		data=sdk.get_board_history(
			security=security,
			start=start_date,
			end=end_date,
			engine='stock',
			market='bonds',
			columns=columns,
			board=board
			)

		benchmark_dates_bonds=pd.concat([benchmark_dates_bonds,data]).groupby(['TRADEDATE'],as_index=False).sum()
	
	benchmark_dates_bonds.sort_values(by=['TRADEDATE'], inplace=True, ascending=False, ignore_index=True)

	benchmark_dates_ndm=benchmark_dates
	# Считаем временные ряды для основного режима РПС
	for board in market_ndm:
		data=sdk.get_board_history(
			security=security,
			start=start_date,
			end=end_date,
			engine='stock',
			market='ndm',
			columns=columns,
			board=board
			)

		benchmark_dates_ndm=pd.concat([benchmark_dates_ndm,data]).groupby(['TRADEDATE'],as_index=False).sum()
	
	benchmark_dates_ndm.sort_values(by=['TRADEDATE'], inplace=True, ascending=False, ignore_index=True)

	# Находим суммы соответствующих объёмов для нужных интервалов времени для основного режима
	res_table={}

	vol_sums={
	'VALUE':0,
	# 'VOLUME':0,
	'NUMTRADES':0
	}
	
	for interval in needed_intervals:
		# берём данные с суммами объёмов для всех режимов торгов и датами (benchmark_dates)
		# оставляем число строк, которое соответствует длине интервала, сумму по которому мы считаем, суммируем
		# называем столбец в стиле "вид объёма_интервал"
		volumes_interval=benchmark_dates_bonds[benchmark_dates_bonds.index<interval]
		for col_name in vol_sums:
			try:			
				vol_sums[col_name]=volumes_interval[col_name].sum()
				res_table.update({'Стакан {}_{}'.format(col_name,interval):vol_sums[col_name]})
			except KeyError:
				res_table.update({'Стакан {}_{}'.format(col_name,interval):0})

	# Находим суммы соответствующих объёмов для нужных интервалов времени для режима РПС
	vol_sums={
	'VALUE':0,
	# 'VOLUME':0,
	'NUMTRADES':0
	}
	
	for interval in needed_intervals:
		# берём данные с суммами объёмов для всех режимов торгов и датами (benchmark_dates)
		# оставляем число строк, которое соответствует длине интервала, сумму по которому мы считаем, суммируем
		# называем столбец в стиле "вид объёма_интервал"
		volumes_interval=benchmark_dates_ndm[benchmark_dates_ndm.index<interval]
		for col_name in vol_sums:
			try:			
				vol_sums[col_name]=volumes_interval[col_name].sum()
				res_table.update({'РПС {}_{}'.format(col_name,interval):vol_sums[col_name]})
			except KeyError:
				res_table.update({'РПС {}_{}'.format(col_name,interval):0})

	
	res_table.update({'ISIN':security})

	return res_table

if needed_securities!=0:
	all_securities=all_securities.head(needed_securities)

vol_table=pd.DataFrame()
iterator=0
tms1=timer_start()
for index,row in all_securities.iterrows():
	vol_row=volumes(security=row['ISIN'])
	vol_row=pd.DataFrame.from_dict(vol_row,orient='index').T 	#pandas глючит, если одну строку словаря пытаться сделать df с index в качестве колонок
																# Приходится делать index в качестве имён рядов и транспонировать через .T
	vol_table=vol_table.append(vol_row,ignore_index=True)
	iterator+=1
	if iterator%50==0:
		print(iterator,'securities processed')
		timer_end(tms1)
		tms1=timer_start()

vol_table.set_index('ISIN',inplace=True)
all_securities.set_index('ISIN',inplace=True)

all_securities=pd.concat([all_securities,vol_table],axis=1,sort=False)

# print('result: \n',all_securities)

print('Total time: ')
timer_end(tms_all)


# file_name=input('В какой файл хотите сохранить данные?\n')
# print(all_securities)
# all_securities.to_excel(f"{file_name}.xlsx")
all_securities.to_excel("securities.xlsx")

# end=input('Press enter to quit') #чтобы окно при компиляции в exe не закрывалось сразу, а перед закрытием запрашивало ввод



