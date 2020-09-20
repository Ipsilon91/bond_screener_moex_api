import asyncio

# import aiomoex
import pandas as pd

MODULE_PATH = "aiomoex_my/__init__.py"
MODULE_NAME = "aiomoex_my"
import importlib
import sys
spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module 
spec.loader.exec_module(module)

import aiomoex_my as aiomoex

## Значения по-умолчанию
board="TQCB" #облигации T+
engine="stock" # фондовый рынок
market="bonds" #рынок облигаций
columns=()#"SECID","ISIN","SHORTNAME","SECNAME","COUPONPERCENT","YIELDATPREVWAPRICE","EFFECTIVEYIELDWAPRICE","ZSPREADBP","GSPREADBP","DURATIONWAPRICE","OFFERDATE","MATDATE","FACEUNIT","LISTLEVEL") #колонки для вывода
# security='RU000A101483'


# Посмотреть все доступные рынки - markets(бонды, рпс)
def get_reference(placeholder="markets"):
	async def main():
	    async with aiomoex.ISSClientSession():
	        data = await aiomoex.get_reference(placeholder=placeholder)
	        df = pd.DataFrame(data)
	        # df.set_index('id', inplace=True)
	        # df2=df[(df.is_traded==1)]
	        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
	        # 	print(df2[['board_title','boardid','is_traded']], '\n')
	    return df
	data=asyncio.get_event_loop().run_until_complete(main()) 	#В asyncio какой-то баг. Если вместо run() использовать эту конструкцию, то не будет RuntimeError
																# https://github.com/aio-libs/aiohttp/issues/4324
	return data


	asyncio.run(main())

# Посмотреть все бумаги в выбранном режиме (board) торгов
def get_securities(board=board,engine=engine,market=market,columns=columns):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_board_securities(board=board,engine=engine,market=market,columns=columns)
			df = pd.DataFrame(data)
			# print(df.head(), '\n')
			# print(df.tail(), '\n')
			# df.info()
		return df
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

# Свечи по выбранной бумаге за выбранный период в формате OHLC
def get_board_candles(security,start='2015-01-01',end='2020-01-01',board=board,engine=engine,market=market):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_board_candles(security=security,start=start,end=end,board=board,engine=engine,market=market)
			df = pd.DataFrame(data)
			# print(df.head(), '\n')
			# print(df.tail(), '\n')
			# df.info()
		return df
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

# Исторические данные по выбранным столбцам в выбранном режиме торгов
def get_board_history(security,start='2020-07-01',end='2020-07-10',board=board,engine=engine,market=market,columns=columns):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_board_history(security=security,start=start,end=end,board=board,engine=engine,market=market,columns=columns)
			df = pd.DataFrame(data)
			# df.set_index('TRADEDATE', inplace=True)
			# print(df.head(), '\n')
			# print(df.tail(), '\n')
			# df.info()
		return df
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

# Исторические данные по выбранным столбцам во всех(!) режимах торгов
def get_market_history(security,start='2020-07-01',end='2020-07-05',engine=engine,market=market,columns=columns):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_market_history(security=security,start=start,end=end,engine=engine,market=market,columns=columns)
			df = pd.DataFrame(data)
			# print(df.head(), '\n')
			# print(df.tail(), '\n')
			# df.info()
		return df
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

# get_board_history_all

### ФУНКЦИЯ ЗАПРОСА ДАННЫХ ПО ВСЕМ БУМАГАМ НА КОНКРЕТНУЮ ДАТУ
# Выдаёт как данные по самим эмиссиям, так и ценовые данные и доходности на дату
# На выходе получаем словарь формата {securities:x1,marketdata:x2,marketdata_yields:x3}
# x1,x2,x3 - dataframe pandas с самими таблицами данных
def get_board_history_all(engine=engine,market=market,board=board,date=None):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_board_history_all(engine=engine,market=market,board=board,date=date)
			ret_data={}
			for ind,item in data.items():
				ret_data[ind] = pd.DataFrame(item)

		return ret_data
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

def bond_cash_flows(security,engine=engine,market=market):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.bond_cash_flows(engine=engine,market=market,security=security)
			ret_data={}
			for ind,item in data.items():
				ret_data[ind] = pd.DataFrame(item)

		return ret_data
	data=asyncio.get_event_loop().run_until_complete(main())
	return data


def get_coupons(security,engine=engine,market=market,):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_coupons(engine=engine,market=market,security=security)
			df = pd.DataFrame(data)

		return df
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

def get_amortizations(security,engine=engine,market=market):
	async def main():
		async with aiomoex.ISSClientSession():
			data = await aiomoex.get_amortizations(engine=engine,market=market,security=security)
			df = pd.DataFrame(data)

		return df
	data=asyncio.get_event_loop().run_until_complete(main())
	return data

# RU000A1005Y9

# Выгрузить данные по всем бумагам в выбранном режиме торгов на текущую дату в три эксель файла
# bonds=get_board_history(security='RU000A1005Y9')


# bonds.to_excel("RU000A1005Y9.xlsx")


