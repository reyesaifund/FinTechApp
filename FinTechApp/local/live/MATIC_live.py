#!/Users/reyestrading/.pyenv/versions/3.7.11/bin/python3.7

from datetime import datetime, timedelta
from unicodedata import decimal
import numpy as np
import time
import pandas as pd
import os, sys
import pytz
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import glob
import gc
from scipy.interpolate import splrep, splev
from binance.client import Client
import random
from scipy.signal import find_peaks
from scipy import stats
from tick.hawkes import HawkesExpKern
from sklearn.model_selection import train_test_split
import pickle
from scipy.signal import argrelextrema
import subprocess
import signal
import warnings
import sched
from math import floor
from pathlib import Path
import schedule


warnings.filterwarnings("ignore")

random.seed(0)
np.random.seed(0)

A_Key = '******'
S_Key = '****'


client = Client(A_Key, S_Key, tld='us')

s = sched.scheduler(time.time, time.sleep)

def rolling_prediction(time_1, price, volume_1):
	try:
		symbol = 'MATIC-USD'
		final_final = 0
		next_jump_time_initial = 0
		final_output = {}
		time_final = time_1
		price_final = price
		volume_final = volume_1
		event_type_final_final = np.array([])
		time_event_type_final_final = np.array([])

		nanny = np.argwhere(np.isnan(price_final))
		time_final = np.delete(time_final, nanny)
		price_final = np.delete(price_final, nanny)
		volume_final = np.delete(price_final, nanny)
		time_now_1 = time_final[-1]
		current_price_1 = price_final[-1]

		if price_final.size < 357:
			os.system('python download_minute_data_copra.py')
		if price_final.size >= 357:
			seconds_back = 60*15
			idxs_start_time = np.argwhere(time_final/1000000000 >= time.time()-(seconds_back)).flatten()
			time_final = time_final[idxs_start_time]
			price_final = price_final[idxs_start_time]
			volume_final = volume_final[idxs_start_time]

			section_time = time_final
			section_price = price_final
			section_time = section_time[np.argwhere(np.isnan(section_price)==False).flatten().astype(int)]
			volume_final = volume_final[np.argwhere(np.isnan(section_price)==False).flatten().astype(int)]
			section_price = section_price[np.argwhere(np.isnan(section_price)==False).flatten().astype(int)]

			idx_sort = np.argsort(section_time)
			section_time = section_time[idx_sort]
			section_price = section_price[idx_sort]
			volume_final = volume_final[idx_sort]

			vals, idx_dups = np.unique(section_time, return_index=True)

			section_time = section_time[idx_dups]
			section_price = section_price[idx_dups]
			volume_final = volume_final[idx_dups]

			section_price_hat = splrep(section_time,section_price,k=5,s=1)
			section_price_hat = splev(section_time,section_price_hat)

			section_der = np.diff(section_price_hat)/np.diff(section_time)
			section_der_2_diff = np.diff(section_der).astype(np.longdouble)
			section_time_2_diff = np.diff(section_time).astype(np.longdouble)[1:]
			section_der_2 = np.divide(section_der_2_diff, section_time_2_diff).astype(np.longdouble)
			zero_crossings = np.argwhere(np.diff(np.sign(section_der))).flatten().astype(int)
			zero_crossings = np.append(zero_crossings, section_price_hat.size-1)
			zero_crossings = np.insert(zero_crossings, 0, 0)
			zero_crossings = np.sort(np.unique(zero_crossings))
			#'''
			extrema_idx_arr = np.array([])
			for i in range(1, zero_crossings.size-1):
				try:
					if section_price[zero_crossings[i]]-section_price[zero_crossings[i-1]] > 0:
						extrema_idx = np.argmax(section_price[zero_crossings[i-1]:zero_crossings[i+1]])
						extrema_idx_arr = np.append(extrema_idx_arr, np.array([zero_crossings[i-1], zero_crossings[i+1], extrema_idx]), axis=0)
					elif section_price[zero_crossings[i]]-section_price[zero_crossings[i-1]] < 0:
						extrema_idx = np.argmin(section_price[zero_crossings[i-1]:zero_crossings[i+1]])
						extrema_idx_arr = np.append(extrema_idx_arr, np.array([zero_crossings[i-1], zero_crossings[i+1], extrema_idx]), axis=0)
				except:
					pass
			zero_crossings = extrema_idx_arr.flatten().astype(int)
			weights_1 = np.ones(len(section_price))
			for i in range(0, zero_crossings.size, 3):
				weights_1[zero_crossings[i]:zero_crossings[i+1]][zero_crossings[i+2]] = 100

			section_price_hat = splrep(section_time,section_price,k=3,s=5, w=weights_1)
			section_price_hat = splev(section_time,section_price_hat)

			section_der = np.diff(section_price_hat)/np.diff(section_time)
			section_der_2_diff = np.diff(section_der).astype(np.longdouble)
			section_time_2_diff = np.diff(section_time).astype(np.longdouble)[1:]
			section_der_2 = np.divide(section_der_2_diff, section_time_2_diff).astype(np.longdouble)
			zero_crossings = np.argwhere(np.diff(np.sign(section_der))).flatten().astype(int)
			zero_crossings = np.append(zero_crossings, section_price_hat.size-1)
			zero_crossings = np.insert(zero_crossings, 0, 0)
			zero_crossings = np.sort(np.unique(zero_crossings))
			zero_crossings_2 = np.argwhere(np.diff(np.sign(section_der_2))).flatten().astype(int)
			zero_crossings = np.sort(np.unique(np.concatenate((zero_crossings, zero_crossings_2), axis=None)))
			#'''
			der_y = section_price_hat[zero_crossings]

			trend_org = section_price_hat[-1]-section_price_hat[-2]

			if np.abs(trend_org) > 0:
		
				last_extrema = zero_crossings[-2]
				trend = section_price_hat[-1]-section_price_hat[-2]

				reader = DataFileReader(open('trend_switch_' + symbol + '.avro',"rb"), DatumReader())
				for r in reader:
					entry_time = r.get('entry_time')
					entry_price = r.get('entry_price')
					entry_direction = r.get('entry_direction')
				reader.close()

				trend_switch_initial = 0

				if entry_direction == 0:
					trend_switch_initial = trend

				elif entry_direction > 0 and trend < 0:
					trend_switch_initial = trend

				elif entry_direction < 0 and trend > 0:
					trend_switch_initial = trend

				secs_passed_since_extrema = (section_time[-1]/1000000000)-(section_time[0]/1000000000)
				avg_vol_per_sec = np.nansum(volume_final)/secs_passed_since_extrema

				if trend_switch_initial != 0 and section_time[zero_crossings[-2]] > entry_time:

					schema = avro.schema.parse(open("trend_switch.avsc", "r").read())
					writer = DataFileWriter(open('trend_switch_' + symbol + '.avro', "wb"), DatumWriter(), schema)
					writer.append({"entry_time": section_time[zero_crossings[-2]], "entry_price": section_price[-1], "entry_direction": trend})
					writer.close()

					event_type_final_final = np.append(event_type_final_final, section_price[-1])
					time_event_type_final_final = np.append(time_event_type_final_final, section_time[-1])

					time_event_type_final_final = time_event_type_final_final.astype(float)
					event_type_final_final = event_type_final_final.astype(float)
					price_pd = pd.DataFrame({'Time':time_event_type_final_final, 'type_event':event_type_final_final})
					price_pd.sort_values(by='Time', inplace=True)
					price_pd.drop_duplicates(subset='Time', keep='first', inplace=True)
					price_pd.reset_index(drop=True, inplace=True)
					price_pd.to_csv('features_transformer_1_' + symbol + '.csv', mode='a', header=False)

					price_pd = pd.read_csv('features_transformer_1_' + symbol + '.csv', names=['Time', 'type_event'], dtype=object, on_bad_lines='skip')
					price_pd['Time'] = price_pd['Time'].astype(float)
					price_pd['type_event'] = price_pd['type_event'].astype(float)

					#price_pd['Time'] = price_pd['Time'].apply(lambda x: x/1000000000)
					#price_pd.reset_index(drop=True, inplace=True)
					#price_pd.sort_values(by='Time', inplace=True)
					start_timestamp = price_pd.iloc[0]['Time']
					price_pd['type_event'] = price_pd['type_event'].astype(float).abs()
					price_pd.drop_duplicates(subset='Time', keep='first', inplace=True)
					price_pd['final_y'] = price_pd['type_event'].pct_change()
					price_pd['final_y'] = price_pd['final_y'].shift(-1)
					
					price_pd = price_pd.reset_index(drop=True)
					price_pd.sort_values(by='Time', inplace=True)
					price_pd.drop(columns=['type_event'], inplace=True)
					price_pd['type_event'] = price_pd['final_y'].round(decimals=3)
					#price_pd.drop(columns=['final_y'], inplace=True)

					price_pd['time_since_start'] = price_pd['Time']-start_timestamp
					price_pd['time_since_last_event'] = price_pd['Time'].diff()
					price_pd.drop(columns=['Time'], inplace=True)
					price_pd = price_pd[['time_since_start', 'time_since_last_event', 'final_y', 'type_event']]
					price_pd['type_event'] = price_pd['type_event'].apply(lambda x: 1 if x>0 else -1)
					price_pd['time_since_last_event'] = price_pd['time_since_start'].diff()
					price_pd['roc'] = price_pd.final_y / price_pd.time_since_last_event
					price_pd.reset_index(drop=True, inplace=True)
					price_pd = price_pd[['time_since_start', 'time_since_last_event', 'roc', 'final_y', 'type_event']]

					price_pd['long'] = pd.DataFrame(np.full(len(price_pd),1))
					price_pd['short'] = pd.DataFrame(np.full(len(price_pd),-1))
					price_pd = price_pd[['time_since_start', 'final_y', 'long', 'short', 'type_event']]
					price_pd.reset_index(drop=True, inplace=True)
					price_pd.to_csv('features_transformer_2_' + symbol + '.csv', mode='w', header=False)
					#time.sleep(1)

					price_pd_1 = pd.read_csv('minute_data_' + symbol + '.csv', names=["Time", "Price", "Volume"], dtype=object, on_bad_lines='skip')
					price_pd_1 = price_pd_1.dropna()
					price_pd_1['Price'] = price_pd_1['Price'].astype(float)
					price_pd_1['Volume'] = price_pd_1['Volume'].astype(float)
					
					price_pd_1['Time'] = pd.to_datetime(price_pd_1['Time'], unit='ns', utc=True)
					price_pd_1.sort_values(by='Time', inplace=True)
					price_pd_1.drop_duplicates(subset='Time', keep='first', inplace=True)
					price_pd_1.reset_index(drop=True, inplace=True)
					price_pd_1 = price_pd_1[['Time', 'Price']]
					price_pd = pd.read_csv('features_transformer_2_' + symbol + '.csv', names=['time_since_start', 'final_y', 'long', 'short', 'type_event'], dtype=object, on_bad_lines='skip')
					price_pd['time_since_start'] = price_pd['time_since_start'].astype(float)
					#price_pd['final_y'] = price_pd['final_y'].astype(float).mul(2).round(decimals=2).div(2).round(decimals=3)
					price_pd['final_y'] = price_pd['final_y'].astype(float).round(decimals=3)
					price_pd = price_pd.replace(0, np.nan)
					#price_pd = price_pd.fillna(method='ffill').fillna(method='bfill')
					price_pd['type_event'] = price_pd['type_event'].astype(float)
					price_pd['long'] = price_pd['long'].astype(float)
					price_pd['short'] = price_pd['short'].astype(float)
					price_pd['Time'] = price_pd['time_since_start'].apply(lambda x: x+start_timestamp)
					#price_pd['Time'] = pd.to_datetime(price_pd['Time'], unit='ns', utc=True)
					price_pd = price_pd.fillna(0)


					#price_pd.sort_values(by='Time', inplace=True)
					#price_pd.reset_index(drop=True, inplace=True)
					price_pd.drop_duplicates(subset='Time', keep='first', inplace=True)
					price_pd.reset_index(drop=True, inplace=True)
					#price_pd.Time = price_pd.Time.astype('datetime64[ns]')
					#price_pd_1.Time = price_pd_1.Time.astype('datetime64[ns]')
					#price_pd_1.sort_values(by='Time', inplace=True)
					price_pd['Time'] = pd.to_datetime(price_pd['Time'], unit='ns', utc=True).dt.round('min')
					price_pd.reset_index(drop=True, inplace=True)
					#price_pd.set_index('Time', drop=False, inplace=True)
					#price_pd = price_pd.resample('1T').agg({'time_since_start': np.nanmean, 'final_y': np.nanmean, 'long': np.nanmean, 'short': np.nanmean, 'type_event': np.nanmean})
					#price_pd.reset_index(drop=False, inplace=True)

					price_pd_1['Time'] = pd.to_datetime(price_pd_1['Time'], unit='ns', utc=True)
					price_pd.Time = price_pd.Time.dt.tz_convert(pytz.utc).astype('datetime64[ns]')
					price_pd_1.Time = price_pd_1.Time.dt.tz_convert(pytz.utc).astype('datetime64[ns]')
					price_pd = price_pd.merge(price_pd_1, on='Time', how='left')
					price_pd.reset_index(drop=True, inplace=True)
					price_pd_1.Time = price_pd_1.Time.astype('datetime64[s]').astype(int).astype(float)
					price_pd.Time = price_pd.Time.astype('datetime64[s]').astype(int).astype(float)
					#price_pd = price_pd.loc[price_pd['final_y'] != 0]
					min_time_data = price_pd_1.Time.values.astype(float)
					min_price_data = price_pd_1.Price.values.astype(float)
					if min_time_data.size > 60*3:
						min_time_data = min_time_data[-int(60*3):]
						min_price_data = min_price_data[-int(60*3):]

					section_price_hat_2 = splrep(min_time_data,min_price_data,k=3,s=5)
					section_price_hat_2 = splev(min_time_data,section_price_hat_2)
					section_der = np.diff(section_price_hat)/np.diff(section_time)
					zero_crossings = np.argwhere(np.diff(np.sign(section_der))).flatten().astype(int)

					if zero_crossings.size >= 1:
						start_time_rows_back = min_time_data[zero_crossings[-1]]
						if len(price_pd.loc[price_pd['Time'] >= start_time_rows_back]) < 3:
							if zero_crossings.size >= 2:
								start_time_rows_back = min_time_data[zero_crossings[-2]]
							else:
								start_time_rows_back = min_time_data[0]
					else:
						start_time_rows_back = min_time_data[0]

					price_pd = price_pd.loc[price_pd['Time'] >= start_time_rows_back]

					#if len(price_pd) > 40:
					#	price_pd = price_pd.tail(40)

					print('check_len')
					print(zero_crossings)
					print(len(price_pd))

					y_pred_org_1 = 0
					acc_final = -100
					proba_final = -100
					#if len(price_pd) > 27:
					#	price_pd = price_pd.tail(27)
					#for i in range(len(price_pd), len(price_pd)+1):
					price_pd_current_2 = price_pd.copy()
					#price_pd_current_2 = price_pd_current_2.tail(i)
					price_pd_current_2.reset_index(drop=True, inplace=True)
					price_pd_current_pre_dummy = price_pd_current_2
					col_1 = np.unique(price_pd_current_2['final_y'].values.astype(float).astype(str))
					price_pd_current_2['type_event'] = price_pd_current_2['final_y']
					price_pd_current_2 = pd.get_dummies(price_pd_current_2.type_event, prefix=None, columns=col_1)
					price_pd_current_2.columns = col_1
					price_pd_current_2.reset_index(drop=True, inplace=True)
					price_pd_current_2['type_event_final'] = price_pd_current_pre_dummy['final_y']
					test = price_pd_current_2.tail(1)
					test = test.applymap(lambda x: 0 if float(x)!=0 else 0)
					test['type_event_final'] = 0
					test.reset_index(drop=True, inplace=True)
					price_pd_train_org = price_pd_current_2
					price_pd_test = test.to_dict('records')
					pkl_file_test = {'dim_process': 2, 'devtest': [], 'args': None, 'dev': [], 'train': [], 'test': [price_pd_test]}
					a_file = open("data/test.pkl", "wb")
					pickle.dump(pkl_file_test, a_file)
					a_file.close()
					#time.sleep(1)
					for step_1 in np.arange(.1, 1.1, .1):
						try:
							dev, train = train_test_split(price_pd_train_org, train_size=step_1, test_size=1-step_1, shuffle=False, random_state=0)
							if len(train) > 0 and len(dev) > 0:
								break
						except:
							pass

					try:
						train.reset_index(drop=True, inplace=True)
						dev.reset_index(drop=True, inplace=True)
						price_pd_train = train.to_dict('records')
					except:
						dev, train = train_test_split(price_pd_train_org, test_size=.5, shuffle=False, random_state=0)
						train.reset_index(drop=True, inplace=True)
						dev.reset_index(drop=True, inplace=True)
						price_pd_train = train.to_dict('records')
					price_pd_dev = dev.to_dict('records')
					pkl_file_train = {'dim_process': 2, 'devtest': [], 'args': None, 'dev': [], 'train': [price_pd_train], 'test': []}
					a_file = open("data/train.pkl", "wb")
					pickle.dump(pkl_file_train, a_file)
					a_file.close()
					#time.sleep(1)

					pkl_file_dev = {'dim_process': 2, 'devtest': [], 'args': None, 'dev': [price_pd_dev], 'train': [], 'test': []}
					a_file = open("data/dev.pkl", "wb")
					pickle.dump(pkl_file_dev, a_file)
					a_file.close()
					#time.sleep(1)
					try:
						os.system("pkill -f Main.py")
						os.system("rm best_model_birthday.csv")
					except:
						pass
					#time.sleep(1)
					os.system("bash run.sh")
							
					price_pd_pred = pd.read_csv('best_model_birthday.csv', names=["Predicted", "pred_amount_abs", "acc", "proba"], dtype=object, on_bad_lines='skip')
					price_pd_pred.reset_index(drop=True, inplace=True)
					pred_1 = float(price_pd_pred['Predicted'].iloc[-1])
					acc_1 = float(price_pd_pred['acc'].iloc[-1])
					proba_1 = float(price_pd_pred['proba'].iloc[-1])
					pred_amount_1 = float(price_pd_pred['pred_amount_abs'].iloc[-1])
					#if acc_1 >= acc_final:
					proba_final = proba_1
					pred_amount_2 = pred_amount_1
					acc_final = acc_1
					y_pred_org_1 = pred_1

					if y_pred_org_1 == 0:
						#os.system('/Users/reyestrading/.pyenv/versions/pyenv_36/bin/python download_minute_data_copra.py')
						os.system('python restart_features_1.py')
						schema = avro.schema.parse(open("trend_switch.avsc", "r").read())
						writer = DataFileWriter(open('trend_switch_' + symbol + '.avro', "wb"), DatumWriter(), schema)
						writer.append({"entry_time": 0, "entry_price": 0, "entry_direction": 0})
						writer.close()


					###CHECK_ME put in trailing (simulation_returns_55.py line 100-126)

					#trailing_1 = .05
					###trailing logic sim55 
					##use below to make live version prev
					#trailing start 

					trailing_multiplier = 1

					reader = DataFileReader(open('total_trades_' + symbol + '_2.avro',"rb"), DatumReader())
					for r in reader:
						last_trailing_1 = float(r.get('avg_roc_ask'))
					reader.close()

					if last_trailing_1 == 0:
						reader = DataFileReader(open('total_trades_' + symbol + '.avro',"rb"), DatumReader())
						for r in reader:
							last_trailing_1 = float(r.get('avg_roc_ask'))
						reader.close()

					price_pd_last_jump_time_1 = pd.read_csv('features_transformer_1_' + symbol + '.csv', names=['Time', 'type_event'], dtype=object, on_bad_lines='skip')
					price_pd_last_jump_time_1['Time'] = price_pd_last_jump_time_1['Time'].astype(float)
					price_pd_last_jump_time_1['type_event'] = price_pd_last_jump_time_1['type_event'].astype(float)
					last_jump_time_1 = float(price_pd_last_jump_time_1['Time'].iloc[-2])

					previous_trade_price_arr = price_pd_1['Price'].values.astype(float)
					previous_trade_price_arr = previous_trade_price_arr[np.argwhere(price_pd_1['Time'].values.astype(float) >= last_jump_time_1).flatten().astype(int)]

					
					##use below to make live version prev 
					trailing_1 = 0
					no_side_change = 0
					for i1 in range(1, previous_trade_price_arr.size+1):
						try:
							section_price = previous_trade_price_arr[:i1+1]
							if previous_trade_price_arr[-1]-previous_trade_price_arr[0] < 0:
								price_since_high = section_price[np.argmin(section_price).flatten()[0]:]
								if (price_since_high[-1]-price_since_high[0])/price_since_high[0] > trailing_1:
									trailing_1 = (price_since_high[-1]-price_since_high[0])/price_since_high[0]

							elif previous_trade_price_arr[-1]-previous_trade_price_arr[0] > 0:
								price_since_high = section_price[np.argmax(section_price).flatten()[0]:]
								if (price_since_high[-1]-price_since_high[0])/price_since_high[0] < trailing_1:
									trailing_1 = (price_since_high[-1]-price_since_high[0])/price_since_high[0]
						except Exception as e:
							exc_type, exc_obj, exc_tb = sys.exc_info()
							fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
							print(exc_type, fname, exc_tb.tb_lineno)
							print(e)

					trailing_1 = np.abs(trailing_1)*trailing_multiplier

					if trailing_1 == 0:
						trailing_1 = last_trailing_1

					reader = DataFileReader(open('total_trades_' + symbol + '_2.avro',"rb"), DatumReader())
					for r in reader:
						new_time = float(r.get('time'))
						new_side = float(r.get('side'))
						new_y_pred_org_1 = float(r.get('avg_roc_bid'))
						new_price = float(r.get('exit_price'))
					reader.close()

					schema = avro.schema.parse(open("total_trades.avsc", "r").read())
					writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
					writer.append({"time": new_time, "side": new_side, "entry_price": new_price, "exit_price": current_price_1, "avg_roc_ask": trailing_1, "avg_roc_bid": new_y_pred_org_1})
					writer.close()


					#if np.round(trailing_1, decimals=3) > trailing_1:
					#	trailing_1 = np.round(trailing_1, decimals=3)-.001
					#elif np.round(trailing_1, decimals=3) < trailing_1:
					#	trailing_1 = np.round(trailing_1, decimals=3)

					#if trailing_1 > .02:
					#	trailing_1 = .02
					#if trailing_1 < .01:
					#	trailing_1 = .01

					#y_pred_org_1 = y_pred_org_1*-1
					#pred_amount_2 = pred_amount_2*-1

					print('DATA_PREDICT')
					print(datetime.utcfromtimestamp(time_now_1/1000000000))
					print(y_pred_org_1)
					print(current_price_1)
					print(trailing_1)
					print(pred_amount_2)
					print(proba_final)
					print(price_final[-1]-new_price)
					print(avg_vol_per_sec)
					print(trend)
					print(acc_final)

					#if np.abs(pred_amount_2) < trailing_1*1:
					#	y_pred_org_1 = y_pred_org_1*-1
					#	pred_amount_2 = pred_amount_2*-1

					if proba_final > 0:

						reader = DataFileReader(open('hat_direction_' + symbol + '.avro',"rb"), DatumReader())
						for r in reader:
							hat_direction = r.get('hat_direction')
						reader.close()

						if (hat_direction==0 or hat_direction < 0) and y_pred_org_1 > 0:

							final_final = 1	

							schema = avro.schema.parse(open("hat_direction_2.avsc", "r").read())
							writer = DataFileWriter(open('hat_direction_' + symbol + '.avro', "wb"), DatumWriter(), schema)
							writer.append({"hat_direction": 1})
							writer.close()
							
							side_0 = -1

							time_arr = np.array([])
							side_arr = np.array([])
							entry_price_arr = np.array([])
							exit_price_arr = np.array([])
							reader = DataFileReader(open('total_trades_' + symbol + '.avro',"rb"), DatumReader())
							for r in reader:
								time_arr = np.append(time_arr, r.get('time'))
								side_arr = np.append(side_arr, r.get('side'))
								entry_price_arr = np.append(entry_price_arr, r.get('entry_price'))
								exit_price_arr = np.append(exit_price_arr, r.get('exit_price'))
							reader.close()

							time_arr = np.append(time_arr, time_now_1)
							side_arr = np.append(side_arr, side_0)
							entry_price_arr = np.append(entry_price_arr, exit_price_arr[-1])
							exit_price_arr = np.append(exit_price_arr, current_price_1)

							schema = avro.schema.parse(open("total_trades.avsc", "r").read())
							writer = DataFileWriter(open('total_trades_' + symbol + '.avro', "wb"), DatumWriter(), schema)
							for i in range(0, len(side_arr)):
								writer.append({"time": time_arr[i], "side": side_arr[i], "entry_price": entry_price_arr[i], "exit_price": exit_price_arr[i], "avg_roc_ask": trailing_1, "avg_roc_bid": y_pred_org_1})
							writer.close()

							profit_now = 0
							total_profit = np.array([])

							for i2 in range(2, len(side_arr)):
								if side_arr[i2] == 1:
									profit_now = exit_price_arr[i2]-entry_price_arr[i2]
								elif side_arr[i2] == -1:
									profit_now = entry_price_arr[i2]-exit_price_arr[i2]
								total_profit = np.append(total_profit, profit_now)

							print('total_total_total elif hd ==0&1 & y_pred<0')
							print(total_profit)
							print(np.nansum(total_profit))




							schema = avro.schema.parse(open("total_trades.avsc", "r").read())
							writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
							writer.append({"time": time_now_1, "side": side_0, "entry_price": new_price, "exit_price": current_price_1, "avg_roc_ask": trailing_1, "avg_roc_bid": y_pred_org_1})
							writer.close()
					

						elif (hat_direction==0 or hat_direction > 0) and y_pred_org_1 < 0:	
							
							final_final = -1

							schema = avro.schema.parse(open("hat_direction_2.avsc", "r").read())
							writer = DataFileWriter(open('hat_direction_' + symbol + '.avro', "wb"), DatumWriter(), schema)
							writer.append({"hat_direction": -1})
							writer.close()	

							side_0 = 1

							time_arr = np.array([])
							side_arr = np.array([])
							entry_price_arr = np.array([])
							exit_price_arr = np.array([])
							reader = DataFileReader(open('total_trades_' + symbol + '.avro',"rb"), DatumReader())
							for r in reader:
								time_arr = np.append(time_arr, r.get('time'))
								side_arr = np.append(side_arr, r.get('side'))
								entry_price_arr = np.append(entry_price_arr, r.get('entry_price'))
								exit_price_arr = np.append(exit_price_arr, r.get('exit_price'))
							reader.close()
							
							time_arr = np.append(time_arr, time_now_1)
							side_arr = np.append(side_arr, side_0)
							entry_price_arr = np.append(entry_price_arr, exit_price_arr[-1])
							exit_price_arr = np.append(exit_price_arr, current_price_1)

							schema = avro.schema.parse(open("total_trades.avsc", "r").read())
							writer = DataFileWriter(open('total_trades_' + symbol + '.avro', "wb"), DatumWriter(), schema)
							for i in range(0, len(side_arr)):
								writer.append({"time": time_arr[i], "side": side_arr[i], "entry_price": entry_price_arr[i], "exit_price": exit_price_arr[i], "avg_roc_ask": trailing_1, "avg_roc_bid": y_pred_org_1})
							writer.close()

							profit_now = 0
							total_profit = np.array([])

							for i2 in range(2, len(side_arr)):
								if side_arr[i2] == 1:
									profit_now = exit_price_arr[i2]-entry_price_arr[i2]
								elif side_arr[i2] == -1:
									profit_now = entry_price_arr[i2]-exit_price_arr[i2]
								total_profit = np.append(total_profit, profit_now)

							print('total_total_total elif hd ==0&1 & y_pred<0')
							print(total_profit)
							print(np.nansum(total_profit))

							schema = avro.schema.parse(open("total_trades.avsc", "r").read())
							writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
							writer.append({"time": time_now_1, "side": side_0, "entry_price": new_price, "exit_price": current_price_1, "avg_roc_ask": trailing_1, "avg_roc_bid": y_pred_org_1})
							writer.close()	

					gc.collect()	


								

	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		print(e)
		#exception AssertionError
		#subprocess.Popen("python renew_avros_2.py &", shell=True)
	
		return [final_final]
	return [final_final]



def algo_run():

	symbol = 'MATIC-USD'

	time_now = datetime.utcnow()
	dt = pytz.timezone('America/Chicago').normalize(time_now.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
	is_dst = bool(dt.dst())
	if is_dst is True:
		time_800 = datetime.strptime(str(datetime.today().strftime('%Y-%m-%d')) + ' 13:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
		time_830 = datetime.strptime(str(datetime.today().strftime('%Y-%m-%d')) + ' 13:30:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
		time_25930 = datetime.strptime(str(datetime.today().strftime('%Y-%m-%d')) + ' 19:59:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)

	elif is_dst is False:
		time_800 = datetime.strptime(str(datetime.today().strftime('%Y-%m-%d')) + ' 14:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
		time_830 = datetime.strptime(str(datetime.today().strftime('%Y-%m-%d')) + ' 14:30:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
		time_25930 = datetime.strptime(str(datetime.today().strftime('%Y-%m-%d')) + ' 20:59:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)


	try:

		#price_pd = pd.read_csv('current_stream_MATIC-USD_1.csv', names=["Time", "Symbol", "Price", "Volume"], dtype=object, on_bad_lines='skip')
		#price_pd.reset_index(drop=True, inplace=True)
		#os.system("rm current_stream_MATIC-USD_1.csv")
		#price_pd.tail(1).to_csv('current_stream_MATIC-USD_1.csv', mode='w', header=False)
		#price_pd.to_csv('current_stream_MATIC-USD_2.csv', mode='a', header=False)

		try:

			price_pd = pd.read_csv('current_stream_MATIC-USD_1.csv', names=["Time", "Symbol", "Price", "Volume"], dtype=object, on_bad_lines='skip')
			price_pd['Time'] = price_pd['Time'].astype(float)
			price_pd['Price'] = price_pd['Price'].astype(float)
			price_pd['Volume'] = price_pd['Volume'].astype(float)

			time_now_1 = (time.time()-(60*60*6))*1000000000
			price_pd = price_pd.loc[price_pd['Time'] >= time_now_1]
			os.system("rm current_stream_MATIC-USD_1.csv")
			time.sleep(1)
			price_pd.to_csv('current_stream_MATIC-USD_1.csv', mode='a', header=False)
		
			price_pd.sort_values(by='Time', inplace=True)
			price_pd.drop_duplicates(subset='Time', keep='first', inplace=True)
			price_pd['Time'] = pd.to_datetime(price_pd['Time'], unit='ns', utc=True)
			price_pd.reset_index(drop=True, inplace=True)
			price_pd.set_index('Time', drop=True, inplace=True)
			price_pd = price_pd[['Price', 'Volume']]
			

			price_pd_tick = price_pd

			price_pd = price_pd.resample('1T').agg({'Price': np.nanmean, 'Volume': np.nansum})
			price_pd.to_csv('minute_data_' + symbol + '.csv', mode='w', header=False)
			#time.sleep(3)
		except:
			pass
		price_pd = pd.read_csv('minute_data_' + symbol + '.csv', names=["Price", "Volume"], dtype=object, on_bad_lines='skip')
		price_pd.index = pd.to_datetime(price_pd.index, unit='ns', utc=True)
		price_pd['Price'] = price_pd['Price'].astype(float)
		price_pd['Volume'] = price_pd['Volume'].astype(float)

		price_pd = price_pd.dropna()

		time_1 = price_pd.index.values.flatten().astype(float)
		price_1 = price_pd.Price.values.flatten().astype(float)
		volume_1 = price_pd.Volume.values.flatten().astype(float)

		current_price_org_min = price_1[-1]
		current_time_org_min = time_1[-1]

		try:

			time_tick = price_pd_tick.index.values.flatten().astype(float)
			price_tick = price_pd_tick.Price.values.flatten().astype(float)
			current_price_org = price_tick[-1]
			current_time_org = time_tick[-1]
		except:
			pass



		

		reader = DataFileReader(open('total_trades_' + symbol + '_2.avro',"rb"), DatumReader())
		for r in reader:
			last_time = r.get('time')
			last_side = r.get('side')
			last_residual_max = r.get('avg_roc_ask')
		reader.close()

		last_residual_max = last_residual_max*2

		if last_residual_max < .005:
			last_residual_max = .005

		last_side = last_side*-1

		reader = DataFileReader(open('order_queue_' + symbol + '.avro',"rb"), DatumReader())
		for r in reader:
			queued_order_direction_1 = r.get('queued_order_direction')
		reader.close()

		if last_side > 0:
			
			try:

			


				last_trade_time = time_tick[np.argwhere(time_tick > last_time).flatten().astype(int)]
				last_trade_price = price_tick[np.argwhere(time_tick > last_time).flatten().astype(int)]

				if last_trade_price.size > 0:

					current_price_org_2 = last_trade_price[-1]

					#print('res')
					#print(last_residual_max)
					#print((current_price_org_2-np.amax(last_trade_price))/np.amax(last_trade_price))

					#section_price_hat = splrep(last_trade_time,last_trade_price,k=5,s=1)
					#section_price_hat = splev(last_trade_time,section_price_hat)

					#print('diff')
					#print(np.diff(section_price_hat, n=1)[-1])

					#if (current_price_org_2-np.amax(last_trade_price))/np.amax(last_trade_price) < -np.abs(last_residual_max) and np.diff(section_price_hat, n=1)[-1] < 0:
					if (current_price_org_2-np.amax(last_trade_price))/np.amax(last_trade_price) < -np.abs(last_residual_max):
						print('MATIC_check_trailing')
						print(datetime.utcfromtimestamp(last_time/1000000000))
						print((current_price_org_2-np.amax(last_trade_price))/np.amax(last_trade_price))
						print(np.amax(last_trade_price))
						print(current_price_org_2)
						print(last_residual_max)

						'''

						schema = avro.schema.parse(open("hat_direction_2.avsc", "r").read())
						writer = DataFileWriter(open('hat_direction_' + symbol + '.avro', "wb"), DatumWriter(), schema)
						writer.append({"hat_direction": 0})
						writer.close()

						schema = avro.schema.parse(open("total_trades.avsc", "r").read())
						writer = DataFileWriter(open('total_trades_' + symbol + '.avro', "wb"), DatumWriter(), schema)
						writer.append({"time": 0, "side": 0, "entry_price": 0, "exit_price": 0, "avg_roc_ask": 0, "avg_roc_bid": 0})
						writer.close()

						'''

						schema = avro.schema.parse(open("total_trades.avsc", "r").read())
						writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
						writer.append({"time": 0, "side": 0, "entry_price": 0, "exit_price": 0, "avg_roc_ask": 0, "avg_roc_bid": 0})
						writer.close()

						schema = avro.schema.parse(open("order_queue.avsc", "r").read())
						writer = DataFileWriter(open('order_queue_' + symbol + '.avro', "wb"), DatumWriter(), schema)
						writer.append({'queued_order_direction': 0})
						writer.close()

						symbol_ticker_general = 'MATIC'
						symbol_ticker_specific = 'MATICUSD'
						balance = client.get_asset_balance(asset=symbol_ticker_general)
						current_position_qty = int(float(balance.get('free')))
						starting_qty = 0

						if current_position_qty > starting_qty:
							try:
								open_orders = client.get_open_orders(symbol=symbol_ticker_specific)[0]
								open_orders_id = open_orders.get('orderId')
								canceled_order = client.cancel_order(symbol=symbol_ticker_specific,orderId=open_orders_id)
								#time.sleep(3)
							except Exception as e:
								print('cancel limit order')
								print(e)
							#order = client.order_limit_sell(symbol=symbol_ticker_specific,quantity=current_position_qty,price=round(current_price_org+(current_price_org*.0015), 4))
							order = client.order_market_sell(symbol=symbol_ticker_specific,quantity=current_position_qty)
							#time.sleep(3)
							print('sell_now_trailing')


			except Exception as e:
				exc_type, exc_obj, exc_tb = sys.exc_info()
				fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
				print(exc_type, fname, exc_tb.tb_lineno)
				print(e)
		

		schema = avro.schema.parse(open("is_running.avsc", "r").read())
		writer = DataFileWriter(open('is_running_' + symbol + '.avro', "wb"), DatumWriter(), schema)
		writer.append({'time': current_time_org_min/1000000000})
		writer.close()
					
		func_1 = rolling_prediction(time_1, price_1, volume_1)

		prediction_1 = func_1[0]

		if prediction_1 != 0:
			schema = avro.schema.parse(open("order_queue.avsc", "r").read())
			writer = DataFileWriter(open('order_queue_' + symbol + '.avro', "wb"), DatumWriter(), schema)
			writer.append({'queued_order_direction': prediction_1})
			writer.close()

		reader = DataFileReader(open('order_queue_' + symbol + '.avro',"rb"), DatumReader())
		for r in reader:
			queued_order_direction_1 = r.get('queued_order_direction')
		reader.close()

		current_side = np.array([])
		reader = DataFileReader(open('total_trades_' + symbol + '.avro',"rb"), DatumReader())
		for r in reader:
			current_side = np.append(current_side, r.get('side'))
		reader.close()

		#if queued_order_direction_1 != 0 and current_side[-2] != 0:
		if queued_order_direction_1 != 0:

			last_trade_time = time_1[np.argwhere(time_1 > last_time).flatten().astype(int)]
			last_trade_price = price_1[np.argwhere(time_1 > last_time).flatten().astype(int)]

			#section_price_hat = splrep(last_trade_time,last_trade_price,k=5,s=1)
			#section_price_hat = splev(last_trade_time,section_price_hat)
			#print('diff')
			#print(np.diff(section_price_hat, n=1)[-1])

			#if queued_order_direction_1 > 0 and np.diff(section_price_hat, n=1)[-1] > 0:
			if queued_order_direction_1 > 0:

				print('trade_up')

				###for paper
				schema = avro.schema.parse(open("order_queue.avsc", "r").read())
				writer = DataFileWriter(open('order_queue_' + symbol + '.avro', "wb"), DatumWriter(), schema)
				writer.append({'queued_order_direction': 0})
				writer.close()

				reader = DataFileReader(open('total_trades_' + symbol + '_2.avro',"rb"), DatumReader())
				for r in reader:
					new_side = r.get('side')
					new_residual_max = r.get('avg_roc_ask')
					new_y_pred_org_1 = r.get('avg_roc_bid')
				reader.close()

				schema = avro.schema.parse(open("total_trades.avsc", "r").read())
				writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
				writer.append({"time": current_time_org, "side": new_side, "entry_price": 0, "exit_price": 0, "avg_roc_ask": new_residual_max, "avg_roc_bid": new_y_pred_org_1})
				writer.close()	

				symbol_ticker_general = 'MATIC'
				symbol_ticker_specific = 'MATICUSD'
				balance = client.get_asset_balance(asset=symbol_ticker_general)
				current_position_qty = int(float(balance.get('free')))
				starting_qty = 0

				total_acct_balance = client.get_account().get('balances')
				for x in total_acct_balance:
					if x.get('asset') == 'USD':
						avail_cash = float(x.get('free'))
						break
				amount_1 = avail_cash-50
				#amount_1 = amount_1/2
				if amount_1 > 25:
					amount_1 = 25
				qty_1 = int(amount_1/price_1[-1])

				try:
					open_orders = client.get_open_orders(symbol=symbol_ticker_specific)[0]
					open_orders_id = open_orders.get('orderId')
					canceled_order = client.cancel_order(symbol=symbol_ticker_specific,orderId=open_orders_id)
					#time.sleep(3)
				except Exception as e:
					print('cancel limit order')
					print(e)

				if current_position_qty < 2:
					order = client.order_limit_buy(symbol=symbol_ticker_specific,quantity=qty_1,price=round(current_price_org_min-(current_price_org_min*.00075), 4))
					#order = client.order_market_buy(symbol=symbol_ticker_specific,quantity=qty_1)
					#time.sleep(3)
					print('buy_now')



			#elif queued_order_direction_1 < 0 and np.diff(section_price_hat, n=1)[-1] < 0:
			elif queued_order_direction_1 < 0:

				print('trade_down')

				###for paper
				schema = avro.schema.parse(open("order_queue.avsc", "r").read())
				writer = DataFileWriter(open('order_queue_' + symbol + '.avro', "wb"), DatumWriter(), schema)
				writer.append({'queued_order_direction': 0})
				writer.close()

				reader = DataFileReader(open('total_trades_' + symbol + '_2.avro',"rb"), DatumReader())
				for r in reader:
					new_side = r.get('side')
					new_residual_max = r.get('avg_roc_ask')
					new_y_pred_org_1 = r.get('avg_roc_bid')
				reader.close()

				schema = avro.schema.parse(open("total_trades.avsc", "r").read())
				writer = DataFileWriter(open('total_trades_' + symbol + '_2.avro', "wb"), DatumWriter(), schema)
				writer.append({"time": current_time_org, "side": new_side, "entry_price": 0, "exit_price": 0, "avg_roc_ask": new_residual_max, "avg_roc_bid": new_y_pred_org_1})
				writer.close()	

				symbol_ticker_general = 'MATIC'
				symbol_ticker_specific = 'MATICUSD'
				balance = client.get_asset_balance(asset=symbol_ticker_general)
				current_position_qty = int(float(balance.get('free')))
				starting_qty = 0

				try:
					open_orders = client.get_open_orders(symbol=symbol_ticker_specific)[0]
					open_orders_id = open_orders.get('orderId')
					canceled_order = client.cancel_order(symbol=symbol_ticker_specific,orderId=open_orders_id)
					#time.sleep(3)
				except Exception as e:
					print('cancel limit order')
					print(e)
				
				if current_position_qty > starting_qty:
					order = client.order_limit_sell(symbol=symbol_ticker_specific,quantity=current_position_qty,price=round(current_price_org_min+(current_price_org_min*.00075), 4))
					#order = client.order_market_sell(symbol=symbol_ticker_specific,quantity=current_position_qty)
					#time.sleep(3)
					print('sell_now')

		#time.sleep(60)
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print(exc_type, fname, exc_tb.tb_lineno)
		print(e)
		gc.collect()

	return 0



symbol = 'MATIC-USD'

schedule.every().minute.at(':01').do(algo_run)

while True:

	try:

		schedule.run_pending()

		'''

		reader = DataFileReader(open('next_jump_' + symbol + '.avro',"rb"), DatumReader())
		for r in reader:
			next_jump_time_org_1 = r.get('next_jump_time')
		reader.close()

		s.enterabs(next_jump_time_org_1/1000000000, 1, algo_run)
		s.run()

		print('next_signal')


		reader = DataFileReader(open('next_jump_' + symbol + '.avro',"rb"), DatumReader())
		for r in reader:
			next_jump_time_org_2 = r.get('next_jump_time')
		reader.close()

		if next_jump_time_org_1 == next_jump_time_org_2:
			os.execv(sys.executable, ['python'] + sys.argv)

		'''

	except:
		pass
			



		
