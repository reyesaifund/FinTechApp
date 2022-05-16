from __future__ import absolute_import

from google.auth import compute_engine
import google.cloud.logging

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromAvro
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
from apache_beam.runners.runner import PipelineState
from apache_beam import pvalue
from apache_beam.io.avroio import WriteToAvro

from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients import bigquery


class UserOptions(PipelineOptions):
	@classmethod
	def _add_argparse_args(cls, parser):
		parser.add_value_provider_argument('--timestamp', type=str)

def run_main(argv=None):

	from google.auth import compute_engine
	import google.cloud.logging

	import argparse
	import logging

	import apache_beam as beam
	from apache_beam.io import ReadFromAvro
	from apache_beam.options.pipeline_options import PipelineOptions
	from apache_beam.options.pipeline_options import SetupOptions
	from apache_beam.internal import pickler
	from apache_beam.options.pipeline_options import GoogleCloudOptions
	from apache_beam.options.pipeline_options import StandardOptions
	from apache_beam.options.pipeline_options import TestOptions
	from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
	from apache_beam.runners.runner import PipelineState
	from apache_beam import pvalue
	from apache_beam.io.avroio import WriteToAvro

	from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
	from apache_beam.io.gcp.internal.clients import bigquery

	parser = argparse.ArgumentParser()
	parser.add_argument('--input',
						dest='input',
						default='gs://your-bucket-name/symbols.avro',
						help='Input file to process.')
	parser.add_argument('--output',
						dest='output',
						default='your-gcloud-account-name:MyFeatures.features_MSFT',
						help='Output file to write results to.')
	known_args, pipeline_args = parser.parse_known_args(None)
	
	pipeline_options = PipelineOptions(pipeline_args)

	pipeline_options.view_as(SetupOptions).requirements_file = "requirements.txt"
	google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
	google_cloud_options.project = 'your-gcloud-account-name'
	google_cloud_options.job_name = 'job-1'
	google_cloud_options.staging_location = '%s/staging' % 'gs://your-bucket-name'
	google_cloud_options.temp_location = '%s/tmp' % 'gs://your-bucket-name'
	pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

	user_options = pipeline_options.view_as(UserOptions)
	
	with beam.Pipeline(options=pipeline_options) as p:

		from apache_beam.io.gcp.internal.clients import bigquery

		import json
		import requests
		from datetime import datetime, timedelta
		import numpy as np
		import time
		import pandas as pd
		from pandas.tseries.offsets import BDay
		from pyearth import Earth
		import os, sys
		import pytz
		from sklearn import linear_model
		from sklearn.impute import SimpleImputer as Imputer
		from math import ceil
		from scipy.stats import zscore

		class final_link(beam.DoFn):
			def process(self, element):
				try:
					element = element.get('symbol').encode("ascii","replace").decode("utf-8")
					starting_date = (datetime.utcnow().date() - BDay(2)).date()
					dates = (starting_date + BDay(0)).date().strftime('%Y-%m-%d')


					final_link = []


					start_ts = datetime.strptime(dates + ' 13:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
					dt = pytz.timezone('America/Chicago').normalize(start_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
					is_dst = bool(dt.dst())
					if is_dst is True:
						start_ts = time.strptime(dates + ' 13:30:00', '%Y-%m-%d %H:%M:%S')
					elif is_dst is False:
						start_ts = time.strptime(dates + ' 14:00:00', '%Y-%m-%d %H:%M:%S')
					offsets = int(time.mktime(start_ts))*1000000000
					end_ts = datetime.strptime(dates + ' 13:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
					dt = pytz.timezone('America/Chicago').normalize(end_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
					is_dst = bool(dt.dst())
					if is_dst is True:
						end_ts = time.strptime(dates + ' 14:30:00', '%Y-%m-%d %H:%M:%S')
					elif is_dst is False:
						end_ts = time.strptime(dates + ' 15:00:00', '%Y-%m-%d %H:%M:%S')
					offsets_2 = int(time.mktime(end_ts))*1000000000







					while True:
							


						
						link = 'https://api.polygon.io/v2/ticks/stocks/trades/*/^?apiKey=****&timestamp=#&timestampLimit=$'
						f = "*"
						f2 = '^'
						f3 = "#"
						f4 = '$'
						splity = link.split(f)
						link_sym = splity[0] + str(element) + splity[1]
						splity_date = link_sym.split(f2)
						link_date = splity_date[0] + str(dates) + splity_date[1]
						splity_offset = link_date.split(f3)
						place_offset_1 = splity_offset[0] + str(offsets) + splity_offset[1]
						splity_offset_2 = place_offset_1.split(f4)
						offsets = int(offsets+int(300000000000))
						final_link.append(splity_offset_2[0] + str(offsets) + splity_offset_2[1])

						if offsets > offsets_2:
							break






					yield [final_link]
				except Exception as e:
					logging.info(e)

		def BreakList(element):
			if len(element) > 0:
				for bl in element:
					return bl


		class tick_data_all(beam.DoFn):
			def process(self, element):
				try:
					symbol = element.split('trades/')[1].split('/')[0]
					price = np.array([])

					data = requests.get(element).json()
					tick_data = data.get('results')
					if tick_data is not None:
						for x2 in range(0, len(tick_data)):
							price = np.append(price, np.array([[tick_data[x2].get('t')], [symbol], [tick_data[x2].get('p')], [tick_data[x2].get('s')]], dtype=object))	
						if price.size > 0:
							shape = len(price)/4
							price = price.reshape((int(shape), 4))
							price_pd = pd.DataFrame(data=price[0:, 0:], index=price[0:, 0], columns=['Time', 'Symbol', 'Price', 'Volume'])
							price_pd.drop_duplicates(keep='first', inplace=True)
							price_pd['Time'] = pd.to_datetime(price_pd['Time'], unit='ns', utc=True)
							price_pd['Price'] = price_pd.Price.astype(float)
							price_pd['Volume'] = price_pd.Volume.astype(float)
							price_pd.set_index('Time', inplace=True)
							price_pd.sort_index(inplace=True)
							price_pd[~price_pd.index.duplicated(keep='first')]
							price_pd_830 = price_pd
							price_pd_1050 = price_pd_830.reset_index().values
							yield [price_pd_1050]
							yield pvalue.TaggedOutput('final_links_filtered', [element])
				except Exception as e:
					exc_type, exc_obj, exc_tb = sys.exc_info()
					fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
					logging.info(exc_type, fname, exc_tb.tb_lineno)


		class AddTimestampDoFn(beam.DoFn):
			def process(self, element):
				try:
					unix_timestamp = int(element[0].value/1000000000)
					yield beam.window.TimestampedValue(element, unix_timestamp)
				except Exception as e:
					logging.info(e)		

		class grouped_by_symbol(beam.DoFn):
			def process(self, element):
				try:
					grouped_by_symbol = (str(element[1]), element)
					yield grouped_by_symbol
				except Exception as e:
					logging.info(e)

		class AddTimestampDoFn_2(beam.DoFn):
			def process(self, element):
				try:
					unix_timestamp = int(element[0].value/1000000000)
					yield beam.window.TimestampedValue(element, unix_timestamp)
				except Exception as e:
					logging.info(e)

		def get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model):
		

			num_ticks_67 = int(67)
			ewm_Px_67 = price_pd['Price'].ewm(span=num_ticks_67).mean()

			num_ticks_150 = int(150)
			ewm_Px_150 = price_pd['Price'].ewm(span=num_ticks_150).mean()

			num_ticks_50_25 = int(50)
			ewm_Px_50_25 = price_pd['Price'][::25].ewm(span=num_ticks_50_25).mean()

			ratio_ewm_50_25 = price_pd['Price'][-1]/ewm_Px_50_25[-1]
			ratio_ewm_150_arr = price_pd['Price'][-1]/ewm_Px_150[-1]
			ratio_ewm_67_arr = price_pd['Price'][-1]/ewm_Px_67[-1]
			ratio_ewm_67_150_arr = ewm_Px_67[-1]/ewm_Px_150[-1]
			pct_ewm_50_25 = ((price_pd['Price'][-1]-ewm_Px_50_25[-1])/ewm_Px_50_25[-1])*100		
			pct_ewm_150_arr = ((price_pd['Price'][-1]-ewm_Px_150[-1])/ewm_Px_150[-1])*100
			pct_ewm_67_arr = ((price_pd['Price'][-1]-ewm_Px_67[-1])/ewm_Px_67[-1])*100	
			pct_ewm_67_150_arr = ((ewm_Px_67[-1]-ewm_Px_150[-1])/ewm_Px_150[-1])*100			
			


			der_y = np.take(section_price_hat, zero_crossings)
			pct_chg = np.diff(der_y) / der_y[:-1]
			pct_chg_wh = np.where(pct_chg > 0)
			pct_chg_wh = pct_chg_wh[0]
			pct_chg_long = np.take(pct_chg, pct_chg_wh)
			avg_return_40_mins = np.nanmean(pct_chg_long)
			pct_chg_long_2 = np.array([])
			running_avg_pcl = np.array([])
			chg_in_avg_rtn = np.array([])
			for pcl in pct_chg_long:
				pct_chg_long_2 = np.append(pct_chg_long_2, pcl)
				running_avg_pcl = np.append(running_avg_pcl, np.nanmean(pct_chg_long_2))
				if running_avg_pcl.size > 1:
					chg_in_avg_rtn_1 = np.subtract(running_avg_pcl[-1], running_avg_pcl[-2])
					chg_in_avg_rtn = np.append(chg_in_avg_rtn, chg_in_avg_rtn_1)
					avg_chg_in_avg_return = np.nanmean(chg_in_avg_rtn)
				else:
					chg_in_avg_rtn = np.append(chg_in_avg_rtn, np.nan)
					avg_chg_in_avg_return = np.nanmean(chg_in_avg_rtn)
			if 'avg_chg_in_avg_return' not in locals():
				avg_chg_in_avg_return = np.nan
			if zero_crossings.size > 5:
				return_minus_5 = pct_chg[-5]
				return_minus_4 = pct_chg[-4]
				return_minus_3 = pct_chg[-3]
				return_minus_2 = pct_chg[-2]
			elif zero_crossings.size == 5:
				return_minus_5 = np.nan
				return_minus_4 = pct_chg[-4]
				return_minus_3 = pct_chg[-3]
				return_minus_2 = pct_chg[-2]
			elif zero_crossings.size == 4:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = pct_chg[-3]
				return_minus_2 = pct_chg[-2]
			elif zero_crossings.size == 3:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = np.nan
				return_minus_2 = pct_chg[-2]
			elif zero_crossings.size == 2:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = np.nan
				return_minus_2 = np.nan
			else:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = np.nan
				return_minus_2 = np.nan

			X = pd.DataFrame(section_time)
			y = pd.DataFrame(section_price)
			lm = linear_model.LinearRegression()
			model_lm = lm.fit(X,y)
			lr_all_day_hat = np.array(lm.predict(X)).flatten()
			lr_all_day_time_passed = np.subtract(section_time[-1], section_time[0])
			lr_all_day_pct_chg = np.divide(np.subtract(lr_all_day_hat[-1], lr_all_day_hat[0]), lr_all_day_hat[0])
			lr_all_day_roc = np.nanmean((np.diff(lr_all_day_hat)/lr_all_day_hat[:-1])/np.diff(section_time))
			lr_all_day_r2 = lm.score(X,y)

			if zero_crossings.size > 1:
				critical_points = np.array([])
				sec_pct_chg = np.array([])
				sec_curve_pct_chg = np.array([])
				curve_der_neg = section_der[int(zero_crossings[-2]):int(zero_crossings[-1]+1)]
				curve_price_hat_neg = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-1]+2)]
				curve_price_neg = section_price[int(zero_crossings[-2]):int(zero_crossings[-1]+2)]
				curve_time_neg = section_time[int(zero_crossings[-2]):int(zero_crossings[-1]+2)]
				critical_points = np.append(critical_points, 0)
				if curve_der_neg.size == 0:
					der_1 = np.array([])
					der_1 = np.append(der_1, np.zeros(8) + np.nan)
					der_2 = np.array([])
					der_2 = np.append(der_2, np.zeros(8) + np.nan)
					der_3 = np.array([])
					der_3 = np.append(der_3, np.zeros(8) + np.nan)
					sec_pct_chg = np.append(sec_pct_chg, np.zeros(8) + np.nan)
					sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(8) + np.nan)
					critical_points = np.append(critical_points, np.zeros(8) + np.nan)
				elif curve_der_neg.size > 0:
					inflect_pt_neg = int(np.nanargmin(curve_der_neg))
					curve_der_sec_1_neg = curve_der_neg[0:inflect_pt_neg+1]
					if curve_der_sec_1_neg.size == 0:
						critical_points = np.append(critical_points, np.zeros(3) + np.nan)
						critical_points = np.append(critical_points, inflect_pt_neg)
					elif curve_der_sec_1_neg.size > 0:
						curve_der_415_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -.415)))
						curve_der_entry_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -1)))
						curve_der_241_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -2.41)))
						critical_points = np.append(critical_points, curve_der_415_neg).astype(np.int64)
						critical_points = np.append(critical_points, curve_der_entry_neg).astype(np.int64)
						critical_points = np.append(critical_points, curve_der_241_neg).astype(np.int64)

						
					if critical_points.size == 4:
						critical_points = np.append(critical_points, inflect_pt_neg)
					elif critical_points.size == 1:
						critical_points = np.append(critical_points, np.zeros(3) + np.nan)
						critical_points = np.append(critical_points, inflect_pt_neg)

					curve_der_sec_2_neg = curve_der_neg[inflect_pt_neg:]
					if curve_der_sec_2_neg.size > 0:
						curve_der_241_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -2.41)))
						curve_der_entry_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -1)))
						curve_der_415_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -.415)))
						curve_der_241_neg = len(curve_der_sec_1_neg)-1 + curve_der_241_neg
						curve_der_entry_neg = len(curve_der_sec_1_neg)-1 + curve_der_entry_neg
						curve_der_415_neg = len(curve_der_sec_1_neg)-1 + curve_der_415_neg
						critical_points = np.append(critical_points, curve_der_241_neg)
						critical_points = np.append(critical_points, curve_der_entry_neg)
						critical_points = np.append(critical_points, curve_der_415_neg)
						critical_points = np.append(critical_points, len(curve_der_neg)-1)
					if critical_points.size == 5:
						critical_points = np.append(critical_points, np.zeros(3) + np.nan)
						critical_points = np.append(critical_points, len(curve_der_neg)-1)
					critical_points = critical_points.astype(np.int64)
					curve_der_neg = np.divide(np.diff(np.divide(np.diff(curve_price_hat_neg), curve_price_hat_neg[:-1])), np.diff(curve_time_neg[:-1]))
					
					der_1 = np.array([])
					for s in range(0, len(critical_points)-1):
						if critical_points[s] <= -9223372036854775 or critical_points[s] == np.nan:
							der_1 = np.append(der_1, np.nan)
							continue
						else:
							if critical_points[s+1] <= -9223372036854775 or critical_points[s+1] == np.nan:
								der_1 = np.append(der_1, np.nan)
								continue
							else:
								
								if critical_points[s+1]+1 < len(curve_der_neg):
									section_mean = np.nanmean(curve_der_neg[critical_points[s]:critical_points[s+1]+1])
									if section_mean.size == 0:
										section_mean = np.nan
									der_1 = np.append(der_1, section_mean)
								else:
									section_mean = np.nanmean(curve_der_neg[critical_points[s]:critical_points[s+1]])
									if section_mean.size == 0:
										section_mean = np.nan
									der_1 = np.append(der_1, section_mean)

					curve_der_neg_diff = np.diff(curve_der_neg)
					section_der_2_neg = np.divide(curve_der_neg_diff, np.diff(curve_time_neg[:-2]))
					
					der_2 = np.array([])
					for s2 in range(0, len(critical_points)-1):
						if critical_points[s2] <= -9223372036854775 or critical_points[s2] == np.nan:
							der_2 = np.append(der_2, np.nan)
							continue
						else:
							if critical_points[s2+1] <= -9223372036854775 or critical_points[s2+1] == np.nan:
								der_2 = np.append(der_2, np.nan)
								continue
							else:
								section_mean = np.nanmean(section_der_2_neg[critical_points[s2]:critical_points[s2+1]])
								if section_mean.size == 0:
									section_mean = np.nan
								der_2 = np.append(der_2, section_mean)
						
					section_der_2_neg_diff = np.diff(section_der_2_neg)
					section_der_3_neg = np.divide(section_der_2_neg_diff, np.diff(curve_time_neg[:-3]))
					
					der_3 = np.array([])
					for s3 in range(0, len(critical_points)-1):
						if critical_points[s3] <= -9223372036854775 or critical_points[s3] == np.nan:
							der_3 = np.append(der_3, np.nan)
							continue
						else:
							if critical_points[s3+1] <= -9223372036854775 or critical_points[s3+1] == np.nan:
								der_3 = np.append(der_3, np.nan)
								continue
							else:
								section_mean = np.nanmean(section_der_3_neg[critical_points[s3]:critical_points[s3+1]-1])
								if section_mean.size == 0:
									section_mean = np.nan
								der_3 = np.append(der_3, section_mean)
					sec_pct_chg = np.array([])
					for s_p_c in range(0, len(critical_points)-1):
						if critical_points[s_p_c] <= -9223372036854775 or critical_points[s_p_c] == np.nan:
							sec_pct_chg = np.append(sec_pct_chg, np.nan)
							continue
						else:
							if critical_points[s_p_c+1] <= -9223372036854775 or critical_points[s_p_c+1] == np.nan:
								sec_pct_chg = np.append(sec_pct_chg, np.nan)
								continue
							else:
								if critical_points[s_p_c+1]+1 < len(curve_price_hat_neg):
									section_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_p_c+1]+1], curve_price_hat_neg[critical_points[s_p_c]]), curve_price_hat_neg[critical_points[s_p_c]])
									if section_pct_chg.size == 0:
										section_pct_chg = np.nan
									sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
								else:
									section_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_p_c+1]], curve_price_hat_neg[critical_points[s_p_c]]), curve_price_hat_neg[critical_points[s_p_c]])
									if section_pct_chg.size == 0:
										section_pct_chg = np.nan
									sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
					
					sec_curve_pct_chg = np.array([])
					for s_c_p_c in range(0, len(critical_points)-1):
						if critical_points[s_c_p_c] <= -9223372036854775 or critical_points[s_c_p_c] == np.nan:
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.nan)
							continue
						else:
							if critical_points[s_c_p_c+1] <= -9223372036854775 or critical_points[s_c_p_c+1] == np.nan:
								sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.nan)
								continue
							else:
								if critical_points[s_c_p_c+1]+1 < len(curve_price_hat_neg):
									section_curve_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_c_p_c+1]+1], curve_price_hat_neg[critical_points[0]]), curve_price_hat_neg[critical_points[0]])
									if section_curve_pct_chg.size == 0:
										section_curve_pct_chg = np.nan
									sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
								else:
									section_curve_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_c_p_c+1]], curve_price_hat_neg[critical_points[0]]), curve_price_hat_neg[critical_points[0]])
									if section_curve_pct_chg.size == 0:
										section_curve_pct_chg = np.nan
									sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)




				curve_der = section_der[int(zero_crossings[-1]):]
				if curve_der.size == 0:
					der_1 = np.append(der_1, np.zeros(2) + np.nan)
					der_2 = np.append(der_2, np.zeros(2) + np.nan)
					der_3 = np.append(der_3, np.zeros(2) + np.nan)
					sec_pct_chg = np.append(sec_pct_chg, np.zeros(2) + np.nan)
					sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(2) + np.nan)
					critical_points = np.append(critical_points, np.zeros(2) + np.nan)
				elif curve_der.size > 0:
					curve_der_sec_1 = curve_der
					curve_der_entry = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, 1)))
					curve_der_415 = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, .415)))
					if critical_points.size == 8:
						critical_points = np.append(critical_points, 0).astype(np.int64)
						critical_points = np.append(critical_points, curve_der_415).astype(np.int64)
						critical_points = np.append(critical_points, curve_der_entry).astype(np.int64)
					elif critical_points.size == 9:
						critical_point_10 = len(curve_der_neg)-1 + curve_der_415
						critical_point_11 = len(curve_der_neg)-1 + curve_der_entry
						critical_points = np.append(critical_points, critical_point_10).astype(np.int64)
						critical_points = np.append(critical_points, critical_point_11).astype(np.int64)
					
					critical_points = critical_points.astype(np.int64)

					curve_price = section_price[int(zero_crossings[-1]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_time = section_time[int(zero_crossings[-1]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_hat = section_price_hat[int(zero_crossings[-1]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_hat_diff_div_diff = np.diff(np.divide(np.diff(curve_price_hat), curve_price_hat[:-1])) 			
					curve_der = np.divide(curve_price_hat_diff_div_diff, np.diff(curve_time)[:-1])
					curve_time_r = section_time[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_r = section_price[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_hat_r = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_vol_r = section_vol[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]

					curve_der_diff = np.diff(curve_der)
					section_der_2 = np.divide(curve_der_diff, np.diff(curve_time[:-2]))
					


					critical_points_pos = np.array([0, critical_points[-2]-(len(curve_der_neg)-1), critical_points[-1]-(len(curve_der_neg)-1)]).astype(np.int64)
					for p in range(0, len(critical_points_pos)-1):
						if critical_points_pos[p+1]+1 < len(curve_der):
							section_mean = np.nanmean(curve_der[critical_points_pos[p]:critical_points_pos[p+1]+1])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
						else:
							section_mean = np.nanmean(curve_der[critical_points_pos[p]:critical_points_pos[p+1]])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
						
					for p2 in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_2[critical_points_pos[p2]:critical_points_pos[p2+1]])
						if section_mean == 0:
							section_mean = np.nan
						der_2 = np.append(der_2, section_mean)

					section_der_2_diff = np.diff(section_der_2)
					section_der_3 = np.divide(section_der_2_diff, np.diff(curve_time[:-3]))			
					
					for p3 in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_3[critical_points_pos[p3]:critical_points_pos[p3+1]-1])
						if section_mean == 0:
							section_mean = np.nan
						der_3 = np.append(der_3, np.nan)
					for s_p_c_2 in range(0, len(critical_points_pos)-1):
						if critical_points_pos[s_p_c_2+1]+1 < len(curve_price_hat):
							section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2+1]+1], curve_price_hat[critical_points_pos[s_p_c_2]]), curve_price_hat[critical_points_pos[s_p_c_2]])
							if section_pct_chg.size == 0:
								section_pct_chg = np.nan
							sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
						else:
							section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2+1]], curve_price_hat[critical_points_pos[s_p_c_2]]), curve_price_hat[critical_points_pos[s_p_c_2]])
							if section_pct_chg.size == 0:
								section_pct_chg = np.nan
							sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
					for s_c_p_c_2 in range(0, len(critical_points_pos)-1):
						if critical_points_pos[s_p_c_2+1]+1 < len(curve_price_hat):
							section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2+1]+1], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
							if section_curve_pct_chg.size == 0:
								section_curve_pct_chg = np.nan
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
						else:
							section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2+1]], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
							if section_curve_pct_chg.size == 0:
								section_curve_pct_chg = np.nan
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)


				r_sq = np.array([])
				avg_mean_per_section = np.array([])
				std_per_section = np.array([])
				residual_max_per_section = np.array([])
				residual_mean_per_section = np.array([])
				zscore_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					r_sq = np.append(r_sq, np.zeros(10) + np.nan)
					avg_mean_per_section = np.append(avg_mean_per_section, np.zeros(10) + np.nan)
					std_per_section = np.append(std_per_section, np.zeros(10) + np.nan)
					residual_max_per_section = np.append(residual_max_per_section, np.zeros(10) + np.nan)
					residual_mean_per_section = np.append(residual_mean_per_section, np.zeros(10) + np.nan)
					zscore_per_section = np.append(zscore_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for r in range(0, len(critical_points)-1):
						if critical_points[r] <= -9223372036854775 or critical_points[r] == np.nan:
							r_sq = np.append(r_sq, np.nan)
							avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
							std_per_section = np.append(std_per_section, np.nan)
							residual_max_per_section = np.append(residual_max_per_section, np.nan)
							residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
							zscore_per_section = np.append(zscore_per_section, np.nan)
							continue
						else:
							if critical_points[r+1] <= -9223372036854775 or critical_points[r+1] == np.nan:
								r_sq = np.append(r_sq, np.nan)
								avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
								std_per_section = np.append(std_per_section, np.nan)
								residual_max_per_section = np.append(residual_max_per_section, np.nan)
								residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
								zscore_per_section = np.append(zscore_per_section, np.nan)
								continue
							else:
								
								if critical_points[r+1] == critical_points[r]:
									if r_sq.size == 0 or avg_mean_per_section.size == 0 or std_per_section.size == 0 or residual_max_per_section.size == 0 or residual_mean_per_section.size == 0 or zscore_per_section.size == 0:
										r_sq = np.append(r_sq, np.nan)
										avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
										std_per_section = np.append(std_per_section, np.nan)
										residual_max_per_section = np.append(residual_max_per_section, np.nan)
										residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
										zscore_per_section = np.append(zscore_per_section, np.nan)
									else:	
										r += 1
										r_sq = np.append(r_sq, r_sq[-1])
										avg_mean_per_section = np.append(avg_mean_per_section, avg_mean_per_section[-1])
										std_per_section = np.append(std_per_section, std_per_section[-1])
										residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section[-1])
										residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section[-1])
										zscore_per_section = np.append(zscore_per_section, zscore_per_section[-1])
										continue
								else:
									r_sq_2 = model.score(curve_time_r[critical_points[r]:critical_points[r+1]+2], curve_price_r[critical_points[r]:critical_points[r+1]+2])
									r_sq = np.append(r_sq, r_sq_2)
									avg_mean = np.nanmean(curve_price_r[critical_points[r]:critical_points[r+1]+2])
									avg_mean_per_section = np.append(avg_mean_per_section, avg_mean)
									std_sec = np.nanstd(curve_price_r[critical_points[r]:critical_points[r+1]+2])
									std_per_section = np.append(std_per_section, std_sec)
									zscore_per_section_0 = np.nanmean(zscore(curve_price_r[critical_points[r]:critical_points[r+1]+2]))
									zscore_per_section = np.append(zscore_per_section, zscore_per_section_0)
									curvy = curve_price_hat_r[critical_points[r]:critical_points[r+1]+2]
									residual_max_per_section_0_0_0 = np.abs(np.subtract(curve_price_r[critical_points[r]:critical_points[r+1]+2], curve_price_hat_r[critical_points[r]:critical_points[r+1]+2]))
									residual_max_per_section_0_0 = np.nanargmax(residual_max_per_section_0_0_0)
									residual_max_per_section_0 = np.take(residual_max_per_section_0_0_0, residual_max_per_section_0_0)
									residual_mean_per_section_0 = np.nanmean(np.abs(np.subtract(curve_price_r[critical_points[r]:critical_points[r+1]+2], curve_price_hat_r[critical_points[r]:critical_points[r+1]+2])))
									residual_max_per_section_0_ph = np.divide(residual_max_per_section_0, np.take(curvy, residual_max_per_section_0_0))
									residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section_0_ph)
									residual_mean_per_section_0_ph = np.nanmean(curvy)
									residual_mean_per_section_div = np.divide(residual_mean_per_section_0, residual_mean_per_section_0_ph)
									residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section_div)

				r_sq = r_sq.astype(np.float64)

				time_since_maxima = np.array([])
				time_since_last_sect = np.array([])
				if curve_price_hat_r.size == 0:
					time_since_maxima = np.append(time_since_maxima, np.zeros(10) + np.nan)
					time_since_last_sect = np.append(time_since_last_sect, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for c in range(0, len(critical_points)-1):
						if critical_points[c] <= -9223372036854775 or critical_points[c] == np.nan:
							time_since_maxima = np.append(time_since_maxima, np.nan)
							time_since_last_sect = np.append(time_since_last_sect, np.nan)
							continue
						else:
							if critical_points[c+1] <= -9223372036854775 or critical_points[c+1] == np.nan:
								time_since_maxima = np.append(time_since_maxima, np.nan)
								time_since_last_sect = np.append(time_since_last_sect, np.nan)
								continue
							else:
								if critical_points[c+1] == critical_points[c]:
									c += 1
									time_since_maxima = np.append(time_since_maxima, np.zeros(1))
									time_since_last_sect = np.append(time_since_last_sect, np.zeros(1))
									continue
								else:
									if critical_points[0] <= -9223372036854775 or critical_points[0] == np.nan:
										time_since_maxima_append = np.nan
									else:
										if critical_points[c+1]+1 < len(curve_time_r)-1:
											time_since_maxima_append = curve_time_r[critical_points[c+1]+1] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[c+1]+1] - curve_time_r[critical_points[c]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
					
										else:
											time_since_maxima_append = curve_time_r[critical_points[c+1]] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[c+1]] - curve_time_r[critical_points[c]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
									
				

				#residual_max_0_0 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
				#residual_max_0 = np.nanargmax(residual_max_0_0)
				#residual_max = np.take(residual_max_0_0, residual_max_0)
				#residual_mean = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))

				residual_max_2 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
				residual_max_1 = np.nanargmax(residual_max_2)
				residual_max_0 = np.take(residual_max_2, residual_max_1)
				residual_max_0_ph = np.take(curve_price_hat_r, residual_max_1)
				residual_max = np.divide(residual_max_0, residual_max_0_ph)
				residual_mean_0 = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))
				residual_mean = np.divide(residual_mean_0, np.nanmean(curve_price_hat_r))


				if critical_points.size > 0 and curve_price_r.size > 0:
					current_price = curve_price_r[-1]
				else:
					current_price = np.nan

				if critical_points.size > 0 and curve_price_hat_r.size > 0:
					current_price_hat_r = curve_price_hat_r[-1]
				else:
					current_price_hat_r = np.nan

				if curve_price_r.size > 0:
					avg_Price = np.nanmean(curve_price_r)
				else:
					avg_Price = np.nan
					
				current_date = datetime(1970, 1, 1) + timedelta(seconds=np.multiply(curve_time[-1],1000000000))
				current_date = current_date.strftime('%Y-%m-%d')
				open_ts = datetime.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')
				dt = pytz.timezone('America/Chicago').normalize(open_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
				is_dst = bool(dt.dst())
				if is_dst is True:
					offset_open = int(time.mktime(time.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')))
				elif is_dst is False:
					offset_open = int(time.mktime(time.strptime(current_date + ' 9:00:00', '%Y-%m-%d %H:%M:%S')))
				if critical_points.size > 0 and curve_time_r.size > 0:
					current_unix_time = np.multiply(curve_time_r[-1],1000000000)
					ms_since_open = np.multiply(curve_time_r[-1],1000000000) - offset_open
				else:
					current_unix_time = np.nan
					ms_since_open = np.nan
				
				current_date = datetime.strptime(current_date, '%Y-%m-%d')

				
				current_year = current_date.isocalendar()[0]
				current_month = current_date.month

				first_day = current_date.replace(day=1)
				dom = current_date.day
				adjusted_dom = dom + first_day.weekday()
				current_week = int(ceil(adjusted_dom/7.0))

				current_weekday = current_date.isocalendar()[2]

				if ms_since_open <= 1800:
					current_30_mins = 1
				elif ms_since_open > 1800 and ms_since_open <= 1800*2:
					current_30_mins = 2
				elif ms_since_open > 1800*2 and ms_since_open <= 1800*3:
					current_30_mins = 3
				elif ms_since_open > 1800*3 and ms_since_open <= 1800*4:
					current_30_mins = 4
				elif ms_since_open > 1800*4 and ms_since_open <= 1800*5:
					current_30_mins = 5
				else:
					current_30_mins = np.nan

				
				total_years = int((current_date - datetime(1970,1,1)).days/365.24)
				total_months = int(((current_date - datetime(1970,1,1)).days/365.24)*12.0)
				total_weeks = int((current_date - datetime(1970,1,1)).days/7.0)
				total_days = int((current_date - datetime(1970,1,1)).days)
				total_30_mins = int(current_unix_time/1800)



				

				price_pcavg_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for avg_p_n in range(0, len(critical_points)-1):
						if critical_points[avg_p_n] <= -9223372036854775 or critical_points[avg_p_n] == np.nan:
							price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_n+1] <= -9223372036854775 or critical_points[avg_p_n+1] == np.nan:
								price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_n+1] == critical_points[avg_p_n]:
									avg_p_n += 1
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(1))
									continue
								else:
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.nanmean(np.diff(curve_price_hat_r[critical_points[avg_p_n]:critical_points[avg_p_n+1]+2])/curve_price_hat_r[critical_points[avg_p_n]:critical_points[avg_p_n+1]+2][:-1]))




				price_pct_chg_neg = np.divide(np.subtract(curve_price_neg[-1], curve_price_neg[0]), curve_price_neg[0])
				price_pct_chg_pos = np.divide(np.subtract(curve_price[-1], curve_price[0]), curve_price[0])





				lm = linear_model.LinearRegression()





				X = pd.DataFrame(curve_time_neg)
				y = pd.DataFrame(curve_price_neg)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				lr_curve_neg_hat = np.array(lm.predict(X)).flatten()
				lr_price_total_pct_chg_neg = np.divide(np.subtract(lr_curve_neg_hat[-1], lr_curve_neg_hat[0]), lr_curve_neg_hat[0])
				lr_price_avg_roc_neg = np.nanmean((np.diff(lr_curve_neg_hat)/lr_curve_neg_hat[:-1])/np.diff(curve_time_neg))
				lr_price_r2_neg = lm.score(X,y)




				X = pd.DataFrame(curve_time)
				y = pd.DataFrame(curve_price)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				lr_curve_pos_hat = np.array(lm.predict(X)).flatten()
				lr_price_total_pct_chg_pos = np.divide(np.subtract(lr_curve_pos_hat[-1], lr_curve_pos_hat[0]), lr_curve_pos_hat[0])
				lr_price_avg_roc_pos = np.nanmean((np.diff(lr_curve_pos_hat)/lr_curve_pos_hat[:-1])/np.diff(curve_time))
				lr_price_r2_pos = lm.score(X,y)






				lr_price_pct_chg_per_section = np.array([])
				lr_price_roc_per_section = np.array([])
				lr_price_r2_per_section = np.array([])
				if curve_price_r.size == 0:
					lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(10) + np.nan)
					lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(10) + np.nan)
				elif curve_price_r.size > 0:
					for avg_p in range(0, len(critical_points)-1):
						if critical_points[avg_p] <= -9223372036854775 or critical_points[avg_p] == np.nan:
							lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
							lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
							lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p+1] <= -9223372036854775 or critical_points[avg_p+1] == np.nan:
								lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
								lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
								lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p+1] == critical_points[avg_p]:
									avg_p += 1
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(1))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(1))
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[avg_p]:critical_points[avg_p+1]+2])
									y = pd.DataFrame(curve_price_r[critical_points[avg_p]:critical_points[avg_p+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.divide(np.subtract(lr_price_per_section_hat[-1], lr_price_per_section_hat[0]), lr_price_per_section_hat[0]))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nanmean((np.diff(lr_price_per_section_hat)/lr_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[avg_p]:critical_points[avg_p+1]+2])))
									lr_price_r2_per_section_2 = lm.score(X,y)
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, lr_price_r2_per_section_2)







				lr_vol_pct_chg_per_section = np.array([])
				lr_vol_roc_per_section = np.array([])
				lr_vol_r2_per_section = np.array([])
				if curve_vol_r.size == 0:
					lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(10) + np.nan)
					lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(10) + np.nan)
				elif curve_vol_r.size > 0:
					for v in range(0, len(critical_points)-1):
						if critical_points[v] <= -9223372036854775 or critical_points[v] == np.nan:
							lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
							lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
							lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
							continue
						else:
							if critical_points[v+1] <= -9223372036854775 or critical_points[v+1] == np.nan:
								lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
								lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
								lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
								continue
							else:
								if critical_points[v+1] == critical_points[v]:
									v += 1
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(1))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(1))
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[v]:critical_points[v+1]+2])
									y = pd.DataFrame(curve_vol_r[critical_points[v]:critical_points[v+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_vol_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.divide(np.subtract(lr_vol_per_section_hat[-1], lr_vol_per_section_hat[0]), lr_vol_per_section_hat[0]))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nanmean((np.diff(lr_vol_per_section_hat)/lr_vol_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[v]:critical_points[v+1]+2])))
									lr_vol_r2_per_section_2 = lm.score(X,y)
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, lr_vol_r2_per_section_2)





				section_time_key_neg = curve_time_neg
				section_vol_key_neg = section_vol[int(zero_crossings[-2]):int(zero_crossings[-1])+2]

				X = pd.DataFrame(section_time_key_neg)
				y = pd.DataFrame(section_vol_key_neg)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_vol_neg_hat = np.array(lm.predict(X)).flatten()
				vol_total_pct_chg_neg = np.divide(np.subtract(section_vol_neg_hat[-1], section_vol_neg_hat[0]), section_vol_neg_hat[0])
				vol_avg_roc_neg = np.nanmean((np.diff(section_vol_neg_hat)/section_vol_neg_hat[:-1])/np.diff(section_time_key_neg))
				vol_r2_neg = lm.score(X,y)




				section_time_key_pos = curve_time_r[int(critical_points[-3]):int(critical_points[-1]+2)]
				section_vol_key_pos = curve_vol_r[int(critical_points[-3]):int(critical_points[-1]+2)]

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_vol_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_vol_pos_hat = np.array(lm.predict(X)).flatten()
				vol_total_pct_chg_pos = np.divide(np.subtract(section_vol_pos_hat[-1], section_vol_pos_hat[0]), section_vol_pos_hat[0])
				vol_avg_roc_pos = np.nanmean((np.diff(section_vol_pos_hat)/section_vol_pos_hat[:-1])/np.diff(section_time_key_pos))
				vol_r2_pos = lm.score(X,y)




				section_mf_key_total = np.multiply(curve_price_r, curve_vol_r)

				lr_mf_price_pct_chg_per_section = np.array([])
				lr_mf_price_roc_per_section = np.array([])
				lr_mf_price_r2_per_section = np.array([])
				if section_mf_key_total.size == 0:
					lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(10) + np.nan)
					lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(10) + np.nan)
				elif section_mf_key_total.size > 0:
					for v2 in range(0, len(critical_points)-1):
						if critical_points[v2] <= -9223372036854775 or critical_points[v2] == np.nan:
							lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
							lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
							lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[v2+1] <= -9223372036854775 or critical_points[v2+1] == np.nan:
								lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
								lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
								lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[v2+1] == critical_points[v2]:
									v2 += 1
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(1))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(1))
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[v2]:critical_points[v2+1]+2])
									y = pd.DataFrame(section_mf_key_total[critical_points[v2]:critical_points[v2+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_mf_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.divide(np.subtract(lr_mf_price_per_section_hat[-1], lr_mf_price_per_section_hat[0]), lr_mf_price_per_section_hat[0]))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nanmean((np.diff(lr_mf_price_per_section_hat)/lr_mf_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[v2]:critical_points[v2+1]+2])))
									lr_mf_price_r2_per_section_2 = lm.score(X,y)
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, lr_mf_price_r2_per_section_2)




				section_time_key_neg = curve_time_neg
				section_mf_price_key_neg = np.multiply(curve_price_neg, section_vol_key_neg)

				X = pd.DataFrame(section_time_key_neg)
				y = pd.DataFrame(section_mf_price_key_neg)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_mf_price_neg_hat = np.array(lm.predict(X)).flatten()
				mf_price_total_pct_chg_neg = np.divide(np.subtract(section_mf_price_neg_hat[-1], section_mf_price_neg_hat[0]), section_mf_price_neg_hat[0])
				mf_price_avg_roc_neg = np.nanmean((np.diff(section_mf_price_neg_hat)/section_mf_price_neg_hat[:-1])/np.diff(section_time_key_neg))
				mf_price_r2_neg = lm.score(X,y)




				section_time_key_pos = curve_time
				section_mf_price_key_pos = np.multiply(curve_price, section_vol_key_pos)

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_mf_price_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_mf_price_pos_hat = np.array(lm.predict(X)).flatten()
				mf_price_total_pct_chg_pos = np.divide(np.subtract(section_mf_price_pos_hat[-1], section_mf_price_pos_hat[0]), section_mf_price_pos_hat[0])
				mf_price_avg_roc_pos = np.nanmean((np.diff(section_mf_price_pos_hat)/section_mf_price_pos_hat[:-1])/np.diff(section_time_key_pos))
				mf_price_r2_pos = lm.score(X,y)



				avg_vol = np.nanmean(curve_vol_r)

				

				quote_key_times_append = np.array([])
				quote_key_times = np.array([])
				for cp in range(0, len(critical_points)):
					if critical_points[cp] <= -9223372036854775 or critical_points[cp] == np.nan:
						quote_key_times = np.append(quote_key_times, np.nan)
						continue
					else:
						if critical_points[cp]+1 < len(curve_time_r) and cp == 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cp]+1 < len(curve_time_r) and cp > 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp]+1)], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cp]+1 >= len(curve_time_r):
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						
						


				final_output.update(dict(ratio_ewm_50_25 = ratio_ewm_50_25))
				final_output.update(dict(ratio_ewm_150_arr = ratio_ewm_150_arr))
				final_output.update(dict(ratio_ewm_67_arr = ratio_ewm_67_arr))
				final_output.update(dict(ratio_ewm_67_150_arr = ratio_ewm_67_150_arr))
				final_output.update(dict(pct_ewm_50_25 = pct_ewm_50_25))
				final_output.update(dict(pct_ewm_150_arr = pct_ewm_150_arr))
				final_output.update(dict(pct_ewm_67_arr = pct_ewm_67_arr))
				final_output.update(dict(pct_ewm_67_150_arr = pct_ewm_67_150_arr))
				final_output.update(dict(avg_return_40_mins = avg_return_40_mins))
				final_output.update(dict(avg_chg_in_avg_return = avg_chg_in_avg_return)) 
				final_output.update(dict(return_minus_5 = return_minus_5)) 
				final_output.update(dict(return_minus_4 = return_minus_4)) 
				final_output.update(dict(return_minus_3 = return_minus_3)) 
				final_output.update(dict(return_minus_2 = return_minus_2)) 
				final_output.update(dict(lr_all_day_time_passed = lr_all_day_time_passed)) 
				final_output.update(dict(lr_all_day_pct_chg = lr_all_day_pct_chg))
				final_output.update(dict(lr_all_day_roc = lr_all_day_roc)) 
				final_output.update(dict(lr_all_day_r2 = lr_all_day_r2)) 
				final_output.update(dict(zip(['sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1'], der_1.T)))
				final_output.update(dict(zip(['sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2'], der_2.T)))
				final_output.update(dict(zip(['sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3'], der_3.T)))
				final_output.update(dict(zip(['sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq'], r_sq.T)))   
				final_output.update(dict(zip(['sec_1_avg_mean_per_section', 'sec_2_avg_mean_per_section', 'sec_3_avg_mean_per_section', 'sec_4_avg_mean_per_section', 'sec_5_avg_mean_per_section', 'sec_6_avg_mean_per_section', 'sec_7_avg_mean_per_section', 'sec_8_avg_mean_per_section', 'sec_9_avg_mean_per_section', 'sec_10_avg_mean_per_section'], avg_mean_per_section.T)))   
				final_output.update(dict(zip(['sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section'], std_per_section.T)))   
				final_output.update(dict(zip(['sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section'], residual_max_per_section.T)))   
				final_output.update(dict(zip(['sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section'], residual_mean_per_section.T)))   
				final_output.update(dict(zip(['sec_1_zscore_per_section', 'sec_2_zscore_per_section', 'sec_3_zscore_per_section', 'sec_4_zscore_per_section', 'sec_5_zscore_per_section', 'sec_6_zscore_per_section', 'sec_7_zscore_per_section', 'sec_8_zscore_per_section', 'sec_9_zscore_per_section', 'sec_10_zscore_per_section'], zscore_per_section.T)))   
				final_output.update(dict(zip(['sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg'], sec_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg'], sec_curve_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima'], time_since_maxima.T)))
				final_output.update(dict(zip(['sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect'], time_since_last_sect.T)))
				final_output.update(dict(sec_1_residual_max = residual_max))
				final_output.update(dict(sec_1_residual_mean = residual_mean))				
				final_output.update(dict(sec_1_current_unix_time = current_unix_time)) 
				final_output.update(dict(sec_1_ms_since_open = ms_since_open)) 
				final_output.update(dict(current_year = current_year)) 
				final_output.update(dict(current_month = current_month)) 
				final_output.update(dict(current_week = current_week)) 
				final_output.update(dict(current_weekday = current_weekday)) 
				final_output.update(dict(current_30_mins = current_30_mins)) 
				final_output.update(dict(total_years = total_years)) 
				final_output.update(dict(total_months = total_months)) 
				final_output.update(dict(total_weeks = total_weeks)) 
				final_output.update(dict(total_days = total_days)) 
				final_output.update(dict(total_30_mins = total_30_mins)) 
				final_output.update(dict(sec_1_current_price = current_price)) 
				final_output.update(dict(sec_1_current_price_hat_r = current_price_hat_r)) 				
				final_output.update(dict(sec_1_avg_price = avg_Price)) 
				final_output.update(dict(zip(['sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section'], price_pcavg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section'], lr_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section'], lr_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section'], lr_price_r2_per_section.T)))
				final_output.update(dict(sec_1_price_pct_chg_neg = price_pct_chg_neg)) 
				final_output.update(dict(sec_1_price_pct_chg_pos = price_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_neg = lr_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_neg = lr_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_lr_price_r2_neg = lr_price_r2_neg)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_pos = lr_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_pos = lr_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_lr_price_r2_pos = lr_price_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_vol_pct_chg_per_section', 'sec_2_lr_vol_pct_chg_per_section', 'sec_3_lr_vol_pct_chg_per_section', 'sec_4_lr_vol_pct_chg_per_section', 'sec_5_lr_vol_pct_chg_per_section', 'sec_6_lr_vol_pct_chg_per_section', 'sec_7_lr_vol_pct_chg_per_section', 'sec_8_lr_vol_pct_chg_per_section', 'sec_9_lr_vol_pct_chg_per_section', 'sec_10_lr_vol_pct_chg_per_section'], lr_vol_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_roc_per_section', 'sec_2_lr_vol_roc_per_section', 'sec_3_lr_vol_roc_per_section', 'sec_4_lr_vol_roc_per_section', 'sec_5_lr_vol_roc_per_section', 'sec_6_lr_vol_roc_per_section', 'sec_7_lr_vol_roc_per_section', 'sec_8_lr_vol_roc_per_section', 'sec_9_lr_vol_roc_per_section', 'sec_10_lr_vol_roc_per_section'], lr_vol_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_r2_per_section', 'sec_2_lr_vol_r2_per_section', 'sec_3_lr_vol_r2_per_section', 'sec_4_lr_vol_r2_per_section', 'sec_5_lr_vol_r2_per_section', 'sec_6_lr_vol_r2_per_section', 'sec_7_lr_vol_r2_per_section', 'sec_8_lr_vol_r2_per_section', 'sec_9_lr_vol_r2_per_section', 'sec_10_lr_vol_r2_per_section'], lr_vol_r2_per_section.T))) 
				final_output.update(dict(sec_1_vol_total_pct_chg_neg = vol_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_vol_avg_roc_neg = vol_avg_roc_neg)) 
				final_output.update(dict(sec_1_vol_total_pct_chg_pos = vol_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_vol_avg_roc_pos = vol_avg_roc_pos)) 
				final_output.update(dict(sec_1_vol_r2_neg = vol_r2_neg)) 
				final_output.update(dict(sec_1_vol_r2_pos = vol_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_pct_chg_per_section', 'sec_2_lr_mf_price_pct_chg_per_section', 'sec_3_lr_mf_price_pct_chg_per_section', 'sec_4_lr_mf_price_pct_chg_per_section', 'sec_5_lr_mf_price_pct_chg_per_section', 'sec_6_lr_mf_price_pct_chg_per_section', 'sec_7_lr_mf_price_pct_chg_per_section', 'sec_8_lr_mf_price_pct_chg_per_section', 'sec_9_lr_mf_price_pct_chg_per_section', 'sec_10_lr_mf_price_pct_chg_per_section'], lr_mf_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_roc_per_section', 'sec_2_lr_mf_price_roc_per_section', 'sec_3_lr_mf_price_roc_per_section', 'sec_4_lr_mf_price_roc_per_section', 'sec_5_lr_mf_price_roc_per_section', 'sec_6_lr_mf_price_roc_per_section', 'sec_7_lr_mf_price_roc_per_section', 'sec_8_lr_mf_price_roc_per_section', 'sec_9_lr_mf_price_roc_per_section', 'sec_10_lr_mf_price_roc_per_section'], lr_mf_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_r2_per_section', 'sec_2_lr_mf_price_r2_per_section', 'sec_3_lr_mf_price_r2_per_section', 'sec_4_lr_mf_price_r2_per_section', 'sec_5_lr_mf_price_r2_per_section', 'sec_6_lr_mf_price_r2_per_section', 'sec_7_lr_mf_price_r2_per_section', 'sec_8_lr_mf_price_r2_per_section', 'sec_9_lr_mf_price_r2_per_section', 'sec_10_lr_mf_price_r2_per_section'], lr_mf_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_neg = mf_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_neg = mf_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_mf_price_r2_neg = mf_price_r2_neg)) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_pos = mf_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_pos = mf_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_mf_price_r2_pos = mf_price_r2_pos)) 
				final_output.update(dict(sec_1_avg_vol = avg_vol)) 






			
			elif zero_crossings.size == 1:
				critical_points = np.array([])
				sec_pct_chg = np.array([])
				sec_curve_pct_chg = np.array([])
				der_1 = np.array([])
				der_1 = np.append(der_1, np.zeros(8) + np.nan)
				der_2 = np.array([])
				der_2 = np.append(der_2, np.zeros(8) + np.nan)
				der_3 = np.array([])
				der_3 = np.append(der_3, np.zeros(8) + np.nan)
				sec_pct_chg = np.append(sec_pct_chg, np.zeros(8) + np.nan)
				sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(8) + np.nan)
				critical_points = np.append(critical_points, np.zeros(8) + np.nan)
				
				curve_der = section_der[int(zero_crossings[-1]):]
				if curve_der.size == 0:
					der_1 = np.append(der_1, np.zeros(2) + np.nan)
					der_2 = np.append(der_2, np.zeros(2) + np.nan)
					der_3 = np.append(der_3, np.zeros(2) + np.nan)
					sec_pct_chg = np.append(sec_pct_chg, np.zeros(2) + np.nan)
					sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(2) + np.nan)
					critical_points = np.append(critical_points, np.zeros(3) + np.nan)
				elif curve_der.size > 0:
					curve_der_sec_1 = curve_der
					curve_der_entry = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, 1)))
					curve_der_415 = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, .415)))
					critical_point_10 = curve_der_415
					critical_point_11 = curve_der_entry
					critical_points = np.append(critical_points, 0)
					critical_points = np.append(critical_points, critical_point_10).astype(np.int64)
					critical_points = np.append(critical_points, critical_point_11).astype(np.int64)
					

					critical_points = critical_points.astype(np.int64)

					curve_price = section_price[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
					curve_time = section_time[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
					curve_price_hat = section_price_hat[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
					curve_price_hat_diff_div_diff = np.diff(np.divide(np.diff(curve_price_hat), curve_price_hat[:-1])) 
					curve_der = np.divide(curve_price_hat_diff_div_diff, np.diff(curve_time)[:-1])
					curve_time_r = section_time[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
					curve_price_r = section_price[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
					curve_price_hat_r = section_price_hat[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]
					curve_vol_r = section_vol[int(zero_crossings[-1]):int(zero_crossings[-1])+int(critical_points[-1])+2]

					
					curve_der_diff = np.diff(curve_der)
					section_der_2 = np.divide(curve_der_diff, np.diff(curve_time[:-2]))
					


					critical_points_pos = np.array([critical_points[-3], critical_points[-2], critical_points[-1]]).astype(np.int64)
					for pnn in range(0, len(critical_points_pos)-1):
						
						if critical_points_pos[pnn+1]+1 < len(curve_der):
							section_mean = np.nanmean(curve_der[critical_points_pos[pnn]:critical_points_pos[pnn+1]+1])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
						else:
							section_mean = np.nanmean(curve_der[critical_points_pos[pnn]:critical_points_pos[pnn+1]])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
					for pnn2 in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_2[critical_points_pos[pnn2]:critical_points_pos[pnn2+1]])
						if section_mean == 0:
							section_mean = np.nan
						der_2 = np.append(der_2, section_mean)
						
					
					section_der_2_diff = np.diff(section_der_2)
					section_der_3 = np.divide(section_der_2_diff, np.diff(curve_time[:-3]))			
					
					for pnn3 in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_3[critical_points_pos[pnn3]:critical_points_pos[pnn3+1]-1])
						if section_mean == 0:
							section_mean = np.nan
						der_3 = np.append(der_3, np.nan)
						
					for s_p_c_2nn in range(0, len(critical_points_pos)-1):
						if critical_points_pos[s_p_c_2nn+1]+1 < len(curve_price_hat):
							section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2nn+1]+1], curve_price_hat[critical_points_pos[s_p_c_2nn]]), curve_price_hat[critical_points_pos[s_p_c_2nn]])
							if section_pct_chg.size == 0:
								section_pct_chg = np.nan
							sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
						else:
							section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2nn+1]], curve_price_hat[critical_points_pos[s_p_c_2nn]]), curve_price_hat[critical_points_pos[s_p_c_2nn]])
							if section_pct_chg.size == 0:
								section_pct_chg = np.nan
							sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
					for s_c_p_c_2nn in range(0, len(critical_points_pos)-1):
						if critical_points_pos[s_c_p_c_2nn+1]+1 < len(curve_price_hat):
							section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2nn+1]+1], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
							if section_curve_pct_chg.size == 0:
								section_curve_pct_chg = np.nan
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
						else:
							section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2nn+1]], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
							if section_curve_pct_chg.size == 0:
								section_curve_pct_chg = np.nan
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)

				
				r_sq = np.array([])
				avg_mean_per_section = np.array([])
				std_per_section = np.array([])
				residual_max_per_section = np.array([])
				residual_mean_per_section = np.array([])
				zscore_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					r_sq = np.append(r_sq, np.zeros(10) + np.nan)
					avg_mean_per_section = np.append(avg_mean_per_section, np.zeros(10) + np.nan)
					std_per_section = np.append(std_per_section, np.zeros(10) + np.nan)
					residual_max_per_section = np.append(residual_max_per_section, np.zeros(10) + np.nan)
					residual_mean_per_section = np.append(residual_mean_per_section, np.zeros(10) + np.nan)
					zscore_per_section = np.append(zscore_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for rnn in range(0, len(critical_points)-1):
						if critical_points[rnn] <= -9223372036854775 or critical_points[rnn] == np.nan:
							r_sq = np.append(r_sq, np.nan)
							avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
							std_per_section = np.append(std_per_section, np.nan)
							residual_max_per_section = np.append(residual_max_per_section, np.nan)
							residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
							zscore_per_section = np.append(zscore_per_section, np.nan)
							continue
						else:
							if critical_points[rnn+1] <= -9223372036854775 or critical_points[rnn+1] == np.nan:
								r_sq = np.append(r_sq, np.nan)
								avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
								std_per_section = np.append(std_per_section, np.nan)
								residual_max_per_section = np.append(residual_max_per_section, np.nan)
								residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
								zscore_per_section = np.append(zscore_per_section, np.nan)
								continue
							else:
								
								if critical_points[rnn+1] == critical_points[rnn]:
									if r_sq.size == 0 or avg_mean_per_section.size == 0 or std_per_section.size == 0 or residual_max_per_section.size == 0 or residual_mean_per_section.size == 0 or zscore_per_section.size == 0:
										r_sq = np.append(r_sq, np.nan)
										avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
										std_per_section = np.append(std_per_section, np.nan)
										residual_max_per_section = np.append(residual_max_per_section, np.nan)
										residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
										zscore_per_section = np.append(zscore_per_section, np.nan)
									else:	
										rnn += 1
										r_sq = np.append(r_sq, r_sq[-1])
										avg_mean_per_section = np.append(avg_mean_per_section, avg_mean_per_section[-1])
										std_per_section = np.append(std_per_section, std_per_section[-1])
										residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section[-1])
										residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section[-1])
										zscore_per_section = np.append(zscore_per_section, zscore_per_section[-1])
										continue
								else:
									r_sq_2 = model.score(curve_time_r[critical_points[rnn]:critical_points[rnn+1]+2], curve_price_r[critical_points[rnn]:critical_points[rnn+1]+2])
									r_sq = np.append(r_sq, r_sq_2)
									avg_mean = np.nanmean(curve_price_r[critical_points[rnn]:critical_points[rnn+1]+2])
									avg_mean_per_section = np.append(avg_mean_per_section, avg_mean)
									std_sec = np.nanstd(curve_price_r[critical_points[rnn]:critical_points[rnn+1]+2])
									std_per_section = np.append(std_per_section, std_sec)
									zscore_per_section_0 = np.nanmean(zscore(curve_price_r[critical_points[rnn]:critical_points[rnn+1]+2]))
									zscore_per_section = np.append(zscore_per_section, zscore_per_section_0)
									curvy = curve_price_hat_r[critical_points[rnn]:critical_points[rnn+1]+2]
									residual_max_per_section_0_0_0 = np.abs(np.subtract(curve_price_r[critical_points[rnn]:critical_points[rnn+1]+2], curve_price_hat_r[critical_points[rnn]:critical_points[rnn+1]+2]))
									residual_max_per_section_0_0 = np.nanargmax(residual_max_per_section_0_0_0)
									residual_max_per_section_0 = np.take(residual_max_per_section_0_0_0, residual_max_per_section_0_0)
									residual_mean_per_section_0 = np.nanmean(np.abs(np.subtract(curve_price_r[critical_points[rnn]:critical_points[rnn+1]+2], curve_price_hat_r[critical_points[rnn]:critical_points[rnn+1]+2])))
									residual_max_per_section_0_ph = np.divide(residual_max_per_section_0, np.take(curvy, residual_max_per_section_0_0))
									residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section_0_ph)
									residual_mean_per_section_0_ph = np.nanmean(curvy)
									residual_mean_per_section_div = np.divide(residual_mean_per_section_0, residual_mean_per_section_0_ph)
									residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section_div)

				r_sq = r_sq.astype(np.float64)

				time_since_maxima = np.array([])
				time_since_last_sect = np.array([])
				if curve_price_hat_r.size == 0:
					time_since_maxima = np.append(time_since_maxima, np.zeros(10) + np.nan)
					time_since_last_sect = np.append(time_since_last_sect, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for cnn in range(0, len(critical_points)-1):
						if critical_points[cnn] <= -9223372036854775 or critical_points[cnn] == np.nan:
							time_since_maxima = np.append(time_since_maxima, np.nan)
							time_since_last_sect = np.append(time_since_last_sect, np.nan)
							continue
						else:
							if critical_points[cnn+1] <= -9223372036854775 or critical_points[cnn+1] == np.nan:
								time_since_maxima = np.append(time_since_maxima, np.nan)
								time_since_last_sect = np.append(time_since_last_sect, np.nan)
								continue
							else:
								if critical_points[cnn+1] == critical_points[cnn]:
									cnn += 1
									time_since_maxima = np.append(time_since_maxima, np.zeros(1))
									time_since_last_sect = np.append(time_since_last_sect, np.zeros(1))
									continue
								else:
									if critical_points[0] <= -9223372036854775 or critical_points[0] == np.nan:
										time_since_maxima_append = np.nan
									else:
										if critical_points[cnn+1]+1 < len(curve_time_r)-1:
											time_since_maxima_append = curve_time_r[critical_points[cnn+1]+1] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[cnn+1]+1] - curve_time_r[critical_points[cnn]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
										else:
											time_since_maxima_append = curve_time_r[critical_points[cnn+1]] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[cnn+1]] - curve_time_r[critical_points[cnn]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)


				residual_max_2 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
				residual_max_1 = np.nanargmax(residual_max_2)
				residual_max_0 = np.take(residual_max_2, residual_max_1)
				residual_max_0_ph = np.take(curve_price_hat_r, residual_max_1)
				residual_max = np.divide(residual_max_0, residual_max_0_ph)
				residual_mean_0 = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))
				residual_mean = np.divide(residual_mean_0, np.nanmean(curve_price_hat_r))



				if critical_points.size > 0 and curve_price_r.size > 0:
					current_price = curve_price_r[-1]
				else:
					current_price = np.nan
				
				if critical_points.size > 0 and curve_price_hat_r.size > 0:
					current_price_hat_r = curve_price_hat_r[-1]
				else:
					current_price_hat_r = np.nan

				if curve_price_r.size > 0:
					avg_Price = np.nanmean(curve_price_r)
				else:
					avg_Price = np.nan

				

				current_date = datetime(1970, 1, 1) + timedelta(seconds=np.multiply(curve_time[-1],1000000000))
				current_date = current_date.strftime('%Y-%m-%d')
				open_ts = datetime.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')
				dt = pytz.timezone('America/Chicago').normalize(open_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
				is_dst = bool(dt.dst())
				if is_dst is True:
					offset_open = int(time.mktime(time.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')))
				elif is_dst is False:
					offset_open = int(time.mktime(time.strptime(current_date + ' 9:00:00', '%Y-%m-%d %H:%M:%S')))
				if critical_points.size > 0 and curve_time_r.size > 0:
					current_unix_time = np.multiply(curve_time_r[-1],1000000000)
					ms_since_open = np.multiply(curve_time_r[-1],1000000000) - offset_open
				else:
					current_unix_time = np.nan
					ms_since_open = np.nan

				current_date = datetime.strptime(current_date, '%Y-%m-%d')
				
				current_year = current_date.isocalendar()[0]
				current_month = current_date.month

				first_day = current_date.replace(day=1)
				dom = current_date.day
				adjusted_dom = dom + first_day.weekday()
				current_week = int(ceil(adjusted_dom/7.0))

				current_weekday = current_date.isocalendar()[2]

				if ms_since_open <= 1800:
					current_30_mins = 1
				elif ms_since_open > 1800 and ms_since_open <= 1800*2:
					current_30_mins = 2
				elif ms_since_open > 1800*2 and ms_since_open <= 1800*3:
					current_30_mins = 3
				elif ms_since_open > 1800*3 and ms_since_open <= 1800*4:
					current_30_mins = 4
				elif ms_since_open > 1800*4 and ms_since_open <= 1800*5:
					current_30_mins = 5
				else:
					current_30_mins = np.nan
				

				total_years = int((current_date - datetime(1970,1,1)).days/365.24)
				total_months = int(((current_date - datetime(1970,1,1)).days/365.24)*12.0)
				total_weeks = int((current_date - datetime(1970,1,1)).days/7.0)
				total_days = int((current_date - datetime(1970,1,1)).days)
				total_30_mins = int(current_unix_time/1800)


				

				
				price_pcavg_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for avg_p_n_2 in range(0, len(critical_points)-1):
						if critical_points[avg_p_n_2] <= -9223372036854775 or critical_points[avg_p_n_2] == np.nan:
							price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_n_2+1] <= -9223372036854775 or critical_points[avg_p_n_2+1] == np.nan:
								price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_n_2+1] == critical_points[avg_p_n_2]:
									avg_p_n_2 += 1
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(1))
									continue
								else:
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.nanmean(np.diff(curve_price_hat_r[critical_points[avg_p_n_2]:critical_points[avg_p_n_2+1]+2])/curve_price_hat_r[critical_points[avg_p_n_2]:critical_points[avg_p_n_2+1]+2][:-1]))



				price_pct_chg_neg = np.nan
				price_pct_chg_pos = np.divide(np.subtract(curve_price[-1], curve_price[0]), curve_price[0])


				lm = linear_model.LinearRegression()

				lr_price_total_pct_chg_neg = np.nan
				lr_price_avg_roc_neg = np.nan
				lr_price_r2_neg = np.nan




				X = pd.DataFrame(curve_time)
				y = pd.DataFrame(curve_price)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				lr_curve_pos_hat = np.array(lm.predict(X)).flatten()
				lr_price_total_pct_chg_pos = np.divide(np.subtract(lr_curve_pos_hat[-1], lr_curve_pos_hat[0]), lr_curve_pos_hat[0])
				lr_price_avg_roc_pos = np.nanmean((np.diff(lr_curve_pos_hat)/lr_curve_pos_hat[:-1])/np.diff(curve_time))
				lr_price_r2_pos = lm.score(X,y)



				lr_price_pct_chg_per_section = np.array([])
				lr_price_roc_per_section = np.array([])
				lr_price_r2_per_section = np.array([])
				if curve_price_r.size == 0:
					lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(10) + np.nan)
					lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(10) + np.nan)
				elif curve_price_r.size > 0:
					for avg_p_2 in range(0, len(critical_points)-1):
						if critical_points[avg_p_2] <= -9223372036854775 or critical_points[avg_p_2] == np.nan:
							lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
							lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
							lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_2+1] <= -9223372036854775 or critical_points[avg_p_2+1] == np.nan:
								lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
								lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
								lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_2+1] == critical_points[avg_p_2]:
									avg_p_2 += 1
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(1))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(1))
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[avg_p_2]:critical_points[avg_p_2+1]+2])
									y = pd.DataFrame(curve_price_r[critical_points[avg_p_2]:critical_points[avg_p_2+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.divide(np.subtract(lr_price_per_section_hat[-1], lr_price_per_section_hat[0]), lr_price_per_section_hat[0]))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nanmean((np.diff(lr_price_per_section_hat)/lr_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[avg_p_2]:critical_points[avg_p_2+1]+2])))
									lr_price_r2_per_section_2 = lm.score(X,y)
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, lr_price_r2_per_section_2)






				lr_vol_pct_chg_per_section = np.array([])
				lr_vol_roc_per_section = np.array([])
				lr_vol_r2_per_section = np.array([])
				if curve_vol_r.size == 0:
					lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(10) + np.nan)
					lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(10) + np.nan)
				elif curve_vol_r.size > 0:
					for vnn in range(0, len(critical_points)-1):
						if critical_points[vnn] <= -9223372036854775 or critical_points[vnn] == np.nan:
							lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
							lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
							lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
							continue
						else:
							if critical_points[vnn+1] <= -9223372036854775 or critical_points[vnn+1] == np.nan:
								lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
								lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
								lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
								continue
							else:
								if critical_points[vnn+1] == critical_points[vnn]:
									vnn += 1
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(1))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(1))
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[vnn]:critical_points[vnn+1]+2])
									y = pd.DataFrame(curve_vol_r[critical_points[vnn]:critical_points[vnn+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_vol_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.divide(np.subtract(lr_vol_per_section_hat[-1], lr_vol_per_section_hat[0]), lr_vol_per_section_hat[0]))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nanmean((np.diff(lr_vol_per_section_hat)/lr_vol_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[vnn]:critical_points[vnn+1]+2])))
									lr_vol_r2_per_section_2 = lm.score(X,y)
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, lr_vol_r2_per_section_2)




				vol_total_pct_chg_neg = np.nan
				vol_avg_roc_neg = np.nan
				vol_r2_neg = np.nan




				section_time_key_pos = curve_time_r[int(critical_points[-3]):int(critical_points[-1]+2)]
				section_vol_key_pos = curve_vol_r[int(critical_points[-3]):int(critical_points[-1]+2)]

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_vol_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_vol_pos_hat = np.array(lm.predict(X)).flatten()
				vol_total_pct_chg_pos = np.divide(np.subtract(section_vol_pos_hat[-1], section_vol_pos_hat[0]), section_vol_pos_hat[0])
				vol_avg_roc_pos = np.nanmean((np.diff(section_vol_pos_hat)/section_vol_pos_hat[:-1])/np.diff(section_time_key_pos))
				vol_r2_pos = lm.score(X,y)




				section_mf_key_total = np.multiply(curve_price_r, curve_vol_r)

				lr_mf_price_pct_chg_per_section = np.array([])
				lr_mf_price_roc_per_section = np.array([])
				lr_mf_price_r2_per_section = np.array([])
				if section_mf_key_total.size == 0:
					lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(10) + np.nan)
					lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(10) + np.nan)
				elif section_mf_key_total.size > 0:
					for vnn2 in range(0, len(critical_points)-1):
						if critical_points[vnn2] <= -9223372036854775 or critical_points[vnn2] == np.nan:
							lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
							lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
							lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[vnn2+1] <= -9223372036854775 or critical_points[vnn2+1] == np.nan:
								lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
								lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
								lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[vnn2+1] == critical_points[vnn2]:
									vnn2 += 1
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(1))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(1))
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[vnn2]:critical_points[vnn2+1]+2])
									y = pd.DataFrame(section_mf_key_total[critical_points[vnn2]:critical_points[vnn2+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_mf_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.divide(np.subtract(lr_mf_price_per_section_hat[-1], lr_mf_price_per_section_hat[0]), lr_mf_price_per_section_hat[0]))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nanmean((np.diff(lr_mf_price_per_section_hat)/lr_mf_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[vnn2]:critical_points[vnn2+1]+2])))
									lr_mf_price_r2_per_section_2 = lm.score(X,y)
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, lr_mf_price_r2_per_section_2)


				mf_price_total_pct_chg_neg = np.nan
				mf_price_avg_roc_neg = np.nan
				mf_price_r2_neg = np.nan




				section_time_key_pos = curve_time
				section_mf_price_key_pos = np.multiply(curve_price, section_vol_key_pos)

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_mf_price_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_mf_price_pos_hat = np.array(lm.predict(X)).flatten()
				mf_price_total_pct_chg_pos = np.divide(np.subtract(section_mf_price_pos_hat[-1], section_mf_price_pos_hat[0]), section_mf_price_pos_hat[0])
				mf_price_avg_roc_pos = np.nanmean((np.diff(section_mf_price_pos_hat)/section_mf_price_pos_hat[:-1])/np.diff(section_time_key_pos))
				mf_price_r2_pos = lm.score(X,y)



				avg_vol = np.nanmean(curve_vol_r)

				
				quote_key_times_append = np.array([])
				quote_key_times = np.array([])
				for cpnn in range(0, len(critical_points)):
					if critical_points[cpnn] <= -9223372036854775 or critical_points[cpnn] == np.nan:
						quote_key_times = np.append(quote_key_times, np.nan)
						continue
					else:
						if critical_points[cpnn]+1 < len(curve_time_r) and cpnn == 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cpnn])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cpnn]+1 < len(curve_time_r) and cpnn > 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cpnn]+1)], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cpnn]+1 >= len(curve_time_r):
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cpnn])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)





				final_output.update(dict(ratio_ewm_50_25 = ratio_ewm_50_25))
				final_output.update(dict(ratio_ewm_150_arr = ratio_ewm_150_arr))
				final_output.update(dict(ratio_ewm_67_arr = ratio_ewm_67_arr))
				final_output.update(dict(ratio_ewm_67_150_arr = ratio_ewm_67_150_arr))
				final_output.update(dict(pct_ewm_50_25 = pct_ewm_50_25))
				final_output.update(dict(pct_ewm_150_arr = pct_ewm_150_arr))
				final_output.update(dict(pct_ewm_67_arr = pct_ewm_67_arr))
				final_output.update(dict(pct_ewm_67_150_arr = pct_ewm_67_150_arr))
				final_output.update(dict(avg_return_40_mins = avg_return_40_mins)) 
				final_output.update(dict(avg_chg_in_avg_return = avg_chg_in_avg_return)) 
				final_output.update(dict(return_minus_5 = return_minus_5)) 
				final_output.update(dict(return_minus_4 = return_minus_4)) 
				final_output.update(dict(return_minus_3 = return_minus_3)) 
				final_output.update(dict(return_minus_2 = return_minus_2)) 
				final_output.update(dict(lr_all_day_time_passed = lr_all_day_time_passed)) 
				final_output.update(dict(lr_all_day_pct_chg = lr_all_day_pct_chg))
				final_output.update(dict(lr_all_day_roc = lr_all_day_roc)) 
				final_output.update(dict(lr_all_day_r2 = lr_all_day_r2)) 
				final_output.update(dict(zip(['sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1'], der_1.T)))
				final_output.update(dict(zip(['sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2'], der_2.T)))
				final_output.update(dict(zip(['sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3'], der_3.T)))
				final_output.update(dict(zip(['sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq'], r_sq.T)))   
				final_output.update(dict(zip(['sec_1_avg_mean_per_section', 'sec_2_avg_mean_per_section', 'sec_3_avg_mean_per_section', 'sec_4_avg_mean_per_section', 'sec_5_avg_mean_per_section', 'sec_6_avg_mean_per_section', 'sec_7_avg_mean_per_section', 'sec_8_avg_mean_per_section', 'sec_9_avg_mean_per_section', 'sec_10_avg_mean_per_section'], avg_mean_per_section.T)))   
				final_output.update(dict(zip(['sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section'], std_per_section.T)))   
				final_output.update(dict(zip(['sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section'], residual_max_per_section.T)))   
				final_output.update(dict(zip(['sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section'], residual_mean_per_section.T)))   
				final_output.update(dict(zip(['sec_1_zscore_per_section', 'sec_2_zscore_per_section', 'sec_3_zscore_per_section', 'sec_4_zscore_per_section', 'sec_5_zscore_per_section', 'sec_6_zscore_per_section', 'sec_7_zscore_per_section', 'sec_8_zscore_per_section', 'sec_9_zscore_per_section', 'sec_10_zscore_per_section'], zscore_per_section.T)))   
				final_output.update(dict(zip(['sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg'], sec_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg'], sec_curve_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima'], time_since_maxima.T)))
				final_output.update(dict(zip(['sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect'], time_since_last_sect.T))) 
				final_output.update(dict(sec_1_residual_max = residual_max))
				final_output.update(dict(sec_1_residual_mean = residual_mean))				
				final_output.update(dict(sec_1_current_unix_time = current_unix_time)) 
				final_output.update(dict(sec_1_ms_since_open = ms_since_open)) 
				final_output.update(dict(current_year = current_year)) 
				final_output.update(dict(current_month = current_month)) 
				final_output.update(dict(current_week = current_week)) 
				final_output.update(dict(current_weekday = current_weekday)) 
				final_output.update(dict(current_30_mins = current_30_mins)) 
				final_output.update(dict(total_years = total_years)) 
				final_output.update(dict(total_months = total_months)) 
				final_output.update(dict(total_weeks = total_weeks)) 
				final_output.update(dict(total_days = total_days)) 
				final_output.update(dict(total_30_mins = total_30_mins)) 
				final_output.update(dict(sec_1_current_price = current_price))
				final_output.update(dict(sec_1_current_price_hat_r = current_price_hat_r)) 					 
				final_output.update(dict(sec_1_avg_price = avg_Price)) 
				final_output.update(dict(zip(['sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section'], price_pcavg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section'], lr_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section'], lr_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section'], lr_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_price_pct_chg_neg = price_pct_chg_neg)) 
				final_output.update(dict(sec_1_price_pct_chg_pos = price_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_neg = lr_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_neg = lr_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_lr_price_r2_neg = lr_price_r2_neg)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_pos = lr_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_pos = lr_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_lr_price_r2_pos = lr_price_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_vol_pct_chg_per_section', 'sec_2_lr_vol_pct_chg_per_section', 'sec_3_lr_vol_pct_chg_per_section', 'sec_4_lr_vol_pct_chg_per_section', 'sec_5_lr_vol_pct_chg_per_section', 'sec_6_lr_vol_pct_chg_per_section', 'sec_7_lr_vol_pct_chg_per_section', 'sec_8_lr_vol_pct_chg_per_section', 'sec_9_lr_vol_pct_chg_per_section', 'sec_10_lr_vol_pct_chg_per_section'], lr_vol_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_roc_per_section', 'sec_2_lr_vol_roc_per_section', 'sec_3_lr_vol_roc_per_section', 'sec_4_lr_vol_roc_per_section', 'sec_5_lr_vol_roc_per_section', 'sec_6_lr_vol_roc_per_section', 'sec_7_lr_vol_roc_per_section', 'sec_8_lr_vol_roc_per_section', 'sec_9_lr_vol_roc_per_section', 'sec_10_lr_vol_roc_per_section'], lr_vol_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_r2_per_section', 'sec_2_lr_vol_r2_per_section', 'sec_3_lr_vol_r2_per_section', 'sec_4_lr_vol_r2_per_section', 'sec_5_lr_vol_r2_per_section', 'sec_6_lr_vol_r2_per_section', 'sec_7_lr_vol_r2_per_section', 'sec_8_lr_vol_r2_per_section', 'sec_9_lr_vol_r2_per_section', 'sec_10_lr_vol_r2_per_section'], lr_vol_r2_per_section.T))) 
				final_output.update(dict(sec_1_vol_total_pct_chg_neg = vol_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_vol_avg_roc_neg = vol_avg_roc_neg)) 
				final_output.update(dict(sec_1_vol_total_pct_chg_pos = vol_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_vol_avg_roc_pos = vol_avg_roc_pos)) 
				final_output.update(dict(sec_1_vol_r2_neg = vol_r2_neg)) 
				final_output.update(dict(sec_1_vol_r2_pos = vol_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_pct_chg_per_section', 'sec_2_lr_mf_price_pct_chg_per_section', 'sec_3_lr_mf_price_pct_chg_per_section', 'sec_4_lr_mf_price_pct_chg_per_section', 'sec_5_lr_mf_price_pct_chg_per_section', 'sec_6_lr_mf_price_pct_chg_per_section', 'sec_7_lr_mf_price_pct_chg_per_section', 'sec_8_lr_mf_price_pct_chg_per_section', 'sec_9_lr_mf_price_pct_chg_per_section', 'sec_10_lr_mf_price_pct_chg_per_section'], lr_mf_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_roc_per_section', 'sec_2_lr_mf_price_roc_per_section', 'sec_3_lr_mf_price_roc_per_section', 'sec_4_lr_mf_price_roc_per_section', 'sec_5_lr_mf_price_roc_per_section', 'sec_6_lr_mf_price_roc_per_section', 'sec_7_lr_mf_price_roc_per_section', 'sec_8_lr_mf_price_roc_per_section', 'sec_9_lr_mf_price_roc_per_section', 'sec_10_lr_mf_price_roc_per_section'], lr_mf_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_r2_per_section', 'sec_2_lr_mf_price_r2_per_section', 'sec_3_lr_mf_price_r2_per_section', 'sec_4_lr_mf_price_r2_per_section', 'sec_5_lr_mf_price_r2_per_section', 'sec_6_lr_mf_price_r2_per_section', 'sec_7_lr_mf_price_r2_per_section', 'sec_8_lr_mf_price_r2_per_section', 'sec_9_lr_mf_price_r2_per_section', 'sec_10_lr_mf_price_r2_per_section'], lr_mf_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_neg = mf_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_neg = mf_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_mf_price_r2_neg = mf_price_r2_neg)) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_pos = mf_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_pos = mf_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_mf_price_r2_pos = mf_price_r2_pos)) 
				final_output.update(dict(sec_1_avg_vol = avg_vol)) 


				
			return final_output

			




		def get_features_final_y(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model):
				
			num_ticks_67 = int(67)
			ewm_Px_67 = price_pd['Price'].ewm(span=num_ticks_67).mean()

			num_ticks_150 = int(150)
			ewm_Px_150 = price_pd['Price'].ewm(span=num_ticks_150).mean()

			num_ticks_50_25 = int(50)
			ewm_Px_50_25 = price_pd['Price'][::25].ewm(span=num_ticks_50_25).mean()

			ratio_ewm_50_25 = price_pd['Price'][-1]/ewm_Px_50_25[-1]
			ratio_ewm_150_arr = price_pd['Price'][-1]/ewm_Px_150[-1]
			ratio_ewm_67_arr = price_pd['Price'][-1]/ewm_Px_67[-1]
			ratio_ewm_67_150_arr = ewm_Px_67[-1]/ewm_Px_150[-1]
			pct_ewm_50_25 = ((price_pd['Price'][-1]-ewm_Px_50_25[-1])/ewm_Px_50_25[-1])*100		
			pct_ewm_150_arr = ((price_pd['Price'][-1]-ewm_Px_150[-1])/ewm_Px_150[-1])*100
			pct_ewm_67_arr = ((price_pd['Price'][-1]-ewm_Px_67[-1])/ewm_Px_67[-1])*100	
			pct_ewm_67_150_arr = ((ewm_Px_67[-1]-ewm_Px_150[-1])/ewm_Px_150[-1])*100
			

			der_y = np.take(section_price_hat, zero_crossings)
			pct_chg = np.diff(der_y) / der_y[:-1]
			pct_chg_wh = np.where(pct_chg > 0)
			pct_chg_wh = pct_chg_wh[0]
			pct_chg_long = np.take(pct_chg, pct_chg_wh)
			avg_return_40_mins = np.nanmean(pct_chg_long)
			pct_chg_long_2 = np.array([])
			running_avg_pcl = np.array([])
			chg_in_avg_rtn = np.array([])
			for pcl in pct_chg_long:
				pct_chg_long_2 = np.append(pct_chg_long_2, pcl)
				running_avg_pcl = np.append(running_avg_pcl, np.nanmean(pct_chg_long_2))
				if running_avg_pcl.size > 1:
					chg_in_avg_rtn_1 = np.subtract(running_avg_pcl[-1], running_avg_pcl[-2])
					chg_in_avg_rtn = np.append(chg_in_avg_rtn, chg_in_avg_rtn_1)
					avg_chg_in_avg_return = np.nanmean(chg_in_avg_rtn)
				else:
					chg_in_avg_rtn = np.append(chg_in_avg_rtn, np.nan)
					avg_chg_in_avg_return = np.nanmean(chg_in_avg_rtn)
			if 'avg_chg_in_avg_return' not in locals():
				avg_chg_in_avg_return = np.nan
			if zero_crossings.size > 6:
				return_minus_5 = pct_chg[-6]
				return_minus_4 = pct_chg[-5]
				return_minus_3 = pct_chg[-4]
				return_minus_2 = pct_chg[-3]
			if zero_crossings.size == 6:
				return_minus_5 = np.nan
				return_minus_4 = pct_chg[-5]
				return_minus_3 = pct_chg[-4]
				return_minus_2 = pct_chg[-3]
			elif zero_crossings.size == 5:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = pct_chg[-4]
				return_minus_2 = pct_chg[-3]
			elif zero_crossings.size == 4:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = np.nan
				return_minus_2 = pct_chg[-3]
			elif zero_crossings.size == 3:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = np.nan
				return_minus_2 = np.nan
			else:
				return_minus_5 = np.nan
				return_minus_4 = np.nan
				return_minus_3 = np.nan
				return_minus_2 = np.nan


			X = pd.DataFrame(section_time)
			y = pd.DataFrame(section_price)
			lm = linear_model.LinearRegression()
			model_lm = lm.fit(X,y)
			lr_all_day_hat = np.array(lm.predict(X)).flatten()
			lr_all_day_time_passed = np.subtract(section_time[-1], section_time[0])
			lr_all_day_pct_chg = np.divide(np.subtract(lr_all_day_hat[-1], lr_all_day_hat[0]), lr_all_day_hat[0])
			lr_all_day_roc = np.nanmean((np.diff(lr_all_day_hat)/lr_all_day_hat[:-1])/np.diff(section_time))
			lr_all_day_r2 = lm.score(X,y)



			if zero_crossings.size > 2:
				zero_crossings = np.unique(zero_crossings)
				critical_points = np.array([])
				sec_pct_chg = np.array([])
				sec_curve_pct_chg = np.array([])
				curve_der_neg = section_der[int(zero_crossings[-3]):int(zero_crossings[-2]+1)]
				curve_price_hat_neg = section_price_hat[int(zero_crossings[-3]):int(zero_crossings[-2]+2)]
				curve_price_neg = section_price[int(zero_crossings[-3]):int(zero_crossings[-2]+2)]
				curve_time_neg = section_time[int(zero_crossings[-3]):int(zero_crossings[-2]+2)]
				critical_points = np.append(critical_points, 0)
				if curve_der_neg.size == 0:
					der_1 = np.array([])
					der_1 = np.append(der_1, np.zeros(8) + np.nan)
					der_2 = np.array([])
					der_2 = np.append(der_2, np.zeros(8) + np.nan)
					der_3 = np.array([])
					der_3 = np.append(der_3, np.zeros(8) + np.nan)
					sec_pct_chg = np.append(sec_pct_chg, np.zeros(8) + np.nan)
					sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(8) + np.nan)
					critical_points = np.append(critical_points, np.zeros(8) + np.nan)
				elif curve_der_neg.size > 0:
					curve_der_neg_diff = np.diff(curve_der_neg)
					section_der_2_neg = np.divide(curve_der_neg_diff, np.diff(curve_time_neg[:-2]))

					inflect_pt_neg = int(np.nanargmin(curve_der_neg))
					curve_der_sec_1_neg = curve_der_neg[0:inflect_pt_neg+1]
					if curve_der_sec_1_neg.size == 0:
						critical_points = np.append(critical_points, np.zeros(3) + np.nan)
						critical_points = np.append(critical_points, inflect_pt_neg)
					elif curve_der_sec_1_neg.size > 0:
						curve_der_415_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -.415)))
						curve_der_entry_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -1)))
						curve_der_241_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_1_neg, -2.41)))
						critical_points = np.append(critical_points, curve_der_415_neg).astype(np.int64)
						critical_points = np.append(critical_points, curve_der_entry_neg).astype(np.int64)
						critical_points = np.append(critical_points, curve_der_241_neg).astype(np.int64)

						
					if critical_points.size == 4:
						critical_points = np.append(critical_points, inflect_pt_neg)
					elif critical_points.size == 1:
						critical_points = np.append(critical_points, np.zeros(3) + np.nan)
						critical_points = np.append(critical_points, inflect_pt_neg)

					curve_der_sec_2_neg = curve_der_neg[inflect_pt_neg:]
					if curve_der_sec_2_neg.size > 0:
						curve_der_241_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -2.41)))
						curve_der_entry_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -1)))
						curve_der_415_neg = np.nanargmin(np.abs(np.subtract(curve_der_sec_2_neg, -.415)))
						curve_der_241_neg = len(curve_der_sec_1_neg)-1 + curve_der_241_neg
						curve_der_entry_neg = len(curve_der_sec_1_neg)-1 + curve_der_entry_neg
						curve_der_415_neg = len(curve_der_sec_1_neg)-1 + curve_der_415_neg
						critical_points = np.append(critical_points, curve_der_241_neg)
						critical_points = np.append(critical_points, curve_der_entry_neg)
						critical_points = np.append(critical_points, curve_der_415_neg)
						critical_points = np.append(critical_points, len(curve_der_neg)-1)
					if critical_points.size == 5:
						critical_points = np.append(critical_points, np.zeros(3) + np.nan)
						critical_points = np.append(critical_points, len(curve_der_neg)-1)
					critical_points = critical_points.astype(np.int64)
					
					der_1 = np.array([])
					for s_y in range(0, len(critical_points)-1):
						if critical_points[s_y] <= -9223372036854775 or critical_points[s_y] == np.nan:
							der_1 = np.append(der_1, np.nan)
							continue
						else:
							if critical_points[s_y+1] <= -9223372036854775 or critical_points[s_y+1] == np.nan:
								der_1 = np.append(der_1, np.nan)
								continue
							else:
								
								if critical_points[s_y+1]+1 < len(curve_der_neg):
									section_mean = np.nanmean(curve_der_neg[critical_points[s_y]:critical_points[s_y+1]+1])
									if section_mean.size == 0:
										section_mean = np.nan
									der_1 = np.append(der_1, section_mean)
								else:
									section_mean = np.nanmean(curve_der_neg[critical_points[s_y]:critical_points[s_y+1]])
									if section_mean.size == 0:
										section_mean = np.nan
									der_1 = np.append(der_1, section_mean)
								
					
					der_2 = np.array([])
					for s2_y in range(0, len(critical_points)-1):
						if critical_points[s2_y] <= -9223372036854775 or critical_points[s2_y] == np.nan:
							der_2 = np.append(der_2, np.nan)
							continue
						else:
							if critical_points[s2_y+1] <= -9223372036854775 or critical_points[s2_y+1] == np.nan:
								der_2 = np.append(der_2, np.nan)
								continue
							else:
								section_mean = np.nanmean(section_der_2_neg[critical_points[s2_y]:critical_points[s2_y+1]])
								if section_mean.size == 0:
									section_mean = np.nan
								der_2 = np.append(der_2, section_mean)

					section_der_2_neg_diff = np.diff(section_der_2_neg)
					section_der_3_neg = np.divide(section_der_2_neg_diff, np.diff(curve_time_neg[:-3]))
					

					der_3 = np.array([])
					for s3_y in range(0, len(critical_points)-1):
						if critical_points[s3_y] <= -9223372036854775 or critical_points[s3_y] == np.nan:
							der_3 = np.append(der_3, np.nan)
							continue
						else:
							if critical_points[s3_y+1] <= -9223372036854775 or critical_points[s3_y+1] == np.nan:
								der_3 = np.append(der_3, np.nan)
								continue
							else:
								section_mean = np.nanmean(section_der_3_neg[critical_points[s3_y]:critical_points[s3_y+1]-1])
								if section_mean.size == 0:
									section_mean = np.nan
								der_3 = np.append(der_3, section_mean)
						

					sec_pct_chg = np.array([])
					for s_p_c_y in range(0, len(critical_points)-1):
						if critical_points[s_p_c_y] <= -9223372036854775 or critical_points[s_p_c_y] == np.nan:
							sec_pct_chg = np.append(sec_pct_chg, np.nan)
							continue
						else:
							if critical_points[s_p_c_y+1] <= -9223372036854775 or critical_points[s_p_c_y+1] == np.nan:
								sec_pct_chg = np.append(sec_pct_chg, np.nan)
								continue
							else:
								if critical_points[s_p_c_y+1]+1 < len(curve_price_hat_neg):
									section_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_p_c_y+1]+1], curve_price_hat_neg[critical_points[s_p_c_y]]), curve_price_hat_neg[critical_points[s_p_c_y]])
									if section_pct_chg.size == 0:
										section_pct_chg = np.nan
									sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
								else:
									section_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_p_c_y+1]], curve_price_hat_neg[critical_points[s_p_c_y]]), curve_price_hat_neg[critical_points[s_p_c_y]])
									if section_pct_chg.size == 0:
										section_pct_chg = np.nan
									sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
					
					sec_curve_pct_chg = np.array([])
					for s_c_p_c_y in range(0, len(critical_points)-1):
						if critical_points[s_c_p_c_y] <= -9223372036854775 or critical_points[s_c_p_c_y] == np.nan:
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.nan)
							continue
						else:
							if critical_points[s_c_p_c_y+1] <= -9223372036854775 or critical_points[s_c_p_c_y+1]+1 == np.nan:
								sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.nan)
								continue
							else:
								if critical_points[s_c_p_c_y+1]+1 < len(curve_price_hat_neg):
									section_curve_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_c_p_c_y+1]+1], curve_price_hat_neg[critical_points[0]]), curve_price_hat_neg[critical_points[0]])
									if section_curve_pct_chg.size == 0:
										section_curve_pct_chg = np.nan
									sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
								else:
									section_curve_pct_chg = np.divide(np.subtract(curve_price_hat_neg[critical_points[s_c_p_c_y+1]], curve_price_hat_neg[critical_points[0]]), curve_price_hat_neg[critical_points[0]])
									if section_curve_pct_chg.size == 0:
										section_curve_pct_chg = np.nan
									sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)



				curve_der = section_der[int(zero_crossings[-2]):]
				if curve_der.size == 0:
					der_1 = np.append(der_1, np.zeros(2) + np.nan)
					der_2 = np.append(der_2, np.zeros(2) + np.nan)
					der_3 = np.append(der_3, np.zeros(2) + np.nan)
					sec_pct_chg = np.append(sec_pct_chg, np.zeros(2) + np.nan)
					sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(2) + np.nan)
					critical_points = np.append(critical_points, np.zeros(2) + np.nan)
				elif curve_der.size > 0:
					curve_der_sec_1 = curve_der
					curve_der_entry = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, 1)))
					curve_der_415 = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, .415)))
					critical_point_10 = len(curve_der_neg)-1 + curve_der_415
					critical_point_11 = len(curve_der_neg)-1 + curve_der_entry
					critical_points = np.append(critical_points, critical_point_10).astype(np.int64)
					critical_points = np.append(critical_points, critical_point_11).astype(np.int64)

					critical_points = critical_points.astype(np.int64)
					curve_price = section_price[int(zero_crossings[-2]):int(zero_crossings[-3])+int(critical_points[-1])+2]
					curve_time = section_time[int(zero_crossings[-2]):int(zero_crossings[-3])+int(critical_points[-1])+2]
					curve_price_hat = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-3])+int(critical_points[-1])+2]
					curve_price_hat_diff_div_diff = np.diff(np.divide(np.diff(curve_price_hat), curve_price_hat[:-1]))
					curve_der = np.divide(curve_price_hat_diff_div_diff, np.diff(curve_time)[:-1])
					curve_time_r = section_time[int(zero_crossings[-3]):int(zero_crossings[-3])+int(critical_points[-1])+2]
					curve_price_r = section_price[int(zero_crossings[-3]):int(zero_crossings[-3])+int(critical_points[-1])+2]
					curve_price_hat_r = section_price_hat[int(zero_crossings[-3]):int(zero_crossings[-3])+int(critical_points[-1])+2]
					curve_vol_r = section_vol[int(zero_crossings[-3]):int(zero_crossings[-3])+int(critical_points[-1])+2]
						
					
					curve_der_diff = np.diff(curve_der)
					section_der_2 = np.divide(curve_der_diff, np.diff(curve_time[:-2]))
					
					

					critical_points_pos = np.array([0, critical_points[-2]-(len(curve_der_neg)-1), critical_points[-1]-(len(curve_der_neg)-1)]).astype(np.int64)
					for p_y in range(0, len(critical_points_pos)-1):
						
						if critical_points_pos[p_y+1]+1 < len(curve_der):
							section_mean = np.nanmean(curve_der[critical_points_pos[p_y]:critical_points_pos[p_y+1]+1])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
						else:
							section_mean = np.nanmean(curve_der[critical_points_pos[p_y]:critical_points_pos[p_y+1]])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
					for p2_y in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_2[critical_points_pos[p2_y]:critical_points_pos[p2_y+1]])
						if section_mean == 0:
							section_mean = np.nan
						der_2 = np.append(der_2, section_mean)

					section_der_2_diff = np.diff(section_der_2)
					section_der_3 = np.divide(section_der_2_diff, np.diff(curve_time[:-3]))	
					
					for p3_y in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_3[critical_points_pos[p3_y]:critical_points_pos[p3_y+1]-1])
						if section_mean == 0:
							section_mean = np.nan
						der_3 = np.append(der_3, np.nan)
					for s_p_c_2_y in range(0, len(critical_points_pos)-1):
						if critical_points_pos[s_p_c_2_y+1]+1 < len(curve_price_hat):
							section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2_y+1]+1], curve_price_hat[critical_points_pos[s_p_c_2_y]]), curve_price_hat[critical_points_pos[s_p_c_2_y]])
							if section_pct_chg.size == 0:
								section_pct_chg = np.nan
							sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
						else:
							section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2_y+1]], curve_price_hat[critical_points_pos[s_p_c_2_y]]), curve_price_hat[critical_points_pos[s_p_c_2_y]])
							if section_pct_chg.size == 0:
								section_pct_chg = np.nan
							sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
					for s_c_p_c_2_y in range(0, len(critical_points_pos)-1):
						if critical_points_pos[s_c_p_c_2_y+1]+1 < len(curve_price_hat):
							section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2_y+1]+1], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
							if section_curve_pct_chg.size == 0:
								section_curve_pct_chg = np.nan
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)
						else:
							section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2_y+1]], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
							if section_curve_pct_chg.size == 0:
								section_curve_pct_chg = np.nan
							sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)

				
				r_sq = np.array([])
				avg_mean_per_section = np.array([])
				std_per_section = np.array([])
				residual_max_per_section = np.array([])
				residual_mean_per_section = np.array([])
				zscore_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					r_sq = np.append(r_sq, np.zeros(10) + np.nan)
					avg_mean_per_section = np.append(avg_mean_per_section, np.zeros(10) + np.nan)
					std_per_section = np.append(std_per_section, np.zeros(10) + np.nan)
					residual_max_per_section = np.append(residual_max_per_section, np.zeros(10) + np.nan)
					residual_mean_per_section = np.append(residual_mean_per_section, np.zeros(10) + np.nan)
					zscore_per_section = np.append(zscore_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for r_y in range(0, len(critical_points)-1):
						if critical_points[r_y] <= -9223372036854775 or critical_points[r_y] == np.nan:
							r_sq = np.append(r_sq, np.nan)
							avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
							std_per_section = np.append(std_per_section, np.nan)
							residual_max_per_section = np.append(residual_max_per_section, np.nan)
							residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
							zscore_per_section = np.append(zscore_per_section, np.nan)
							continue
						else:
							if critical_points[r_y+1] <= -9223372036854775 or critical_points[r_y+1] == np.nan:
								r_sq = np.append(r_sq, np.nan)
								avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
								std_per_section = np.append(std_per_section, np.nan)
								residual_max_per_section = np.append(residual_max_per_section, np.nan)
								residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
								zscore_per_section = np.append(zscore_per_section, np.nan)
								continue
							else:
								 
								if critical_points[r_y+1] == critical_points[r_y]:
									if r_sq.size == 0 or avg_mean_per_section.size == 0 or std_per_section.size == 0 or residual_max_per_section.size == 0 or residual_mean_per_section.size == 0 or zscore_per_section.size == 0:
										r_sq = np.append(r_sq, np.nan)
										avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
										std_per_section = np.append(std_per_section, np.nan)
										residual_max_per_section = np.append(residual_max_per_section, np.nan)
										residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
										zscore_per_section = np.append(zscore_per_section, np.nan)
									else:	
										r_y += 1
										r_sq = np.append(r_sq, r_sq[-1])
										avg_mean_per_section = np.append(avg_mean_per_section, avg_mean_per_section[-1])
										std_per_section = np.append(std_per_section, std_per_section[-1])
										residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section[-1])
										residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section[-1])
										zscore_per_section = np.append(zscore_per_section, zscore_per_section[-1])
										continue
								else:
									r_sq_2 = model.score(curve_time_r[critical_points[r_y]:critical_points[r_y+1]+2], curve_price_r[critical_points[r_y]:critical_points[r_y+1]+2])
									r_sq = np.append(r_sq, r_sq_2)
									avg_mean = np.nanmean(curve_price_r[critical_points[r_y]:critical_points[r_y+1]+2])
									avg_mean_per_section = np.append(avg_mean_per_section, avg_mean)
									std_sec = np.nanstd(curve_price_r[critical_points[r_y]:critical_points[r_y+1]+2])
									std_per_section = np.append(std_per_section, std_sec)
									zscore_per_section_0 = np.nanmean(zscore(curve_price_r[critical_points[r_y]:critical_points[r_y+1]+2]))
									zscore_per_section = np.append(zscore_per_section, zscore_per_section_0)
									curvy = curve_price_hat_r[critical_points[r_y]:critical_points[r_y+1]+2]
									residual_max_per_section_0_0_0 = np.abs(np.subtract(curve_price_r[critical_points[r_y]:critical_points[r_y+1]+2], curve_price_hat_r[critical_points[r_y]:critical_points[r_y+1]+2]))
									residual_max_per_section_0_0 = np.nanargmax(residual_max_per_section_0_0_0)
									residual_max_per_section_0 = np.take(residual_max_per_section_0_0_0, residual_max_per_section_0_0)
									residual_mean_per_section_0 = np.nanmean(np.abs(np.subtract(curve_price_r[critical_points[r_y]:critical_points[r_y+1]+2], curve_price_hat_r[critical_points[r_y]:critical_points[r_y+1]+2])))
									residual_max_per_section_0_ph = np.divide(residual_max_per_section_0, np.take(curvy, residual_max_per_section_0_0))
									residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section_0_ph)
									residual_mean_per_section_0_ph = np.nanmean(curvy)
									residual_mean_per_section_div = np.divide(residual_mean_per_section_0, residual_mean_per_section_0_ph)
									residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section_div)

				
				r_sq = r_sq.astype(np.float64)

				time_since_maxima = np.array([])
				time_since_last_sect = np.array([])
				if curve_price_hat_r.size == 0:
					time_since_maxima = np.append(time_since_maxima, np.zeros(10) + np.nan)
					time_since_last_sect = np.append(time_since_last_sect, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for c_y in range(0, len(critical_points)-1):
						if critical_points[c_y] <= -9223372036854775 or critical_points[c_y] == np.nan:
							time_since_maxima = np.append(time_since_maxima, np.nan)
							time_since_last_sect = np.append(time_since_last_sect, np.nan)
							continue
						else:
							if critical_points[c_y+1] <= -9223372036854775 or critical_points[c_y+1] == np.nan:
								time_since_maxima = np.append(time_since_maxima, np.nan)
								time_since_last_sect = np.append(time_since_last_sect, np.nan)
								continue
							else:
								if critical_points[c_y+1] == critical_points[c_y]:
									c_y += 1
									time_since_maxima = np.append(time_since_maxima, np.zeros(1))
									time_since_last_sect = np.append(time_since_last_sect, np.zeros(1))
									continue
								else:
									if critical_points[0] <= -9223372036854775 or critical_points[0] == np.nan:
										time_since_maxima_append = np.nan
									else:
										if critical_points[c_y+1]+1 < len(curve_time_r)-1:
											time_since_maxima_append = curve_time_r[critical_points[c_y+1]+1] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[c_y+1]+1] - curve_time_r[critical_points[c_y]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
											
										else:
											time_since_maxima_append = curve_time_r[critical_points[c_y+1]] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[c_y+1]] - curve_time_r[critical_points[c_y]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)


				residual_max_2 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
				residual_max_1 = np.nanargmax(residual_max_2)
				residual_max_0 = np.take(residual_max_2, residual_max_1)
				residual_max_0_ph = np.take(curve_price_hat_r, residual_max_1)
				residual_max = np.divide(residual_max_0, residual_max_0_ph)
				residual_mean_0 = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))
				residual_mean = np.divide(residual_mean_0, np.nanmean(curve_price_hat_r))

				if critical_points.size > 0 and curve_price_r.size > 0:
					current_price = curve_price_r[-1]
				else:
					current_price = np.nan

				##added here
				if critical_points.size > 0 and curve_price_hat_r.size > 0:
					current_price_hat_r = curve_price_hat_r[-1]
				else:
					current_price_hat_r = np.nan

				if curve_price_r.size > 0:
					avg_Price = np.nanmean(curve_price_r)
				else:
					avg_Price = np.nan
				current_date = datetime(1970, 1, 1) + timedelta(seconds=np.multiply(curve_time[-1],1000000000))
				current_date = current_date.strftime('%Y-%m-%d')
				open_ts = datetime.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')
				dt = pytz.timezone('America/Chicago').normalize(open_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
				is_dst = bool(dt.dst())
				if is_dst is True:
					offset_open = int(time.mktime(time.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')))
				elif is_dst is False:
					offset_open = int(time.mktime(time.strptime(current_date + ' 9:00:00', '%Y-%m-%d %H:%M:%S')))
				if critical_points.size > 0 and curve_time_r.size > 0:
					current_unix_time = np.multiply(curve_time_r[-1],1000000000)
					ms_since_open = np.multiply(curve_time_r[-1],1000000000) - offset_open
				else:
					current_unix_time = np.nan
					ms_since_open = np.nan

				current_date = datetime.strptime(current_date, '%Y-%m-%d')

				current_year = current_date.isocalendar()[0]
				current_month = current_date.month

				first_day = current_date.replace(day=1)
				dom = current_date.day
				adjusted_dom = dom + first_day.weekday()
				current_week = int(ceil(adjusted_dom/7.0))

				current_weekday = current_date.isocalendar()[2]

				if ms_since_open <= 1800:
					current_30_mins = 1
				elif ms_since_open > 1800 and ms_since_open <= 1800*2:
					current_30_mins = 2
				elif ms_since_open > 1800*2 and ms_since_open <= 1800*3:
					current_30_mins = 3
				elif ms_since_open > 1800*3 and ms_since_open <= 1800*4:
					current_30_mins = 4
				elif ms_since_open > 1800*4 and ms_since_open <= 1800*5:
					current_30_mins = 5
				else:
					current_30_mins = np.nan


				total_years = int((current_date - datetime(1970,1,1)).days/365.24)
				total_months = int(((current_date - datetime(1970,1,1)).days/365.24)*12.0)
				total_weeks = int((current_date - datetime(1970,1,1)).days/7.0)
				total_days = int((current_date - datetime(1970,1,1)).days)
				total_30_mins = int(current_unix_time/1800)




				price_pcavg_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for avg_p_n_y in range(0, len(critical_points)-1):
						if critical_points[avg_p_n_y] <= -9223372036854775 or critical_points[avg_p_n_y] == np.nan:
							price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_n_y+1] <= -9223372036854775 or critical_points[avg_p_n_y+1] == np.nan:
								price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_n_y+1] == critical_points[avg_p_n_y]:
									avg_p_n_y += 1
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(1))
									continue
								else:
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.nanmean(np.diff(curve_price_hat_r[critical_points[avg_p_n_y]:critical_points[avg_p_n_y+1]+2])/curve_price_hat_r[critical_points[avg_p_n_y]:critical_points[avg_p_n_y+1]+2][:-1]))


				price_pct_chg_neg = np.divide(np.subtract(curve_price_neg[-1], curve_price_neg[0]), curve_price_neg[0])
				price_pct_chg_pos = np.divide(np.subtract(curve_price[-1], curve_price[0]), curve_price[0])


				lm = linear_model.LinearRegression()





				X = pd.DataFrame(curve_time_neg)
				y = pd.DataFrame(curve_price_neg)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				lr_curve_neg_hat = np.array(lm.predict(X)).flatten()
				lr_price_total_pct_chg_neg = np.divide(np.subtract(lr_curve_neg_hat[-1], lr_curve_neg_hat[0]), lr_curve_neg_hat[0])
				lr_price_avg_roc_neg = np.nanmean((np.diff(lr_curve_neg_hat)/lr_curve_neg_hat[:-1])/np.diff(curve_time_neg))
				lr_price_r2_neg = lm.score(X,y)




				X = pd.DataFrame(curve_time)
				y = pd.DataFrame(curve_price)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				lr_curve_pos_hat = np.array(lm.predict(X)).flatten()
				lr_price_total_pct_chg_pos = np.divide(np.subtract(lr_curve_pos_hat[-1], lr_curve_pos_hat[0]), lr_curve_pos_hat[0])
				lr_price_avg_roc_pos = np.nanmean((np.diff(lr_curve_pos_hat)/lr_curve_pos_hat[:-1])/np.diff(curve_time))
				lr_price_r2_pos = lm.score(X,y)





				lr_price_pct_chg_per_section = np.array([])
				lr_price_roc_per_section = np.array([])
				lr_price_r2_per_section = np.array([])
				if curve_price_r.size == 0:
					lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(10) + np.nan)
					lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(10) + np.nan)
				elif curve_price_r.size > 0:
					for avg_p_y in range(0, len(critical_points)-1):
						if critical_points[avg_p_y] <= -9223372036854775 or critical_points[avg_p_y] == np.nan:
							lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
							lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
							lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_y+1] <= -9223372036854775 or critical_points[avg_p_y+1] == np.nan:
								lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
								lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
								lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_y+1] == critical_points[avg_p_y]:
									avg_p_y += 1
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(1))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(1))
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[avg_p_y]:critical_points[avg_p_y+1]+2])
									y = pd.DataFrame(curve_price_r[critical_points[avg_p_y]:critical_points[avg_p_y+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.divide(np.subtract(lr_price_per_section_hat[-1], lr_price_per_section_hat[0]), lr_price_per_section_hat[0]))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nanmean((np.diff(lr_price_per_section_hat)/lr_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[avg_p_y]:critical_points[avg_p_y+1]+2])))
									lr_price_r2_per_section_2 = lm.score(X,y)
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, lr_price_r2_per_section_2)






				lr_vol_pct_chg_per_section = np.array([])
				lr_vol_roc_per_section = np.array([])
				lr_vol_r2_per_section = np.array([])
				if curve_vol_r.size == 0:
					lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(10) + np.nan)
					lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(10) + np.nan)
				elif curve_vol_r.size > 0:
					for v_y in range(0, len(critical_points)-1):
						if critical_points[v_y] <= -9223372036854775 or critical_points[v_y] == np.nan:
							lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
							lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
							lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
							continue
						else:
							if critical_points[v_y+1] <= -9223372036854775 or critical_points[v_y+1] == np.nan:
								lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
								lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
								lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
								continue
							else:
								if critical_points[v_y+1] == critical_points[v_y]:
									v_y += 1
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(1))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(1))
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[v_y]:critical_points[v_y+1]+2])
									y = pd.DataFrame(curve_vol_r[critical_points[v_y]:critical_points[v_y+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_vol_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.divide(np.subtract(lr_vol_per_section_hat[-1], lr_vol_per_section_hat[0]), lr_vol_per_section_hat[0]))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nanmean((np.diff(lr_vol_per_section_hat)/lr_vol_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[v_y]:critical_points[v_y+1]+2])))
									lr_vol_r2_per_section_2 = lm.score(X,y)
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, lr_vol_r2_per_section_2)



				section_time_key_neg = curve_time_neg
				section_vol_key_neg = section_vol[int(zero_crossings[-3]):int(zero_crossings[-2])+2]

				X = pd.DataFrame(section_time_key_neg)
				y = pd.DataFrame(section_vol_key_neg)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_vol_neg_hat = np.array(lm.predict(X)).flatten()
				vol_total_pct_chg_neg = np.divide(np.subtract(section_vol_neg_hat[-1], section_vol_neg_hat[0]), section_vol_neg_hat[0])
				vol_avg_roc_neg = np.nanmean((np.diff(section_vol_neg_hat)/section_vol_neg_hat[:-1])/np.diff(section_time_key_neg))
				vol_r2_neg = lm.score(X,y)




				section_time_key_pos = curve_time_r[int(critical_points[-3]):int(critical_points[-1]+2)]
				section_vol_key_pos = curve_vol_r[int(critical_points[-3]):int(critical_points[-1]+2)]

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_vol_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_vol_pos_hat = np.array(lm.predict(X)).flatten()
				vol_total_pct_chg_pos = np.divide(np.subtract(section_vol_pos_hat[-1], section_vol_pos_hat[0]), section_vol_pos_hat[0])
				vol_avg_roc_pos = np.nanmean((np.diff(section_vol_pos_hat)/section_vol_pos_hat[:-1])/np.diff(section_time_key_pos))
				vol_r2_pos = lm.score(X,y)




				section_mf_key_total = np.multiply(curve_price_r, curve_vol_r)

				lr_mf_price_pct_chg_per_section = np.array([])
				lr_mf_price_roc_per_section = np.array([])
				lr_mf_price_r2_per_section = np.array([])
				if section_mf_key_total.size == 0:
					lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(10) + np.nan)
					lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(10) + np.nan)
				elif section_mf_key_total.size > 0:
					for v2_y in range(0, len(critical_points)-1):
						if critical_points[v2_y] <= -9223372036854775 or critical_points[v2_y] == np.nan:
							lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
							lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
							lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[v2_y+1] <= -9223372036854775 or critical_points[v2_y+1] == np.nan:
								lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
								lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
								lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[v2_y+1] == critical_points[v2_y]:
									v2_y += 1
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(1))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(1))
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[v2_y]:critical_points[v2_y+1]+2])
									y = pd.DataFrame(section_mf_key_total[critical_points[v2_y]:critical_points[v2_y+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_mf_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.divide(np.subtract(lr_mf_price_per_section_hat[-1], lr_mf_price_per_section_hat[0]), lr_mf_price_per_section_hat[0]))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nanmean((np.diff(lr_mf_price_per_section_hat)/lr_mf_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[v2_y]:critical_points[v2_y+1]+2])))
									lr_mf_price_r2_per_section_2 = lm.score(X,y)
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, lr_mf_price_r2_per_section_2)




				section_time_key_neg = curve_time_neg
				section_mf_price_key_neg = np.multiply(curve_price_neg, section_vol_key_neg)

				X = pd.DataFrame(section_time_key_neg)
				y = pd.DataFrame(section_mf_price_key_neg)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_mf_price_neg_hat = np.array(lm.predict(X)).flatten()
				mf_price_total_pct_chg_neg = np.divide(np.subtract(section_mf_price_neg_hat[-1], section_mf_price_neg_hat[0]), section_mf_price_neg_hat[0])
				mf_price_avg_roc_neg = np.nanmean((np.diff(section_mf_price_neg_hat)/section_mf_price_neg_hat[:-1])/np.diff(section_time_key_neg))
				mf_price_r2_neg = lm.score(X,y)


				section_time_key_pos = curve_time
				section_mf_price_key_pos = np.multiply(curve_price, section_vol_key_pos)

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_mf_price_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_mf_price_pos_hat = np.array(lm.predict(X)).flatten()
				mf_price_total_pct_chg_pos = np.divide(np.subtract(section_mf_price_pos_hat[-1], section_mf_price_pos_hat[0]), section_mf_price_pos_hat[0])
				mf_price_avg_roc_pos = np.nanmean((np.diff(section_mf_price_pos_hat)/section_mf_price_pos_hat[:-1])/np.diff(section_time_key_pos))
				mf_price_r2_pos = lm.score(X,y)



				avg_vol = np.nanmean(curve_vol_r)
				


				quote_key_times_append = np.array([])
				quote_key_times = np.array([])
				for cp_y in range(0, len(critical_points)):
					if critical_points[cp_y] <= -9223372036854775 or critical_points[cp_y] == np.nan:
						quote_key_times = np.append(quote_key_times, np.nan)
						continue
					else:
						if critical_points[cp_y]+1 < len(curve_time_r) and cp_y == 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp_y])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cp_y]+1 < len(curve_time_r) and cp_y > 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp_y]+1)], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cp_y]+1 >= len(curve_time_r):
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp_y])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)





				final_output.update(dict(ratio_ewm_50_25 = ratio_ewm_50_25))
				final_output.update(dict(ratio_ewm_150_arr = ratio_ewm_150_arr))
				final_output.update(dict(ratio_ewm_67_arr = ratio_ewm_67_arr))
				final_output.update(dict(ratio_ewm_67_150_arr = ratio_ewm_67_150_arr))
				final_output.update(dict(pct_ewm_50_25 = pct_ewm_50_25))
				final_output.update(dict(pct_ewm_150_arr = pct_ewm_150_arr))
				final_output.update(dict(pct_ewm_67_arr = pct_ewm_67_arr))
				final_output.update(dict(pct_ewm_67_150_arr = pct_ewm_67_150_arr))
				final_output.update(dict(avg_return_40_mins = avg_return_40_mins)) 
				final_output.update(dict(avg_chg_in_avg_return = avg_chg_in_avg_return)) 
				final_output.update(dict(return_minus_5 = return_minus_5)) 
				final_output.update(dict(return_minus_4 = return_minus_4)) 
				final_output.update(dict(return_minus_3 = return_minus_3)) 
				final_output.update(dict(return_minus_2 = return_minus_2)) 
				final_output.update(dict(lr_all_day_time_passed = lr_all_day_time_passed)) 
				final_output.update(dict(lr_all_day_pct_chg = lr_all_day_pct_chg))
				final_output.update(dict(lr_all_day_roc = lr_all_day_roc)) 
				final_output.update(dict(lr_all_day_r2 = lr_all_day_r2)) 
				final_output.update(dict(zip(['sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1'], der_1.T)))
				final_output.update(dict(zip(['sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2'], der_2.T)))
				final_output.update(dict(zip(['sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3'], der_3.T)))
				final_output.update(dict(zip(['sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq'], r_sq.T)))   
				final_output.update(dict(zip(['sec_1_avg_mean_per_section', 'sec_2_avg_mean_per_section', 'sec_3_avg_mean_per_section', 'sec_4_avg_mean_per_section', 'sec_5_avg_mean_per_section', 'sec_6_avg_mean_per_section', 'sec_7_avg_mean_per_section', 'sec_8_avg_mean_per_section', 'sec_9_avg_mean_per_section', 'sec_10_avg_mean_per_section'], avg_mean_per_section.T))) 
				final_output.update(dict(zip(['sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section'], std_per_section.T))) 
				final_output.update(dict(zip(['sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section'], residual_max_per_section.T))) 
				final_output.update(dict(zip(['sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section'], residual_mean_per_section.T))) 
				final_output.update(dict(zip(['sec_1_zscore_per_section', 'sec_2_zscore_per_section', 'sec_3_zscore_per_section', 'sec_4_zscore_per_section', 'sec_5_zscore_per_section', 'sec_6_zscore_per_section', 'sec_7_zscore_per_section', 'sec_8_zscore_per_section', 'sec_9_zscore_per_section', 'sec_10_zscore_per_section'], zscore_per_section.T))) 
				final_output.update(dict(zip(['sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg'], sec_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg'], sec_curve_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima'], time_since_maxima.T)))
				final_output.update(dict(zip(['sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect'], time_since_last_sect.T))) 
				final_output.update(dict(sec_1_residual_max = residual_max))
				final_output.update(dict(sec_1_residual_mean = residual_mean))
				final_output.update(dict(sec_1_current_unix_time = current_unix_time)) 
				final_output.update(dict(sec_1_ms_since_open = ms_since_open)) 
				final_output.update(dict(current_year = current_year)) 
				final_output.update(dict(current_month = current_month)) 
				final_output.update(dict(current_week = current_week)) 
				final_output.update(dict(current_weekday = current_weekday)) 
				final_output.update(dict(current_30_mins = current_30_mins)) 
				final_output.update(dict(total_years = total_years)) 
				final_output.update(dict(total_months = total_months)) 
				final_output.update(dict(total_weeks = total_weeks)) 
				final_output.update(dict(total_days = total_days)) 
				final_output.update(dict(total_30_mins = total_30_mins)) 
				final_output.update(dict(sec_1_current_price = current_price)) 
				final_output.update(dict(sec_1_current_price_hat_r = current_price_hat_r)) 	
				final_output.update(dict(sec_1_avg_price = avg_Price)) 
				final_output.update(dict(zip(['sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section'], price_pcavg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section'], lr_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section'], lr_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section'], lr_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_price_pct_chg_neg = price_pct_chg_neg)) 
				final_output.update(dict(sec_1_price_pct_chg_pos = price_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_neg = lr_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_neg = lr_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_lr_price_r2_neg = lr_price_r2_neg)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_pos = lr_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_pos = lr_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_lr_price_r2_pos = lr_price_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_vol_pct_chg_per_section', 'sec_2_lr_vol_pct_chg_per_section', 'sec_3_lr_vol_pct_chg_per_section', 'sec_4_lr_vol_pct_chg_per_section', 'sec_5_lr_vol_pct_chg_per_section', 'sec_6_lr_vol_pct_chg_per_section', 'sec_7_lr_vol_pct_chg_per_section', 'sec_8_lr_vol_pct_chg_per_section', 'sec_9_lr_vol_pct_chg_per_section', 'sec_10_lr_vol_pct_chg_per_section'], lr_vol_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_roc_per_section', 'sec_2_lr_vol_roc_per_section', 'sec_3_lr_vol_roc_per_section', 'sec_4_lr_vol_roc_per_section', 'sec_5_lr_vol_roc_per_section', 'sec_6_lr_vol_roc_per_section', 'sec_7_lr_vol_roc_per_section', 'sec_8_lr_vol_roc_per_section', 'sec_9_lr_vol_roc_per_section', 'sec_10_lr_vol_roc_per_section'], lr_vol_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_r2_per_section', 'sec_2_lr_vol_r2_per_section', 'sec_3_lr_vol_r2_per_section', 'sec_4_lr_vol_r2_per_section', 'sec_5_lr_vol_r2_per_section', 'sec_6_lr_vol_r2_per_section', 'sec_7_lr_vol_r2_per_section', 'sec_8_lr_vol_r2_per_section', 'sec_9_lr_vol_r2_per_section', 'sec_10_lr_vol_r2_per_section'], lr_vol_r2_per_section.T))) 
				final_output.update(dict(sec_1_vol_total_pct_chg_neg = vol_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_vol_avg_roc_neg = vol_avg_roc_neg)) 
				final_output.update(dict(sec_1_vol_total_pct_chg_pos = vol_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_vol_avg_roc_pos = vol_avg_roc_pos)) 
				final_output.update(dict(sec_1_vol_r2_neg = vol_r2_neg)) 
				final_output.update(dict(sec_1_vol_r2_pos = vol_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_pct_chg_per_section', 'sec_2_lr_mf_price_pct_chg_per_section', 'sec_3_lr_mf_price_pct_chg_per_section', 'sec_4_lr_mf_price_pct_chg_per_section', 'sec_5_lr_mf_price_pct_chg_per_section', 'sec_6_lr_mf_price_pct_chg_per_section', 'sec_7_lr_mf_price_pct_chg_per_section', 'sec_8_lr_mf_price_pct_chg_per_section', 'sec_9_lr_mf_price_pct_chg_per_section', 'sec_10_lr_mf_price_pct_chg_per_section'], lr_mf_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_roc_per_section', 'sec_2_lr_mf_price_roc_per_section', 'sec_3_lr_mf_price_roc_per_section', 'sec_4_lr_mf_price_roc_per_section', 'sec_5_lr_mf_price_roc_per_section', 'sec_6_lr_mf_price_roc_per_section', 'sec_7_lr_mf_price_roc_per_section', 'sec_8_lr_mf_price_roc_per_section', 'sec_9_lr_mf_price_roc_per_section', 'sec_10_lr_mf_price_roc_per_section'], lr_mf_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_r2_per_section', 'sec_2_lr_mf_price_r2_per_section', 'sec_3_lr_mf_price_r2_per_section', 'sec_4_lr_mf_price_r2_per_section', 'sec_5_lr_mf_price_r2_per_section', 'sec_6_lr_mf_price_r2_per_section', 'sec_7_lr_mf_price_r2_per_section', 'sec_8_lr_mf_price_r2_per_section', 'sec_9_lr_mf_price_r2_per_section', 'sec_10_lr_mf_price_r2_per_section'], lr_mf_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_neg = mf_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_neg = mf_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_mf_price_r2_neg = mf_price_r2_neg)) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_pos = mf_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_pos = mf_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_mf_price_r2_pos = mf_price_r2_pos)) 
				final_output.update(dict(sec_1_avg_vol = avg_vol)) 







						
			elif zero_crossings.size == 2:
				critical_points = np.array([])
				sec_pct_chg = np.array([])
				sec_curve_pct_chg = np.array([])
				der_1 = np.array([])
				der_1 = np.append(der_1, np.zeros(8) + np.nan)
				der_2 = np.array([])
				der_2 = np.append(der_2, np.zeros(8) + np.nan)
				der_3 = np.array([])
				der_3 = np.append(der_3, np.zeros(8) + np.nan)
				sec_pct_chg = np.append(sec_pct_chg, np.zeros(8) + np.nan)
				sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(8) + np.nan)
				critical_points = np.append(critical_points, np.zeros(8) + np.nan)
				curve_der = section_der[int(zero_crossings[-2]):]
				if curve_der.size == 0:
					der_1 = np.append(der_1, np.zeros(2) + np.nan)
					der_2 = np.append(der_2, np.zeros(2) + np.nan)
					der_3 = np.append(der_3, np.zeros(2) + np.nan)
					sec_pct_chg = np.append(sec_pct_chg, np.zeros(2) + np.nan)
					sec_curve_pct_chg = np.append(sec_curve_pct_chg, np.zeros(2) + np.nan)
					critical_points = np.append(critical_points, np.zeros(3) + np.nan)
				elif curve_der.size > 0:
					curve_der_sec_1 = curve_der
					curve_der_entry = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, 1)))
					curve_der_415 = np.nanargmin(np.abs(np.subtract(curve_der_sec_1, .415)))
					critical_point_10 = curve_der_415
					critical_point_11 = curve_der_entry
					critical_points = np.append(critical_points, 0)
					critical_points = np.append(critical_points, critical_point_10).astype(np.int64)
					critical_points = np.append(critical_points, critical_point_11).astype(np.int64)
					
					
					critical_points = critical_points.astype(np.int64)

				
					curve_price = section_price[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_time = section_time[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_hat = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_hat_diff_div_diff = np.diff(np.divide(np.diff(curve_price_hat), curve_price_hat[:-1])) 
					curve_der = np.divide(curve_price_hat_diff_div_diff, np.diff(curve_time)[:-1])
					curve_time_r = section_time[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_r = section_price[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_price_hat_r = section_price_hat[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					curve_vol_r = section_vol[int(zero_crossings[-2]):int(zero_crossings[-2])+int(critical_points[-1])+2]
					 
					curve_der_diff = np.diff(curve_der)
					section_der_2 = np.divide(curve_der_diff, np.diff(curve_time[:-2]))
					
					critical_points_pos = np.array([critical_points[-3], critical_points[-2], critical_points[-1]]).astype(np.int64)
					for p_2 in range(0, len(critical_points_pos)-1):
						
						if critical_points_pos[p_2+1]+1 < len(curve_der):
							section_mean = np.nanmean(curve_der[critical_points_pos[p_2]:critical_points_pos[p_2+1]+1])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
						else:
							section_mean = np.nanmean(curve_der[critical_points_pos[p_2]:critical_points_pos[p_2+1]])
							if section_mean.size == 0:
								section_mean = np.nan
							der_1 = np.append(der_1, section_mean)
					for p2_2 in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_2[critical_points_pos[p2_2]:critical_points_pos[p2_2+1]])
						if section_mean == 0:
							section_mean = np.nan
						der_2 = np.append(der_2, section_mean)

					section_der_2_diff = np.diff(section_der_2)
					section_der_3 = np.divide(section_der_2_diff, np.diff(curve_time[:-3]))
					for p3_2 in range(0, len(critical_points_pos)-1):
						section_mean = np.nanmean(section_der_3[critical_points_pos[p3_2]:critical_points_pos[p3_2+1]-1])
						if section_mean == 0:
							section_mean = np.nan
						der_3 = np.append(der_3, np.nan)
					for s_p_c_2_2 in range(0, len(critical_points_pos)-1):
						section_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_p_c_2_2+1]+1], curve_price_hat[critical_points_pos[s_p_c_2_2]]), curve_price_hat[critical_points_pos[s_p_c_2_2]])
						if section_pct_chg.size == 0:
							section_pct_chg = np.nan
						sec_pct_chg = np.append(sec_pct_chg, section_pct_chg)
					for s_c_p_c_2_2 in range(0, len(critical_points_pos)-1):
						section_curve_pct_chg = np.divide(np.subtract(curve_price_hat[critical_points_pos[s_c_p_c_2_2+1]+1], curve_price_hat[critical_points_pos[0]]), curve_price_hat[critical_points_pos[0]])
						if section_curve_pct_chg.size == 0:
							section_curve_pct_chg = np.nan
						sec_curve_pct_chg = np.append(sec_curve_pct_chg, section_curve_pct_chg)


				r_sq = np.array([])
				avg_mean_per_section = np.array([])
				std_per_section = np.array([])
				residual_max_per_section = np.array([])
				residual_mean_per_section = np.array([])
				zscore_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					r_sq = np.append(r_sq, np.zeros(10) + np.nan)
					avg_mean_per_section = np.append(avg_mean_per_section, np.zeros(10) + np.nan)
					std_per_section = np.append(std_per_section, np.zeros(10) + np.nan)
					residual_max_per_section = np.append(residual_max_per_section, np.zeros(10) + np.nan)
					residual_mean_per_section = np.append(residual_mean_per_section, np.zeros(10) + np.nan)
					zscore_per_section = np.append(zscore_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for r_2 in range(0, len(critical_points)-1):
						if critical_points[r_2] <= -9223372036854775 or critical_points[r_2] == np.nan:
							r_sq = np.append(r_sq, np.nan)
							avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
							std_per_section = np.append(std_per_section, np.nan)
							residual_max_per_section = np.append(residual_max_per_section, np.nan)
							residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
							zscore_per_section = np.append(zscore_per_section, np.nan)
							continue
						else:
							if critical_points[r_2+1] <= -9223372036854775 or critical_points[r_2+1] == np.nan:
								r_sq = np.append(r_sq, np.nan)
								avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
								std_per_section = np.append(std_per_section, np.nan)
								residual_max_per_section = np.append(residual_max_per_section, np.nan)
								residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
								zscore_per_section = np.append(zscore_per_section, np.nan)
								continue
							else:
								
								if critical_points[r_2+1] == critical_points[r_2]:
									if r_sq.size == 0 or avg_mean_per_section.size == 0 or std_per_section.size == 0 or residual_max_per_section.size == 0 or residual_mean_per_section.size == 0 or zscore_per_section.size == 0:
										r_sq = np.append(r_sq, np.nan)
										avg_mean_per_section = np.append(avg_mean_per_section, np.nan)
										std_per_section = np.append(std_per_section, np.nan)
										residual_max_per_section = np.append(residual_max_per_section, np.nan)
										residual_mean_per_section = np.append(residual_mean_per_section, np.nan)
										zscore_per_section = np.append(zscore_per_section, np.nan)
									else:	
										r_2 += 1
										r_sq = np.append(r_sq, r_sq[-1])
										avg_mean_per_section = np.append(avg_mean_per_section, avg_mean_per_section[-1])
										std_per_section = np.append(std_per_section, std_per_section[-1])
										residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section[-1])
										residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section[-1])
										zscore_per_section = np.append(zscore_per_section, zscore_per_section[-1])
										continue
								else:
									r_sq_2 = model.score(curve_time_r[critical_points[r_2]:critical_points[r_2+1]+2], curve_price_r[critical_points[r_2]:critical_points[r_2+1]+2])
									r_sq = np.append(r_sq, r_sq_2)
									avg_mean = np.nanmean(curve_price_r[critical_points[r_2]:critical_points[r_2+1]+2])
									avg_mean_per_section = np.append(avg_mean_per_section, avg_mean)
									std_sec = np.nanstd(curve_price_r[critical_points[r_2]:critical_points[r_2+1]+2])
									std_per_section = np.append(std_per_section, std_sec)
									zscore_per_section_0 = np.nanmean(zscore(curve_price_r[critical_points[r_2]:critical_points[r_2+1]+2]))
									zscore_per_section = np.append(zscore_per_section, zscore_per_section_0)
									curvy = curve_price_hat_r[critical_points[r_2]:critical_points[r_2+1]+2]
									residual_max_per_section_0_0_0 = np.abs(np.subtract(curve_price_r[critical_points[r_2]:critical_points[r_2+1]+2], curve_price_hat_r[critical_points[r_2]:critical_points[r_2+1]+2]))
									residual_max_per_section_0_0 = np.nanargmax(residual_max_per_section_0_0_0)
									residual_max_per_section_0 = np.take(residual_max_per_section_0_0_0, residual_max_per_section_0_0)
									residual_mean_per_section_0 = np.nanmean(np.abs(np.subtract(curve_price_r[critical_points[r_2]:critical_points[r_2+1]+2], curve_price_hat_r[critical_points[r_2]:critical_points[r_2+1]+2])))
									residual_max_per_section_0_ph = np.divide(residual_max_per_section_0, np.take(curvy, residual_max_per_section_0_0))
									residual_max_per_section = np.append(residual_max_per_section, residual_max_per_section_0_ph)
									residual_mean_per_section_0_ph = np.nanmean(curvy)
									residual_mean_per_section_div = np.divide(residual_mean_per_section_0, residual_mean_per_section_0_ph)
									residual_mean_per_section = np.append(residual_mean_per_section, residual_mean_per_section_div)


				r_sq = r_sq.astype(np.float64)

				time_since_maxima = np.array([])
				time_since_last_sect = np.array([])
				if curve_price_hat_r.size == 0:
					time_since_maxima = np.append(time_since_maxima, np.zeros(10) + np.nan)
					time_since_last_sect = np.append(time_since_last_sect, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for c_2 in range(0, len(critical_points)-1):
						if critical_points[c_2] <= -9223372036854775 or critical_points[c_2] == np.nan:
							time_since_maxima = np.append(time_since_maxima, np.nan)
							time_since_last_sect = np.append(time_since_last_sect, np.nan)
							continue
						else:
							if critical_points[c_2+1] <= -9223372036854775 or critical_points[c_2+1] == np.nan:
								time_since_maxima = np.append(time_since_maxima, np.nan)
								time_since_last_sect = np.append(time_since_last_sect, np.nan)
								continue
							else:
								if critical_points[c_2+1] == critical_points[c_2]:
									c_2 += 1
									time_since_maxima = np.append(time_since_maxima, np.zeros(1))
									time_since_last_sect = np.append(time_since_last_sect, np.zeros(1))
									continue
								else:
									if critical_points[0] <= -9223372036854775 or critical_points[0] == np.nan:
										time_since_maxima_append = np.nan
									else:
										if critical_points[c_2+1]+1 < len(curve_time_r)-1:
											time_since_maxima_append = curve_time_r[critical_points[c_2+1]+1] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[c_2+1]+1] - curve_time_r[critical_points[c_2]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
										else:
											time_since_maxima_append = curve_time_r[critical_points[c_2+1]] - curve_time_r[critical_points[0]]
											if time_since_maxima_append.size == 0:
												time_since_maxima_append = np.nan
											time_since_maxima = np.append(time_since_maxima, time_since_maxima_append)
											time_since_last_sect_append = curve_time_r[critical_points[c_2+1]] - curve_time_r[critical_points[c_2]]
											time_since_last_sect = np.append(time_since_last_sect, time_since_last_sect_append)
									

				residual_max_2 = np.abs(np.subtract(curve_price_r, curve_price_hat_r))
				residual_max_1 = np.nanargmax(residual_max_2)
				residual_max_0 = np.take(residual_max_2, residual_max_1)
				residual_max_0_ph = np.take(curve_price_hat_r, residual_max_1)
				residual_max = np.divide(residual_max_0, residual_max_0_ph)
				residual_mean_0 = np.nanmean(np.abs(np.subtract(curve_price_r, curve_price_hat_r)))
				residual_mean = np.divide(residual_mean_0, np.nanmean(curve_price_hat_r))



				if critical_points.size > 0 and curve_price_r.size > 0:
					current_price = curve_price_r[-1]
				else:
					current_price = np.nan

				##added here
				if critical_points.size > 0 and curve_price_hat_r.size > 0:
					current_price_hat_r = curve_price_hat_r[-1]
				else:
					current_price_hat_r = np.nan

				if curve_price_r.size > 0:
					avg_Price = np.nanmean(curve_price_r)
				else:
					avg_Price = np.nan
				current_date = datetime(1970, 1, 1) + timedelta(seconds=np.multiply(curve_time[-1],1000000000))
				current_date = current_date.strftime('%Y-%m-%d')
				open_ts = datetime.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')
				dt = pytz.timezone('America/Chicago').normalize(open_ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago')))
				is_dst = bool(dt.dst())
				if is_dst is True:
					offset_open = int(time.mktime(time.strptime(current_date + ' 8:00:00', '%Y-%m-%d %H:%M:%S')))
				elif is_dst is False:
					offset_open = int(time.mktime(time.strptime(current_date + ' 9:00:00', '%Y-%m-%d %H:%M:%S')))
				if critical_points.size > 0 and curve_time_r.size > 0:
					current_unix_time = np.multiply(curve_time_r[-1],1000000000)
					ms_since_open = np.multiply(curve_time_r[-1],1000000000) - offset_open
				else:
					current_unix_time = np.nan
					ms_since_open = np.nan

				current_date = datetime.strptime(current_date, '%Y-%m-%d')

				current_year = current_date.isocalendar()[0]
				current_month = current_date.month

				first_day = current_date.replace(day=1)
				dom = current_date.day
				adjusted_dom = dom + first_day.weekday()
				current_week = int(ceil(adjusted_dom/7.0))

				current_weekday = current_date.isocalendar()[2]

				if ms_since_open <= 1800:
					current_30_mins = 1
				elif ms_since_open > 1800 and ms_since_open <= 1800*2:
					current_30_mins = 2
				elif ms_since_open > 1800*2 and ms_since_open <= 1800*3:
					current_30_mins = 3
				elif ms_since_open > 1800*3 and ms_since_open <= 1800*4:
					current_30_mins = 4
				elif ms_since_open > 1800*4 and ms_since_open <= 1800*5:
					current_30_mins = 5
				else:
					current_30_mins = np.nan


				
				total_years = int((current_date - datetime(1970,1,1)).days/365.24)
				total_months = int(((current_date - datetime(1970,1,1)).days/365.24)*12.0)
				total_weeks = int((current_date - datetime(1970,1,1)).days/7.0)
				total_days = int((current_date - datetime(1970,1,1)).days)
				total_30_mins = int(current_unix_time/1800)

				

				

				price_pcavg_per_section = np.array([])
				if curve_price_hat_r.size == 0:
					price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(10) + np.nan)
				elif curve_price_hat_r.size > 0:
					for avg_p_n_y_2 in range(0, len(critical_points)-1):
						if critical_points[avg_p_n_y_2] <= -9223372036854775 or critical_points[avg_p_n_y_2] == np.nan:
							price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_n_y_2+1] <= -9223372036854775 or critical_points[avg_p_n_y_2+1] == np.nan:
								price_pcavg_per_section = np.append(price_pcavg_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_n_y_2+1] == critical_points[avg_p_n_y_2]:
									avg_p_n_y_2 += 1
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.zeros(1))
									continue
								else:
									price_pcavg_per_section = np.append(price_pcavg_per_section, np.nanmean(np.diff(curve_price_hat_r[critical_points[avg_p_n_y_2]:critical_points[avg_p_n_y_2+1]+2])/curve_price_hat_r[critical_points[avg_p_n_y_2]:critical_points[avg_p_n_y_2+1]+2][:-1]))



				price_pct_chg_neg = np.nan
				price_pct_chg_pos = np.divide(np.subtract(curve_price[-1], curve_price[0]), curve_price[0])



				lm = linear_model.LinearRegression()

				lr_price_total_pct_chg_neg = np.nan
				lr_price_avg_roc_neg = np.nan
				lr_price_r2_neg = np.nan




				X = pd.DataFrame(curve_time)
				y = pd.DataFrame(curve_price)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				lr_curve_pos_hat = np.array(lm.predict(X)).flatten()
				lr_price_total_pct_chg_pos = np.divide(np.subtract(lr_curve_pos_hat[-1], lr_curve_pos_hat[0]), lr_curve_pos_hat[0])
				lr_price_avg_roc_pos = np.nanmean((np.diff(lr_curve_pos_hat)/lr_curve_pos_hat[:-1])/np.diff(curve_time))
				lr_price_r2_pos = lm.score(X,y)






				lr_price_pct_chg_per_section = np.array([])
				lr_price_roc_per_section = np.array([])
				lr_price_r2_per_section = np.array([])
				if curve_price_r.size == 0:
					lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(10) + np.nan)
					lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(10) + np.nan)
				elif curve_price_r.size > 0:
					for avg_p_y_2 in range(0, len(critical_points)-1):
						if critical_points[avg_p_y_2] <= -9223372036854775 or critical_points[avg_p_y_2] == np.nan:
							lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
							lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
							lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[avg_p_y_2+1] <= -9223372036854775 or critical_points[avg_p_y_2+1] == np.nan:
								lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.nan)
								lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nan)
								lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[avg_p_y_2+1] == critical_points[avg_p_y_2]:
									avg_p_y_2 += 1
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.zeros(1))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.zeros(1))
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[avg_p_y_2]:critical_points[avg_p_y_2+1]+2])
									y = pd.DataFrame(curve_price_r[critical_points[avg_p_y_2]:critical_points[avg_p_y_2+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_price_pct_chg_per_section = np.append(lr_price_pct_chg_per_section, np.divide(np.subtract(lr_price_per_section_hat[-1], lr_price_per_section_hat[0]), lr_price_per_section_hat[0]))
									lr_price_roc_per_section = np.append(lr_price_roc_per_section, np.nanmean((np.diff(lr_price_per_section_hat)/lr_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[avg_p_y_2]:critical_points[avg_p_y_2+1]+2])))
									lr_price_r2_per_section_2 = lm.score(X,y)
									lr_price_r2_per_section = np.append(lr_price_r2_per_section, lr_price_r2_per_section_2)






				lr_vol_pct_chg_per_section = np.array([])
				lr_vol_roc_per_section = np.array([])
				lr_vol_r2_per_section = np.array([])
				if curve_vol_r.size == 0:
					lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(10) + np.nan)
					lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(10) + np.nan)
				elif curve_vol_r.size > 0:
					for v_2 in range(0, len(critical_points)-1):
						if critical_points[v_2] <= -9223372036854775 or critical_points[v_2] == np.nan:
							lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
							lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
							lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
							continue
						else:
							if critical_points[v_2+1] <= -9223372036854775 or critical_points[v_2+1] == np.nan:
								lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.nan)
								lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nan)
								lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.nan)
								continue
							else:
								if critical_points[v_2+1] == critical_points[v_2]:
									v_2 += 1
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.zeros(1))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.zeros(1))
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[v_2]:critical_points[v_2+1]+2])
									y = pd.DataFrame(curve_vol_r[critical_points[v_2]:critical_points[v_2+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_vol_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_vol_pct_chg_per_section = np.append(lr_vol_pct_chg_per_section, np.divide(np.subtract(lr_vol_per_section_hat[-1], lr_vol_per_section_hat[0]), lr_vol_per_section_hat[0]))
									lr_vol_roc_per_section = np.append(lr_vol_roc_per_section, np.nanmean((np.diff(lr_vol_per_section_hat)/lr_vol_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[v_2]:critical_points[v_2+1]+2])))
									lr_vol_r2_per_section_2 = lm.score(X,y)
									lr_vol_r2_per_section = np.append(lr_vol_r2_per_section, lr_vol_r2_per_section_2)




				vol_total_pct_chg_neg = np.nan
				vol_avg_roc_neg = np.nan
				vol_r2_neg = np.nan




				section_time_key_pos = curve_time_r[int(critical_points[-3]):int(critical_points[-1]+2)]
				section_vol_key_pos = curve_vol_r[int(critical_points[-3]):int(critical_points[-1]+2)]

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_vol_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_vol_pos_hat = np.array(lm.predict(X)).flatten()
				vol_total_pct_chg_pos = np.divide(np.subtract(section_vol_pos_hat[-1], section_vol_pos_hat[0]), section_vol_pos_hat[0])
				vol_avg_roc_pos = np.nanmean((np.diff(section_vol_pos_hat)/section_vol_pos_hat[:-1])/np.diff(section_time_key_pos))
				vol_r2_pos = lm.score(X,y)




				section_mf_key_total = np.multiply(curve_price_r, curve_vol_r)

				lr_mf_price_pct_chg_per_section = np.array([])
				lr_mf_price_roc_per_section = np.array([])
				lr_mf_price_r2_per_section = np.array([])
				if section_mf_key_total.size == 0:
					lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(10) + np.nan)
					lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(10) + np.nan)
					lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(10) + np.nan)
				elif section_mf_key_total.size > 0:
					for v2_2 in range(0, len(critical_points)-1):
						if critical_points[v2_2] <= -9223372036854775 or critical_points[v2_2] == np.nan:
							lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
							lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
							lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
							continue
						else:
							if critical_points[v2_2+1] <= -9223372036854775 or critical_points[v2_2+1] == np.nan:
								lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.nan)
								lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nan)
								lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.nan)
								continue
							else:
								if critical_points[v2_2+1] == critical_points[v2_2]:
									v2_2 += 1
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.zeros(1))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.zeros(1))
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, np.zeros(1))
									continue
								else:
									X = pd.DataFrame(curve_time_r[critical_points[v2_2]:critical_points[v2_2+1]+2])
									y = pd.DataFrame(section_mf_key_total[critical_points[v2_2]:critical_points[v2_2+1]+2])
									y =  Imputer().fit_transform(y)
									model_lm = lm.fit(X,y)
									lr_mf_price_per_section_hat = np.array(lm.predict(X)).flatten()
									lr_mf_price_pct_chg_per_section = np.append(lr_mf_price_pct_chg_per_section, np.divide(np.subtract(lr_mf_price_per_section_hat[-1], lr_mf_price_per_section_hat[0]), lr_mf_price_per_section_hat[0]))
									lr_mf_price_roc_per_section = np.append(lr_mf_price_roc_per_section, np.nanmean((np.diff(lr_mf_price_per_section_hat)/lr_mf_price_per_section_hat[:-1])/np.diff(curve_time_r[critical_points[v2_2]:critical_points[v2_2+1]+2])))
									lr_mf_price_r2_per_section_2 = lm.score(X,y)
									lr_mf_price_r2_per_section = np.append(lr_mf_price_r2_per_section, lr_mf_price_r2_per_section_2)


				mf_price_total_pct_chg_neg = np.nan
				mf_price_avg_roc_neg = np.nan
				mf_price_r2_neg = np.nan




				section_time_key_pos = curve_time
				section_mf_price_key_pos = np.multiply(curve_price, section_vol_key_pos)

				X = pd.DataFrame(section_time_key_pos)
				y = pd.DataFrame(section_mf_price_key_pos)
				y =  Imputer().fit_transform(y)
				model_lm = lm.fit(X,y)
				section_mf_price_pos_hat = np.array(lm.predict(X)).flatten()
				mf_price_total_pct_chg_pos = np.divide(np.subtract(section_mf_price_pos_hat[-1], section_mf_price_pos_hat[0]), section_mf_price_pos_hat[0])
				mf_price_avg_roc_pos = np.nanmean((np.diff(section_mf_price_pos_hat)/section_mf_price_pos_hat[:-1])/np.diff(section_time_key_pos))
				mf_price_r2_pos = lm.score(X,y)



				avg_vol = np.nanmean(curve_vol_r)
				

				quote_key_times_append = np.array([])
				quote_key_times = np.array([])
				for cp_2 in range(0, len(critical_points)):
					if critical_points[cp_2] <= -9223372036854775 or critical_points[cp_2] == np.nan:
						quote_key_times = np.append(quote_key_times, np.nan)
						continue
					else:
						if critical_points[cp_2]+1 < len(curve_time_r) and cp_2 == 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp_2])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cp_2]+1 < len(curve_time_r) and cp_2 > 0:
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp_2]+1)], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)
						elif critical_points[cp_2]+1 >= len(curve_time_r):
							quote_key_times_append = np.multiply(curve_time_r[int(critical_points[cp_2])], 1000000000)
							if quote_key_times_append.size == 0:
								quote_key_times_append = np.nan
							quote_key_times = np.append(quote_key_times, quote_key_times_append)



				final_output.update(dict(ratio_ewm_50_25 = ratio_ewm_50_25))
				final_output.update(dict(ratio_ewm_150_arr = ratio_ewm_150_arr))
				final_output.update(dict(ratio_ewm_67_arr = ratio_ewm_67_arr))
				final_output.update(dict(ratio_ewm_67_150_arr = ratio_ewm_67_150_arr))
				final_output.update(dict(pct_ewm_50_25 = pct_ewm_50_25))
				final_output.update(dict(pct_ewm_150_arr = pct_ewm_150_arr))
				final_output.update(dict(pct_ewm_67_arr = pct_ewm_67_arr))
				final_output.update(dict(pct_ewm_67_150_arr = pct_ewm_67_150_arr))
				final_output.update(dict(avg_return_40_mins = avg_return_40_mins))
				final_output.update(dict(avg_chg_in_avg_return = avg_chg_in_avg_return))
				final_output.update(dict(return_minus_5 = return_minus_5))
				final_output.update(dict(return_minus_4 = return_minus_4))
				final_output.update(dict(return_minus_3 = return_minus_3)) 
				final_output.update(dict(return_minus_2 = return_minus_2)) 
				final_output.update(dict(lr_all_day_time_passed = lr_all_day_time_passed)) 
				final_output.update(dict(lr_all_day_pct_chg = lr_all_day_pct_chg))
				final_output.update(dict(lr_all_day_roc = lr_all_day_roc)) 
				final_output.update(dict(lr_all_day_r2 = lr_all_day_r2)) 
				final_output.update(dict(zip(['sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1'], der_1.T)))
				final_output.update(dict(zip(['sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2'], der_2.T)))
				final_output.update(dict(zip(['sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3'], der_3.T)))
				final_output.update(dict(zip(['sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq'], r_sq.T)))   
				final_output.update(dict(zip(['sec_1_avg_mean_per_section', 'sec_2_avg_mean_per_section', 'sec_3_avg_mean_per_section', 'sec_4_avg_mean_per_section', 'sec_5_avg_mean_per_section', 'sec_6_avg_mean_per_section', 'sec_7_avg_mean_per_section', 'sec_8_avg_mean_per_section', 'sec_9_avg_mean_per_section', 'sec_10_avg_mean_per_section'], avg_mean_per_section.T)))   
				final_output.update(dict(zip(['sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section'], std_per_section.T)))   
				final_output.update(dict(zip(['sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section'], residual_max_per_section.T)))   
				final_output.update(dict(zip(['sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section'], residual_mean_per_section.T)))   
				final_output.update(dict(zip(['sec_1_zscore_per_section', 'sec_2_zscore_per_section', 'sec_3_zscore_per_section', 'sec_4_zscore_per_section', 'sec_5_zscore_per_section', 'sec_6_zscore_per_section', 'sec_7_zscore_per_section', 'sec_8_zscore_per_section', 'sec_9_zscore_per_section', 'sec_10_zscore_per_section'], zscore_per_section.T)))   
				final_output.update(dict(zip(['sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg'], sec_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg'], sec_curve_pct_chg.T)))
				final_output.update(dict(zip(['sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima'], time_since_maxima.T)))
				final_output.update(dict(zip(['sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect'], time_since_last_sect.T))) 
				final_output.update(dict(sec_1_residual_max = residual_max))
				final_output.update(dict(sec_1_residual_mean = residual_mean))				
				final_output.update(dict(sec_1_current_unix_time = current_unix_time)) 
				final_output.update(dict(sec_1_ms_since_open = ms_since_open)) 
				final_output.update(dict(current_year = current_year)) 
				final_output.update(dict(current_month = current_month)) 
				final_output.update(dict(current_week = current_week)) 
				final_output.update(dict(current_weekday = current_weekday)) 
				final_output.update(dict(current_30_mins = current_30_mins)) 
				final_output.update(dict(total_years = total_years)) 
				final_output.update(dict(total_months = total_months)) 
				final_output.update(dict(total_weeks = total_weeks)) 
				final_output.update(dict(total_days = total_days)) 
				final_output.update(dict(total_30_mins = total_30_mins)) 
				final_output.update(dict(sec_1_current_price = current_price)) 
				final_output.update(dict(sec_1_current_price_hat_r = current_price_hat_r)) 	
				final_output.update(dict(sec_1_avg_price = avg_Price)) 
				final_output.update(dict(zip(['sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section'], price_pcavg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section'], lr_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section'], lr_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section'], lr_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_price_pct_chg_neg = price_pct_chg_neg)) 
				final_output.update(dict(sec_1_price_pct_chg_pos = price_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_neg = lr_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_neg = lr_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_lr_price_r2_neg = lr_price_r2_neg)) 
				final_output.update(dict(sec_1_lr_price_total_pct_chg_pos = lr_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_lr_price_avg_roc_pos = lr_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_lr_price_r2_pos = lr_price_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_vol_pct_chg_per_section', 'sec_2_lr_vol_pct_chg_per_section', 'sec_3_lr_vol_pct_chg_per_section', 'sec_4_lr_vol_pct_chg_per_section', 'sec_5_lr_vol_pct_chg_per_section', 'sec_6_lr_vol_pct_chg_per_section', 'sec_7_lr_vol_pct_chg_per_section', 'sec_8_lr_vol_pct_chg_per_section', 'sec_9_lr_vol_pct_chg_per_section', 'sec_10_lr_vol_pct_chg_per_section'], lr_vol_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_roc_per_section', 'sec_2_lr_vol_roc_per_section', 'sec_3_lr_vol_roc_per_section', 'sec_4_lr_vol_roc_per_section', 'sec_5_lr_vol_roc_per_section', 'sec_6_lr_vol_roc_per_section', 'sec_7_lr_vol_roc_per_section', 'sec_8_lr_vol_roc_per_section', 'sec_9_lr_vol_roc_per_section', 'sec_10_lr_vol_roc_per_section'], lr_vol_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_vol_r2_per_section', 'sec_2_lr_vol_r2_per_section', 'sec_3_lr_vol_r2_per_section', 'sec_4_lr_vol_r2_per_section', 'sec_5_lr_vol_r2_per_section', 'sec_6_lr_vol_r2_per_section', 'sec_7_lr_vol_r2_per_section', 'sec_8_lr_vol_r2_per_section', 'sec_9_lr_vol_r2_per_section', 'sec_10_lr_vol_r2_per_section'], lr_vol_r2_per_section.T))) 
				final_output.update(dict(sec_1_vol_total_pct_chg_neg = vol_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_vol_avg_roc_neg = vol_avg_roc_neg)) 
				final_output.update(dict(sec_1_vol_total_pct_chg_pos = vol_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_vol_avg_roc_pos = vol_avg_roc_pos)) 
				final_output.update(dict(sec_1_vol_r2_neg = vol_r2_neg)) 
				final_output.update(dict(sec_1_vol_r2_pos = vol_r2_pos)) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_pct_chg_per_section', 'sec_2_lr_mf_price_pct_chg_per_section', 'sec_3_lr_mf_price_pct_chg_per_section', 'sec_4_lr_mf_price_pct_chg_per_section', 'sec_5_lr_mf_price_pct_chg_per_section', 'sec_6_lr_mf_price_pct_chg_per_section', 'sec_7_lr_mf_price_pct_chg_per_section', 'sec_8_lr_mf_price_pct_chg_per_section', 'sec_9_lr_mf_price_pct_chg_per_section', 'sec_10_lr_mf_price_pct_chg_per_section'], lr_mf_price_pct_chg_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_roc_per_section', 'sec_2_lr_mf_price_roc_per_section', 'sec_3_lr_mf_price_roc_per_section', 'sec_4_lr_mf_price_roc_per_section', 'sec_5_lr_mf_price_roc_per_section', 'sec_6_lr_mf_price_roc_per_section', 'sec_7_lr_mf_price_roc_per_section', 'sec_8_lr_mf_price_roc_per_section', 'sec_9_lr_mf_price_roc_per_section', 'sec_10_lr_mf_price_roc_per_section'], lr_mf_price_roc_per_section.T))) 
				final_output.update(dict(zip(['sec_1_lr_mf_price_r2_per_section', 'sec_2_lr_mf_price_r2_per_section', 'sec_3_lr_mf_price_r2_per_section', 'sec_4_lr_mf_price_r2_per_section', 'sec_5_lr_mf_price_r2_per_section', 'sec_6_lr_mf_price_r2_per_section', 'sec_7_lr_mf_price_r2_per_section', 'sec_8_lr_mf_price_r2_per_section', 'sec_9_lr_mf_price_r2_per_section', 'sec_10_lr_mf_price_r2_per_section'], lr_mf_price_r2_per_section.T))) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_neg = mf_price_total_pct_chg_neg)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_neg = mf_price_avg_roc_neg)) 
				final_output.update(dict(sec_1_mf_price_r2_neg = mf_price_r2_neg)) 
				final_output.update(dict(sec_1_mf_price_total_pct_chg_pos = mf_price_total_pct_chg_pos)) 
				final_output.update(dict(sec_1_mf_price_avg_roc_pos = mf_price_avg_roc_pos)) 
				final_output.update(dict(sec_1_mf_price_r2_pos = mf_price_r2_pos)) 
				final_output.update(dict(sec_1_avg_vol = avg_vol)) 







			return final_output




		class final(beam.DoFn):
			def process(self, element):
				try:	
					symbol = str(element[0])

					element = element[1]
					element_2 = []
					for x in element:
						element_2.append(x)
					element = element_2

					#element = list(element[1])
					def takeLastTime(elem):
						return int(elem[-1][0].value)
					element.sort(key=takeLastTime)	

					count_1 = 0
					real_last_zero_crossings = np.array([])
					real_last_zero_crossings_time = np.array([])
					date_change = np.array([])
					final_output_2 = []
					final_output_3 = []
					final_output = {}
					
					find_curve_entry = np.array([])
					zero_crossings_0_total = np.array([])
					zero_crossings_0_total_time = np.array([])
					for x5 in element:
						try:
							count_1 += 1
							price = np.vstack(x5)
							price_pd = pd.DataFrame(data=price[0:, 0:], index=price[0:, 0], columns=['Time', 'Symbol', 'Price', 'Volume'])
							price_pd = price_pd.iloc[::5, :]
							price_pd['Price'] = price_pd.Price.astype(float)
							price_pd['Volume'] = price_pd.Volume.astype(float)
							price_pd.set_index('Time', inplace=True, drop=False)
							if price_pd.empty is False:


								



								price_pd.dropna(inplace=True)
								price_pd[~price_pd.index.duplicated(keep='first')]
								#values[values==0] = np.NaN
								#missings = np.isnan(values)
								#cumsum = np.cumsum(~missings)
								#diff = np.diff(np.concatenate(([0.], cumsum[missings])))
								#values[missings] = -diff
								#serialized_datetime = price_pd.index + np.cumsum(values).astype(np.timedelta64)
								#price_pd.index = serialized_datetime
								price_pd.sort_index(inplace=True)








								price_0 = price_pd['Price'].values
								time_0 = price_pd['Time'].values.astype(float)
								#model_0 = Earth(smooth=True, allow_missing=True, endspan_alpha=.0001, check_every=1, allow_linear=False)
								#model_0.fit(time_0, price_0)
								#price_hat = model_0.predict(time_0)
								#first_der = np.diff(price_hat)/np.diff(time_0)
								#zero_crossings_0 = np.where(np.diff(np.sign(first_der)))[0]
								#zero_crossings_0 = np.unique(zero_crossings_0)
								#if zero_crossings_0.size > 0:
								#	logging.info('step_2')
								#	price = price_pd['Price'].ewm(span=price_hat[zero_crossings_0[-1]:].size).mean().values
									
								
								section_time = np.multiply(time_0,0.000000000000000001)
								section_vol = price_pd['Volume'].values
								section_price = price_0
								model = Earth(smooth=True, allow_missing=True, endspan_alpha=1.0, check_every=1, allow_linear=False)
								model.fit(section_time, section_price)
								section_price_hat = model.predict(section_time)



								section_der = np.diff(section_price_hat)/np.diff(section_time)
								section_der_2_diff = np.diff(section_der).astype(np.longdouble)
								section_time_2_diff = np.diff(section_time).astype(np.longdouble)[1:]
								section_der_2 = np.divide(section_der_2_diff, section_time_2_diff).astype(np.longdouble)
								zero_crossings_0 = np.where(np.diff(np.sign(section_der)))[0]
								if count_1 < 41:
									zero_crossings_0 = np.insert(zero_crossings_0, 0, 0)

								date_change = np.append(date_change, datetime.fromtimestamp(section_time[-1]*1000000000, pytz.utc).date())
								
								if date_change.size > 1:				
									if date_change[-1] != date_change[-2]:
										count_1 = 1
										zero_crossings_0_total = np.array([])
								zero_crossings_0 = np.unique(zero_crossings_0)
								
								zc = 0
								zc_2 = zc + 1
								zero_crossings_0_unique = np.array([])
								if zero_crossings_0.size > 0:
									zero_crossings_0_unique = np.append(zero_crossings_0_unique, zero_crossings_0[zc])
								else:
									continue
								while zc_2 + 1 < len(zero_crossings_0):
									zero_crossings_0_check_dup = np.take(section_price_hat, zero_crossings_0[zc])
									zero_crossings_0_check_dup_2 = np.take(section_price_hat, zero_crossings_0[zc_2])
									zero_crossings_0_check_dup_time = np.take(section_time, zero_crossings_0[zc])
									zero_crossings_0_check_dup_2_time = np.take(section_time, zero_crossings_0[zc_2])
									if zero_crossings_0_check_dup == zero_crossings_0_check_dup_2 or zero_crossings_0_check_dup_time == zero_crossings_0_check_dup_2_time:
										zero_crossings_0_unique = np.append(zero_crossings_0_unique, zero_crossings_0[zc])
										zc_2 = zc_2 + 1
									else:
										zc = zc_2
										zero_crossings_0_unique = np.append(zero_crossings_0_unique, zero_crossings_0[zc])
										zc_2 = zc + 1
								zero_crossings_0 = np.unique(zero_crossings_0_unique).astype(np.int64)
								
								zero_crossings = np.array([])


								for zcp in zero_crossings_0:
									try:
										logging.info('step_3')
										#if real_last_zero_crossings_time.size > 0:
										#	logging.info('check_this')
										#	logging.info(real_last_zero_crossings_time[-1])
										#if (zcp == 0 and np.take(section_der, zcp) != 0) or (zcp != 0 and np.take(section_der, zcp+1) != 0):
										if real_last_zero_crossings_time.size == 0:
											zero_crossings_0_total = np.append(zero_crossings_0_total, zcp).astype(np.int64)
											zero_crossings = np.append(zero_crossings, zcp).astype(np.int64)
											real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
											real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]))
											find_curve_entry = np.array([])
											if real_last_zero_crossings[-1] > 0:
												find_curve_entry = np.where(section_der[int(zero_crossings[-1]):] >= 1)[0]
												if find_curve_entry.size > 1:
													find_curve_entry = find_curve_entry[0]
												if find_curve_entry.size == 1:
													final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model)
													find_curve_entry_der_y = final_output.get('sec_1_current_price_hat_r')
													continue
												elif find_curve_entry.size == 0:
													check_if_inflect = np.where(section_der_2[int(zero_crossings[-1]):] < 0)[0]
													if check_if_inflect.size > 1:
														check_if_inflect = check_if_inflect[0]
													if check_if_inflect.size == 1:
														find_curve_entry = check_if_inflect
														final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model)
														find_curve_entry_der_y = final_output.get('sec_1_current_price_hat_r')
														continue
													elif check_if_inflect.size == 0:
														continue
											else:
												continue
												
										elif real_last_zero_crossings_time.size > 0:
											zero_crossings = np.append(zero_crossings, zcp).astype(np.int64)
											if real_last_zero_crossings_time[-1] >= section_time[zcp+1]:
												continue
											elif real_last_zero_crossings_time[-1] < section_time[zcp+1]:
												if real_last_zero_crossings[-1] < 0 and section_der[zcp+1] < 0:
													continue
												elif real_last_zero_crossings[-1] > 0 and section_der[zcp+1] > 0:
													continue
												elif real_last_zero_crossings[-1] < 0 and section_der[zcp+1] > 0:
													find_curve_entry = np.where(section_der[int(zero_crossings[-1]):] >= 1)[0]
													if find_curve_entry.size > 1:
														find_curve_entry = find_curve_entry[0]
													if find_curve_entry.size == 1:
															
														final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model)
														find_curve_entry_der_y = final_output.get('sec_1_current_price_hat_r')

														real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
														real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))

														continue

													elif find_curve_entry.size == 0:
														check_if_inflect = np.where(section_der_2[int(zero_crossings[-1]):] < 0)[0]
														if check_if_inflect.size > 1:
															check_if_inflect = check_if_inflect[0]
														if check_if_inflect.size == 1:
															find_curve_entry = check_if_inflect
															final_output = get_features(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model)
															find_curve_entry_der_y = final_output.get('sec_1_current_price_hat_r')

															real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
															real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))

															continue

														elif check_if_inflect.size == 0:
															real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
															real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))
															continue

												elif real_last_zero_crossings[-1] > 0 and section_der[zcp+1] < 0:
													if zero_crossings.size == 1:
														if len(final_output) > 0:
															final_y = np.divide(np.subtract(float(np.take(section_price_hat,zero_crossings[-1]+1)), find_curve_entry_der_y), find_curve_entry_der_y)
															final_output.update(dict(ts = float(np.take(section_time,zero_crossings[-1]+1))))
															final_output.update(dict(final_y = float(final_y)))


															final_output_2.append(final_output)
															final_output = {}
															
															find_curve_entry = np.array([])

															real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
															real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))
															continue

														elif len(final_output) == 0:
															find_curve_entry = np.where(section_der[:int(zero_crossings[-1]+1)] >= 1)[0]
															if find_curve_entry.size > 1:
																find_curve_entry = find_curve_entry[0]
															if find_curve_entry.size == 0:
																find_curve_entry = np.nanargmax(section_der[:int(zero_crossings[-1]+1)])
															final_output = get_features_final_y(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model)
															find_curve_entry_der_y = final_output.get('sec_1_current_price_hat_r')

															final_y = np.divide(np.subtract(float(np.take(section_price_hat,zero_crossings[-1]+1)), find_curve_entry_der_y), find_curve_entry_der_y)
															final_output.update(dict(ts = float(np.take(section_time,zero_crossings[-1]+1))))
															final_output.update(dict(final_y = float(final_y)))
															

															final_output_2.append(final_output)
															final_output = {}
															
															find_curve_entry = np.array([])

															real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
															real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))
															continue

													elif zero_crossings.size > 1:
														if len(final_output) > 0:
															final_y = np.divide(np.subtract(float(np.take(section_price_hat,zero_crossings[-1]+1)), find_curve_entry_der_y), find_curve_entry_der_y)
															
															final_output.update(dict(ts = float(np.take(section_time,zero_crossings[-1]+1))))
															final_output.update(dict(final_y = float(final_y)))


															final_output_2.append(final_output)
															final_output = {}
															
															find_curve_entry = np.array([])

															real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
															real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))
															continue

														elif len(final_output) == 0:
															find_curve_entry = np.where(section_der[int(zero_crossings[-2]):int(zero_crossings[-1]+1)] >= 1)[0]
															if find_curve_entry.size > 1:
																find_curve_entry = find_curve_entry[0]
															if find_curve_entry.size == 0:
																find_curve_entry = np.nanargmax(section_der[int(zero_crossings[-2]):int(zero_crossings[-1]+1)])
															final_output = get_features_final_y(symbol, final_output, zero_crossings, section_price, section_der, section_time, section_price_hat, section_vol, price_pd, model)
															find_curve_entry_der_y = final_output.get('sec_1_current_price_hat_r')

															final_y = np.divide(np.subtract(float(np.take(section_price_hat,zero_crossings[-1]+1)), find_curve_entry_der_y), find_curve_entry_der_y)
															final_output.update(dict(ts = float(np.take(section_time,zero_crossings[-1]+1))))
															final_output.update(dict(final_y = float(final_y)))
															

															final_output_2.append(final_output)
															final_output = {}
															

															find_curve_entry = np.array([])

															real_last_zero_crossings = np.append(real_last_zero_crossings, np.take(section_der, zero_crossings[-1]+1))
															real_last_zero_crossings_time = np.append(real_last_zero_crossings_time, np.take(section_time,zero_crossings[-1]+1))
															continue

									except Exception as e:
										exc_type, exc_obj, exc_tb = sys.exc_info()
										fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
										logging.info(exc_type, fname, exc_tb.tb_lineno)
										continue

						except Exception as e:
							exc_type, exc_obj, exc_tb = sys.exc_info()
							fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
							logging.info(exc_type, fname, exc_tb.tb_lineno)
							continue

					keys_arr = np.array(['ratio_ewm_50_25', 'ratio_ewm_150_arr', 'ratio_ewm_67_arr', 'ratio_ewm_67_150_arr', 'pct_ewm_50_25', 'pct_ewm_150_arr', 'pct_ewm_67_arr', 'pct_ewm_67_150_arr', 'avg_return_40_mins', 'avg_chg_in_avg_return', 'return_minus_5', 'return_minus_4', 'return_minus_3', 'return_minus_2', 'lr_all_day_time_passed', 'lr_all_day_pct_chg', 'lr_all_day_roc', 'lr_all_day_r2', 'sec_1_der_1', 'sec_2_der_1', 'sec_3_der_1', 'sec_4_der_1', 'sec_5_der_1', 'sec_6_der_1', 'sec_7_der_1', 'sec_8_der_1', 'sec_9_der_1', 'sec_10_der_1', 'sec_1_der_2', 'sec_2_der_2', 'sec_3_der_2', 'sec_4_der_2', 'sec_5_der_2', 'sec_6_der_2', 'sec_7_der_2', 'sec_8_der_2', 'sec_9_der_2', 'sec_10_der_2', 'sec_1_der_3', 'sec_2_der_3', 'sec_3_der_3', 'sec_4_der_3', 'sec_5_der_3', 'sec_6_der_3', 'sec_7_der_3', 'sec_8_der_3', 'sec_9_der_3', 'sec_10_der_3', 'sec_1_r_sq', 'sec_2_r_sq', 'sec_3_r_sq', 'sec_4_r_sq', 'sec_5_r_sq', 'sec_6_r_sq', 'sec_7_r_sq', 'sec_8_r_sq', 'sec_9_r_sq', 'sec_10_r_sq', 'sec_1_avg_mean_per_section', 'sec_2_avg_mean_per_section', 'sec_3_avg_mean_per_section', 'sec_4_avg_mean_per_section', 'sec_5_avg_mean_per_section', 'sec_6_avg_mean_per_section', 'sec_7_avg_mean_per_section', 'sec_8_avg_mean_per_section', 'sec_9_avg_mean_per_section', 'sec_10_avg_mean_per_section', 'sec_1_std_per_section', 'sec_2_std_per_section', 'sec_3_std_per_section', 'sec_4_std_per_section', 'sec_5_std_per_section', 'sec_6_std_per_section', 'sec_7_std_per_section', 'sec_8_std_per_section', 'sec_9_std_per_section', 'sec_10_std_per_section', 'sec_1_residual_max_per_section', 'sec_2_residual_max_per_section', 'sec_3_residual_max_per_section', 'sec_4_residual_max_per_section', 'sec_5_residual_max_per_section', 'sec_6_residual_max_per_section', 'sec_7_residual_max_per_section', 'sec_8_residual_max_per_section', 'sec_9_residual_max_per_section', 'sec_10_residual_max_per_section', 'sec_1_residual_mean_per_section', 'sec_2_residual_mean_per_section', 'sec_3_residual_mean_per_section', 'sec_4_residual_mean_per_section', 'sec_5_residual_mean_per_section', 'sec_6_residual_mean_per_section', 'sec_7_residual_mean_per_section', 'sec_8_residual_mean_per_section', 'sec_9_residual_mean_per_section', 'sec_10_residual_mean_per_section', 'sec_1_zscore_per_section', 'sec_2_zscore_per_section', 'sec_3_zscore_per_section', 'sec_4_zscore_per_section', 'sec_5_zscore_per_section', 'sec_6_zscore_per_section', 'sec_7_zscore_per_section', 'sec_8_zscore_per_section', 'sec_9_zscore_per_section', 'sec_10_zscore_per_section', 'sec_1_sec_pct_chg', 'sec_2_sec_pct_chg', 'sec_3_sec_pct_chg', 'sec_4_sec_pct_chg', 'sec_5_sec_pct_chg', 'sec_6_sec_pct_chg', 'sec_7_sec_pct_chg', 'sec_8_sec_pct_chg', 'sec_9_sec_pct_chg', 'sec_10_sec_pct_chg', 'sec_1_sec_curve_pct_chg', 'sec_2_sec_curve_pct_chg', 'sec_3_sec_curve_pct_chg', 'sec_4_sec_curve_pct_chg', 'sec_5_sec_curve_pct_chg', 'sec_6_sec_curve_pct_chg', 'sec_7_sec_curve_pct_chg', 'sec_8_sec_curve_pct_chg', 'sec_9_sec_curve_pct_chg', 'sec_10_sec_curve_pct_chg', 'sec_1_time_since_maxima', 'sec_2_time_since_maxima', 'sec_3_time_since_maxima', 'sec_4_time_since_maxima', 'sec_5_time_since_maxima', 'sec_6_time_since_maxima', 'sec_7_time_since_maxima', 'sec_8_time_since_maxima', 'sec_9_time_since_maxima', 'sec_10_time_since_maxima', 'sec_1_time_since_last_sect', 'sec_2_time_since_last_sect', 'sec_3_time_since_last_sect', 'sec_4_time_since_last_sect', 'sec_5_time_since_last_sect', 'sec_6_time_since_last_sect', 'sec_7_time_since_last_sect', 'sec_8_time_since_last_sect', 'sec_9_time_since_last_sect', 'sec_10_time_since_last_sect', 'sec_1_residual_max', 'sec_1_residual_mean', 'sec_1_current_unix_time', 'sec_1_ms_since_open', 'current_year', 'current_month', 'current_week', 'current_weekday', 'current_30_mins', 'total_years', 'total_months', 'total_weeks', 'total_days', 'total_30_mins', 'sec_1_current_price', 'sec_1_current_price_hat_r', 'sec_1_avg_price', 'sec_1_price_pcavg_per_section', 'sec_2_price_pcavg_per_section', 'sec_3_price_pcavg_per_section', 'sec_4_price_pcavg_per_section', 'sec_5_price_pcavg_per_section', 'sec_6_price_pcavg_per_section', 'sec_7_price_pcavg_per_section', 'sec_8_price_pcavg_per_section', 'sec_9_price_pcavg_per_section', 'sec_10_price_pcavg_per_section', 'sec_1_lr_price_pct_chg_per_section', 'sec_2_lr_price_pct_chg_per_section', 'sec_3_lr_price_pct_chg_per_section', 'sec_4_lr_price_pct_chg_per_section', 'sec_5_lr_price_pct_chg_per_section', 'sec_6_lr_price_pct_chg_per_section', 'sec_7_lr_price_pct_chg_per_section', 'sec_8_lr_price_pct_chg_per_section', 'sec_9_lr_price_pct_chg_per_section', 'sec_10_lr_price_pct_chg_per_section', 'sec_1_lr_price_roc_per_section', 'sec_2_lr_price_roc_per_section', 'sec_3_lr_price_roc_per_section', 'sec_4_lr_price_roc_per_section', 'sec_5_lr_price_roc_per_section', 'sec_6_lr_price_roc_per_section', 'sec_7_lr_price_roc_per_section', 'sec_8_lr_price_roc_per_section', 'sec_9_lr_price_roc_per_section', 'sec_10_lr_price_roc_per_section', 'sec_1_lr_price_r2_per_section', 'sec_2_lr_price_r2_per_section', 'sec_3_lr_price_r2_per_section', 'sec_4_lr_price_r2_per_section', 'sec_5_lr_price_r2_per_section', 'sec_6_lr_price_r2_per_section', 'sec_7_lr_price_r2_per_section', 'sec_8_lr_price_r2_per_section', 'sec_9_lr_price_r2_per_section', 'sec_10_lr_price_r2_per_section', 'sec_1_price_pct_chg_neg', 'sec_1_price_pct_chg_pos', 'sec_1_lr_price_total_pct_chg_neg', 'sec_1_lr_price_avg_roc_neg', 'sec_1_lr_price_r2_neg', 'sec_1_lr_price_total_pct_chg_pos', 'sec_1_lr_price_avg_roc_pos', 'sec_1_lr_price_r2_pos', 'sec_1_lr_vol_pct_chg_per_section', 'sec_2_lr_vol_pct_chg_per_section', 'sec_3_lr_vol_pct_chg_per_section', 'sec_4_lr_vol_pct_chg_per_section', 'sec_5_lr_vol_pct_chg_per_section', 'sec_6_lr_vol_pct_chg_per_section', 'sec_7_lr_vol_pct_chg_per_section', 'sec_8_lr_vol_pct_chg_per_section', 'sec_9_lr_vol_pct_chg_per_section', 'sec_10_lr_vol_pct_chg_per_section', 'sec_1_lr_vol_roc_per_section', 'sec_2_lr_vol_roc_per_section', 'sec_3_lr_vol_roc_per_section', 'sec_4_lr_vol_roc_per_section', 'sec_5_lr_vol_roc_per_section', 'sec_6_lr_vol_roc_per_section', 'sec_7_lr_vol_roc_per_section', 'sec_8_lr_vol_roc_per_section', 'sec_9_lr_vol_roc_per_section', 'sec_10_lr_vol_roc_per_section', 'sec_1_lr_vol_r2_per_section', 'sec_2_lr_vol_r2_per_section', 'sec_3_lr_vol_r2_per_section', 'sec_4_lr_vol_r2_per_section', 'sec_5_lr_vol_r2_per_section', 'sec_6_lr_vol_r2_per_section', 'sec_7_lr_vol_r2_per_section', 'sec_8_lr_vol_r2_per_section', 'sec_9_lr_vol_r2_per_section', 'sec_10_lr_vol_r2_per_section', 'sec_1_vol_total_pct_chg_neg', 'sec_1_vol_avg_roc_neg', 'sec_1_vol_total_pct_chg_pos', 'sec_1_vol_avg_roc_pos', 'sec_1_vol_r2_neg', 'sec_1_vol_r2_pos', 'sec_1_lr_mf_price_pct_chg_per_section', 'sec_2_lr_mf_price_pct_chg_per_section', 'sec_3_lr_mf_price_pct_chg_per_section', 'sec_4_lr_mf_price_pct_chg_per_section', 'sec_5_lr_mf_price_pct_chg_per_section', 'sec_6_lr_mf_price_pct_chg_per_section', 'sec_7_lr_mf_price_pct_chg_per_section', 'sec_8_lr_mf_price_pct_chg_per_section', 'sec_9_lr_mf_price_pct_chg_per_section', 'sec_10_lr_mf_price_pct_chg_per_section', 'sec_1_lr_mf_price_roc_per_section', 'sec_2_lr_mf_price_roc_per_section', 'sec_3_lr_mf_price_roc_per_section', 'sec_4_lr_mf_price_roc_per_section', 'sec_5_lr_mf_price_roc_per_section', 'sec_6_lr_mf_price_roc_per_section', 'sec_7_lr_mf_price_roc_per_section', 'sec_8_lr_mf_price_roc_per_section', 'sec_9_lr_mf_price_roc_per_section', 'sec_10_lr_mf_price_roc_per_section', 'sec_1_lr_mf_price_r2_per_section', 'sec_2_lr_mf_price_r2_per_section', 'sec_3_lr_mf_price_r2_per_section', 'sec_4_lr_mf_price_r2_per_section', 'sec_5_lr_mf_price_r2_per_section', 'sec_6_lr_mf_price_r2_per_section', 'sec_7_lr_mf_price_r2_per_section', 'sec_8_lr_mf_price_r2_per_section', 'sec_9_lr_mf_price_r2_per_section', 'sec_10_lr_mf_price_r2_per_section', 'sec_1_mf_price_total_pct_chg_neg', 'sec_1_mf_price_avg_roc_neg', 'sec_1_mf_price_r2_neg', 'sec_1_mf_price_total_pct_chg_pos', 'sec_1_mf_price_avg_roc_pos', 'sec_1_mf_price_r2_pos', 'sec_1_avg_vol', 'ts', 'final_y'])
					
					for fo in final_output_2:
						final_output_3_first = {each_key: fo.get(each_key) for each_key in keys_arr}
						final_output_3_first = {k: float(v) if isinstance(v,np.floating) == True else v for k, v in final_output_3_first.items()}
						final_output_3_first = {k: float(v) if isinstance(v,int) == True else v for k, v in final_output_3_first.items()}
						final_output_3_first = {k: np.nan if v is None else v for k, v in final_output_3_first.items()}
						final_output_3.append({k: None if isinstance(v,float) == True and np.isinf(v) == True or v <= -9223372036854775 or isinstance(v,float) == True and np.isnan(v) == True else v for k, v in final_output_3_first.items()})
										
					lenofdict = np.array([])
					for fo3 in final_output_3:
						lenofdict = np.append(lenofdict, len(fo3))
					yield final_output_3
						
				except Exception as e:
					exc_type, exc_obj, exc_tb = sys.exc_info()
					fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
					logging.info(exc_type, fname, exc_tb.tb_lineno)


		def generate_schema_2():
			"""BigQuery schema for the features table."""
			json_str = json.dumps(json.load(open("schema_1.json"))["schema"])

			return parse_table_schema_from_json(json_str)



		def BreakList2(element):
			if len(element) > 0:
				for bl in element:
					yield bl
					


		tick_data_all = (p | 'read' >> ReadFromAvro(known_args.input)
			| 'final_link' >> beam.ParDo(final_link())
			| "flatten_final_link" >> beam.FlatMap(BreakList)
			| 'tick_data_all' >> beam.ParDo(tick_data_all()).with_outputs('final_links_filtered', main='price_pd_1050'))
		price_final = tick_data_all.price_pd_1050
		final_links_filtered = tick_data_all.final_links_filtered
		features = (price_final | "flatten_tick_data_all" >> beam.FlatMap(BreakList)
			| 'Add_Timestamp_1' >> beam.ParDo(AddTimestampDoFn())
			| 'Window_Sliding' >> beam.WindowInto(beam.window.SlidingWindows(size=2 * 60, period=5))
			| 'Add_Tuple_1' >> beam.Map(lambda x : (str(x[1]),tuple(x)))
			| 'Window_GroupByKey_1' >> beam.GroupByKey()
			| 'Window_Global' >> beam.WindowInto(beam.window.GlobalWindows())
			| 'Add_Tuple_2' >> beam.Map(lambda x : (str(x[0]),list(x[1])))
			| 'Window_GroupByKey_2' >> beam.GroupByKey()
			| 'final' >> beam.ParDo(final())
			| "flatten_final" >> beam.FlatMap(BreakList2)
			| 'write_to_bq' >> beam.io.Write(beam.io.BigQuerySink(table='features_MSFT',
				dataset='MyFeatures',
				project='your-gcloud-account-name',
				schema=generate_schema_2(),
				create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
				write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run_main()
