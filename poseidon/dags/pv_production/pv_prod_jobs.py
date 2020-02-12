import datetime
import pendulum
import requests
import json 
import numpy as np
import pandas as pd
import logging

from trident.util import general

conf = general.config
PRIMARY_KEY = conf['pf_api_key']

def get_pv_data_write_temp(**context):
	currTime = context['execution_date'].in_timezone('America/Los_Angeles')
	elem_paths = ['2000.05.088.SWG01.MTR01']
	API_to_csv(elem_paths, 'hourly', currTime)

	if currTime.hour in [15,16]:
		elem_paths = ['2000.05.066.SWG01.MTR01', 
					'2000.05.088.SWG01.MTR01', 
					'2000.05.100.SWG01.MTR01', 
					'2000.05.114.SWG01.MTR01', 
					'2000.05.117.SWG01.MTR01', 
					'2000.05.120.SWG01.MTR01', 
					'2000.06.005.SWG01.MTR01', 
					'2000.06.007.SWG01.MTR01', 
					'2000.06.010.SWG01.MTR01', 
					'2000.06.021.SWG01.MTR01', 
					'2000.06.027.SWG01.MTR01', 
					'2000.06.029.SWG01.MTR01', 
					'2000.06.046.SWG01.MTR01', 
					'2000.06.047.SWG01.MTR01', 
					'2000.06.053.SWG01.MTR01']

		API_to_csv(elem_paths, 'daily', currTime)

	return f"Successfully wrote temp files"

def get_data(start_date, end_date, elem_paths, attr, two_hours=False, resolution="raw", fp=None):
	baseurl = 'https://api.powerfactorscorp.com'
	headers = {"Ocp-Apim-Subscription-Key": PRIMARY_KEY}
	dataURL = baseurl + '/drive/v2/data'
	# To store values for each element
	results = {path: [] for path in elem_paths}
	dates = [(start_date, end_date)]

	num_vals = 0
	start_tstamp = None
	end_tstamp = None
	for path in elem_paths: # Loop through each element in list
		for start, end in dates: # Iterate through list of start and end times
			body = {"startTime": start,
					"endTime": end,
					"resolution": resolution,
					"attributes": attr,
					"ids": path}
			# Use POST to avoid hitting max URL length w/ many params
			r = requests.post(dataURL, headers=headers, data=body).json()
			results[path] += r['assets'][0]['attributes'][0]['values'][1:] # Append readings for this time period to list of readings for this element
			if start_tstamp is None: # Get the start timestamp of the entire time period
				start_tstamp = pd.to_datetime(r['assets'][0]['startTime'][:19])+datetime.timedelta(minutes=5)  
				
		end_tstamp = pd.to_datetime(r['assets'][0]['endTime'][:19])
		num_vals = len(results[path])
	   
	tstamps = pd.date_range(start_tstamp, end_tstamp, periods=num_vals)
	df_5min = pd.DataFrame(index=tstamps, data=results)
	df_15min = df_5min.resample('15T', label='right', closed='right').mean()
		
	if fp:
		df_15min.to_csv(fp)
		return
	else:
		return df_15min, df_5min

def API_to_csv(elem_paths, interval, execution_date):
	if interval == 'hourly':
		temp_file = conf['temp_data_dir'] + '/pv_hourly_results.csv'
		endDate = execution_date.subtract(minutes=30)
		startDate = endDate.subtract(hours=3)
		rename_cols = {'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa Library'}

	elif interval == 'daily':
		temp_file = conf['temp_data_dir'] + '/pv_daily_results.csv'
		endDate = execution_date
		startDate = endDate.subtract(days=3)
		rename_cols = {'2000.05.066.SWG01.MTR01': 'Carmel Valley Rec Center', 
					'2000.05.088.SWG01.MTR01': 'Serra Mesa-Kearny Mesa Library', 
					'2000.05.100.SWG01.MTR01': 'Fire Repair Facility', 
					'2000.05.114.SWG01.MTR01': 'Rancho Bernardo Senior Center', 
					'2000.05.117.SWG01.MTR01': 'Police Station Eastern Division', 
					'2000.05.120.SWG01.MTR01': 'Police Station Southern Division', 
					'2000.06.005.SWG01.MTR01': 'Mountain View Rec Center', 
					'2000.06.007.SWG01.MTR01': 'Police Station Northern Division', 
					'2000.06.010.SWG01.MTR01': 'Tierrasanta Rec Center & Pool', 
					'2000.06.021.SWG01.MTR01': 'Police Station Western Division ', 
					'2000.06.027.SWG01.MTR01': 'Mission Valley Library', 
					'2000.06.029.SWG01.MTR01': 'Police Station Central Division', 
					'2000.06.046.SWG01.MTR01': 'Mission Trails Regional Park', 
					'2000.06.047.SWG01.MTR01': 'Balboa Park Inspiration Point', 
					'2000.06.053.SWG01.MTR01': 'Park De La Cruz Rec Center'}
	
	attr = 'AC_POWER'

	print("CALLING API WITH {} {} {} {}".format(startDate, endDate, elem_paths, attr))

	df_5min, df_15min = get_data(startDate, endDate, elem_paths, attr, True)

	df_5min = df_5min.rename(columns=rename_cols)
	df_15min = df_15min.rename(columns=rename_cols)
	df_15min.index.name = 'Timestamp'

	general.pos_write_csv(df_15min, temp_file, index=True, date_format=conf['date_format_ymd_hms'])

def update_pv_prod(**context):
	prod_file = conf['prod_data_dir'] + '/pv_production.csv'
	temp_file = conf['temp_data_dir'] + '/pv_daily_results.csv'
	df_prod = pd.read_csv(prod_file,low_memory=False,index_col=0)
	df_temp = pd.read_csv(temp_file,low_memory=False,index_col=0)
	df_prod = pd.concat([df_prod,df_temp])
	df_prod = df_prod.groupby(df_prod.index).first()
	df_prod = df_prod.round(decimals=3)
	general.pos_write_csv(df_prod, prod_file, index=True, date_format=conf['date_format_ymd_hms'])

	hourly_file = conf['temp_data_dir'] + '/pv_hourly_results.csv'
	prod_hourly_file = conf['prod_data_dir'] + '/pv_hourly_production.csv'
	df_prod = pd.read_csv(prod_hourly_file,low_memory=False,index_col=0)
	df_temp = pd.read_csv(hourly_file,low_memory=False,index_col=0)
	df_prod = pd.concat([df_prod,df_temp])
	df_prod = df_prod.groupby(df_prod.index).first()
	df_prod = df_prod.round(decimals=3)
	general.pos_write_csv(df_prod, prod_hourly_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_prod.shape[0]

	logging.info('Writing ' + str(df_prod.shape[0]) + ' rows')

	return f"Successfully wrote prod file with {results} records"

#Push to Lucid
#TO DO