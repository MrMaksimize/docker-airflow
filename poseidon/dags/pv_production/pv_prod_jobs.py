import datetime
import pendulum
import requests
import json 
import numpy as np
import pandas as pd
import logging
from trident.util import general
from airflow import AirflowException

conf = general.config
PRIMARY_KEY = conf['pf_api_key']
LUCID_USER = conf["lucid_api_user"]
LUCID_PASS = conf["lucid_api_pass"]

pv_meters = {'2000.05.066.SWG01.MTR01': 'Carmel Valley Rec Center', 
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
					'2000.06.053.SWG01.MTR01': 'Park De La Cruz Rec Center',
					'2000.05.073.SWG01.MTR01': 'Malcolm X Library',
					'2000.06.006.SWG01.MTR01':'Point Loma Library'}

daily_pv_meters = [*pv_meters]
hourly_pv_meters = ['2000.05.088.SWG01.MTR01','2000.05.073.SWG01.MTR01','2000.06.006.SWG01.MTR01']

#: DAG Function
def get_pv_data_write_temp(**context):
	currTime = context['execution_date'].in_timezone('America/Los_Angeles')
	#currTime = currTime.replace(tzinfo=None)
	
	API_to_csv(hourly_pv_meters, 'hourly', currTime)	

	if currTime.hour in [15,16]:	
		API_to_csv(daily_pv_meters, 'daily', currTime)		
	
	return f"Successfully wrote temp files"

#: Helper Function
def API_to_csv(elem_paths, interval, execution_date):
	if interval == 'hourly':
		temp_file = conf['temp_data_dir'] + '/pv_hourly_results.csv'
		endDate = execution_date.subtract(minutes=30)
		startDate = endDate.subtract(hours=3)		

	elif interval == 'daily':
		temp_file = conf['temp_data_dir'] + '/pv_daily_results.csv'
		endDate = execution_date.subtract(minutes=30)
		startDate = endDate.subtract(days=6)
	
	logging.info(f'Calling API with: {startDate}, {endDate}, # of elements: {len(elem_paths)} ')
	df_5min = get_data(startDate, endDate, elem_paths, 'AC_POWER', True)
	df_5min = df_5min.rename(columns=pv_meters)
	df_5min.index.name = 'Timestamp'
	df_5min.index = df_5min.index.round('5min')
	df_5min = df_5min.div(12)
	df_5min = df_5min.round(decimals=3)

	logging.info(f"Writing {interval} data to temp")
	general.pos_write_csv(df_5min, temp_file, index=True, date_format=conf['date_format_ymd_hms'])

#: Helper Function
def get_data(start_date, end_date, elem_paths, attr, two_hours=False, resolution="raw", fp=None):
	baseurl = 'https://api.powerfactorscorp.com'
	headers = {"Ocp-Apim-Subscription-Key": PRIMARY_KEY}
	dataURL = baseurl + '/drive/v2/data'
	# To store values for each element
	results = {path: [] for path in elem_paths}
	dates = [(start_date, end_date)]

	num_vals = 0
	start_tstamp = start_date.replace(tzinfo=None)
	end_tstamp = end_date.replace(tzinfo=None)
	sites = len(elem_paths)
	logging.info(f"Looping through {sites} sites")
	for index, path in enumerate(elem_paths): # Loop through each element in list
		for start, end in dates: # Iterate through list of start and end times
			body = {"startTime": start,
					"endTime": end,
					"resolution": resolution,
					"attributes": attr,
					"ids": path}
			# Use POST to avoid hitting max URL length w/ many params
			
			logging.info(f"Sending post request for {index+1}/{sites}")
			try:
				r = requests.post(dataURL, headers=headers, data=body).json()
			except requests.exceptions.RequestException as e:
				logging.info('Request failed with status code {}'.format(e))				
				raise AirflowException('Request failed with status code {}'.format(e))

			results[path] += r['assets'][0]['attributes'][0]['values'][1:] # Append readings for this time period to list of readings for this element

		num_vals = len(results[path])

	tstamps = pd.date_range(start_tstamp, end_tstamp, periods=num_vals)
	df_5min = pd.DataFrame(index=tstamps, data=results)
	
	logging.info(f'API returned {df_5min.shape[0]} rows')

	return df_5min

#: DAG Function
def update_pv_prod(**context):
	currTime = context['execution_date'].in_timezone('America/Los_Angeles')
	hourly_file = conf['temp_data_dir'] + '/pv_hourly_results.csv'
	prod_hourly_file = conf['prod_data_dir'] + '/pv_hourly_production.csv'
	build_production_files(prod_hourly_file, hourly_file)
	
	if currTime.hour in [15,16]:
		prod_file = conf['prod_data_dir'] + '/pv_production.csv'
		temp_file = conf['temp_data_dir'] + '/pv_daily_results.csv'
		build_production_files(prod_file, temp_file)
	
	return f"Successfully wrote production files"

#: Helper Function
def build_production_files(prod_file, temp_file, **context):
	logging.info("Reading prod and temp files")
	df_prod = pd.read_csv(prod_file,low_memory=False,index_col=0,parse_dates=True)
	df_temp = pd.read_csv(temp_file,low_memory=False,index_col=0,parse_dates=True)
	df_prod = pd.concat([df_prod,df_temp])
	# Index is timestamp
	# Group by timestamp to deduplicate by keeping first
	df_prod = df_prod.groupby(df_prod.index).first()
	df_prod = df_prod.round(decimals=3)
	general.pos_write_csv(df_prod, prod_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_prod.shape[0]
	logging.info(f'Writing to production {results} rows in {prod_file}')
	return f"Successfully wrote prod file with {results} records"

#: DAG Function
def get_lucid_token(**context):
	url = "https://api.buildingos.com/o/token/"
	payload = f'client_id={LUCID_USER}&client_secret={LUCID_PASS}&grant_type=client_credentials'
	headers = {'Content-Type': 'application/x-www-form-urlencoded'}
	response = requests.request("POST", url, headers=headers, data = payload)
	token_data = json.loads(response.text)
	token = token_data['access_token']
	logging.info('Successfully got access_token  ' + str(token) + ' from Lucid')
	return token

#: DAG Function
def push_lucid_data(**context):

	meter_credentials_list = [
					("90822aa2575a11ea978002420aff27ae",'34893','Serra Mesa-Kearny Mesa Library'),
					("b49fced28f1111ea84e802420aff2b28",'37643','Malcolm X Library'),
					("4131e5c68f1a11eaa69002420aff2b29",'37693','Point Loma Library')
					  ]	

	task_instance = context['task_instance']
	token = task_instance.xcom_pull(task_ids='get_lucid_token')
	logging.info('Lucid token is {}'.format(token))

	response_logged = []
	response_flag = False

	for meter_credentials in meter_credentials_list:
		payload_str, meter_url, meter_name = meter_credentials
		df_payload = pd.read_csv(conf['temp_data_dir'] + '/pv_hourly_results.csv')
		df_payload = df_payload[['Timestamp',meter_name]]
		logging.info("df payload is {}".format(df_payload))
		temp = df_payload.values.tolist()		
		payload = ("""{\"meta\":{\"naive_timestamp_utc\":false},\"data\":{\"""" + '{}'.format(payload_str) +'\":')
		payload_f = payload+str(json.dumps(temp))+'}}'
		url = ("https://api.buildingos.com/gateways/" + '{}'.format(meter_url) + '/data/')
		headers = {'Content-Type': 'application/json','Authorization': f'Bearer {token}'}
		response = requests.request("POST", url, headers=headers, data = payload_f)
		results = df_payload.shape[0]
		logging.info("Response was {}".format(response))
		response_logged.append([meter_name,response.status_code])
		logging.info('Attempting to write to Lucid ' + str(results) + ' rows of data')

	for responses in response_logged:
		logging.info('Respones for {} was {}\n'.format(responses[0],responses[1]))
		if responses[1] != 200:
			response_flag = responses[1]
			logging.info("Failed to upload data for {}".format(responses[0]))

	if response_flag == True:		
		pass
	else:
		return f"Successfully wrote to Lucid with {results} records"

#: DAG Function
def check_upload_time(**context):
	currTime = context['execution_date'].in_timezone('America/Los_Angeles')
	if currTime.hour in [15,16]:
		logging.info('Calling downstream tasks, hour is: {}'.format(currTime.hour))
		return True
	else:
		logging.info('Skipping downstream tasks, hour is: '.format(currTime.hour))
		return False
