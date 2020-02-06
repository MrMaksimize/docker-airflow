import datetime 
import requests
import json 
import numpy as np
import pandas as pd
import logging

from trident.util import general, power

conf = general.config

conf['prod_data_dir']
conf['temp_data_dir']

PRIMARY_KEY = general.config['pf_api_key']
temp_file = conf['temp_data_dir'] + '/pv_daily_results.csv'
prod_file = conf['prod_data_dir'] + '/pv_production.csv'
hourly_file = conf['temp_data_dir'] + '/pv_hourly_results.csv'
prod_hourly_file = conf['prod_data_dir'] + '/pv_hourly_production.csv'

#API Call
def get_pv_data_write_temp(**context):
	# 2000.05.088 = Sierra/Kearney Mesa Library, 2000.05.073 = Malcolm X Library, 2000.06.006 = Pt. Loma Library
	p = power.Power(PRIMARY_KEY)

	pv_sites = {'2000.05.066.SWG01.MTR01': 'Carmel Valley Rec Center', 
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

	currHour = datetime.datetime.now()
	currHour = currHour.hour

	print("CURRENT HOUR IS: ",currHour)

	if currHour in [0,1]:
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

		startDate = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime("%Y-%m-%d %H:%M:00")
		endDate = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:00")

		startDate = general.utc_to_pst(startDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")
		endDate = general.utc_to_pst(endDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")

		df_5min, df_15min = p.get_data(startDate, endDate, elem_paths, attr, True)

		df_5min = df_5min.rename(columns=pv_sites)
		df_15min = df_15min.rename(columns=pv_sites)
		df_15min.index.name = 'Timestamp'

		general.pos_write_csv(df_15min, temp_file, index=True, date_format=conf['date_format_ymd_hms'])

	startDate = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:00")
	endDate = (datetime.datetime.now() - datetime.timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:00")
	startDate = general.utc_to_pst(startDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")
	endDate = general.utc_to_pst(endDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")

	elem_paths = ['2000.05.088.SWG01.MTR01']

	print("CALLING API WITH {} {} {} {}".format(startDate, endDate, elem_paths, attr))

	df_5min, df_15min = p.get_data(startDate, endDate, elem_paths, attr, True)

	df_5min = df_5min.rename(columns={'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa Library'})
	df_15min = df_15min.rename(columns={'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa Library'})
	df_15min.index.name = 'Timestamp'

	general.pos_write_csv(df_15min, hourly_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_15min.shape[0]

	return f"Successfully wrote temp file with {results} records"

#Make prod csv
def update_pv_prod(**context):
	df_prod = pd.read_csv(prod_file,low_memory=False,index_col=0)
	df_temp = pd.read_csv(temp_file,low_memory=False,index_col=0)
	df_prod = pd.concat([df_prod,df_temp])
	df_prod = df_prod.groupby(df_prod.index).first()
	general.pos_write_csv(df_prod, prod_file, index=True, date_format=conf['date_format_ymd_hms'])

	df_prod = pd.read_csv(prod_hourly_file,low_memory=False,index_col=0)
	df_temp = pd.read_csv(hourly_file,low_memory=False,index_col=0)
	df_prod = pd.concat([df_prod,df_temp])
	df_prod = df_prod.groupby(df_prod.index).first()
	general.pos_write_csv(df_prod, prod_hourly_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_prod.shape[0]

	logging.info('Writing ' + str(df_prod.shape[0]) + ' rows')

	return f"Successfully wrote prod file with {results} records"

#Push to Lucid

#Helper Functions

#TODO (THIS HAS NOT BEEN TESTED)
def API_call(start_date, end_date, elem_paths, attr, two_hours=False, resolution="raw", fp=None):
	    baseurl = 'https://api.powerfactorscorp.com' 
        headers = {"Ocp-Apim-Subscription-Key": PRIMARY_KEY}

        dataURL = baseurl + '/drive/v2/data'
        results = {path: [] for path in elem_paths} # To store values for each element
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
                r = requests.post(dataURL, headers=headers, data=body).json() # Use POST to avoid hitting max URL length w/ many params
                results[path] += r['assets'][0]['attributes'][0]['values'][1:] # Append readings for this time period to list of readings for this element
                if start_tstamp is None: # Get the start timestamp of the entire time period
                    start_tstamp = pd.to_datetime(r['assets'][0]['startTime'][:19])+dt.timedelta(minutes=5)  
                
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

#TO DO (THIS NEEDS TO BE REWRITTEN)
def API_to_csv():
	startDate = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:00")
	endDate = (datetime.datetime.now() - datetime.timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:00")
	startDate = general.utc_to_pst(startDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")
	endDate = general.utc_to_pst(endDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")

	elem_paths = ['2000.05.088.SWG01.MTR01']

	print("CALLING API WITH {} {} {} {}".format(startDate, endDate, elem_paths, attr))

	df_5min, df_15min = p.get_data(startDate, endDate, elem_paths, attr, True)

	df_5min = df_5min.rename(columns={'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa Library'})
	df_15min = df_15min.rename(columns={'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa Library'})
	df_15min.index.name = 'Timestamp'

	general.pos_write_csv(df_15min, hourly_file, index=True, date_format=conf['date_format_ymd_hms'])