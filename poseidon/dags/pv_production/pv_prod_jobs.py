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
temp_file = conf['temp_data_dir'] + '/pv_hourly_results.csv'
prod_file = conf['prod_data_dir'] + '/pv_production.csv'

startDate = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime("%Y-%m-%d %H:%M:00")
endDate = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:00")

startDate = general.utc_to_pst(startDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")
endDate = general.utc_to_pst(endDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")

#API Call
def get_pv_data_write_temp(**context):
	# 2000.05.088 = Sierra/Kearney Mesa Library, 2000.05.073 = Malcolm X Library, 2000.06.006 = Pt. Loma Library
	p = power.Power(PRIMARY_KEY)

	print('THE PRIMARY KEY IS ',PRIMARY_KEY)

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
	df_5min, df_15min = p.get_data(startDate, endDate, elem_paths, attr, True)

	df_5min = df_5min.rename(columns=pv_sites)
	df_15min = df_15min.rename(columns=pv_sites)
	df_15min.index.name = 'Timestamp'

	general.pos_write_csv(df_15min, temp_file, index=True, date_format=conf['date_format_ymd_hms'])

	results = df_15min.shape[0]

	return f"Successfully wrote temp file with {results} records"

#Make prod csv
def update_pv_prod(**context):
	df_prod = pd.read_csv(prod_file,low_memory=False,index_col=0)
	df_temp = pd.read_csv(temp_file,low_memory=False,index_col=0)
	df_prod = pd.concat([df_prod,df_temp])
	df_prod = df_prod.groupby(df_prod.index).first()

	results = df_prod.shape[0]

	logging.info('Writing ' + str(df_prod.shape[0]) + ' rows')
	general.pos_write_csv(df_prod, prod_file, index=True, date_format=conf['date_format_ymd_hms'])

	return f"Successfully wrote prod file with {results} records"

#Push to Lucid

