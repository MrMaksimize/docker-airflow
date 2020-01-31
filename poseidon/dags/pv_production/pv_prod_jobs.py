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

startDate = (datetime.datetime.now() - datetime.timedelta(minutes=120)).strftime("%Y-%m-%d %H:%M:00")
endDate = (datetime.datetime.now() - datetime.timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:00")

startDate = general.utc_to_pst(startDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")
endDate = general.utc_to_pst(endDate,"%Y-%m-%d %H:%M:00","%Y-%m-%d %H:%M:00")

#API Call
def get_pv_data_write_temp(**context):
	# 2000.05.088 = Sierra/Kearney Mesa Library, 2000.05.073 = Malcolm X Library, 2000.06.006 = Pt. Loma Library
	p = power.Power(PRIMARY_KEY)	
	elem_paths = ['2000.05.088.SWG01.MTR01']
	attr = 'AC_POWER'
	df_5min, df_15min = p.get_data(startDate, endDate, elem_paths, attr, True)

	df_5min = df_5min.rename(columns={'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa'})
	df_15min = df_15min.rename(columns={'2000.05.088.SWG01.MTR01':'Serra Mesa-Kearny Mesa'})
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

