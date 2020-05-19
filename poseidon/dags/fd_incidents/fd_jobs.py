import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import pymssql
from airflow.hooks.mssql_hook import MsSqlHook
from trident.util import general

conf = general.config
cur_yr = general.get_year()
prod_file = f"{conf['prod_data_dir']}/fd_incidents_{cur_yr}_datasd_v1.csv"
            
def get_fd_data( **kwargs):
	"""Get fire department data from Data Base."""
	
	logging.info("Get fire department data from CAD archive")
	
	
	fd_query = general.file_to_string('./sql/fd.sql', __file__)
	fd_conn = MsSqlHook(mssql_conn_id='fire_department')

	logging.info("Read data to panda DataFrame")
	
	df = fd_conn.get_pandas_df(fd_query)
	# Write csv
	logging.info('Writing ' + prod_file)
	general.pos_write_csv(
		df, prod_file, date_format=conf['date_format_ymd_hms'])
		
	return "Successfully wrote prod file at " + prod_file

