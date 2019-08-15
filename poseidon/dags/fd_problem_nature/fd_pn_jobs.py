import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
from airflow.hooks.mssql_hook import MsSqlHook
from trident.util import general

conf = general.config

prod_file = conf['prod_data_dir'] + '/fd_problem_nature_agg_datasd_v1.csv'

def get_fd_data(**kwargs):
	"""Get fire department data from Data Base."""
	
	logging.info("Get fire department data from Data Base")
	fd_query = general.file_to_string('./sql/fd_pn.sql', __file__)
	fd_conn = MsSqlHook(mssql_conn_id='fire_department')
	
	logging.info("Read data to panda DataFrame")
	df = fd_conn.get_pandas_df(fd_query)

	df = df.rename(columns={'city':'address_city',
		'response_month':'month_response',
		'response_year':'year_response'
		})

	logging.info("Writing {} rows to prod".format(df.shape[0]))

	# Write csv
	general.pos_write_csv(df, prod_file)
	return "Successfully wrote prod file"

