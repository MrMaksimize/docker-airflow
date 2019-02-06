import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import cx_Oracle
from poseidon.util import general

conf = general.config
fiscal_yr =  general.get_FY_year()
credentials = general.source['cip']


prod_file = conf['prod_data_dir'] + '/cip_{0}_datasd.csv'.format(fiscal_yr)
            

def get_cip_data(**kwargs):
	"""Get CIP data from Data Base."""
	
	logging.info("Get CIP data from Oracle DataBase")
	
	
	cip_query = general.file_to_string('./sql/cip.sql', __file__)
	cip_conn= cx_Oracle.connect(credentials)


	logging.info("Read data to Panda DataFrame")
	df = pd.read_sql_query(cip_query, cip_conn)

	rows = df.shape[0]
	
	# Write csv
	logging.info('Writing {} rows to prod'.format(rows))
	general.pos_write_csv(
		df, prod_file, date_format=conf['date_format_ymd_hms'])
		
	return "Successfully wrote prod file"

