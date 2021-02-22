import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import cx_Oracle
from trident.util import general
from airflow.hooks.base_hook import BaseHook

conf = general.config
fiscal_yr =  general.get_FY_year()
prod_file = f"{conf['prod_data_dir']}/cip_{fiscal_yr}_datasd_v1.csv"
            

def get_cip_data(**kwargs):
	"""Get CIP data from Data Base."""

	logging.info("Get CIP data from Oracle DataBase")

	cip_query = general.file_to_string('./sql/cip.sql', __file__)
	credentials = BaseHook.get_connection(conn_id="CIP")
	conn_config = {
		'user': credentials.login,
		'password': credentials.password
	}

	dsn = credentials.extra_dejson.get('dsn', None)
	sid = credentials.extra_dejson.get('sid', None)
	port = credentials.port if credentials.port else 1521
	conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, sid)

	db = cx_Oracle.connect(conn_config['user'],
		conn_config['password'],
		conn_config['dsn'],
		encoding="UTF-8")

	logging.info("Read data to Panda DataFrame")
	
	df = pd.read_sql_query(cip_query, cip_conn)

	rows = df.shape[0]

	# Write csv
	logging.info(f'Writing {rows} rows to prod')
	general.pos_write_csv(
		df, prod_file, date_format="%Y-%m-%d %H:%M:%S")

	return "Successfully wrote prod file"

