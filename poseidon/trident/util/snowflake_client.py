""" Utilities for connecting to Snowflake """
from trident.util import general
from shlex import quote
from datetime import datetime as dt
import logging

config = general.config
today = dt.today()

def format_stage_sql(file_name):
	"""
	Puts new files in internal Snowflake stage
	File name should equal the prefix of the file
	That is common for all file subsets
	As well as name of table in database
	"""
	query_string = f"PUT file://{config['prod_data_dir']}/" \
	+ f"{file_name}* @airflow_etl/{file_name}/" \
	+ f"{today.year}/{today.month}/{today.day}" \
	+ " overwrite=true" \

	query_string = query_string.format(quote(query_string))
	logging.info(query_string)

	return query_string

def format_delete_sql(table_name):
	"""
	Deletes rows in existing table
	For data that is overwritten and not appended 
	file_name should equal name of table
	"""
	query_string = f"DELETE FROM {table_name}"

	query_string = query_string.format(quote(query_string))

	return query_string

def format_copy_sql(table_name, file_name):
	"""
	Copies into table from internal Snowflake stage
	file_name should equal name of table
	"""
	query_string = f"COPY INTO {table_name} from " \
	+ f"@airflow_etl/{file_name}/" \
	+ f"{today.year}/{today.month}/{today.day}" \
	+ " on_error=abort_statement purge=true" \

	query_string = query_string.format(quote(query_string))

	return query_string

