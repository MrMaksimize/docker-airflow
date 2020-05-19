""" Data quality utilities """
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import logging

def date_range(df,time=False):
	""" Take a subset of datetime columns
	 Must already be in datetime format
	 return a dataframe with date ranges
	 IF the column includes timestamp, pass time=True """

	values = {}

	for column in df:
		try:
			if time:
				max_date = df[column].max()
				min_date = df[column].min()
				
			else:
				max_dates = df[column].max().date()
				min_dates = df[column].min().date()

			values[column] = {'min_date':min_date,'max_date':max_date}
		
		except Exception as e:
			logging.info(f"Date range op failed on {column} because {e}")

	
	return values

def unique_values_list(df,limit=15):
	""" Take a subset of string columns
	 Compile unique values
	 Return a string list per field """

	logging.info(f"Checking for string lists in {df.columns}")
	values = {}

	df = df.dropna(axis=1,how='all')

	for column in df:
		try:
			uniques = df[column].unique()
			if len(uniques) <= limit:
				no_na = [str(x) for x in uniques if x not in (None, '', 'nan', ' ')]
				string_final = ', '.join(str(x) for x in no_na)
				# Blank cols may not have dropped if na was filled with blanks
				# When the column was converted from a float
				if string_final != '':
					values[column] = string_final
		except Exception as e:
			logging.info(f"Uniques failed on {column} because {e}")

	return values

def number_stats(df):
	""" Take a subset of numeric columns """
	""" return a dataframe with numeric descriptions """

	values = {}

	for column in df:
		try:
			max_no = df[column].max()
			min_no = df[column].min()
			mean_no = df[column].mean()
			median_no = df[column].median()
			values[column] = {'min_no':min_no,'max_no':max_no,'mean_no':mean_no,'median_no':median_no}
		except Exception as e:
			logging.info(f"Number ranges failed on {column} because {e}")

	return values

def geo_bounds(df):
	""" Take a single pair of lat lng cols
	 return a string with the geospatial boundary """

	lat_cols = [col for col in df.columns if 'lat' in col]
	lng_cols = [col for col in df.columns if 'lng' in col]
	latitude = lat_cols[0]
	longitude = lng_cols[0]
	df_bounds = df.loc[(df[latitude].notnull() & df[longitude].notnull()),[latitude,longitude]]

	try:
	    df_bounds[latitude] = pd.to_numeric(df_bounds[latitude],errors='coerce')
	    df_bounds[longitude] = pd.to_numeric(df_bounds[longitude],errors='coerce')
	    df_bounds['geometry'] = df_bounds.apply(lambda z: Point(z[longitude], z[latitude]), axis=1)
	    gdf = gpd.GeoDataFrame(df_bounds)
	    bounds = gdf['geometry'].total_bounds
	    bounds_str = ','.join(map(str, bounds))
	    
	except Exception as e:
		logging.info(f"Geo bounds failed because {e}")
		bounds_str = ''

	if bounds_str == '':
		return None
	else:
		return bounds_str

def float_to_string(df):
	""" Accepts a series that should be string but is float """
	
	df = df.fillna(-999999.0).astype(np.int64)
	df = df.replace(-999999,'')
	df = df.astype(str)

	return df

def update_possible_values(df,
						   string_vals={},
						   date_vals={},
						   geo_val='',
						   num_vals={}
						   ):
	""" Take output from functions above and update data dictionary """
	df['possible_values'] = ''

	if string_vals:
		string_cols = string_vals.keys()
		for field in string_cols:
			df.loc[df['field'] == field,'possible_values'] = string_vals[field]

	if num_vals:
		num_cols = num_vals.keys()
		for field in num_cols:
			df.loc[df['field'] == field,'possible_values'] = f"{str(num_vals[field]['min_no'])}/{str(num_vals[field]['max_no'])}"

	if geo_val != '':
		df.loc[(df['field'].str.contains('lat'))|(df['field'].str.contains('lng')),'possible_values'] = geo_val

	if date_vals:
		date_cols = date_vals.keys()
		for field in date_cols:
			df.loc[df['field'] == field,
	'possible_values'] = f"{str(date_vals[field]['min_date'])}/{str(date_vals[field]['max_date'])}"

	return df
