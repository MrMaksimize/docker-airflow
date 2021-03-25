""" Fleet VMT _jobs file """

# Required imports

from trident.util import general
import logging

# Required variables

conf = general.config

# Optional imports depending on job

# -- Imports for connecting to something

import ftplib
import subprocess
from subprocess import Popen, PIPE
import requests
import glob
import os
from shlex import quote
from urllib.parse import quote_plus

# -- Imports for transformations

import pandas as pd
import numpy as np
import math
from pandas.tseries.frequencies import to_offset

from collections import OrderedDict
import json

import string
import re #regex

from datetime import datetime as dt
from datetime import timedelta
import time
from dateutil.parser import parse
import pendulum # This is the date library Airflow uses with context

from airflow.hooks.base_hook import BaseHook

prod_path = conf['prod_data_dir']
temp_path = conf['temp_data_dir']

# Generic functions called in template dags


#: DAG function
def download_calamp_daily(**context):
    """ 
    Download files from CalAmp FTP
    """
    today = context['next_execution_date'].in_tz(tz='US/Pacific')
    # Exec date returns a Pendulum object
    # Runs at 10pm the night before and has today's date
    yesterday = today.subtract(days=1)
    dates = [f"{today.year}{today.strftime('%m')}{today.strftime('%d')}",
    f"{yesterday.year}{yesterday.strftime('%m')}{yesterday.strftime('%d')}"]

    ftp_conn = BaseHook.get_connection(conn_id="CALAMP")
    extras = ftp_conn.extra_dejson
    password = quote_plus(extras.get('password'))
    
    for day in dates:

        logging.info(f"Looking for file for {day}")

        # MUST use curl for future needed SFTP support
        command = f"curl -o {temp_path}/calamp_{day}.csv " \
                + f"sftp://{ftp_conn.login}:{password}" \
                + f"@sftpgoweb.calamp.com/mnt/array1/SanDiego_FTP/Databases/" \
                + f"{day}.csv " \
                + f"--insecure"

        command = command.format(quote(command))
        logging.info(command)

        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
    
        if p.returncode != 0:
            logging.info(error)
            raise Exception(p.returncode)

    return dates

#: DAG function
def process_calamp_daily(**context):
    """ Reading in daily temp file and processing """

    dates = context['task_instance'].xcom_pull(dag_id="fleet_vmt",
        task_ids='get_calamp')

    today = pd.read_csv(f"{temp_path}/calamp_{dates[0]}.csv",
        low_memory=False
        )
    yesterday = pd.read_csv(f"{temp_path}/calamp_{dates[1]}.csv",
        low_memory=False
        )

    logging.info(today.shape)
    logging.info(yesterday.shape)

    # Do more stuff
    tempdata = Path('/Users/mwesterfield/Documents/GitHubRepos/poseidon-airflow/data/temp/')

    # Code to calculate rows to append to homebase file
    START_DATE = datetime(2021, 2, 18)  # First file date
    END_DATE = datetime(2021, 2, 18)    # Last file date
    file2_date = START_DATE
    while file2_date <= END_DATE:
        # process one single day of data (needs two .csv files to do it)
        data_date = file2_date - timedelta(days=2)
        file1_date = file2_date - timedelta(days=1)
        file1_name = file1_date.strftime('%Y%m%d') + '.csv'
        file1 = pd.read_csv(tempdata / file1_name, names=['intHVSID', 'intVID', 'strVName', 'strVAlias',
            'strGroupName', 'dtTime', 'dtTimeFix', 'dblLat', 'dblLon', 'dblAlt', 'dblSpeed', 'dblHeading',
            'strHeading', 'strLocation', 'strLMGFLocation', 'intLocationType', 'strDescription',
            'strVStatus', 'tintGPSStatus', 'tintDOP', 'tintFixStatus', 'tintUnitStatus'])

        file2_name = file2_date.strftime('%Y%m%d') + '.csv'
        file2 = pd.read_csv(tempdata / file2_name, names=['intHVSID', 'intVID', 'strVName', 'strVAlias',
            'strGroupName', 'dtTime', 'dtTimeFix', 'dblLat', 'dblLon', 'dblAlt', 'dblSpeed', 'dblHeading',
            'strHeading', 'strLocation', 'strLMGFLocation', 'intLocationType', 'strDescription',
            'strVStatus', 'tintGPSStatus', 'tintDOP', 'tintFixStatus', 'tintUnitStatus'])
 
        oneDay = pd.concat([file1, file2], ignore_index=True)
        oneDay['strVName'] = oneDay['strVName'].astype(str)
        oneDay['strVName'] = oneDay['strVName'].str.strip()
        oneDay['strGroupName'] = oneDay['strGroupName'].str.strip()
        oneDay['strLocation'] = oneDay['strLocation'].str.strip()
        oneDay['strLMGFLocation'] = oneDay['strLMGFLocation'].str.strip()
        oneDay['strDescription'] = oneDay['strDescription'].str.strip()

        # drop all unneeded columns
        oneDay = oneDay.drop(['intHVSID', 'intVID', 'strVAlias', 'strGroupName', 'dtTimeFix',
                               'dblAlt', 'dblSpeed', 'dblHeading', 'strHeading', 'intLocationType',
                               'strVStatus', 'tintGPSStatus', 'tintDOP', 'tintFixStatus', 'tintUnitStatus'], axis=1)

        # if the strLMGFLocation value is missing, replace it with the strLocation from the same event
        oneDay.loc[~oneDay['strLMGFLocation'].str.upper().str.isupper(), 'strLMGFLocation'] = oneDay['strLocation']

        # replace NaN's in string columns with ""
        oneDay[['strLocation', 'strLMGFLocation', 'strDescription']] = \
            oneDay[['strLocation', 'strLMGFLocation', 'strDescription']].fillna(value="")

        # add Vehicle_On column; set all "Ignition OFF" events to Vehicle_On = 0
        oneDay['Ignition_On'] = 1
        oneDay.loc[oneDay['strDescription'].str.contains("Ignition Off"), 'Ignition_On'] = 0

        # Set timestamp as index, convert from GMT to US/Pacific time, and grab single 24-hr period out of middle
        oneDay['dtTime'] = pd.to_datetime(oneDay['dtTime'])
        oneDay = oneDay.set_index('dtTime')
        oneDay.index = oneDay.index.tz_localize('UTC').tz_convert('US/Pacific')
        oneDay.index = oneDay.index.tz_localize(None)
        oneDay = oneDay.loc[(oneDay.index >= data_date) & (oneDay.index < file1_date)]

        # Get list of all vehicles that moved that day
        vehicle_list = oneDay['strVName'].unique().tolist()
        # select a single vehicle
        for vehicle in vehicle_list:
            #vehicle = '201246'
            vehicle_oneDay = oneDay.copy()
            vehicle_oneDay = vehicle_oneDay[vehicle_oneDay['strVName'] == vehicle]
            # resample to 1 minute samples
            # first round all timestamps to the nearest minute
            vehicle_oneDay.index = vehicle_oneDay.index.round("min")

            # If there aren't any strDescription events, set all Ignition_On = 0
            if vehicle_oneDay['strDescription'].str.upper().str.isupper().sum() == 0:
                vehicle_oneDay['Ignition_On'] = 0

            # this removes events with exact same datetime stamp; but gives priority to Ignition_On == 0 events
            vehicle_oneDay = vehicle_oneDay.sort_values(by='Ignition_On', ascending=True).groupby(level=0).first()

            # Add a column calculated from Lat/Long data. The value will be identical for locations that
            # are close (like within a block) even if not exactly equal
            vehicle_oneDay['lat_lon_combo'] = round(vehicle_oneDay['dblLat'] / 1000) * 1000 + \
                                                 abs(round(vehicle_oneDay['dblLon'] / 1000)) * 1000
            # Upsample to freq=1minute, whole day
            beg_day = data_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_day = beg_day + timedelta(minutes=1439)
            t_index = pd.DatetimeIndex(pd.date_range(start=beg_day, end=end_day, freq='1min'))
            vehicle_resample = vehicle_oneDay.resample('1min', origin='start_day', closed='right').ffill()
            vehicle_resample = vehicle_resample.reindex(t_index).fillna(method='ffill')

            # backfill to start of day (midnight)
            # 'Ignition_On' = opposite of first recorded value
            vehicle_resample[['strVName', 'dblLat', 'dblLon', 'strLocation', 'strLMGFLocation', 'lat_lon_combo']] = \
                vehicle_resample[['strVName', 'dblLat', 'dblLon',
                                  'strLocation', 'strLMGFLocation', 'lat_lon_combo']].fillna(method='bfill')

            # store the (original) Vehicle_On values in a separate df
            ignition_events = vehicle_oneDay['Ignition_On']
            ignition_events = ignition_events.loc[(ignition_events.index >= beg_day) & (ignition_events.index <= end_day)]

            # reset upsampled Ignition_On to original values. Then fills in all NaN values with
            # Ignition_On = 0 because CalAMP naturally samples at 1 min whenever the vehicle is on
            # (so if the vehicle was on, the sample should already be there)
            vehicle_resample['Ignition_On'] = np.nan
            vehicle_resample.loc[ignition_events.index, 'Ignition_On'] = ignition_events.to_list()
            # Carry Ignition_On values forward for one NaN to smooth over missing sample points
            vehicle_resample['Ignition_On'] = vehicle_resample['Ignition_On'].fillna(method='ffill', limit=1)
            # Remaining NaN values are Ignition_On = 0
            vehicle_resample['Ignition_On'] = vehicle_resample['Ignition_On'].fillna(0)

            # remove location when vehicle is ON
            vehicle_resample.loc[
                vehicle_resample['Ignition_On'] == 1, ['dblLat', 'dblLon', 'strLocation', 'strLMGFLocation', 'lat_lon_combo']] = ""
            # calculate daily use stats
            # ---- Calculate this day's use pattern (15 min intervals)
            resamp15 = vehicle_resample[['strVName', 'Ignition_On']].copy()
            resamp15 = resamp15.resample('15T').max()
            resamp15.index.name = 'datetime'
            resamp15 = resamp15.reset_index()
            resamp15.rename(columns={'strVName': 'Vehicle Number', 'Ignition_On': 'Engine_On'}, inplace=True)
            resamp15 = resamp15[['Vehicle Number', 'datetime', 'Engine_On']]
            tmp_filename = tmpdata / 'tmp_daily_driving_pattern.csv'
            write_df_to_csv(tmp_filename, resamp15)

            # ---- Calculate homebase, hours_on, hours_in_use ----
            total_on_hrs = (vehicle_resample['Ignition_On'].sum()) / 60

            # Use sub-df with only vehicle off events to find "homebase"
            vehicle_off = vehicle_resample.copy()
            vehicle_off = vehicle_off[vehicle_off['Ignition_On'] == 0]

            # Defining 'homebase' as the most frequent 'Vehicle Off' location that exceeded total of 6 hrs
            # (doesn't have to be consecutive). First look at strLMGF, then Lat/Long;
            # whichever meets criteria first wins
            if vehicle_off.empty or (~vehicle_off['strLMGFLocation'].isna()).sum() == 0 or \
                (~vehicle_off['dblLat'].isna()).sum() == 0:
                total_homebase_hrs = 0
                total_hrs_in_use = 0
                homebase_Location = ""
                homebase_dblLat = ""
                homebase_dblLon = ""
            else:
                if (vehicle_off['strLMGFLocation'].value_counts()[0] >= 360) and \
                        (vehicle_off['strLMGFLocation'].value_counts().index[0] != ""):
                    # set location
                    homebase_Location = vehicle_off['strLMGFLocation'].value_counts().index[0]
                    homebase_dblLat = \
                        vehicle_off.loc[vehicle_off['strLMGFLocation'] == homebase_Location, 'dblLat'].value_counts().index[0]
                    homebase_dblLon = \
                        vehicle_off.loc[vehicle_off['strLMGFLocation'] == homebase_Location, 'dblLon'].value_counts().index[0]
                    total_homebase_hrs = (vehicle_off['strLMGFLocation'].value_counts()[0]) / 60
                    # calculate time spent away from location
                    if vehicle_resample['Ignition_On'].sum() > 0:
                        total_hrs_in_use = (1440 - vehicle_off['strLMGFLocation'].value_counts()[0]) / 60
                    else:
                        total_hrs_in_use = 0
                elif (vehicle_off['lat_lon_combo'].value_counts().iloc[0] >= 360) and \
                        (vehicle_off['lat_lon_combo'].value_counts().index[0] != ""):
                    homebase_combo = vehicle_off['lat_lon_combo'].value_counts().index[0]
                    homebase_dblLat = vehicle_off.loc[vehicle_off['lat_lon_combo'] == homebase_combo, 'dblLat'].value_counts().index[0]
                    homebase_dblLon = vehicle_off.loc[vehicle_off['lat_lon_combo'] == homebase_combo, 'dblLon'].value_counts().index[0]
                    homebase_Location = \
                        vehicle_off.loc[vehicle_off['lat_lon_combo'] == homebase_combo, 'strLMGFLocation'].value_counts().index[0]
                    total_homebase_hrs = (vehicle_off['lat_lon_combo'].value_counts().iloc[0]) / 60
                    # calculate time spent away from location
                    if vehicle_resample['Ignition_On'].sum() > 0:
                        total_hrs_in_use = (1440 - vehicle_off['lat_lon_combo'].value_counts().iloc[0]) / 60
                    else:
                        total_hrs_in_use = 0
                else:
                    total_homebase_hrs = 0
                    total_hrs_in_use = 0
                    homebase_Location = ""
                    homebase_dblLat = ""
                    homebase_dblLon = ""
            # END of define homebase code

            tmp_filename = tmpdata / 'tmp_daily_hours.csv'
            one_row = [vehicle, data_date, total_on_hrs, total_hrs_in_use]
            write_to_csv(tmp_filename, one_row)

            tmp_filename = tmpdata / 'tmp_daily_homebase.csv'
            one_row = [vehicle, data_date, homebase_Location, homebase_dblLat, homebase_dblLon, total_homebase_hrs]
            write_to_csv(tmp_filename, one_row)
        # END of single vehicle loop

        # increment counter (and go back to beginning of WHILE loop)
        print("Completed file2 = ", file2_date)
        file2_date = file2_date + timedelta(days=1)  # end of while loop

    #df_homebase = vehicle_homebase(df)

    return "Process daily calamp records into data files"


#: DAG function
def process_daily_file(df_orig):
    df_orig['Vehicle Number'] = df_orig['Vehicle Number'].astype(str)
    df_orig['Date'] = pd.to_datetime(df_orig['Date'])
    # deduplicate, if necessary
    df_orig.drop_duplicates(inplace=True)

    beg_day = df_orig['Date'].min()
    end_day = df_orig['Date'].max()
    t_index = pd.DatetimeIndex(pd.date_range(start=beg_day, end=end_day, freq='D'))

    df_proc = pd.DataFrame()
    vehicle_list = df_orig['Vehicle Number'].unique().tolist()
    # select a single vehicle
    for vehicle in vehicle_list:
        #vehicle = '718082'
        oneVehicle = df_orig.copy()
        oneVehicle = oneVehicle[oneVehicle['Vehicle Number'] == vehicle]

        # resample code
        cols = oneVehicle.columns.tolist()  # preserve original column order
        oneVehicle_resamp = oneVehicle.copy()
        oneVehicle_resamp = oneVehicle_resamp.set_index('Date')
        oneVehicle_resamp = oneVehicle_resamp.sort_index()
        oneVehicle_resamp = oneVehicle_resamp.reindex(t_index)
        oneVehicle_resamp['Vehicle Number'] = vehicle
        oneVehicle_resamp.index.name = 'Date'
        oneVehicle_resamp = oneVehicle_resamp.reset_index()
        oneVehicle_resamp = oneVehicle_resamp[cols]

        # append single vehicle to larger dataframe
        df_proc = df_proc.append(oneVehicle_resamp, ignore_index=True)
    # END of loop through vehicles

    logging.info(df_proc.head())
    logging.info(df_proc.tail())
    return df_proc

def process_weekly_file(df_orig):
    #df_orig['Vehicle Number'] = df_orig['Vehicle Number'].astype(str)
    df_orig['Date'] = pd.to_datetime(df_orig['Date'])
    # deduplicate, if necessary
    df_orig.drop_duplicates(inplace=True)

    beg_day = df_orig['Date'].min()
    end_day = df_orig['Date'].max() + to_offset("1W")
    t_index = pd.DatetimeIndex(pd.date_range(start=beg_day, end=end_day, freq='W-SAT'))
    # The next line is because using resamp with any other day than the default
    # makes the labels show the END date of the interval. Solution found at
    # https://stackoverflow.com/questions/30989224/python-pandas-dataframe-resample-daily-data-to-week-by-mon-sun-weekly-definition/46712821
    t_index = t_index - to_offset("1W")

    df_proc = pd.DataFrame()
    vehicle_list = df_orig['Vehicle Number'].unique().tolist()
    # select a single vehicle
    for vehicle in vehicle_list:
        # vehicle = '201246'
        oneVehicle = df_orig.copy()
        oneVehicle = oneVehicle[oneVehicle['Vehicle Number'] == vehicle]

        # resample code
        cols = oneVehicle.columns.tolist()  # preserve original column order
        oneVehicle_resamp = oneVehicle.copy()
        # one more deduplicate pass
        oneVehicle_resamp = oneVehicle_resamp.sort_values('avg_daily_vmt', ascending=False).drop_duplicates('Date')
        oneVehicle_resamp = oneVehicle_resamp.set_index('Date')
        oneVehicle_resamp = oneVehicle_resamp.sort_index()
        oneVehicle_resamp = oneVehicle_resamp.reindex(t_index)
        oneVehicle_resamp['Vehicle Number'] = vehicle
        oneVehicle_resamp.index.name = 'Date'
        oneVehicle_resamp = oneVehicle_resamp.reset_index()
        oneVehicle_resamp = oneVehicle_resamp[cols]

        # append single vehicle to larger dataframe
        df_proc = df_proc.append(oneVehicle_resamp, ignore_index=True)

    return df_proc

def cleanup_calamp_temp_files_daily(**context):
    """ Resamples daily tmp files to add NaNs to all missing days; adds column
    names because the original temp files lack those; saves new file in prod  """

    # -- tmp_daily_hours --
    logging.info('Reading temp_daily_hours')
    df_orig = pd.read_csv(f'{temp_path}/tmp_daily_hours.csv',
                          names=['Vehicle Number', 'Date', 'hrs_on', 'hrs_in_use'], low_memory=False)
    logging.info('Starting daily function')
    df_proc = process_daily_file(df_orig)
    df_proc.to_csv(f'{prod_path}/dev_daily_hours.csv', index=False)

    # -- tmp_avg_daily_VMT --
    logging.info('Reading temp_avg_daily_VMT')
    df_orig = pd.read_csv(f'{temp_path}/tmp_avg_daily_VMT.csv' ,low_memory=False)
    df_proc = process_weekly_file(df_orig)
    df_proc.to_csv(f'{prod_path}/dev_avg_daily_VMT.csv', index=False)

    # -- all done!
    logging.info('Finished cleaning up daily CalAmp files')