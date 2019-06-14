"""waze _jobs file."""
from airflow.hooks.postgres_hook import PostgresHook
from trident.util import general
from geojson import LineString
from datetime import datetime
from pytz import timezone
from geomet import wkt
import os
import pandas as pd
import requests
import json
import logging


conf = general.config


def waze_jams_to_csv():
    """Waze jams feed to csv."""
    url = 'https://na-georss.waze.com/rtserver/web/TGeoRSS?'\
        + 'tk=ccp_partner&ccp_partner_name=SanDiego&format=JSON'\
        + '&types=traffic&polygon=-117.338791,32.515842;-116.763725,'\
        + '32.565044;-116.967316,32.741082;-116.924744,33.086939;'\
        + '-117.000618,33.165720;-117.141037,33.065075;-117.351837,'\
        + '32.969500;-117.338791,32.515842;-117.338791,32.515842'

    datetime_utc = datetime.now(timezone('UTC'))
    datetime_pst = datetime_utc.astimezone(timezone('US/Pacific'))
    date_pst = datetime_pst.strftime('%Y-%m-%d')
    timestamp_pst = datetime_pst.strftime('%Y-%m-%d %H:%M:%S')
    logfile = conf['prod_data_dir'] + '/{0}_waze_jams_datasd_v1.csv'.format(date_pst)
    tempfile = conf['temp_data_dir'] + '/waze_temp.csv'

    rows_csv = []
    rows_db = []

    cols = ['uuid', 'waze_timestamp', 'street', 'start_node',
            'end_node', 'city', 'length', 'delay', 'speed', 'level',
            'road_type', 'geom']

    fields_dict = {
            'uuid': 'uuid',
            'length': 'length',
            'delay': 'delay',
            'speed': 'speed',
            'level': 'level',
            'road_type': 'roadType',
            'city': 'city',
            'street': 'street',
            'start_node': 'startNode',
            'end_node': 'endNode'}

    r = requests.get(url)
    body = json.loads(r.content)
    jams = body['jams']

    for jam in jams:

        for key, value in fields_dict.iteritems():
            if value not in jam:
                exec("{0}='NaN'".format(key))
            else:
                exec("{0}=jam['{1}']".format(key, value))

        line = []
        coordinates = jam['line']
        for i in coordinates:
            lon = i['x']
            lat = i['y']
            xy = (lon, lat)
            line.append(xy)
        line = LineString(line)
        line_wkt = wkt.dumps(line, decimals=6)
        line_db = 'SRID=4326;' + line_wkt

        row_csv = [uuid, timestamp_pst, street, start_node, end_node,
                   city, length, delay, speed, level, road_type, line_wkt]

        row_db = [uuid, timestamp_pst, street, start_node, end_node, city,
                  length, delay, speed, level, road_type, line_db]

        for el in row_db:
            if isinstance(el, basestring):
                el.encode('utf-8')

        rows_csv.append(row_csv)
        rows_db.append(row_db)

    logging.info('Saving Waze data to temp csv file.')
    temp_df = pd.DataFrame(data=rows_db, columns=cols)
    general.pos_write_csv(temp_df, tempfile, header=False)

    logging.info('Saving Waze data to daily csv log.')
    if not os.path.exists(logfile):
        df = pd.DataFrame(data=rows_csv, columns=cols)
        general.pos_write_csv(df, logfile)
    else:
        log_df = pd.read_csv(logfile)
        df = pd.DataFrame(data=rows_csv, columns=cols)
        log_df = log_df.append(df, ignore_index=True)
        general.pos_write_csv(log_df, logfile)

    return 'Successfully saved data to csv.'


def waze_jams_to_db():
    """Waze jams feed to PostGIS."""
    pg_hook = PostgresHook(postgres_conn_id='waze')
    tempfile = conf['temp_data_dir'] + '/waze_temp.csv'

    temp_df = pd.read_csv(tempfile, header=None, encoding='utf-8')

    rows_db = temp_df.values

    cols = ['uuid', 'waze_timestamp', 'street', 'start_node',
            'end_node', 'city', 'length', 'delay', 'speed', 'level',
            'road_type', 'geom']


    logging.info('Pushing Waze data to Postgis.')
    pg_hook.insert_rows('public.waze_jams',
                        rows_db,
                        target_fields=cols,
                        commit_every=0)

    return 'Successfully pushed data to PostGIS.'
