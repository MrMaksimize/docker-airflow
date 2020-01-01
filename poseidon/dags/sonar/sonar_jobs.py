import os
import pandas as pd
import logging
from datetime import datetime, timedelta
import json
from trident.util import general
#from keen.client import KeenClient
from trident.util.notifications import notify_keen

conf = general.config


def get_sonar_json(**kwargs):
    prod_file = conf['prod_data_dir'] + '/sonar_payload.json'

    with open(prod_file, 'r') as fp:
        data = json.load(fp)

    template_data = {'metrics': data}
    return template_data


#def get_keen_sonar(collection, timeframe, filters = [], timezone="US/Pacific"):
    #client = KeenClient(
        #project_id=conf['keen_project_id'], read_key=conf['keen_read_key'])

    #return client.extraction(
        #collection, timeframe=timeframe, filters=filters, timezone=timezone)



def build_sonar_json(**kwargs):
    """Provide JSON file"""
    prod_file = conf['prod_data_dir'] + '/sonar_payload.json'
    keen_collection = 'sonar_pings_{}'.format(conf['env']).lower()

    sonar_data = get_keen_sonar(keen_collection, 'this_30_days')

    df = pd.DataFrame(sonar_data)
    df['keen_timestamp'] = df['keen'].map(lambda x: x['created_at'])
    df['exec_date'] = pd.to_datetime(df['exec_date'], format=conf['date_format_keen'])
    df['keen_timestamp'] = pd.to_datetime(df['keen_timestamp'], format=conf['date_format_keen'])
    df.sort_values(by=['exec_date', 'keen_timestamp'], ascending=False, inplace=True)
    df.drop_duplicates(subset='value_key', keep='first', inplace=True)
    df.reset_index(inplace=True)
    df.drop(['index', 'keen'], 1, inplace=True)
    df['exec_date'] = df['exec_date'].dt.strftime(conf['date_format_keen'])
    df['keen_timestamp'] = df['keen_timestamp'].dt.strftime(conf[
        'date_format_keen'])
    sonar_payload = json.loads(df.to_json(orient='records'))



    notify_keen(
        {
            'sonar_payload': sonar_payload
        },
        'sonar_payloads_{}'.format(conf['env']).lower(),
        raise_for_status=True)

    with open(prod_file, 'w') as outfile:
        json.dump(sonar_payload, outfile, indent=4, sort_keys=True)


    return sonar_payload
