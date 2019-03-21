"""inventory_jobs file."""
from trident.util import general
import pandas as pd
import requests, json
import logging


conf = general.config

def get_profiles(access_token):
    profiles = {}
    payload = {'access_token': access_token}
    r = requests.get('https://api.bufferapp.com/1/profiles.json', params=payload)
    resp = json.loads(r.text)
    for profile in resp:
        service = profile['formatted_service'].lower()
        service_id = profile['id'].lower()
        profiles[service] = service_id

    return profiles

def get_random_message(gdoc_url, col_name):
    df = pd.read_csv(gdoc_url)
    chosen = df.sample(1)
    message = chosen[col_name].iloc[0]
    return message

def buffer_add_message(message, access_token, profile_ids):
    params = {'access_token': access_token}
    payload = {
        'profile_ids': profile_ids,
        'text': message,
        'shorten': True
    }
    r = requests.post('https://api.bufferapp.com/1/updates/create.json', params=params, data=payload)
    r.raise_for_status()

def queue_random_buffer_message(gdoc_url, col_name, access_token,
                                profile_service, **kwargs):
    profiles = get_profiles(access_token)
    profile_ids = [profiles[profile_service]]
    message = get_random_message(gdoc_url, col_name)

    buffer_add_message(message, access_token, profile_ids)



def inventory_to_csv():
    inventory_prod_path = conf['prod_data_dir'] + '/inventory_datasd.csv'
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/1LAx0GyM-HNbqsKg5zp-sKB2ieQ5nQQChWgazBuqy8d4/pub?gid=970785642&single=true&output=csv")
    general.pos_write_csv(df, inventory_prod_path)

    return "Successfuly wrote inventory file to prod."
