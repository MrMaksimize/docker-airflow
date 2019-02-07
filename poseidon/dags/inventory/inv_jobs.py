"""inventory_jobs file."""
from poseidon.util import general
import pandas as pd
import logging


conf = general.config


def inventory_to_csv():
    inventory_prod_path = conf['prod_data_dir'] + '/inventory_datasd.csv'
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRaEHNs_h56ia6MSa-oTs22qAUjG9lD0t4Sqisq3G0swYRgp0DUoT83WE3mah4amCI0P3me9Bffxcqp/pub?gid=269959199&single=true&output=csv")
    general.pos_write_csv(df, inventory_prod_path)

    return "Successfuly wrote inventory file to prod."
