import os
import pandas as pd
import logging
from poseidon.util import general
from poseidon.util.sf_client import Salesforce

conf = general.config

prod_file_potholes = conf['prod_data_dir'] + '/potholes_agg_districts_datasd.csv'


def get_sf_gid_potholes():
    """Get aggregated potholes counts per council districts from sf, creates prod file."""

    username = conf['mrm_sf_user']
    password = conf['mrm_sf_pass']
    security_token = conf['mrm_sf_token']

    query_string = general.file_to_string('./sql/gid_potholes_counts.sql', __file__)

    # Init salesforce client
    sf = Salesforce(username, password, security_token)

    # Pull records
    logging.info('Pull records from SF')

    response = sf.get_query_all(query_string)
    records = response['records']

    rows = []
    cols = ['SAP_notification_number', 'functional_loc', 'council_district', 'count']

    for record in records:
        sap_not_num = record['SAP_Original_Notification_Number__c']
        func_loc = record['Functional_Location__c']
        district = record['Council_District__c']
        district = district.split()[0]
        district_int = int(district)
        count = record['expr0']

        row = (sap_not_num, func_loc, district_int, count)
        rows.append(row)

    df = pd.DataFrame(data=rows, columns=cols)
    df = df.sort_values(by=['council_district', 'count'], ascending=[True, False])

    general.pos_write_csv(df, prod_file_potholes)

    return "Successfully wrote records for potholes prod file"
