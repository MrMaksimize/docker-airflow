"""DSD Approvals _jobs file."""
import os
from datetime import datetime, timedelta
from dateutil.parser import parse
import pandas as pd
from lxml import etree
from bs4 import BeautifulSoup as bs
import requests
import time
from trident.util import general
import logging

conf = general.config

dsd_temp_dir = general.create_path_if_not_exists(conf['temp_data_dir'] + '/')

approval_dict = {
    'completed':
    ['currentweekdsdpermitscompleted.xml', 'permits_completed_ytd_datasd.csv'],
    'issued':
    ['currentweekdsdpermitsissued.xml', 'permits_issued_ytd_datasd.csv'],
    'applied':
    ['currentweekdsdapplicationsreceived.xml', 'apps_received_ytd_datasd.csv']
}


def scrape_dsd(key):
    """Consolidate weekly data by scraping OpenDSD API."""
    src = approval_dict[key][0]
    xml_file = dsd_temp_dir + src
    tree = etree.parse(xml_file)
    root = tree.getroot()
    approvals = root.find('approvals')
    list_app = approvals.findall('approval')
    len_list = len(list_app)
    max_val = len_list - 1

    rows = list()

    count_log = range(0, max_val, 5)
    for i in range(0, max_val):

        app_id = int(list_app[i].attrib['approval_id'])
        app_type_id = int(list_app[i].find('approval_type_id').text)

        if key == 'applied':
            app_type = list_app[i].find('approval_type').text
            pj_id = list_app[i].find('project_id').text
            lat = float(list_app[i].find('latitude').text)
            lon = float(list_app[i].find('longitude').text)
            app_date = list_app[i].find('application_date').text
            iss_date = insp_date = comp_date = iss_by = pmt_hldr = 'NaN'
            scope = jb_id = status = dep = sqft = val = 'NaN'
        else:
            dsd_api = 'https://opendsd.sandiego.gov/'\
                  + 'api//approval/{0}'.format(app_id)
            page = requests.get(dsd_api, timeout=20)
            content = page.content
            xmlsoup = bs(content, 'lxml-xml')

            if i in count_log:
                logging.info(str(i) + ' / ' + str(max_val) + ' completed.')

            app_xml_dict = {
                'app_date': 'xmlsoup.Project.ApplicationDate.text.strip()',
                'iss_date': 'xmlsoup.Approval.IssueDate.text.strip()',
                'insp_date': 'xmlsoup.Approval.FirstInspectionDate.text.strip()',
                'comp_date': 'xmlsoup.Approval.CompleteCancelDate.text.strip()',
                'app_type': 'xmlsoup.Approval.Type.text.strip()',
                'lat': 'xmlsoup.Job.Latitude.text.strip()',
                'lon': 'xmlsoup.Job.Longitude.text.strip()',
                'iss_by': 'xmlsoup.Approval.IssuedBy.text.strip()',
                'pmt_hldr': 'xmlsoup.Approval.PermitHolder.text.strip()',
                'scope': 'xmlsoup.Approval.Scope.text.strip()',
                'pj_id': 'xmlsoup.Project.ProjectId.text.strip()',
                'jb_id': 'xmlsoup.Approval.JobId.text.strip()',
                'status': 'xmlsoup.Approval.Status.text.strip()',
                'dep': 'xmlsoup.Approval.Depiction.text.strip()',
                'sqft': 'xmlsoup.Approval.SquareFootage.text.strip()',
                'val': 'xmlsoup.Approval.Valuation.text.strip()'
                }

            for param, value in app_xml_dict.iteritems():
                try:
                    exec("{param}={value}".format(param=param, value=value))
                except:
                    exec("{param}='NaN'".format(param=param))

        row = (app_id, app_type, app_type_id, app_date, iss_date, insp_date,
               comp_date, lat, lon, iss_by, pmt_hldr, scope, pj_id, jb_id,
               status, dep, sqft, val)

        rows.append(row)

    df = pd.DataFrame(
        rows,
        columns=[
            'approval_id', 'approval_type', 'approval_type_id',
            'application_date', 'issue_date', 'first_inspection_date',
            'complete_cancel_date', 'latitude', 'longitude', 'issued_by',
            'permit_holder', 'scope', 'project_id', 'job_id', 'status',
            'depiction', 'square_footage', 'valuation'
        ])

    df['application_date'] = pd.to_datetime(
        df['application_date'], errors='coerce')

    df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')

    df['first_inspection_date'] = pd.to_datetime(
        df['first_inspection_date'], errors='coerce')

    df['complete_cancel_date'] = pd.to_datetime(
        df['complete_cancel_date'], errors='coerce')

    if key == 'issued':
        df = df.drop(
            ['first_inspection_date', 'complete_cancel_date'], axis=1)
        df = df.sort_values(by='issue_date')

    elif key == 'applied':
        df = df.drop(
            [
                'issue_date', 'first_inspection_date',
                'complete_cancel_date', 'issued_by', 'permit_holder',
                'scope', 'job_id', 'status', 'depiction',
                'square_footage', 'valuation'
            ],
            axis=1)
        df = df.sort_values(by='application_date')

    elif key == 'completed':
        df = df.sort_values(by='complete_cancel_date')

    general.pos_write_csv(
        df,
        dsd_temp_dir + '{0}_week.csv'.format(key),
        date_format=conf['date_format_ymd_hms'])

    return 'Successfully scraped ' + key + ' permits.'


def update_dsd(key):
    """Add weekly data to current production data."""
    y_src = conf['prod_data_dir'] + '/' + approval_dict[key][1]
    w_src = dsd_temp_dir + key + '_week.csv'
    ytd = pd.read_csv(y_src)
    week = pd.read_csv(w_src)
    prod = ytd.append(week, ignore_index=True)
    prod = prod.drop_duplicates(subset=['approval_id'])

    if key == 'applied':
        prod = prod.sort_values(by='application_date')
    elif key == 'issued':
        prod = prod.sort_values(by='issue_date')
    elif key == 'completed':
        prod = prod.sort_values(by='complete_cancel_date')

    general.pos_write_csv(prod, y_src, date_format=conf['date_format_ymd_hms'])

    return 'Successfully updated ' + key + 'permits.'


def extract_solar(key):
    """Extract solar permits from production files."""
    prod_src = conf['prod_data_dir'] + '/' + approval_dict[key][1]
    solar_pmts = conf['prod_data_dir'] + '/' + 'solar_permits_' + key + '_ytd_datasd.csv'

    ytd = pd.read_csv(prod_src)

    solar = ytd[ytd['approval_type_id'] == 293]

    if key == 'applied':
        solar = solar.sort_values(by='application_date')
    elif key == 'issued':
        solar = solar.sort_values(by='issue_date')
    elif key == 'completed':
        solar = solar.sort_values(by='complete_cancel_date')

    general.pos_write_csv(solar, solar_pmts, date_format=conf['date_format_ymd_hms'])

    return 'Successfully updated ' + key + ' solar permits.'
