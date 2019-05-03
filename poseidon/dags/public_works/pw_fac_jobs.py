"""PW Facilities _jobs file."""
import ftplib
import operator
import string
import logging
import pandas as pd
from datetime import datetime
from poseidon.util import general


conf = general.config


#: Download Access file for city occupied facilities via CURL
def get_fac_data(infile, outfile):
    """Access DB From SFTP"""
    curl_str = "curl -o $out_file " \
            + "sftp://webftp.alphafacilities.com/"\
            + "$fpath " \
            + "-u $user:$passwd -k"

    tmpl = string.Template(curl_str)
    command = tmpl.substitute(
        out_file=outfile,
        fpath=infile,
        user=conf['ftp_alpha_user'],
        passwd=conf['ftp_alpha_pass'])

    return command


#: Combine two Access tables - attributes and forecasts.
def process_occupied_fac_data():
    """Process collision data."""
    prod_file = conf['prod_data_dir'] + '/gf_city_occupied_fac_datasd.csv'
    temp_ca_file = conf['temp_data_dir'] + '/city_occupied_gf_campus_attr.csv'
    temp_cf_file = conf['temp_data_dir'] + '/city_occupied_gf_campus_forecasts.csv'

    campus_attr = pd.read_csv(temp_ca_file, low_memory=False)
    campus_forecasts = pd.read_csv(temp_cf_file, low_memory=False)

    # Prep Campus Attribute table.
    campus_attr = campus_attr[[
        'CampusAttributesId',
        'CampusGuid',
        'AccessibilitySurvey',
        'Active',
        'Address1',
        'Address2',
        'Age',
        'AssetFunction',
        'AssetType',
        'BuildingDescription',
        'BuildingName',
        'BuildingNumber',
        'CampusName',
        'Category',
        'City',
        'CommunityAreaName',
        'CouncilDistrictName',
        'DepartmentName',
        'Function',
        'GrossSquareFeet',
        'InspectionFiscalYear',
        'LastRenovationYear',
        'Latitude',
        'Longitude',
        'SolarSurvey',
        'ZipCode',
        'SolarEnergyFeasible',
        'YearBuilt',
        'FlagColor',
        'AssetGroup',
        'ServiceLevel']]

    campus_attr = campus_attr.rename(columns={
        'CampusAttributesId': 'campus_attributes_id',
        'CampusGuid': 'campus_guid',
        'AccessibilitySurvey': 'accessibility_survey',
        'Active': 'active',
        'Address1': 'address_1',
        'Address2': 'address_2',
        'Age': 'age',
        'AssetFunction': 'asset_function',
        'AssetType': 'asset_type',
        'BuildingDescription': 'building_desc',
        'BuildingName': 'building_name',
        'BuildingNumber': 'building_number',
        'CampusName': 'campus_name',
        'Category': 'category',
        'City': 'city',
        'CommunityAreaName': 'community_area_name',
        'CouncilDistrictName': 'council_district_name',
        'DepartmentName': 'dept_name',
        'Function': 'function',
        'GrossSquareFeet': 'gross_sq_ft',
        'InspectionFiscalYear': 'inspection_fy',
        'LastRenovationYear': 'last_renovation_fy',
        'Latitude': 'lattitude',
        'Longitude': 'longitude',
        'SolarSurvey': 'solar_survey',
        'ZipCode': 'zip',
        'SolarEnergyFeasible': 'solar_energy_feasible',
        'YearBuilt': 'yr_build',
        'FlagColor': 'flag_color',
        'AssetGroup': 'asset_group',
        'ServiceLevel': 'service_level'
    })

    # Prep campus forecasts table.
    campus_forecasts = campus_forecasts[[
        'CampusGuid', 'Year', 'Condition', 'CapitalRequirement',
        'MaintenanceRequirement', 'TotalRequirement', 'FacilityConditionIndex'
    ]].rename(columns={
        "CampusGuid": "campus_guid",
        "Year": "survey_year",
        "Condition": "condition",
        "FacilityConditionIndex": "fci",
        "CapitalRequirement": "capital_req",
        "MaintenanceRequirement": "maintenance_req",
        "TotalRequirement": "total_req"
    })

    campus_forecasts = campus_forecasts[campus_forecasts.survey_year ==
                                        2015].copy().reset_index().drop(
                                            ['index'], axis=1)

    #Perform the join.
    facilities = pd.merge(
        campus_attr, campus_forecasts, how='inner', on='campus_guid')

    general.pos_write_csv(
        facilities, prod_file, date_format=conf['date_format_ymd_hms'])

    return 'Successfully processed city occupied facilities data.'
