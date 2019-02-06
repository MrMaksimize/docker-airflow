import requests
import os
import zipfile
import glob
from poseidon.util import general

#%%
#target_dir = "/Users/MrMaksimize/Code/ETL/temp_source/dsd/"

base_url = "https://www.sandiego.gov/sites/default/files/dsdpar_"

file_dict = {
    "ytd_permits_issued": "yeartodatedsdpermitsissued",
    "ytd_apps_received": "yeartodatedsdapplicationsreceived",
    "ytd_permits_completed": "yeartodatedsdpermitscompleted",
    "curr_week_apps_received": "currentweekdsdapplicationsreceived",
    "curr_week_permits_issued": "currentweekdsdpermitsissued",
    "curr_week_permits_completed": "currentweekdsdpermitscompleted",
    "code_enf_past_6_mo": "mappedcedcases6months",
    "code_enf_past_3_yr": "mappedcedcases3years",
}

conf = general.config

dsd_temp_dir = general.create_path_if_not_exists(conf['temp_data_dir'] + '/')


def download_file(data_name, target_dir):
    zip_dir = general.create_path_if_not_exists(target_dir)
    local_zip = os.path.normpath(zip_dir + data_name + ".zip")
    url = base_url + data_name + ".zip"

    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_zip, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return local_zip


def unzip_file(local_zip, target_dir):
    zip_ref = zipfile.ZipFile(local_zip, 'r')
    xml_dir = general.create_path_if_not_exists(target_dir)
    zip_ref.extractall(xml_dir)


def get_files(fname_list, target_dir):
    for i in fname_list:
        print(i)
        lz = download_file(file_dict[i], target_dir)
        unzip_file(lz, target_dir)
