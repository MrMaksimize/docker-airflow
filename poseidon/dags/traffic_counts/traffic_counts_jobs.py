"""Traffic counts _jobs file."""
import pandas as pd
import logging
from subprocess import Popen, PIPE
from trident.util import general
from shlex import quote

conf = general.config
fy = general.get_FY_year()


def get_traffic_counts(out_fname='traffic_counts_file'):
    """Get traffic counts file from shared drive."""
    
    logging.info(f'Retrieving data for FY {fy}.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + f"--user={conf['svc_acct_user']}%{conf['svc_acct_pass']} -W ad -c " \
        + "'cd \"TSW-TEO-Shared/TEO/" \
        + "TEO-Transportation-Systems-and-Safety-Programs/" \
        + f"Traffic Data/{fy}/RECORD FINDER\";" \
        + f" ls; get Machine_Count_Index.xlsx {conf['temp_data_dir']}/{out_fname}.xlsx;'"

    command = command.format(quote(command))

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    
    if p.returncode != 0:
        raise Exception(output)
    else:
        return f'Successfully retrieved {fy} data.'


def clean_traffic_counts(src_fname='traffic_counts_file',
                         out_fname='traffic_counts_raw_clean'):
    """Clean traffic counts data."""
    xlsx_file = f"{conf['temp_data_dir']}/{src_fname}.xlsx"
    out_csv_file = f"{conf['temp_data_dir']}/{out_fname}.csv"

    names = ['street_name',
             'limits',
             'northbound_count',
             'southbound_count',
             'eastbound_count',
             'westbound_count',
             'total_count',
             'file_no',
             'date_count']

    worksheet = pd.read_excel(xlsx_file,
                              sheet_name='TRAFFIC',
                              header=None,
                              skiprows=[0, 1, 2, 3],
                              usecols=[8, 9, 10, 11, 12, 13, 14, 15, 16],
                              names=names)

    # Write temp csv
    general.pos_write_csv(
        worksheet,
        out_csv_file,
        date_format=conf['date_format_ymd_hms'])

    return "Successfully cleaned traffic counts data."


def build_traffic_counts(src_fname='traffic_counts_raw_clean',
                         out_fname='traffic_counts_datasd_v1'):
    """Build traffic counts production data."""
    src_file = f"{conf['temp_data_dir']}/{src_fname}.csv"
    out_file = f"{conf['prod_data_dir']}/{out_fname}.csv"

    # read in csv from temp
    counts = pd.read_csv(src_file)

    # remove rows that are part of the main worksheet but empty for some reason
    counts = counts[counts['street_name'] != ' ']

    # date type
    counts['date_count'] = pd.to_datetime(counts['date_count'],errors='coerce')

    # create id field based on file id and street
    counts['id'] = counts.street_name.str.cat(counts.file_no, sep="")\
                         .str.replace(" ", "")\
                         .str.replace("-", "")

    # reorder columns
    cols = counts.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    counts = counts[cols]

    # write to production file
    new_file_path = out_file

    general.pos_write_csv(
        counts,
        new_file_path,
        date_format=conf['date_format_ymd_hms'])

    return "Successfully built traffic counts production file."
