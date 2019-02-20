"""Traffic counts _jobs file."""
import pandas as pd
import logging
from subprocess import Popen, PIPE
from poseidon.util import general

conf = general.config
fy = general.get_FY_year()


def get_traffic_counts(out_fname='traffic_counts_file'):
    """Get traffic counts file from shared drive."""
    logging.info('Retrieving data for current FY.')
    command = "smbclient //ad.sannet.gov/dfs " \
        + "--user={adname}%{adpass} -W ad -c " \
        + "'cd \"TSW-TEO-Shared/TEO/" \
        + "TEO-Transportation-Systems-and-Safety-Programs/" \
        + "TEO-Traffic Data Gathering Services/{fy}/RECORD FINDER\";" \
        + " ls; get Machine_Count_Index.xlsx {temp_dir}/{out_f}.xlsx;'"

    command = command.format(adname=conf['mrm_sannet_user'],
                             adpass=conf['mrm_sannet_pass'],
                             fy=fy,
                             temp_dir=conf['temp_data_dir'],
                             out_f=out_fname)

    logging.info(command)

    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
    output, error = p.communicate()
    if p.returncode != 0:
        logging.info(fy + ' folder does not exist.')

        logging.info('Retieving data for previous FY.')
        fy1 = general.get_prev_FY_year()

        command = "smbclient //ad.sannet.gov/dfs " \
            + "--user={adname}%{adpass} -W ad -c " \
            + "'cd \"TSW-TEO-Shared/TEO/" \
            + "TEO-Transportation-Systems-and-Safety-Programs/" \
            + "TEO-Traffic Data Gathering Services/{fy}/RECORD FINDER\";" \
            + " ls; get Machine_Count_Index.xlsx {temp_dir}/{out_f}.xlsx;'"

        command = command.format(adname=conf['mrm_sannet_user'],
                                 adpass=conf['mrm_sannet_pass'],
                                 fy=fy1,
                                 temp_dir=conf['temp_data_dir'],
                                 out_f=out_fname)
        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate()
        if p.returncode != 0:
            logging.info(fy1 + ' data does not exist.')
            # print fy1 + ' data does not exist.'
            return 'Task failed.'

        else:
            return 'Successfully retrieved ' + fy1 + ' data.'

    else:
        return 'Successfully retrieved ' + fy + ' data.'


def clean_traffic_counts(src_fname='traffic_counts_file',
                         out_fname='traffic_counts_raw_clean'):
    """Clean traffic counts data."""
    xlsx_file = "{0}/{1}.xlsx"\
        .format(conf['temp_data_dir'], src_fname)
    out_csv_file = "{0}/{1}.csv"\
        .format(conf['temp_data_dir'], out_fname)

    names = ['street_name',
             'limits',
             'all_count',
             'northbound_count',
             'southbound_count',
             'eastbound_count',
             'westbound_count',
             'total_count',
             'file_no',
             'count_date']

    worksheet = pd.read_excel(xlsx_file,
                              sheetname='TRAFFIC',
                              header_row=None,
                              skiprows=[0, 1, 2, 3],
                              parse_cols=[8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
                              names=names)

    # Write temp csv
    general.pos_write_csv(
        worksheet,
        out_csv_file,
        date_format=conf['date_format_ymd_hms'])

    return "Successfully cleaned traffic counts data."


def build_traffic_counts(src_fname='traffic_counts_raw_clean',
                         out_fname='traffic_counts_datasd'):
    """Build traffic counts production data."""
    src_file = "{0}/{1}.csv"\
        .format(conf['temp_data_dir'], src_fname)

    out_file = "{0}/{1}.csv"\
        .format(conf['prod_data_dir'], out_fname)

    # read in csv from temp
    counts = pd.read_csv(src_file)

    # remove rows that are part of the main worksheet but empty for some reason
    counts = counts[counts['street_name'] != ' ']

    # date type
    counts['count_date'] = pd.to_datetime(counts['count_date'])

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
