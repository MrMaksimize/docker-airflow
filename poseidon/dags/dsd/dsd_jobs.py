"""Jobs for dsd."""

from trident.util import general
import logging

conf = general.config


def get_bash_command(key):
    """Return bash command for running dsd_ce."""
    if key == "code_enf_past_6_mo":
        src = "mappedcedcases6months"
    else:
        src = "mappedcedcases3years"

    dsd_ce = conf['executable_path'] + '/' + 'dsd_ce'
    xml_path = "{}/{}.xml".format(conf['temp_data_dir'], src)
    ce_out_path = "{}/{}_datasd.csv".format(conf['prod_data_dir'], key)
    cmpl_out_path = "{}/{}_complaints_datasd.csv".format(conf['prod_data_dir'],
                                                         key)

    command = "{} -xmlPath={} -ceOutPath={} -cmplOutPath={}"\
        .format(dsd_ce, xml_path, ce_out_path, cmpl_out_path)


    return command
