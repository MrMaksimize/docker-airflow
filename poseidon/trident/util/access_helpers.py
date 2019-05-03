"""Geospatial Utilities."""


def extract_access_to_csv(access_file, table, out_file):
    command = "mdb-export {0} {1} > {2}".format(access_file, table,
                                                   out_file)
    return command
