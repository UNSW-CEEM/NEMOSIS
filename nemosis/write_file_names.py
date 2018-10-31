import os
from nemosis import defaults

def write_file_names(name, month, year, day, index, raw_data_location):
    # Add the year and month information to the generic AEMO file name
    filename_full = defaults.names[name] + "_" + str(year) + str(month) + "010000.CSV"
    # Check if file already exists in the raw data folder.
    path_and_name = raw_data_location + '/' + filename_full
    # Add the year and month information to the generic AEMO file name
    filename_full_feather = defaults.names[name] + "_" + str(year) + str(month) + "010000.feather"
    # Check if file already exists in the raw data folder.
    path_and_name_feather = raw_data_location + '/' + filename_full_feather
    return filename_full, path_and_name, filename_full_feather, path_and_name_feather


def write_file_names_fcas(name, month, year, day, index, raw_data_location):
    # Add the year and month information to the generic AEMO file name
    filename_full = defaults.names[name] + "_" + str(year) + str(month) + day + index + ".CSV"
    # Check if file already exists in the raw data folder.
    path_and_name = raw_data_location + '/' + filename_full
    # Add the year and month information to the generic AEMO file name
    filename_full_feather = defaults.names[name] + "_" + str(year) + str(month) + day + index + ".feather"
    # Check if file already exists in the raw data folder.
    path_and_name_feather = raw_data_location + '/' + filename_full_feather
    return filename_full, path_and_name, filename_full_feather, path_and_name_feather