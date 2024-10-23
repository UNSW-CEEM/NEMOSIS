import os
from nemosis import defaults


def write_mms_file_names(name, month, year, day, chunk, index, raw_data_location):
    # Add the year and month information to the generic AEMO file name
    if int(year) > 2024 or (int(year) == 2024 and int(month) >= 8):
        filename_stub = (
                'PUBLIC_ARCHIVE#' + name + "#FILE" + str(chunk).zfill(2) + '#' + str(year) + str(month) + "010000"
        )
    else:
        filename_stub = defaults.names[name] + "_" + str(year) + str(month) + "010000"
    path_and_name = os.path.join(raw_data_location, filename_stub)
    return filename_stub, path_and_name


def write_file_names_mms_and_current(name, month, year, day, chunk, index, raw_data_location):
    if day is None:
        filename_stub, path_and_name = write_mms_file_names(name, month, year, day, index, chunk, raw_data_location)
    else:
        filename_stub = (
                defaults.names[name] + "_" + str(year) + str(month) + str(day)
        )
        path_and_name = os.path.join(raw_data_location, filename_stub)
    return filename_stub, path_and_name


def write_file_names_current(name, month, year, day, chunk, index, raw_data_location):
    # Add the year and month information to the generic AEMO file name
    filename_stub = (
            defaults.names[name] + "_" + str(year) + str(month) + str(day)
    )
    path_and_name = os.path.join(raw_data_location, filename_stub)
    return filename_stub, path_and_name


def write_file_names_fcas(name, month, year, day, chunk, index, raw_data_location):
    # Add the year and month information to the generic AEMO file name
    filename_stub = defaults.names[name] + "_" + str(year) + str(month) + day + index
    path_and_name = os.path.join(raw_data_location, filename_stub)
    return filename_stub, path_and_name