""""
Name: QIBConverter
Function: Convert QIB datatype to directory structure that can be uploaded to Transmart
Author: Jarno van Erp
Company: The Hyve

Parameters:
--all           Location of the configuration file,  Location of the configuration file, containing all the information.
--connection    Location of the configuration file for establishing XNAT connection.
--params        Location of the configuration file for the variables in the .param files.
--tags          Location of the configuration file for the tags.

Requirements:
xnatpy      Downloadable here: https://bitbucket.org/bigr_erasmusmc/xnatpy

Formats of configuration files:

--connection configuration file:

[Connection]
url =
user =
password =
project =
patient_map_file =

--params configuration file:

[Study]
STUDY_ID =
SECURITY_REQUIRED =
TOP_NODE =

[Directory]
path =

--tags configuration file:

[Tags]
Taglist =


"""


import argparse
import logging

import sys
import time

import QIB2TBatch
from ConfigStorage import ConfigStorage


def main(args):
    """
    Function: Call all the methods, passing along all the needed variables.
    Parameters:
        -args   ArgumentParser      Contains the location of the configuration files.
    """
    start = time.time()
    logging.info("Start.")

    print("Storing configurations\n")
    config = ConfigStorage(args)

    if config.__dict__ .__contains__("error"):
        print(config.error)
        sys.exit()

    print('Establishing connection\n')
    project, connection = QIB2TBatch.make_connection(config)

    print('Creating directory structure\n')
    path = QIB2TBatch.create_dir(config)

    print('Write .params files\n')
    QIB2TBatch.write_params(path, config)

    print('Write headers\n')
    tag_file, data_file, concept_file = QIB2TBatch.write_headers(path, config)

    print('Load patient mapping\n')
    patient_map = QIB2TBatch.get_patient_mapping(config)
    print(patient_map)

    print('Obtaining data from XNAT\n')
    data_list, data_header_list = QIB2TBatch.obtain_data(project, tag_file, patient_map, config)
    logging.info("Data obtained from XNAT.")

    subject_logger = QIB2TBatch.set_subject_logger(False, config)

    print('Write data to files\n')
    QIB2TBatch.write_data(data_file, concept_file, data_list, data_header_list)
    logging.info("Data written to files.")

    connection.disconnect()
    logging.info("Exit.")
    end = time.time()
    print(end - start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", help="Location of the configuration file, which contains connection, params and tag "
                                      "configurations.")
    parser.add_argument("--connection", help="Location of the configuration file for establishing XNAT connection.")
    parser.add_argument("--params", help="Location of the configuration file for the variables in the .params files.")
    parser.add_argument("--tags", help="Location of the configuration file for the tags.")
    args = parser.parse_args()
    main(args)
