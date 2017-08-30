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
from datetime import datetime
import QIB2TBatch
from ConfigStorage import ConfigStorage


def main(args):
    """
    Function: Call all the methods, passing along all the needed variables.
    Parameters:
        -args   ArgumentParser      Contains the location of the configuration files.
    """

    print("Storing configurations")
    config = ConfigStorage(args)

    if config.__dict__.__contains__("error"):
        print(config.error)
        sys.exit()

    timestamp = datetime.now().strftime("_%Y%m%d%H%M%S")
    path = config.base_path + config.study_id + timestamp

    print('Creating directory structure')
    QIB2TBatch.create_dir(path)

    logger = set_general_logger(path)
    logging.info("Start.")

    logging.info('Establishing connection')
    project, connection = QIB2TBatch.make_connection(config)

    continue_writing = QIB2TBatch.check_if_updated(project, config)
    if not continue_writing:
        logging.info("No data added. Exiting")
        sys.exit(9)

    logging.info('Write .params files')
    QIB2TBatch.write_params(path, config)

    logging.info('Write headers')
    tag_file, data_file, concept_file = QIB2TBatch.write_headers(path, config)

    logging.info('Load patient mapping')
    patient_map = QIB2TBatch.get_patient_mapping(config)
    if not patient_map:
        logging.warning('No patient mapping found')
    else:
        logging.info(['Found the following patient map:', patient_map])

    logging.info('Obtaining data from XNAT')
    data_list, data_header_list = QIB2TBatch.obtain_data(project,
                                                         tag_file,
                                                         patient_map,
                                                         config)
    logging.info("Data obtained from XNAT.")

    logging.info('Write data to files')
    QIB2TBatch.write_data(data_file, concept_file, data_list, data_header_list)
    logging.info("Data written to files.")

    connection.disconnect()
    logging.info("Exit.")


def set_general_logger(filepath):
    """
    Function: set general file logger
    Args:
        filepath: String

    Returns:
        logger
    """
    logger = logging.basicConfig(filename=filepath + '/QIBimporter.log',
                                 format='%(asctime)s:%(levelname)s:%(message)s',
                                 level=logging.DEBUG)
    return logger


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", help="Location of the configuration file, which contains connection, params and tag "
                                      "configurations.")
    parser.add_argument("--connection", help="Location of the configuration file for establishing XNAT connection.")
    parser.add_argument("--params", help="Location of the configuration file for the variables in the .params files.")
    parser.add_argument("--tags", help="Location of the configuration file for the tags.")
    args = parser.parse_args()
    main(args)