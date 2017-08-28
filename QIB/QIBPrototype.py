""""
Name: QIBPrototype 
Function: Convert QIB datatype to directory structure that can be uploaded to Transmart
Author: Jarno van Erp
Company: The Hyve

Parameters:
--connection    Location of the configuration file for establishing XNAT connection.
--params        Location of the configuration file for the variables in the .param files.
--tags          Location of the configuration file for the tags.

Requirements:
xnatpy      Downloadable here: https://bitbucket.org/bigr_erasmusmc/xnatpy

Formats:

--connection configuration file:

[Connection]
url = 
user =
password =
project =

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
import os
import sys
import logging
if sys.version_info.major == 3:
    import configparser as ConfigParser
elif sys.version_info.major == 2:
    import ConfigParser
import xnat
from datetime import datetime as dt

def main(args):
    """
    Function: Call all the methods, passing along all the needed variables.
    Parameters:
        -args   ArgumentParser      Contains the location of the configuration files.
    """

    #Maybe create an object for the config reading part. Because it's taking in a lot of space.
    #Maybe create a liberary of this script and a seperate one as running script.
    logging.info("Start.")
    print('Establishing connection')
    project, connection = make_connection(args)

    print('Creating directory structure')
    path = create_dir(args)

    print('Write .params files')
    write_params(path, args)

    print('Write headers\n')
    tag_file, data_file, concept_file = write_headers(path, args)

    print('Obtaining data from XNAT')
    data_list, data_header_list = obtain_data(project, tag_file, args)
    logging.info("Data obtained from XNAT.")

    print('Write data to files')
    write_data(data_file, concept_file, data_list, data_header_list)
    logging.info("Data written to files.")

    connection.disconnect()
    logging.info("Exit.")


def make_connection(args):
    """
    Function: Create the connection to XNAT.
    Returns: 
        -project    xnatpy object   Xnat connection to a specific project.
    """
    config = check_file_existence(args.connection, "connection")

    try:
        connection_name = config.get('Connection', 'url')
        user = config.get('Connection', 'user')
        pssw = config.get('Connection', 'password')
        project_name = config.get('Connection', 'project')
        connection = xnat.connect(connection_name, user=user, password=pssw)
        project = connection.projects[project_name]
        logging.info("Connection established.")
        return project, connection

    except ConfigParser.NoSectionError as e:
        configError(e)

    except KeyError:
        print("Project not found in XNAT.\nExit")
        logging.critical("Project not found in XNAT.")
        if __name__ == "__main__":
            sys.exit()
        else:
            return None, None

    except Exception as e:
        print(str(e) + "\nExit")
        if __name__ == "__main__":
            logging.critical(e.message)
            sys.exit()
        else:
            return e, None


def create_dir(args):
    """
    Function: Create the directory structure.
    Returns: 
        -new_path    String   Path to the directory where all the files will be saved.
    """
    config = check_file_existence(args.params, "params")

    timestamp = dt.now().strftime("%Y%m%d_%H%M%S")

    try:
        base_path = config.get('Directory', 'path')
        new_path = config.get('Study', 'STUDY_ID')
        path = base_path+new_path+'_'+timestamp
        #Test if the /tags/ and /clinical are also there
        if not os.path.exists(path):
            os.makedirs(path)
            os.makedirs(path + "/tags/")
            os.makedirs(path + "/clinical/")
        ### insert logging here logging.path
        return path

    except ConfigParser.NoSectionError as e:
        configError(e)


def write_params(path, args):
    """
    Function: Uses the configuration files to write the .params files.
    Parameters:
        -path   String  Path to the directory where all the files will be saved.
    """
    config = check_file_existence(args.params, "Params")

    try:
        study_id = config.get('Study', "STUDY_ID")
        security_req = config.get('Study', 'SECURITY_REQUIRED')
        top_node = config.get('Study', 'TOP_NODE')
        tag_param_file = open(path + '/tags/tags.params', 'w')
        tag_param_file.write("TAGS_FILE=tags.txt")
        study_param_file = open(path + '/study.params', 'w')
        study_param_file.write("STUDY_ID=" + study_id +
                         "\nSECURITY_REQUIRED=" + security_req +
                         "\nTOP_NODE=" + top_node)
        clinical_param_file = open(path + '/clinical/clinical.params', 'w')
        clinical_param_file.write("COLUMN_MAP_FILE=" + str(study_id) + "_columns.txt")
        tag_param_file.close()
        study_param_file.close()
        clinical_param_file.close()

    except ConfigParser.NoSectionError as e:
        configError(e)


def write_headers(path, args):
    """
    Function: Uses the configuration files to write the headers of the .txt files. 
    Parameters:
        -path   String  Path to the directory where all the files will be saved.
    Returns:
        -tag_file        File    tags.txt, used to upload the metadata into TranSMART.
        -data_file       File    (STUDY_ID)_clinical.txt, used to upload the clinical data into TranSMART.
        -concept_file    File    (STUDY_ID)_columns.txt, used to determine which values are in which columns for uploading to TranSMART.
    """
    config = check_file_existence(args.params, "Params")

    try:
        study_id = config.get('Study', "STUDY_ID")
        data_file = open(path + '/clinical/' + study_id + '_clinical.txt', 'w')
        concept_file = open(path + '/clinical/' + study_id + '_columns.txt', 'w')
        tag_file = open(path + '/tags/tags.txt', 'w')
        #Hardcoded right now, because transmart does not need other headers. But this can be subject to change.
        concept_headers = ['Filename', 'Category Code', 'Column Number', 'Data Label']
        tag_headers = ['Concept Path', 'Title', 'Description', 'Weight']
        concept_file.write("\t".join(concept_headers) + '\n')
        tag_file.write("\t".join(tag_headers) + "\n")
        tag_file.flush()
        data_file.flush()
        concept_file.flush()
        return tag_file, data_file, concept_file

    except ConfigParser.NoSectionError as e:
        configError(e)


def obtain_data(project, tag_file, args):
    """
    Function: Obtains all the QIB data from the XNAT project.
    Parameters: 
        -project        xnatpy object   Xnat connection to a specific project.
        -tag_file        File            tags.txt, used to upload the metadata into TranSMART.
    Returns:
        -data_list           List    List containing directories per subject, key = header, value = value.
        -data_header_list     List    List containing all the headers.
    """
    concept_key_list = []
    data_header_list = []
    data_list = []
    tag_dict = {}
    for subject in project.subjects.values():
        data_row_dict = {}
        subject_obj = project.subjects[subject.label]
        for experiment in subject_obj.experiments.values():
            if "qib" in experiment.label.lower():
                #Make number of returns and parameters less.
                data_header_list, data_row_dict, concept_key_list, tag_dict = retrieveQIB(subject_obj, experiment, tag_file, data_row_dict,
                                                                                          subject, data_header_list, concept_key_list, tag_dict, args)
        data_list.append(data_row_dict)
    if data_list == [{}] or data_list == []:
        logging.warning("No QIB datatypes found.")
        print("No QIB datatypes found.\nExit")
        if __name__ == "__main__":
            sys.exit()
        else:
            return data_list

    return data_list, data_header_list


def retrieveQIB(subject_obj, experiment, tag_file, data_row_dict, subject, data_header_list, concept_key_list, tag_dict, args):
    """
    Function: Retrieve the biomarker information from the QIB datatype.
    
    Parameters:
        - subject_obj        XNAT.subject    Subject object derived from XNATpy
        - experiment        XNAT.experiment Experiment object derived from XNATpy
        - tag_file           File            File used to write the metadata tags to
        - data_row_dict       Dict            Dictionary for storing the subject information, headers = key
        - subject           Subject         Subject derived from XNATpy
        - data_header_list    List            List used for storing all the headers
        - concept_key_list    List            List used for storing the concept keys
    
    Returns:
        - data_header_list    List            List with the headers stored
        - data_row_dict       Dict            Dict containing all the QIB information of the subject
        - concept_key_list    List            List containing all the concept keys so far.
    """
    session = subject_obj.experiments[experiment.label]
    begin_concept_key, tag_dict = writeMetaData(session, tag_file, tag_dict, args)
    data_row_dict['subject'] = subject.label
    if 'subject' not in data_header_list:
        data_header_list.append('subject')

    label_list = experiment.label.split('_')
    for biomarker_category in session.biomarker_categories:
        results = session.biomarker_categories[biomarker_category]
        for biomarker in results.biomarkers:
            concept_value = results.biomarkers[biomarker].value
            label = label_list[2]
            if label_list[2].lower() == "l":
                label = "Left"
            elif label_list[2].lower() == "r":
                label = "Right"
            concept_key = str(begin_concept_key) + '\\' + str(biomarker_category)+ " " + str(label_list[3])+ "\\" + label + "\\" + str(biomarker)
            data_row_dict[concept_key] = concept_value
            if concept_key not in data_header_list:
                data_header_list.append(concept_key)
                if __name__ == "__main__":
                    concept_key_list, tag_dict = writeOntologyTag(results, biomarker, concept_key, concept_key_list, tag_file, tag_dict, session)

    return data_header_list, data_row_dict, concept_key_list, tag_dict


def writeOntologyTag(results, biomarker, concept_key, concept_key_list, tag_file, tag_dict, session):
    """

    Parameters:
        - results           XNAT object     Parsed QIB XML object.
        - biomarker         XNAT object     biomarker from parsed XML.
        - concept_key        String          concept key for TranSMART
        - concept_key_list    List            List containing the already used concept keys.
        - tag_file           File            File for the metadata tags.

    Returns:
        -concept_key_list     List    List containing the already used concept keys.

    """
    ontology_name = results.biomarkers[biomarker].ontology_name
    ontology_IRI = results.biomarkers[biomarker].ontology_iri
    if concept_key not in concept_key_list:
        ontology_name_row = [concept_key, "Ontology name", ontology_name]
        ontology_iri_row = [concept_key, "Ontology IRI", ontology_IRI]
        weight = "1"
        tag_file.write('\t'.join(ontology_name_row) + '\t'+weight+'\n')
        tag_file.write('\t'.join(ontology_iri_row) + '\t'+weight+'\n')
        if len(session.base_sessions.values()) >= 1:
            accession_identifier = session.base_sessions.values()[0].accession_identifier
            line = concept_key + "\taccession identifier\t" + str(accession_identifier) + "\t2\n"
            if line in tag_dict.keys():
                tag_dict[line] = True
                tag_file.write(line)
        concept_key_list.append(concept_key)
    return concept_key_list, tag_dict


def writeMetaData(session, tag_file, tag_dict, args):
    """
    Function: Write the metadata tags to the tag file.

    Parameters:
        - session       XNAT object     QIB datatype object in XNATpy
        - tag_file       File            File for the metadata tags.
        - args          ArgumentParser

    Returns:
         - concept_key   String          concept key for TranSMART

    """
    config = check_file_existence(args.tags, "Tags")

    try:
        tag_list = config.get("Tags", "Taglist").split(', ')
        analysis_tool = getattr(session, "analysis_tool")
        analysis_tool_version = getattr(session, "analysis_tool_version")
        if analysis_tool and analysis_tool_version:
            concept_key = str(analysis_tool + " " + analysis_tool_version)
        elif analysis_tool:
            concept_key = (analysis_tool)
        else:
            concept_key = "Generic Tool"
        i = len(tag_list)
        for tag in tag_list:
            try:
                info_tag = getattr(session, tag)
                if info_tag:
                    line = concept_key + "\t" + tag.replace('_', ' ') + "\t" + str(info_tag) + "\t"+str(i)+"\n"
                    i -= 1
                    if line not in tag_dict.keys():
                        tag_dict[line] = True
                        tag_file.write(line)
            except AttributeError:
                logging.info(tag + " not found for " + str(concept_key))
        return concept_key, tag_dict

    except ConfigParser.NoSectionError as e:
        configError(e)


def write_data(data_file, concept_file, data_list, data_header_list):
    """
    Function: Writes the data from data_list to data_file.
    Parameters: 
        -data_file           File    (STUDY_ID)_clinical.txt, used to upload the clinical data into TranSMART.
        -concept_file        File    (STUDY_ID)_columns.txt, used to determine which values are in which columns for uploading to TranSMART.
        -data_list           List    List containing a directory per subject, key = header, value = value.
        -data_header_list     List    List containing all the headers.
    """
    data_file.write("\t".join(data_header_list) + '\n')
    column_list = []
    rows = []
    for line in data_list:
        row = []
        i = 0

        while i < len(data_header_list):
            row.append('\t')
            i += 1

        for header in data_header_list:
            if header in line.keys():
                info_piece = line[header]
                index = data_header_list.index(header)
                row[index] = info_piece + '\t'
                if header == "subject":
                    concept_file.write(str(os.path.basename(data_file.name)) + '\t' + str(header) + '\t' + str(
                        index + 1) + '\tSUBJ_ID\n')
                elif header not in column_list:
                    data_label = header.split("\\")[-1]
                    concept_file.write(str(os.path.basename(data_file.name)) + '\t' + str(
                        "\\".join(header.split("\\")[:-1])) + '\t' + str(index + 1) + '\t' + str(data_label) + '\n')
                    column_list.append(header)
        row[-1] = row[-1].replace('\t', '\n')
        data_file.write(''.join(row))
        print(''.join(row))
        rows.append(row)
    check_subject(rows)
    data_file.close()


def check_subject(rows):
    """
    Function: Checks in a log file if the subject is new or if there is information added or removed.

    Parameters:
        - rows   List    List containing lists with the retrieved QIB information of a subject.
    """
    #Could become slow, because of the way it is checking if a row is already presented. Also needs a way to read the header for each datapiece.

    if __name__ != "__main__":
        set_subject_logger(True)
        subject_logger = logging.getLogger("QIBSubjects")
    else:
        subject_logger = logging.getLogger("QIBSubjects")

    with open(subject_logger.handlers[0].baseFilename, "r") as log_file:
        log_data = log_file.read()

    written_to_file = []

    for row in rows:
        found_info = False
        found_subject = False
        if row[0] in log_data:
            found_subject = True
            print("subject found")
            if ''.join(row) in log_data:
                found_info = True
                print("info found")

        if not found_subject and row not in written_to_file:
            subject_logger.info("New subject: " + ''.join(row))
            written_to_file.append(row)
            print ("subject log written")
        elif not found_info and row not in written_to_file:
            subject_logger.info("New info for Subject: " + ''.join(row))
            written_to_file.append(row)
            print ("info log written")


def check_file_existence(file, type):
    try:
        file_test = open(file, 'r')
        config = ConfigParser.SafeConfigParser()
        config.read(file)
        return config

    except IOError:
        print("%s config file not found") % type
        logging.critical(type + " config file not found")
        sys.exit()


def configError(e):
    """
    Function: Error for when a variable is not found in a config file.

    Parameters:
        - e     Exception
    """
    logging.critical(e)
    if __name__ == "__main__":
        print(str(e) + "\nExit")
        sys.exit()


def set_subject_logger(test_bool):
    subject_logger = logging.getLogger("QIBSubjects")
    subject_logger.setLevel(logging.INFO)

    if test_bool:
        ch = logging.FileHandler("test_files/QIBSubjects.log")
    else:
        ch = logging.FileHandler("QIBSubjects.log")

    ch.setLevel(logging.INFO)
    subform = logging.Formatter('%(asctime)s:%(message)s')
    ch.setFormatter(subform)
    subject_logger.addHandler(ch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", help="Location of the configuration file for establishing XNAT connection.")
    parser.add_argument("--params", help="Location of the configuration file for the variables in the .params files.")
    parser.add_argument("--tags", help="Location of the configuration file for the tags.")
    args = parser.parse_args()
    logging.basicConfig(filename="QIBlog.log", format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)
    set_subject_logger(False)
    main(args)
