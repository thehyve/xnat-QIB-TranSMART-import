""""
Name: QIB2TBatch
Function: Library used by QIBconverter. This library contains all the functions needed to convert a QIB datatype to a
directory which can be uploaded to a TranSMART database.
Author: Jarno van Erp
Company: The Hyve

Requirements:
xnatpy      Downloadable here: https://bitbucket.org/bigr_erasmusmc/xnatpy

"""

import os
import sys
import logging

import xnat
import json
from datetime import datetime

if sys.version_info.major == 3:
    import configparser as ConfigParser
elif sys.version_info.major == 2:
    import ConfigParser


def make_connection(config):
    """
    Function: Create the connection to XNAT.
    Parameters:
        -config     ConfigStorage object    Object which holds the information stored in the configuration files.
    Returns: 
        -project    xnatpy object           Xnat connection to a specific project.
        -connection xnatpy object           Xnat wide connection.
    """

    try:
        connection = xnat.connect(config.connection_name, user=config.user, password=config.pssw)
        project = connection.projects[config.project_name]
        logging.info("Connection established.")
        return project, connection

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


def create_dir(path):
    """
    Function: Create the directory structure.
    Parameters:
        -path    String                  Path to the directory where all the files will be saved.
    """
    if os.path.exists(path):
        raise ValueError('Path already exists: {0}'.format(path))
    os.makedirs(path + "/tags/", exist_ok=True)
    os.makedirs(path + "/clinical/", exist_ok=True)


def write_params(path, config):
    """
    Function: Uses the configuration files to write the .params files.
    Parameters:
        -path       String                  Path to the directory where all the files will be saved.
        -config     ConfigStorage object    Object which holds the information stored in the configuration files.
    """
    tag_param_file = open(path + '/tags/tags.params', 'w')
    tag_param_file.write("TAGS_FILE=tags.txt")
    study_param_file = open(path + '/study.params', 'w')
    study_param_file.write("STUDY_ID=" + config.study_id +
                           "\nSECURITY_REQUIRED=" + config.security_req +
                           "\nTOP_NODE=" + config.top_node +
                           "\nAPPEND_FACTS=" + config.append_facts)
    clinical_param_file = open(path + '/clinical/clinical.params', 'w')
    clinical_param_file.write("COLUMN_MAP_FILE=" + str(config.study_id) + "_columns.txt\nTAGS_FILE=../tags/tags.txt")
    tag_param_file.close()
    study_param_file.close()
    clinical_param_file.close()


def write_headers(path, config):
    """
    Function: Uses the configuration files to write the headers of the .txt files. 
    Parameters:
        -path           String                  Path to the directory where all the files will be saved.
        -config         ConfigStorage object    Object which holds the information stored in the configuration files.
    Returns:
        -tag_file        File                   tags.txt, used to upload the metadata into TranSMART.
        -data_file       File                   (STUDY_ID)_clinical.txt, used to upload the clinical data into TranSMART.
        -concept_file    File                   (STUDY_ID)_columns.txt, used to determine which values are in which columns for uploading to TranSMART.
    """

    data_file = open(path + '/clinical/' + config.study_id + '_clinical.txt', 'w')
    concept_file = open(path + '/clinical/' + config.study_id + '_columns.txt', 'w')
    tag_file = open(path + '/tags/tags.txt', 'w')
    # Hardcoded right now, because transmart does not need other headers. But this can be subject to change.
    concept_headers = ['Filename', 'Category Code', 'Column Number', 'Data Label']
    tag_headers = ['Concept Path', 'Title', 'Description', 'Weight']
    concept_file.write("\t".join(concept_headers) + '\n')
    tag_file.write("\t".join(tag_headers) + "\n")
    tag_file.flush()
    data_file.flush()
    concept_file.flush()
    return tag_file, data_file, concept_file


def obtain_data(project, tag_file, patient_map, config):
    """
    Function: Obtains all the QIB data from the XNAT project.
    Parameters: 
        -project             xnatpy object           Xnat connection to a specific project.
        -tag_file            File                    tags.txt, used to upload the metadata into TranSMART.
        -patient_map         Dictionary              Dictionary with the patient mapping with the XNAT identifier as key.
        -config              ConfigStorage object    Object which holds the information stored in the configuration files.
    Returns:
        -data_list           List                     List containing directories per subject, key = header, value = value.
        -data_header_list    List                     List containing all the headers.
    """
    concept_key_list = []
    data_header_list = []
    data_list = []
    tag_dict = {}
    scanner_dict = {}
    if os.path.isfile(config.scanner_dict_file):
        with open(config.scanner_dict_file) as f:
            for line in f:
                (key, val) = line.replace('\n','').split('\t')
                scanner_dict[key] = val
    for subject in project.subjects.values():
        data_row_dict = {}
        subject_obj = project.subjects[subject.label]
        for experiment in subject_obj.experiments.values():
            if "qib" in experiment.label.lower() and experiment.project == config.project_name:
                # TODO: Make number of returns and parameters less.
                data_header_list, data_row_dict, concept_key_list, tag_dict, scanner_dict = retrieve_QIB(experiment,
                                                                                           tag_file, data_row_dict,
                                                                                           subject, data_header_list,
                                                                                           concept_key_list, tag_dict,
                                                                                           patient_map, config,
                                                                                           scanner_dict, project)
        if len(data_row_dict) > 0:
            data_list.append(data_row_dict)

    if data_list == [{}] or data_list == []:
        logging.warning("No QIB datatypes found.")
        print("No QIB datatypes found.\nExit")
        if __name__ == "__main__":
            sys.exit()
        else:
            return data_list

    return data_list, data_header_list


def retrieve_QIB(experiment, tag_file, data_row_dict, subject, data_header_list, concept_key_list, tag_dict,
                 patient_map, config, scanner_dict ,project):
    """
    Function: Retrieve the biomarker information from the QIB datatype.
    
    Parameters:
        -subject_obj         Xnatpy.subject          Subject object derived from XNATpy
        -experiment          Xnatpy.experiment       Experiment object derived from XNATpy
        -tag_file            File                    File used to write the metadata tags to
        -data_row_dict       Dict                    Dictionary for storing the subject information, headers = key
        -subject             Subject                 Subject derived from XNATpy
        -data_header_list    List                    List used for storing all the headers
        -concept_key_list    List                    List used for storing the concept keys
        -tag_dict            Dictionary              Dictionary used to check if certain lines are already in the tagsfile.
        -patient_map         Dictionary              Dictionary with the patient mapping with the XNAT identifier as key.
        -config              ConfigStorage object    Object which holds the information stored in the configuration files.
    
    Returns:
        -data_header_list    List            List with the headers stored
        -data_row_dict       Dictionary      Dict containing all the QIB information of the subject
        -concept_key_list    List            List containing all the concept keys so far.
        -tag_dict            Dictionary      Dictionary used to check if certain lines are already in the tagsfile.
    """
    subject_obj = project.subjects[subject.label]
    session = subject_obj.experiments[experiment.label]

    begin_concept_key, tag_dict = write_project_metadata(session, tag_file, tag_dict, config)

    # TODO: build in check to see if patiet_map file exists, if not skip this step
    if subject.label in patient_map:
        data_row_dict['subject'] = patient_map[subject.label]
    else:
        data_row_dict['subject'] = subject.label
    if 'subject' not in data_header_list:
        data_header_list.append('subject')

    label_list = experiment.label.split('_')
    metadata, scanner_dict = get_session_data(label_list, project, session.base_sessions.values(), scanner_dict, config)
    for x in metadata:
        if "scanner " in x:
            line = '\t'.join([begin_concept_key+'\\'+metadata["scanner"], x, str(metadata[x])]) + "\t" + str(1) + "\n"
            if line not in tag_dict:
                tag_file.write(line)
                tag_dict[line] = True

    for biomarker_category in session.biomarker_categories:
        results = session.biomarker_categories[biomarker_category]

        for biomarker in results.biomarkers:
            biomarker_id = results.biomarkers[biomarker].id
            concept_value = results.biomarkers[biomarker].value

            concept_path_items = [
                str(begin_concept_key),
                str(metadata["scanner"]),
                str(results.category_name),
                metadata["laterality"],
                metadata["timepoint"],
                str(biomarker_id)
            ]
            concept_key = '\\'.join(concept_path_items)

            data_row_dict[concept_key] = concept_value

            if concept_key not in data_header_list and concept_key not in concept_key_list:
                data_header_list.append(concept_key)
                concept_key_list.append(concept_key)

                if __name__ == "QIB2TBatch":
                    tag_dict = write_concept_tags(results, biomarker, concept_key, tag_file, tag_dict, session,
                                                  metadata)

    return data_header_list, data_row_dict, concept_key_list, tag_dict, scanner_dict


def write_concept_tags(results, biomarker, concept_key, tag_file, tag_dict, session, metadata):
    """

    Parameters:
        -results            XNAT object         Parsed QIB XML object.
        -biomarker          XNAT object         biomarker from parsed XML.
        -concept_key        String              concept key for TranSMART
        -concept_key_list   List                List containing the already used concept keys.
        -tag_file           File                File for the metadata tags.
        -tag_dict           Dictionary          Dictionary used to check if certain lines are already in the tagsfile.
        -session            Xnatpy.session      Session object from xnatpy, used to retrieve the accession identifier.

    Returns:
        -concept_key_list     List          List containing the already used concept keys.
        -tag_dict             Dictionary    Dictionary used to check if certain lines are already in the tagsfile.

    """
    ontology_name = results.biomarkers[biomarker].ontology_name
    ontology_IRI = results.biomarkers[biomarker].ontology_iri
    lines = []
    weight = 1
    lines.append('\t'.join([concept_key, "Ontology name", ontology_name]) + '\t' + str(weight) + '\n')
    lines.append('\t'.join([concept_key, "Ontology IRI", ontology_IRI]) + '\t' + str(weight) + '\n')
    weight = 2

    for x in session.base_sessions.values():
        lines.append(
            '\t'.join([concept_key, "accession identifier", x.accession_identifier]) + "\t" + str(weight) + "\n")

    for x in metadata:
        lines.append('\t'.join([concept_key, x, str(metadata[x])]) + "\t" + str(weight) + "\n")

    for line in lines:
        if (concept_key+x) not in tag_dict.keys():
            tag_dict[line] = True
            tag_file.write(line)

    return tag_dict


def write_project_metadata(session, tag_file, tag_dict, config):
    """
    Function: Write the metadata tags to the tag file.

    Parameters:
        -session       XNAT object              QIB datatype object in XNATpy
        -tag_file      File                     File for the metadata tags.
        -tag_dict      Dictionary               Dictionary used to check if certain lines are already in the tagsfile.
        -config        ConfigStorage object     Object which holds the information stored in the configuration files.

    Returns:
         -concept_key   String          concept key for TranSMART
         -tag_dict      Dictionary      Dictionary used to check if certain lines are already in the tagsfile.
    """

    analysis_tool = getattr(session, "analysis_tool")
    analysis_tool_version = getattr(session, "analysis_tool_version")
    if analysis_tool and analysis_tool_version:
        concept_key = str(analysis_tool + " " + analysis_tool_version)
    elif analysis_tool:
        concept_key = (analysis_tool)
    else:
        concept_key = "Generic Tool"
    i = len(config.tag_list)
    for tag in config.tag_list:
        try:
            info_tag = getattr(session, tag)
            if info_tag:
                line = concept_key + "\t" + tag.replace('_', ' ') + "\t" + str(info_tag) + "\t" + str(i) + "\n"
                i -= 1
                if (concept_key+tag) not in tag_dict.keys():
                    tag_dict[concept_key+tag] = info_tag
                    tag_file.write(line)
        except AttributeError:
            logging.info(tag + " not found for " + str(concept_key))
    return concept_key, tag_dict


def write_data(data_file, concept_file, data_list, data_header_list):
    """
    Function: Writes the data from data_list to data_file.
    Parameters: 
        -data_file           File    (STUDY_ID)_clinical.txt, used to upload the clinical data into TranSMART.
        -concept_file        File    (STUDY_ID)_columns.txt, used to determine which values are in which columns for uploading to TranSMART.
        -data_list           List    List containing a directory per subject, key = header, value = value.
        -data_header_list    List    List containing all the headers.
    """
    data_file.write("\t".join(data_header_list) + '\n')
    column_list = []
    subject_written = False
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
                if header == "subject" and subject_written == False:
                    concept_file.write(str(os.path.basename(data_file.name)) + '\t' + str(header) + '\t' + str(
                        index + 1) + '\tSUBJ_ID\n')
                    subject_written = True
                    column_list.append(header)
                elif header not in column_list:
                    data_label = header.split("\\")[-1]
                    column_list.append(header)
                    concept_file.write(str(os.path.basename(data_file.name)) + '\t' + str(
                        "\\".join(header.split("\\")[:-1])) + '\t' + str(index + 1) + '\t' + str(data_label) + '\n')
        row[-1] = row[-1].replace('\t', '\n')
        data_file.write(''.join(row))
    data_file.close()

def check_config_existence(file_, type):
    """
    Function: Checks if the configuration file exists and reads its content.
    Parameters:
        -file_      String      Path to configuration file.
        -type       String      Which type of configuration file.
    Returns:
        -config     Config      Parsed configuration file object.
    """

    try:
        file_test = open(file_, 'r')
        config = ConfigParser.SafeConfigParser()
        config.read(file_)
        file_test.close()
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

def get_patient_mapping(config):
    """
    Function: Parse the patient mapping file to a dictionary.
    Parameter:
        -config         ConfigStorage object     Object which holds the information stored in the configuration files.
    Returns:
        -patient_dict   Dictionary               Dictionary used to map the patient identifiers. Key is identifier from XNAT.
    """
    patient_dict = {}
    if os.path.isfile(config.scanner_dict_file):
        with open(config.patient_file, 'r') as patient_file:
            for line in patient_file:
                line_list = line.replace("\n", "").split('\t')
                patient_dict[line_list[0]] = line_list[1]
    return patient_dict


def get_session_data(label_list, project, sessions, scanner_dict, config):
    """
    Function: get metadata from session through accession number
    Parameters:
        -subject_obj    xnatpy object   Object which contains the sessions
        -label_list     List            parsed list of the label
    Returns:
        -metadata       Dictionary      Dictionary with metadata stored inside it.
    """
    metadata = {}
    #TODO: Add parameter to config for scanner dict file. read file to dict and look up scanner/model to determin scanner number.
    _session = [project.experiments[x.accession_identifier] for x in sessions][0]
    metadata["laterality"] = _session._fields.get('laterality', label_list[3])
    metadata["timepoint"] = _session._fields.get('timepoint', label_list[4])
    _session = project.experiments['_'.join(label_list[1:])]
    #metadata["scanner"] = _session.get('scanner') or label_list[2]
    metadata["scanner model"] = _session.get('scanner/model') or "Not specified"
    metadata["scanner manufacturer"] = _session.get('scanner/manufacturer') or "Not specified"
    scanner_name = metadata["scanner manufacturer"]+metadata["scanner model"]
    if scanner_dict.get(scanner_name):
        metadata["scanner"] = "scanner" + str(scanner_dict.get(metadata["scanner manufacturer"] +
                                                               metadata["scanner model"]))
    else:
        scanner_number = len(scanner_dict)+1
        metadata["scanner"] = "scanner"+str(scanner_number)
        with open(config.scanner_dict_file, 'a') as f:
            f.write(scanner_name+'\t'+str(scanner_number)+'\n')
        scanner_dict[scanner_name] = scanner_number
    return metadata, scanner_dict


def check_if_updated(project, config):
    updated = False
    json_dict = {'last_updated' : datetime.now().isoformat(), 'session_list' : {}}

    for subject in project.subjects.values():
        subject_obj = project.subjects[subject.label]
        for experiment in subject_obj.experiments.values():
            if "qib" in experiment.label.lower() and experiment.project == config.project_name:
                session = subject_obj.experiments[experiment.label]
                # TODO: Ask only processing_end_date_time?
                subject_timestamp = getattr(session, 'processing_end_date_time').isoformat()
                json_dict['session_list'][experiment.label] = subject_timestamp

    if os.path.isfile(config.timestamp_json_log):
        with open(config.timestamp_json_log, 'r') as fp:
            data = json.load(fp)
            for experiment_label in json_dict['session_list']:
                if not data['session_list'][experiment_label] == json_dict['session_list'][experiment_label]:
                    updated = True
    if updated or not os.path.isfile(config.timestamp_json_log):
        with open(config.timestamp_json_log, 'w') as fp:
           json.dump(json_dict, fp)
    return updated