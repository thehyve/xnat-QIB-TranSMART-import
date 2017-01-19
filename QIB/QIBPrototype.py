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

--params configuration file:

[Study]
STUDY_ID = 
SECURITY_REQUIRED =
TOP_NODE = 

[Tags]
TAGS_FILE = 

--tags configuration file:

[Tags]
Taglist = 


"""

import ConfigParser
import argparse
import os
import sys
import logging
import xnat


def main(args):
    """
    Function: Call all the methods, passing along all the needed variables.
    Parameters:
        -args   ArgumentParser      Contains the location of the configuration files.
    """
    logging.info("Start.")
    print('Establishing connection\n')
    project = make_connection(args)

    print('Creating directory structure\n')
    path = create_dir(args)

    print('Write .params files\n')
    write_params(path, args)

    print('Write headers\n')
    tag_file, data_file, concept_file = write_headers(path, args)

    print('Obtaining data from XNAT\n')
    data_list, data_header_list = obtain_data(project, tag_file, args)
    logging.info("Data obtained from XNAT.")

    print('Write data to files\n')
    write_data(data_file, concept_file, data_list, data_header_list)
    logging.info("Data written to files.")

    logging.info("Exit.")


def make_connection(args):
    """
    Function: Create the connection to XNAT.
    Returns: 
        -project    xnatpy object   Xnat connection to a specific project.
    """
    try:
        file_test = open(args.connection, 'r')
        config = ConfigParser.SafeConfigParser()
        config.read(args.connection)
    except IOError:
        print("Connection config file not found")
        logging.critical("Connection config file not found")
        sys.exit()

    try:
        connection_name = config.get('Connection', 'url')
        user = config.get('Connection', 'user')
        pssw = config.get('Connection', 'password')
        project_name = config.get('Connection', 'project')
        connection = xnat.connect(connection_name, user=user, password=pssw)
        project = connection.projects[project_name]
        logging.info("Connection established.")
        return project

    except ConfigParser.NoSectionError as e:
        configError(e)

    except KeyError:
        print("Project not found in XNAT.\nExit")
        logging.critical("Project not found in XNAT.")
        if __name__ == "__main__":
            sys.exit()
        else:
            return None

    except Exception as e:
        print(str(e) + "\nExit")
        logging.critical(e.message)
        if __name__ == "__main__":
            sys.exit()
        else:
            return e


def create_dir(args):
    """
    Function: Create the directory structure.
    Returns: 
        -newpath    String   Path to the directory where all the files will be saved.
    """
    try:
        file_test = open(args.params, 'r')
        config = ConfigParser.ConfigParser()
        config.read(args.params)
    except IOError:
        print("Params config file not found")
        logging.critical("Params config file not found")
        sys.exit()

    try:
        base_path = config.get('Directory', 'path')
        newpath = config.get('Study', 'STUDY_ID')
        path = base_path+newpath
        if not os.path.exists(path):
            os.makedirs(path)
            os.makedirs(path + "/tags/")
            os.makedirs(path + "/clinical/")
        print(path)
        return path

    except ConfigParser.NoSectionError as e:
        configError(e)


def write_params(path, args):
    """
    Function: Uses the configuration files to write the .params files.
    Parameters:
        -path   String  Path to the directory where all the files will be saved.
    """
    try:
        file_test = open(args.params, 'r')
        config = ConfigParser.ConfigParser()
        config.read(args.params)
    except IOError:
        print("Params config file not found")
        logging.critical("Params config file not found")
        sys.exit()

    try:
        study_id = config.get('Study', "STUDY_ID")
        security_req = config.get('Study', 'SECURITY_REQUIRED')
        top_node = config.get('Study', 'TOP_NODE')
        tag_file = config.get('Tags', 'TAGS_FILE')
        tag_param_file = open(path + '/tags/tags.params', 'w')
        tag_param_file.write("TAGS_FILE=" + tag_file)
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
        -tagFile        File    tags.txt, used to upload the metadata into TranSMART.
        -dataFile       File    (STUDY_ID)_clinical.txt, used to upload the clinical data into TranSMART.
        -conceptFile    File    (STUDY_ID)_columns.txt, used to determine which values are in which columns for uploading to TranSMART.
    """
    try:
        file_test = open(args.params, 'r')
        config = ConfigParser.ConfigParser()
        config.read(args.params)

    except IOError:
        print("Params config file not found")
        logging.critical("Params config file not found")
        sys.exit()

    try:
        study_id = config.get('Study', "STUDY_ID")
        dataFile = open(path + '/clinical/' + study_id + '_clinical.txt', 'w')
        conceptFile = open(path + '/clinical/' + study_id + '_columns.txt', 'w')
        tagFile = open(path + '/tags/tags.txt', 'w')
        conceptHeaders = ['Filename', 'Category Code', 'Column Number', 'Data Label']
        tagHeaders = ['Concept Path', 'Title', 'Description', 'Weight']
        conceptFile.write("\t".join(conceptHeaders) + '\n')
        tagFile.write("\t".join(tagHeaders) + "\n")
        tagFile.flush()
        dataFile.flush()
        conceptFile.flush()
        return tagFile, dataFile, conceptFile

    except ConfigParser.NoSectionError as e:
        configError(e)


def obtain_data(project, tagFile, args):
    """
    Function: Obtains all the QIB data from the XNAT project.
    Parameters: 
        -project        xnatpy object   Xnat connection to a specific project.
        -tagFile        File            tags.txt, used to upload the metadata into TranSMART.
    Returns:
        -dataList           List    List containing directories per subject, key = header, value = value.
        -dataHeaderList     List    List containing all the headers. 
    """
    conceptKeyList = []
    dataHeaderList = []
    dataList = []
    tag_dict = {}
    for subject in project.subjects.values():
        dataRowDict = {}
        print subject.label
        subjectObj = project.subjects[subject.label]
        for experiment in subjectObj.experiments.values():
            if "qib" in experiment.label.lower():
                dataHeaderList, dataRowDict, conceptKeyList, tag_dict = retrieveQIB(subjectObj, experiment, tagFile, dataRowDict,
                                                                          subject, dataHeaderList, conceptKeyList, tag_dict, args)
        dataList.append(dataRowDict)
    print(dataList)
    if dataList == [{}] or dataList == []:
        logging.warning("No QIB datatypes found.")
        print("No QIB datatypes found.\nExit")
        if __name__ == "__main__":
            sys.exit()
        else:
            return dataList
    print(dataList)
    print(dataHeaderList)
    return dataList, dataHeaderList


def retrieveQIB(subjectObj, experiment, tagFile, dataRowDict, subject, dataHeaderList, conceptKeyList, tag_dict, args):
    """
    Function: Retrieve the biomarker information from the QIB datatype.
    
    Parameters:
        - subjectObj        XNAT.subject    Subject object derived from XNATpy
        - experiment        XNAT.experiment Experiment object derived from XNATpy
        - tagFile           File            File used to write the metadata tags to
        - dataRowDict       Dict            Dictionary for storing the subject information, headers = key
        - subject           Subject         Subject derived from XNATpy
        - dataHeaderList    List            List used for storing all the headers
        - conceptKeyList    List            List used for storing the concept keys
    
    Returns:
        - dataHeaderList    List            List with the headers stored
        - dataRowDict       Dict            Dict containing all the QIB information of the subject
        - conceptKeyList    List            List containing all the concept keys so far.
    """
    session = subjectObj.experiments[experiment.label]
    beginConceptKey, tag_dict = writeMetaData(session, tagFile, tag_dict, args)
    dataRowDict['subject'] = subject.label
    if 'subject' not in dataHeaderList:
        dataHeaderList.append('subject')
    for biomarker_categorie in session.biomarker_categories:
        results = session.biomarker_categories[biomarker_categorie]
        for biomarker in results.biomarkers:
            print biomarker_categorie
            print biomarker
            conceptValue = results.biomarkers[biomarker].value
            conceptKey = str(beginConceptKey) + '\\' + str(biomarker_categorie) + "\\" + str(biomarker)
            dataRowDict[conceptKey] = conceptValue
            if conceptKey not in dataHeaderList:
                dataHeaderList.append(conceptKey)
                if __name__ == "__main__":
                    conceptKeyList, tag_dict = writeOntologyTag(results, biomarker, conceptKey, conceptKeyList, tagFile, tag_dict)

    return dataHeaderList, dataRowDict, conceptKeyList, tag_dict


def writeOntologyTag(results, biomarker, conceptKey, conceptKeyList, tagFile, tag_dict):
    """

    Parameters:
        - results           XNAT object     Parsed QIB XML object.
        - biomarker         XNAT object     biomarker from parsed XML.
        - conceptKey        String          concept key for TranSMART
        - conceptKeyList    List            List containing the already used concept keys.
        - tagFile           File            File for the metadata tags.

    Returns:
        -conceptKeyList     List    List containing the already used concept keys.

    """
    ontologyName = results.biomarkers[biomarker].ontology_name
    ontologyIRI = results.biomarkers[biomarker].ontology_iri
    if conceptKey not in conceptKeyList:
        ontology_name_row = [conceptKey, ontologyName, "Ontology name"]
        ontology_iri_row = [conceptKey, ontologyIRI, "Ontology IRI"]
        tagFile.write('\t'.join(ontology_name_row) + '\t5\n')
        tagFile.write('\t'.join(ontology_iri_row) + '\t5\n')
        conceptKeyList.append(conceptKey)
    return conceptKeyList, tag_dict


def writeMetaData(session, tagFile, tag_dict, args):
    """
    Function: Write the metadata tags to the tag file.

    Parameters:
        - session       XNAT object     QIB datatype object in XNATpy
        - tagFile       File            File for the metadata tags.
        - args          ArgumentParser

    Returns:
         - conceptKey   String          concept key for TranSMART


    fix: Check if line is already in file
    """
    try:
        file_test = open(args.tags, 'r')
        config = ConfigParser.SafeConfigParser()
        config.read(args.tags)

    except IOError:
        print("Tags config file not found")
        logging.critical("Tags config file not found")
        sys.exit()

    try:
        tagList = config.get("Tags", "Taglist").split(', ')
        print(session.__class__.__dict__)
        #accession_identifier = session.basesessions
        analysis_tool = getattr(session, "analysis_tool")
        analysis_tool_version = getattr(session, "analysis_tool_version")
        if analysis_tool and analysis_tool_version:
            conceptKey = str(analysis_tool + " " + analysis_tool_version)
        elif analysis_tool:
            conceptKey = (analysis_tool)
        else:
            conceptKey =   "Generic Tool"
        for tag in tagList:
            info_tag = getattr(session, tag)
            if info_tag:
                line = conceptKey + "\t" + str(info_tag) + "\t" + tag.replace('_', ' ') + "\t5\n"
                try:
                    found = tag_dict[line]
                except KeyError:
                    tag_dict[line] = True
                    tagFile.write(line)
        if len(session.base_sessions.values()) >= 1:
            accession_identifier = session.base_sessions.values()[0].accession_identifier
            line = conceptKey + "\t" + str(accession_identifier) + "\taccession identifier\t5\n"
            try:
                found = tag_dict[line]
            except KeyError:
                tag_dict[line] = True
                tagFile.write(line)
        return conceptKey, tag_dict

    except ConfigParser.NoSectionError as e:
        configError(e)


def write_data(dataFile, conceptFile, dataList, dataHeaderList):
    """
    Function: Writes the data from dataList to dataFile.
    Parameters: 
        -dataFile           File    (STUDY_ID)_clinical.txt, used to upload the clinical data into TranSMART.
        -conceptFile        File    (STUDY_ID)_columns.txt, used to determine which values are in which columns for uploading to TranSMART. 
        -dataList           List    List containing a directory per subject, key = header, value = value.
        -dataHeaderList     List    List containing all the headers.   
    """
    dataFile.write("\t".join(dataHeaderList) + '\n')
    columnList = []
    rows = []
    for line in dataList:
        row = []
        i = 0
        while i < len(dataHeaderList):
            row.append('\t')
            i += 1
        for header in dataHeaderList:
            if header in line.keys():
                infoPiece = line[header]
                index = dataHeaderList.index(header)
                row[index] = infoPiece + '\t'
                if header == "subject":
                    conceptFile.write(str(os.path.basename(dataFile.name)) + '\t' + str(header) + '\t' + str(
                        index + 1) + '\tSUBJ_ID\n')
                elif header not in columnList:
                    dataLabel = header.split("\\")[-1]
                    conceptFile.write(str(os.path.basename(dataFile.name)) + '\t' + str(
                        "\\".join(header.split("\\")[:-1])) + '\t' + str(index + 1) + '\t' + str(dataLabel) + '\n')
                    columnList.append(header)
        row[-1] = row[-1].replace('\t', '\n')
        dataFile.write(''.join(row))
        rows.append(row)
    check_subject(rows)
    dataFile.close()


def check_subject(rows):
    """
    Function: Checks in a log file if the subject is new or if there is information added or removed.

    Parameters:
        - rows   List    List containing lists with the retrieved QIB information of a subject.
    """
    if __name__ != "__main__":
        set_subject_logger(True)
        subjectlogger = logging.getLogger("QIBSubjects")
    else:
        subjectlogger = logging.getLogger("QIBSubjects")
    with open(subjectlogger.handlers[0].baseFilename, "r") as logFile:
        logData = logFile.read()
    written_to_file = []
    for row in rows:
        foundInfo = False
        foundSubject = False
        if row[0] in logData:
            foundSubject = True
            print("subject found")
            if ''.join(row) in logData:
                foundInfo = True
                print("info found")
        if not foundSubject and row not in written_to_file:
            subjectlogger.info("New subject: " + ''.join(row))
            written_to_file.append(row)
            print ("subject log written")
        elif not foundInfo and row not in written_to_file: 
            subjectlogger.info("New info for Subject: " + ''.join(row))
            written_to_file.append(row)
            print ("info log written")


def configError(e):
    """
    Function: Error for when a variable is not found in a config file.

    Parameters:
        - e     Exception
    """
    logging.critical(e)
    print(__name__)
    if __name__ == "__main__":
        print(str(e) + "\nExit")
        sys.exit()

def set_subject_logger(test_bool):
    subjectlogger = logging.getLogger("QIBSubjects")
    subjectlogger.setLevel(logging.INFO)
    if test_bool:
        ch = logging.FileHandler("test_files/QIBSubjects.log")
    else:
        ch = logging.FileHandler("QIBSubjects.log")
    ch.setLevel(logging.INFO)
    subform = logging.Formatter('%(asctime)s:%(message)s')
    ch.setFormatter(subform)
    subjectlogger.addHandler(ch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", help="Location of the configuration file for establishing XNAT connection.")
    parser.add_argument("--params", help="Location of the configuration file for the variables in the .params files.")
    parser.add_argument("--tags", help="Location of the configuration file for the tags.")
    args = parser.parse_args()
    logging.basicConfig(filename="QIBlog.log", format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)
    set_subject_logger(False)
    main(args)