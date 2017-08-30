# QIB importer

**Function:** Connect to an XNAT instance. Inside the given project in XNAT it searches for QIB datatypes.
These datatypes are than transformed into a directory structure that can be uploaded to TranSMART.

**Functional:** The script is tested with Python2.7 and Python3.6

**Requirements:**
- *xnatpy*      Downloadable here: https://bitbucket.org/bigr_erasmusmc/xnatpy, for Python3 functionality use the feature/xsdparse branch.
- *nose*        Can be installed by running pip install nose on the command line

A requirements.txt file is in the repository. To use this run the following statement on the command line.

```
pip install -r requirements.txt
```

**Parameters:**

- *--all*           Location of the configuration file, containing all the information.
- *--connection*    Location of the configuration file for establishing XNAT connection.
- *--params*        Location of the configuration file for the variables in the .param files.
- *--tags*          Location of the configuration file for the tags.

It is optional whether you use --all or the other three.

**Configuration file format:**

--connection configuration file:

```
[Connection]
url =
user =
password =
project =
patient_map_file =

[Directory]
path =
```

--params configuration file:

```
[Study]
STUDY_ID =
SECURITY_REQUIRED =
TOP_NODE =
```

--tags configuration file:

```
[Tags]
Taglist =
```


## Testing

Testing can be done by entering

```
python test_QIB.py
```

on the command line.

Functions that are tested in test_QIB.py:

   - Establishing connection
        - Good (test_main_connection)
        - Wrong (test_wrong_connection)
        - Not finding project (test_unfound_project)
   - Create dir structure (test_create_dir_structure)
   - Write params (test_write_params)
   - Write header (test_write_headers)
   - Obtain data (test_obtain_data)
   - If no QIB is present (test_no_QIB)
   - Write meta_data (test_write_meta_data)
   - Write data (test_write_data)
   - Write logging of subjects
        - New subject (test_write_logging_new_subject)
        - New information (test_write_logging_new_information)
        - Nothing new (test_write_logging_existing_information)