'''
Name: test_QIB
Function: Testing different functional aspects of QIB2TBatch
Author: Jarno van Erp
Company: The Hvye

Functions that are tested:
   - Establishing connection
        - Good (test_main_connection)
        - Wrong (test_wrong_connection)
        - not finding project (test_unfound_project)
   - Create dir structure (test_create_dir_structure)
   - Write params (test_write_params)
   - Write header (test_write_headers)
   - Obtain data (test_obtain_data)
   - if no QIB is present (test_no_QIB)
   - Write meta_data (test_write_meta_data)
   - Write data (test_write_data)
   - write logging of subjects
        - New subject (test_write_logging_new_subject)
        - New information (test_write_logging_new_information)
        - Nothing new (test_write_logging_existing_information)
'''

import re
import time
import shutil
import unittest
import QIB2TBatch
from nose.tools import assert_not_equal
import argparse
import os
import sys
from ConfigStorage import ConfigStorage
if sys.version_info.major == 3:
    import configparser as ConfigParser
elif sys.version_info.major == 2:
    import ConfigParser


class TestQIBDatatypeRetrieval(unittest.TestCase):

    file_path = "test_files/"
    configPath = "test_files/test_confs/"

    def setup(self, conf_file):
        parser = argparse.ArgumentParser()
        parser.add_argument("--connection")
        args = parser.parse_args()        
        args.connection = conf_file
        config = ConfigStorage(args)
        project, connection = QIB2TBatch.make_connection(config)
        return project, connection

    def empty_file(self, file_name):
        with open(file_name, 'w') as file_:
            file_.write("")
        with open(file_name, 'r') as file_:
            assert file_.read() == ""

    def test_main_connection(self):
        conf_file = self.configPath+"/test.conf"
        project, connection = self.setup(conf_file)
        assert_not_equal(project, None)
        connection.disconnect()

    def test_wrong_connection(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--connection")
        args = parser.parse_args()        
        args.connection = self.configPath+"fail_conn.conf"
        config = ConfigStorage(args)
        project, connection = QIB2TBatch.make_connection(config)
        self.assertEqual(project.__class__.__name__, "ConnectionError")

    def test_unfound_project(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--connection")
        args = parser.parse_args()        
        args.connection = self.configPath+"wrong_project.conf"
        config = ConfigStorage(args)
        project, connection = QIB2TBatch.make_connection(config)
        self.assertEqual(project, None)

    def test_create_dir_structure(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--params")
        args = parser.parse_args()
        args.params = self.configPath+"test.conf"
        config = ConfigStorage(args)
        path = QIB2TBatch.create_dir(config)
        assert os.path.exists(path)
        assert os.path.exists(path+"/tags/")
        assert os.path.exists(path + "/clinical/")

    def test_write_params(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--params")
        args = parser.parse_args()
        args.params = self.configPath+"test.conf"
        config = ConfigStorage(args)
        path = config.base_path+config.study_id
        QIB2TBatch.write_params(path, config)

        with open(path + '/tags/tags.params', 'r') as tag_param_file:
            with open('test_files/tags.params', 'r') as test_tag_param_file:
                self.assertEqual(tag_param_file.read(), test_tag_param_file.read())

        with open(path + '/clinical/clinical.params', 'r') as clinical_param_file:
            with open('test_files/clinical.params', 'r') as test_clinical_param_file:
                self.assertEqual(clinical_param_file.read(), test_clinical_param_file.read())

        with open(path + '/study.params', 'r') as study_param_file:
            with open('test_files/study.params', 'r') as test_study_param_file:
                self.assertEqual(study_param_file.read(), test_study_param_file.read())

    def test_write_headers(self):
        args = argparse.ArgumentParser().parse_args()
        args.all = self.configPath+"test.conf"
        config = ConfigStorage(args)
        path = config.base_path+config.study_id
        tag_file, data_file, concept_file = QIB2TBatch.write_headers(path, config)

        assert os.path.exists(os.path.realpath(tag_file.name))
        assert os.path.exists(os.path.realpath(concept_file.name))

        with open(tag_file.name, 'r') as tag_file_r:
            first_line = tag_file_r.readline()
            self.assertEqual(first_line, "\t".join(['Concept Path', 'Title', 'Description', 'Weight'])+"\n")

        with open(concept_file.name, 'r') as concept_file_r:
            first_line = concept_file_r.readline()
            self.assertEqual(first_line, "\t".join(['Filename', 'Category Code', 'Column Number', 'Data Label'])+"\n")

    def test_obtain_data(self):
        data_structure = [{'subject': 'PROOF001', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\General Results\\L\\T0\\Time elapsed since baseline': '11', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T0\\Femoral cartilage volume': '6980.625', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T0\\Lateral Tibial Cartilage volume': '2450619.375', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T0\\Patellar cartilage volume': '2457600.0', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Bone\\L\\T0\\Femur volume': '10980.625', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Bone\\L\\T0\\Patella volume': '2450619.375'}, {'subject': 'PROOF002', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\Femoral Cartilage volume (mm^3)': '10337.25', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\entire (masked) image volume (mm^3)': '2457600.0', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\Femoral Cartilage volume (mm^3)': '10719.011926174164', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\entire (masked) image volume (mm^3)': '2457602.734375'}, {'subject': 'PROOF004', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\Femoral Cartilage volume (mm^3)': '9140.2768768668175', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\entire (masked) image volume (mm^3)': '2457607.2265625', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\Femoral Cartilage volume (mm^3)': '8626.1393963992596', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\entire (masked) image volume (mm^3)': '2457604.1015625'}, {'subject': 'PROOF005', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\Femoral Cartilage volume (mm^3)': '9982.8400918245316', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\entire (masked) image volume (mm^3)': '2457591.40625', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\Femoral Cartilage volume (mm^3)': '8842.1271081268787', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\entire (masked) image volume (mm^3)': '2457600.5859375'}]
        header_test_list = ['subject', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\General Results\\L\\T0\\Time elapsed since baseline', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T0\\Femoral cartilage volume', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T0\\Lateral Tibial Cartilage volume', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T0\\Patellar cartilage volume', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Bone\\L\\T0\\Femur volume', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Bone\\L\\T0\\Patella volume', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\Femoral Cartilage volume (mm^3)', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\L\\T78\\entire (masked) image volume (mm^3)', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\Femoral Cartilage volume (mm^3)', 'MultiAtlas Appearance Model Segmentation with Volume Calculation 0.1\\Cartilage\\R\\T78\\entire (masked) image volume (mm^3)']
        conf_file = self.configPath+"/test.conf"
        tag_file = open("test.txt", "w")
        project, connection = self.setup(conf_file)
        args = argparse.ArgumentParser().parse_args()
        args.all = conf_file
        config = ConfigStorage(args)
        patient_map = QIB2TBatch.get_patient_mapping(config)
        data_list, data_header_list = QIB2TBatch.obtain_data(project, tag_file, patient_map, config)
        tag_file.close()
        os.remove(tag_file.name)
        self.assertEqual(data_header_list, header_test_list)
        self.assertEqual(data_structure, data_list)
        connection.disconnect()


    def test_no_QIB(self):
        config = ConfigParser.ConfigParser()
        config.read(self.configPath+"test.conf")
        study_id = config.get('Study', 'STUDY_ID')
        path = config.get('Directory', 'path')
        tagFile = open(path+study_id+"/tags/tags.txt", "w")
        conf_file = self.configPath+"no_QIB.conf"
        args = argparse.ArgumentParser().parse_args()
        args.all = conf_file
        config = ConfigStorage(args)
        project, connection = self.setup(conf_file)
        patient_map = QIB2TBatch.get_patient_mapping(config)
        data_list = QIB2TBatch.obtain_data(project, tagFile, patient_map, config)
        self.assertEqual(data_list, [])

    def test_write_meta_data(self):
        tag_file = open("test.txt", "w")
        conf_file = self.configPath+"/test.conf"
        project, connection = self.setup(conf_file)
        args = argparse.ArgumentParser().parse_args()
        args.all = conf_file
        config = ConfigStorage(args)
        patient_map = QIB2TBatch.get_patient_mapping(config)
        data_list, data_header_list = QIB2TBatch.obtain_data(project, tag_file, patient_map, config)
        tag_file.flush()
        with open("test.txt", "r") as tag_read_file:
            with open(self.file_path+ "tagstest.txt") as tag_test_file:
                self.assertEqual(tag_read_file.read(), tag_test_file.read())
        tag_file.close()
        os.remove(tag_file.name)
        connection.disconnect()

    def test_write_data(self):
        data_file_name = "writedata.txt"
        concept_file_name = "writeconcepts.txt"
        data_list = [{"hoi":"hoi", "foo":"bar"}, {"foo":"bar"}, {"hoi":"hoi"}]
        data_header_list = ["hoi", "foo"]
        data_file = open(data_file_name, 'w')
        concept_file = open(concept_file_name, 'w')
        QIB2TBatch.write_data(data_file, concept_file, data_list, data_header_list)

        with open(data_file_name, 'r') as data_final_file:
            with open(self.file_path+ "datatest.txt") as data_test_file:
                self.assertEqual(data_final_file.read(), data_test_file.read())

        with open(concept_file_name, 'r') as concept_final_file:
            with open(self.file_path+ "concepttest.txt", 'r') as  concept_test_file:
                self.assertEqual(concept_final_file.read(), concept_test_file.read())
        data_file.close()
        concept_file.close()
        os.remove(data_file.name)
        os.remove(concept_file.name)

    def test_write_logging_new_subject(self):
        rows = [["subject1\t","foo\n"], ["subject2\t", "bar\n"]]
        test_log = ["subject1\tfoo\n","subject2\tbar\n"]
        log_file = (self.file_path + "QIBSubjects.log")
        self.empty_file(log_file)
        current_date = time.strftime("%Y-%m-%d %X")
        QIB2TBatch.check_subject(rows)
        time_regex = ",[0-9]{3}:New subject: "        
        found1 = False
        found2 = False
        with open(log_file, 'r') as open_log_file:
                for line in open_log_file:
                    if re.match(current_date+time_regex+test_log[0], line):
                        found1 = True
                    if re.match(current_date+time_regex+test_log[1], line):
                        found2 = True
        assert found1
        assert found2

    def test_write_logging_new_information(self):
        rows_new_subject= [["subject3\t","foo\n"]]
        rows_new_info = [["subject3\t","foobar\n"]]
        test_log = "subject3\tfoobar\n"
        log_file = (self.file_path + "QIBSubjects.log")
        current_date = time.strftime("%Y-%m-%d %X")
        self.empty_file(log_file)
        QIB2TBatch.check_subject(rows_new_subject)
        QIB2TBatch.check_subject(rows_new_info)
        time_regex = ",[0-9]{3}:New info for Subject: "
        found = False
        with open(log_file, 'r') as open_log_file:
                for line in open_log_file:
                    if re.match(current_date+time_regex+test_log, line):
                        found = True
        assert found

    def test_write_logging_existing_information(self):
        rows_old_info = [["subject1\t","foo\n"]]
        test_log = "subject1\tfoo\n"
        log_file = (self.file_path + "QIBSubjects.log")
        current_date = time.strftime("%Y-%m-%d %X")
        QIB2TBatch.check_subject(rows_old_info)
        time_regex = ",[0-9]{3}:New info for Subject: "
        not_found = True
        with open(log_file, 'r') as open_log_file:
                for line in open_log_file:
                    if re.match(current_date+time_regex+test_log, line):
                        not_found = False
        assert not_found

    @classmethod
    def tearDownClass(self):
        conf_file = self.configPath+"/test.conf"
        config = ConfigParser.ConfigParser()
        config.read(conf_file)
        path = config.get('Directory', 'path')
        shutil.rmtree(path)


if __name__ == '__main__':
    unittest.main()

