# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_import_data_from_csv.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2016-01-21 21:27:21
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time
import csv
import numpy as np

################################### PART2 CLASS && FUNCTION ###########################
class ImportDataToDB(object):
    def __init__(self):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = 'main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name = ImportDataToDB.__name__))

        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    def __del__(self):
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in quiting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        logging.info("END CLASS {class_name}.".format(class_name = ImportDataToDB.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = ImportDataToDB.__name__, delta_time = self.end - self.start))



    def read_data_from_csv(self, train_data_dir, test_data_dir):
        # sub-function
        def get_list_from_csv_file(data_dir):
            data_file_handle = open(data_dir, "rb")
            csv_file_object = csv.reader(data_file_handle)
            csv_header = csv_file_object.next()
            logging.info("csv_header:{0}".format(csv_header))

            data_list = []
            for row in csv_file_object:
                data_list.append(row)
            return data_list

        train_list = get_list_from_csv_file(train_data_dir)
        test_list = get_list_from_csv_file(test_data_dir)

        logging.info("len(train_list):{0}".format(len(train_list)))
        logging.info("train_list[0]:{0}".format(train_list[0]))
        logging.info("len(test_list):{0}".format(len(test_list)))
        logging.info("test_list[0]:{0}".format(test_list[0]))

        return train_list, test_list



    def sql_generator(self, database_name, passenger_table_name, train_list, test_list):
        def sql_generator_for_list(is_train, data_list, database_name, passenger_table_name):
            sql_list = []
            for idx in xrange(len(data_list)):
                sample = data_list[idx]
                if is_train == 1:
                    PassengerId = sample[0]
                    Is_train = 1
                    Survived = sample[1]
                    Pclass = sample[2]
                    Name = sample[3].replace("'", '"')
                    Sex = sample[4]
                    Age = sample[5]
                    SibSp = sample[6]
                    Parch = sample[7]
                    Ticket = sample[8].replace("'", '"').replace("\\", "\\").strip().replace(" ", "-")
                    Fare = sample[9]
                    Cabin = sample[10].replace("'", '"')
                    Embarked = sample[11].replace("'", '"')
                else:
                    PassengerId = sample[0]
                    Is_train = 0
                    Survived = -1
                    Pclass = sample[1]
                    Name = sample[2].replace("'", '"')
                    Sex = sample[3]
                    Age = sample[4]
                    SibSp = sample[5]
                    Parch = sample[6]
                    Ticket = sample[7].replace("'", '"').replace("\\", "\\").strip().replace(" ", "-")
                    Fare = sample[8]
                    Cabin = sample[9].replace("'", '"')
                    Embarked = sample[10].replace("'", '"')
                sql = """INSERT INTO {database_name}.{table_name}(PassengerId, Is_train, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked) """\
                      """VALUES ('{PassengerId}', {Is_train}, {Survived}, '{Pclass}', '{Name}', '{Sex}', '{Age}', '{SibSp}', '{Parch}', '{Ticket}', '{Fare}', '{Cabin}', '{Embarked}')"""\
                        .format(database_name = database_name, table_name = passenger_table_name,\
                                PassengerId = PassengerId, Is_train = Is_train, Survived = Survived,\
                                Pclass = Pclass, Name = Name, Sex = Sex, Age = Age, SibSp = SibSp,\
                                Parch = Parch, Ticket = Ticket, Fare = Fare, Cabin = Cabin, Embarked = Embarked)
                try:
                    sql_list.append(sql)
                except Exception as e:
                    logging.error(e)
            return sql_list

        train_sql_list = sql_generator_for_list(is_train = 1, data_list = train_list, database_name = database_name, passenger_table_name = passenger_table_name)
        test_sql_list = sql_generator_for_list(is_train = 0, data_list = test_list, database_name = database_name, passenger_table_name = passenger_table_name)

        logging.info("len(train_sql_list):{0}".format(len(train_sql_list)))
        logging.info("train_sql_list[0]:{0}".format(train_sql_list[0]))
        logging.info("len(test_sql_list):{0}".format(len(test_sql_list)))
        logging.info("test_sql_list[0]:{0}".format(test_sql_list[0]))

        return train_sql_list, test_sql_list



    def save_data_to_database(self, sql_list):
        cursor = self.con.cursor()
        success_insert = 0
        failure_insert = 0
        for idx in xrange(len(sql_list)):
            sql = sql_list[idx]
            try:
                cursor.execute(sql)
                self.con.commit()
                success_insert = success_insert + 1
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in insert sql into  MySQL.")
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                logging.error("Error sql:{0}".format(sql))
                failure_insert = failure_insert + 1

        logging.info("success_insert:{0}".format(success_insert))
        logging.info("failure_insert:{0}".format(failure_insert))
        logging.info("success insert rate:{0}".format(float(success_insert)/(success_insert+failure_insert)))


################################### PART3 CLASS TEST ##################################
"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"
train_data_dir = "../data/input/train.csv"
test_data_dir = "../data/input/test.csv"

Importer = ImportDataToDB()
train_list, test_list = Importer.read_data_from_csv(train_data_dir = train_data_dir,\
                                                    test_data_dir = test_data_dir)

train_sql_list, test_sql_list = Importer.sql_generator(train_list = train_list,\
                                                       test_list = test_list,\
                                                       database_name = database_name,\
                                                       passenger_table_name = passenger_table_name)
Importer.save_data_to_database(sql_list = train_sql_list)
Importer.save_data_to_database(sql_list = test_sql_list)
"""