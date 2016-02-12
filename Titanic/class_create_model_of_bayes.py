# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_model_of_bayes.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-21 21:04:53
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time

################################### PART2 CLASS && FUNCTION ###########################
class CreateBayesModel(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateBayesModel.__name__))

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
        logging.info("END CLASS {class_name}.".format(class_name = CreateBayesModel.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateBayesModel.__name__, delta_time = self.end - self.start))



    def get_data_from_database(self, database_name, passenger_table_name):
        cursor = self.con.cursor()
        sql_list = []
        # training set
        sql_list.append("""SELECT PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch FROM {database_name}.{table_name} WHERE Is_train=1"""\
                        .format(database_name = database_name,\
                                table_name = passenger_table_name)\
                        )
        # test set
        sql_list.append("""SELECT PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch FROM {database_name}.{table_name} WHERE Is_train=0"""\
                        .format(database_name = database_name,\
                                table_name = passenger_table_name)\
                        )
        for sql_idx in xrange(len(sql_list)):
            sql = sql_list[sql_idx]
            try:
                cursor.execute(sql)
                if sql_idx == 0:
                    train_data = cursor.fetchall()
                    logging.info("len(train_data):{0}".format(len(train_data)))
                    logging.info("train_data[0]:{0}".format(train_data[0]))
                    logging.info("type(train_data[0]):{0}".format(type(train_data[0])))
                elif sql_idx == 1:
                    test_data = cursor.fetchall()
                    logging.info("len(test_data):{0}".format(len(test_data)))
                    logging.info("test_data[0]:{0}".format(test_data[0]))
                    logging.info("type(test_data[0]):{0}".format(type(test_data[0])))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in fetch data from MySQL.")
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        train_data = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch):\
                             (int(PassengerId),\
                              int(Survived),\
                              int(Pclass),\
                              Sex,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch)\
                              ),\
                         train_data)
        logging.info("len(train_data):{0}".format(len(train_data)))
        logging.info("train_data[0]:{0}".format(train_data[0]))
        logging.info("type(train_data[0]):{0}".format(type(train_data[0])))

        test_data = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch):\
                             (int(PassengerId),\
                              int(Survived),\
                              int(Pclass),\
                              Sex,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch)\
                              ),\
                         test_data)
        logging.info("len(test_data):{0}".format(len(test_data)))
        logging.info("test_data[0]:{0}".format(test_data[0]))
        logging.info("type(test_data[0]):{0}".format(type(test_data[0])))
        return train_data, test_data


################################### PART3 CLASS TEST ##################################
#"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"

BayesModel = CreateBayesModel()
train_data, test_data = BayesModel.get_data_from_database(database_name = database_name,\
                                                          passenger_table_name = passenger_table_name)

#"""