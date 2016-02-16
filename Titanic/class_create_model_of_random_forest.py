# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_model_of_random_forest.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2016-02-16 22:09:44
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time
import decorator_of_function

from sklearn.ensemble import RandomForestClassifier

################################### PART2 CLASS && FUNCTION ###########################
class CreateRandomForestModel(object):

    Decorator = decorator_of_function.CreateDecorator()

    @Decorator.log_of_function
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateRandomForestModel.__name__))

        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        self.model = RandomForestClassifier()



    @Decorator.log_of_function
    def __del__(self):
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in quiting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        logging.info("END CLASS {class_name}.".format(class_name = CreateRandomForestModel.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateRandomForestModel.__name__, delta_time = self.end - self.start))


    @Decorator.log_of_function
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
                              1 if Sex == u'male' else 0,\
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
                              1 if Sex == u'male' else 0,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch)\
                              ),\
                         test_data)
        logging.info("len(test_data):{0}".format(len(test_data)))
        logging.info("test_data[0]:{0}".format(test_data[0]))
        logging.info("type(test_data[0]):{0}".format(type(test_data[0])))
        return train_data, test_data


    @Decorator.log_of_function
    def create_data_set(self, train_data, test_data):

        train_X = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch):\
                          (Pclass, Sex, Age, SibSp, Parch, Survived),\
                      train_data)
        logging.info("len(train_X):{0}".format(len(train_X)))
        logging.info("train_X[0]:{0}".format(train_X[0]))

        train_y = map(lambda train_sample:\
                          train_sample[-1],\
                      train_X)
        logging.info("len(train_y):{0}".format(len(train_y)))
        logging.info("train_y[0]:{0}".format(train_y[0]))

        test_X = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch):\
                          (Pclass, Sex, Age, SibSp, Parch),\
                      test_data)
        logging.info("len(test_X):{0}".format(len(test_X)))
        logging.info("test_X[0]:{0}".format(test_X[0]))

        return train_X, train_y, test_X



    @Decorator.log_of_function
    def fit(self, X, y):
        self.model.fit(X, y)



    @Decorator.log_of_function
    def predict(self, x_test):
        predicted = self.model.predict(x_test)
        logging.info("len(predicted):{0}".format(len(predicted)))
        logging.info("predicted[0]:{0}".format(predicted[0]))
        return predicted


################################### PART3 CLASS TEST ##################################
#"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"

RandomForestModel = CreateRandomForestModel()
train_data, test_data = RandomForestModel.get_data_from_database(database_name = database_name,\
                                                                 passenger_table_name = passenger_table_name)
train_X,\
train_y,\
test_X = RandomForestModel.create_data_set(train_data = train_data, test_data = test_data)
RandomForestModel.fit(X = train_data)
predicted = RandomForestModel.predict(x_test = train_data)
#"""