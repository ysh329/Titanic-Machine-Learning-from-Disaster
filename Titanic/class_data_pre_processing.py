# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_data_pre_processing.py
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
from numpy import *
import decorator_of_function

################################### PART2 CLASS && FUNCTION ###########################
class DataPreProcess(object):

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
        logging.info("START CLASS {class_name}.".format(class_name = DataPreProcess.__name__))

        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    @Decorator.log_of_function
    def __del__(self):
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in quiting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        logging.info("END CLASS {class_name}.".format(class_name = DataPreProcess.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = DataPreProcess.__name__, delta_time = self.end - self.start))



    @Decorator.log_of_function
    def get_data_from_database(self, database_name, passenger_table_name):
        cursor = self.con.cursor()
        sql_list = []
        # training set
        sql_list.append("""SELECT PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked """\
                        """FROM {database_name}.{table_name} WHERE Is_train=1"""\
                        .format(database_name = database_name,\
                                table_name = passenger_table_name)\
                        )
        # test set
        sql_list.append("""SELECT PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked """\
                        """FROM {database_name}.{table_name} WHERE Is_train=0"""\
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

        train_data = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked):\
                             (int(PassengerId),\
                              int(Survived),\
                              int(Pclass),\
                              Sex,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch),\
                              Ticket,\
                              float(Fare),\
                              Cabin,\
                              Embarked\
                              ),\
                         train_data)
        logging.info("len(train_data):{0}".format(len(train_data)))
        logging.info("train_data[0]:{0}".format(train_data[0]))
        logging.info("type(train_data[0]):{0}".format(type(train_data[0])))

        test_data = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked):\
                             (int(PassengerId),\
                              int(Survived),\
                              int(Pclass),\
                              Sex,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch),\
                              Ticket,\
                              float(Fare),\
                              Cabin,\
                              Embarked\
                              ),\
                         test_data)
        logging.info("len(test_data):{0}".format(len(test_data)))
        logging.info("test_data[0]:{0}".format(test_data[0]))
        logging.info("type(test_data[0]):{0}".format(type(test_data[0])))
        return train_data, test_data



    @Decorator.log_of_function
    def feature_selection(self, train_data, test_data):
        # PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked
        # Select 6 feature
        # PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare

        def feature_selection_from_data_list(data_list):
            feature_list = []
            label_list = []
            for sample_idx in xrange(len(data_list)):
                sample = data_list[sample_idx]

                PassengerId = sample[0]
                Survived = sample[1]
                Pclass = sample[2]
                Sex = sample[3]
                Age = sample[4]
                SibSp = sample[5]
                Parch = sample[6]
                #Ticket = sample[7]
                Fare = sample[8]
                #Cabin = sample[9]
                #Embarked = sample[10]

                feature_list.append((PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare))
                label_list.append(Survived)
            return feature_list, label_list

        train_feature_tuple_list, train_label_list = feature_selection_from_data_list(data_list = train_data)
        logging.info("len(train_feature_tuple_list):{0}".format(len(train_feature_tuple_list)))
        logging.info("train_feature_tuple_list[0]:{0}".format(train_feature_tuple_list[0]))
        logging.info("len(train_label_list):{0}".format(len(train_label_list)))
        logging.info("train_label_list[0]:{0}".format(train_label_list[0]))

        test_feature_tuple_list, test_label_list = feature_selection_from_data_list(data_list = test_data)
        logging.info("len(test_feature_tuple_list):{0}".format(len(test_feature_tuple_list)))
        logging.info("test_feature_tuple_list[0]:{0}".format(test_feature_tuple_list[0]))
        logging.info("len(test_label_list)".format(len(test_label_list)))
        logging.info("test_label_list[0]:{0}".format(test_label_list[0]))

        return train_feature_tuple_list, train_label_list, test_feature_tuple_list



    @Decorator.log_of_function
    def feature_extraction(self):
        pass



    @Decorator.log_of_function
    def feature_transformation(self, train_feature_tuple_list, test_feature_tuple_list):
        # PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare

        def find_string_feature(feature_tuple_list):
            string_feature_num = 0
            string_feature_idx_list = []
            first_sample = feature_tuple_list[0]

            for feature_idx in xrange(len(first_sample)):
                feature = first_sample[feature_idx]
                if type(feature) == unicode:
                    string_feature_num = string_feature_num + 1
                    string_feature_idx_list.append(feature_idx)
            return string_feature_num, string_feature_idx_list

        def feature_transformation_from_feature_tuple_list(feature_tuple_list, string_feature_idx_list):
            # PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare
            # transform String to Int
            # Include: Sex
            for sample_idx in xrange(len(feature_tuple_list)):
                sample = feature_tuple_list[sample_idx]
                # PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare
                transformed_feature_tuple_list = map(lambda (PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare):\
                                                         (PassengerId, Pclass, 1 if Sex == u"male" else -1, Age, SibSp, Parch, Fare),\
                                                     feature_tuple_list)
            return transformed_feature_tuple_list

        # string feature find
        string_feature_num, string_feature_idx_list = find_string_feature(feature_tuple_list = train_feature_tuple_list)
        logging.info("string_feature_num:{0}".format(string_feature_num))
        logging.info("len(string_feature_idx_list):{0}".format(len(string_feature_idx_list)))
        logging.info("string_feature_idx_list:{0}".format(string_feature_idx_list))

        # feature transformation
        train_transformed_feature_tuple_list = feature_transformation_from_feature_tuple_list(feature_tuple_list = train_feature_tuple_list,\
                                                                                              string_feature_idx_list = string_feature_idx_list)
        logging.info("len(train_transformed_feature_tuple_list):{0}".format(len(train_transformed_feature_tuple_list)))
        logging.info("train_transformed_feature_tuple_list[0]:{0}".format(train_transformed_feature_tuple_list[0]))

        test_transformed_feature_tuple_list = feature_transformation_from_feature_tuple_list(feature_tuple_list = test_feature_tuple_list,\
                                                                                             string_feature_idx_list = string_feature_idx_list)
        logging.info("len(test_transformed_feature_tuple_list):{0}".format(len(test_transformed_feature_tuple_list)))
        logging.info("test_transformed_feature_tuple_list[0]:{0}".format(test_transformed_feature_tuple_list[0]))

        return train_transformed_feature_tuple_list, test_transformed_feature_tuple_list



    @Decorator.log_of_function
    def feature_normalization(self, train_transformed_feature_tuple_list, test_transformed_feature_tuple_list):
        # features: (PassengerId,) Pclass, Sex, Age, SibSp, Parch, Fare
        train_matrix = mat(train_transformed_feature_tuple_list)
        logging.info("train_matrix.tolist()[0]:{0}".format(train_matrix.tolist()[0]))
        test_matrix = mat(test_transformed_feature_tuple_list)
        logging.info("test_matrix.tolist()[0]:{0}".format(test_matrix.tolist()[0]))

        def feature_normalize(feature_matrix):
            sample_num, feature_num = shape(feature_matrix)
            normalized_feature_matrix = mat(ones((sample_num, feature_num)))
            for feature_idx in xrange(feature_num):
                if feature_idx == 0:
                    # PassengerId
                    normalized_feature_matrix[:, feature_idx] = feature_matrix[:, feature_idx]
                    continue
                feature_vector = feature_matrix[:, feature_idx]
                normalized_feature_vector = feature_normalization_from_one_feature(feature_vector = feature_vector)
                logging.info("normalized_feature_vector.tolist()[0]:{0}".format(normalized_feature_vector.tolist()[0]))
                normalized_feature_matrix[:, feature_idx] = normalized_feature_vector
            return normalized_feature_matrix

        def feature_normalization_from_one_feature(feature_vector):
            max_value = float(feature_vector.max())
            min_value = float(feature_vector.min())
            mean_value = float(feature_vector.mean())
            nan_to_num((mean_value - min_value) / (max_value - min_value + 1E-100))
            normalized_feature_vector = (feature_vector - min_value) / (max_value - min_value + 1E-100)
            return normalized_feature_vector

        train_normalized_feature_matrix = feature_normalize(feature_matrix = train_matrix)
        logging.info("len(train_normalized_feature_matrix.tolist()[0]):{0}".format(len(train_normalized_feature_matrix.tolist()[0])))
        logging.info("train_normalized_feature_matrix.tolist()[0]:{0}".format(train_normalized_feature_matrix.tolist()[0]))
        logging.info("train_normalized_feature_matrix.tolist()[3]:{0}".format(train_normalized_feature_matrix.tolist()[3]))
        train_normalized_feature_2d_list = train_normalized_feature_matrix.tolist()

        test_normalized_feature_matrix = feature_normalize(feature_matrix = test_matrix)
        logging.info("len(test_normalized_feature_matrix.tolist()[0]):{0}".format(len(test_normalized_feature_matrix.tolist()[0])))
        logging.info("test_normalized_feature_matrix.tolist()[0]:{0}".format(test_normalized_feature_matrix.tolist()[0]))
        test_normalized_feature_2d_list = test_normalized_feature_matrix.tolist()

        return train_normalized_feature_2d_list, test_normalized_feature_2d_list


################################### PART3 CLASS TEST ##################################
"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"

DataPreProcessing = DataPreProcess()
train_data, test_data = DataPreProcessing.get_data_from_database(database_name = database_name,\
                                                                 passenger_table_name = passenger_table_name)
train_feature_tuple_list, train_label_list,\
test_feature_tuple_list = DataPreProcessing.feature_selection(train_data = train_data,\
                                                              test_data = test_data)
train_feature_tuple_list,\
test_feature_tuple_list = DataPreProcessing\
    .feature_transformation(train_feature_tuple_list = train_feature_tuple_list,\
                            test_feature_tuple_list = test_feature_tuple_list)

train_feature_tuple_list,\
test_feature_tuple_list = DataPreProcessing\
    .feature_transformation(train_feature_tuple_list = train_feature_tuple_list,\
                            test_feature_tuple_list = test_feature_tuple_list)

"""