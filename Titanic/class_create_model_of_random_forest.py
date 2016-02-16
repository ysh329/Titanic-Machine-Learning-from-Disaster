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
import csv

from sklearn.ensemble import RandomForestClassifier

import decorator_of_function


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
        sql_list.append("""SELECT PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Fare FROM {database_name}.{table_name} WHERE Is_train=1"""\
                        .format(database_name = database_name,\
                                table_name = passenger_table_name)\
                        )
        # test set
        sql_list.append("""SELECT PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Fare FROM {database_name}.{table_name} WHERE Is_train=0"""\
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

        train_data = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Fare):\
                             (int(PassengerId),\
                              int(Survived),\
                              int(Pclass),\
                              1 if Sex == u'male' else 0,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch),\
                              Fare,\
                              ),\
                         train_data)
        logging.info("len(train_data):{0}".format(len(train_data)))
        logging.info("train_data[0]:{0}".format(train_data[0]))
        logging.info("type(train_data[0]):{0}".format(type(train_data[0])))

        test_data = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Fare):\
                             (int(PassengerId),\
                              int(Survived),\
                              int(Pclass),\
                              1 if Sex == u'male' else 0,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch),\
                              Fare,\
                              ),\
                         test_data)
        logging.info("len(test_data):{0}".format(len(test_data)))
        logging.info("test_data[0]:{0}".format(test_data[0]))
        logging.info("type(test_data[0]):{0}".format(type(test_data[0])))
        return train_data, test_data


    @Decorator.log_of_function
    def create_data_set(self, train_data, test_data):

        train_X = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Fare):\
                          (Age, Sex, Fare),\
                      train_data)
        logging.info("len(train_X):{0}".format(len(train_X)))
        logging.info("train_X[0]:{0}".format(train_X[0]))

        train_y = map(lambda train_sample:\
                          train_sample[-1],\
                      train_X)
        logging.info("len(train_y):{0}".format(len(train_y)))
        logging.info("train_y[0]:{0}".format(train_y[0]))

        test_X = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch, Fare):\
                          (Age, Sex, Fare),\
                      test_data)
        logging.info("len(test_X):{0}".format(len(test_X)))
        logging.info("test_X[0]:{0}".format(test_X[0]))

        return train_X, train_y, test_X



    @Decorator.log_of_function
    def fit(self, X, y):
        self.model.fit(X, y)



    @Decorator.log_of_function
    def predict(self, x_test):
        predicted_label_list = self.model.predict(x_test)
        # modified the result of regression tree to classify
        predicted_label_list = map(lambda predicted_label: 0 if predicted_label <= 0 else 1,predicted_label_list)
        logging.info("len(predicted_label_list):{0}".format(len(predicted_label_list)))
        logging.info("predicted_label_list[0]:{0}".format(predicted_label_list[0]))
        return predicted_label_list



    @Decorator.log_of_function
    def accuracy(self, train_label_list, predict_label_list):
        logging.info("len(train_label_list):{0}".format(len(train_label_list)))
        logging.info("len(predict_label_list):{0}".format(len(predict_label_list)))

        # compute accuracy
        def compute_accuracy(train_label_list, predict_label_list):
            right_predict_num = 0
            if len(train_label_list) == len(predict_label_list):
                for idx in xrange(len(train_label_list)):
                    if train_label_list[idx] == predict_label_list[idx]:
                        right_predict_num = right_predict_num + 1
                accuracy = float(right_predict_num)/len(train_label_list)
            return right_predict_num, accuracy

        def compute_precision_and_recall_and_F1(train_label_list, predict_label_list):
            if len(train_label_list) == len(predict_label_list):
                # compute precision and recall
                true_positive_num = 10E-10
                true_negative_num = 10E-10
                predicted_positive_num = predict_label_list.count(1)
                predicted_negative_num = predict_label_list.count(0)

                for idx in xrange(len(train_label_list)):
                    if predict_label_list[idx] == train_label_list[idx] == 1:
                        true_positive_num = true_positive_num + 1
                    elif predict_label_list[idx] == train_label_list[idx] == 0:
                        true_negative_num = true_negative_num + 1
                precision = float(true_positive_num) / (predicted_positive_num + 10E-10)
                recall = float(true_negative_num) / (predicted_negative_num + 10E-10)
                F1 = 2 * precision * recall / (precision + recall)

            return precision, recall, F1

        right_predict_num, accuracy = compute_accuracy(train_label_list = train_label_list,\
                                                       predict_label_list = predict_label_list)
        logging.info("right_predict_num:{0}".format(right_predict_num))
        logging.info("accuracy:{0}".format(accuracy))

        precision, recall, F1 = compute_precision_and_recall_and_F1(train_label_list = train_label_list,\
                                                                    predict_label_list = predict_label_list)
        logging.info("precision:{0}".format(precision))
        logging.info("recall:{0}".format(recall))
        logging.info("F1:{0}".format(F1))

        return accuracy, precision, recall, F1



    @Decorator.log_of_function
    def write_csv_file(self, start_id, predict_label_list, result_csv_dir):
        # open csv file
        try:
            result_csv_handle = file(result_csv_dir, 'wb')

            logging.info("Success in attaining file handle of {0}.".format(result_csv_dir))
        except Exception as e:
            logging.error("Fail in attaining file handle of {0}.".format(result_csv_dir))
            logging.error(e)
            return -1

        # create csv writer
        result_csv_writer = csv.writer(result_csv_handle)

        # write csv file
        result_csv_writer.writerow(["PassengerId", "Survived"])
        for list_idx in xrange(len(predict_label_list)):
            PassengerId = start_id + list_idx
            predict_label = predict_label_list[list_idx]
            result_csv_writer.writerow([PassengerId, predict_label])

        # close csv file
        try:
            result_csv_handle.close()
            logging.info("Success in closing file handle of {0}.".format(result_csv_dir))
        except Exception as e:
            logging.error("Fail in closing file handle of {0}.".format(result_csv_dir))
            logging.error(e)
################################### PART3 CLASS TEST ##################################
#"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"
test_data_start_id = 892
result_csv_dir = "../data/output/RFModel.csv"

RandomForestModel = CreateRandomForestModel()
train_data, test_data = RandomForestModel.get_data_from_database(database_name = database_name,\
                                                                 passenger_table_name = passenger_table_name)
train_X,\
train_y,\
test_X = RandomForestModel.create_data_set(train_data = train_data, test_data = test_data)
RandomForestModel.fit(X = train_X, y = train_y)
test_data_predicted_label_list = RandomForestModel.predict(x_test = test_X)
#RandomForestModel.accuracy(train_label_list = train_y, predict_label_list = predicted_label_list)
RandomForestModel.write_csv_file(start_id = test_data_start_id,\
                                 predict_label_list = test_data_predicted_label_list,\
                                 result_csv_dir = result_csv_dir)
#"""