# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_model_of_logistic_regression.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2016-01-23 23:32:49
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time
import pylab
from numpy import *
from math import exp
import csv
import decorator_of_function

################################### PART2 CLASS && FUNCTION ###########################
class CreateLogisticRegressionModel(object):

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
        logging.info("START CLASS {class_name}.".format(class_name = CreateLogisticRegressionModel.__name__))

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
        logging.info("END CLASS {class_name}.".format(class_name = CreateLogisticRegressionModel.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateLogisticRegressionModel.__name__, delta_time = self.end - self.start))



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



    @Decorator.log_of_function
    def add_intercept_term(self, train_feature_tuple_list, test_feature_tuple_list):
        logging.info("len(train_feature_tuple_list[0]):{0}".format(len(train_feature_tuple_list[0])))
        logging.info("len(train_feature_tuple_list):{0}".format(len(train_feature_tuple_list)))
        logging.info("train_feature_tuple_list[0]:{0}".format(train_feature_tuple_list[0]))

        logging.info("test_feature_tuple_list[0]:{0}".format(len(test_feature_tuple_list[0])))
        logging.info("len(test_feature_tuple_list):{0}".format(len(test_feature_tuple_list)))
        logging.info("test_feature_tuple_list[0]:{0}".format(test_feature_tuple_list[0]))

        # len(train_feature_tuple_list[0]): 7
        # PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare
        train_feature_intercept_term_added_tuple_list = map(lambda (PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare): \
                                                                (PassengerId, 1.0, Pclass, Sex, Age, SibSp, Parch, Fare),\
                                                            train_feature_tuple_list)

        test_feature_intercept_term_added_tuple_list = map(lambda (PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare): \
                                                               (PassengerId, 1.0, Pclass, Sex, Age, SibSp, Parch, Fare),\
                                                           test_feature_tuple_list)

        logging.info("len(train_feature_intercept_term_added_tuple_list):{0}".format(len(train_feature_intercept_term_added_tuple_list)))
        logging.info("train_feature_intercept_term_added_tuple_list[0]:{0}".format(train_feature_intercept_term_added_tuple_list[0]))

        logging.info("len(test_feature_intercept_term_added_tuple_list):{0}".format(len(test_feature_intercept_term_added_tuple_list)))
        logging.info("test_feature_intercept_term_added_tuple_list[0]:{0}".format(test_feature_intercept_term_added_tuple_list[0]))

        return train_feature_intercept_term_added_tuple_list,\
               test_feature_intercept_term_added_tuple_list



    @Decorator.log_of_function
    def sigmoid_function(self, inX):
        return 1.0 / (1.0 + exp(-inX))



    @Decorator.log_of_function
    def gradient_descent(self, train_feature_tuple_list, train_label_list, learning_rate = 0.01, max_iteration_time = 500, lambda_regularization = 0.1):
        ############################
        # Initial parameters
        # learning_rate = 0.01
        # max_iteration_time = 500
        ############################
        '''
        train_feature_tuple_list_without_PassengerId = map(lambda (PassengerId, InterceptTerm, Pclass, Sex, Age, SibSp, Parch, Fare):\
                                                               (InterceptTerm, Pclass, Sex, Age, SibSp, Parch, Fare),\
                                                           train_feature_tuple_list)
        '''
        train_feature_tuple_list_without_PassengerId = map(lambda (PassengerId, InterceptTerm, Pclass, Sex, Age, SibSp, Parch, Fare):\
                                                               (InterceptTerm, Sex, Fare),\
                                                           train_feature_tuple_list)
        # [891, 7]
        train_input_matrix = mat(train_feature_tuple_list_without_PassengerId)
        # [891, 1]
        train_label_matrix = mat(train_label_list).transpose()

        train_sample_num, feature_num = shape(train_input_matrix)
        weight_matrix = ones((feature_num, 1))

        cost_list = []
        error_list = []
        optimal_solution = {}

        for cur_iter in xrange(max_iteration_time):
            # [891, 1] <- [891, 7]*[7, 1]
            hypothesis = self.sigmoid_function(train_input_matrix * weight_matrix)
            # real <- sum([891, 1]T*[891, 1] + [891, 1]T*[891, 1])
            cost = -float(1) / (train_sample_num) * \
                   sum( train_label_matrix.transpose()*log(hypothesis) + (1-train_label_matrix.transpose())*log(1-hypothesis) ) + \
                   lambda_regularization / (2*train_sample_num) * (array(weight_matrix[1:]) * array(weight_matrix[1:])).sum()

            cost_list.append(cost)
            # [891, 1]
            error = train_label_matrix - hypothesis
            error_list.append(error)
            logging.info("cur_iter:{0}, cost:{1}, sum(error):{2}".format(cur_iter+1, cost, sum(error)))

            # 1 = 1 + 1 * [891, 1].T *[891, 1]
            weight_matrix[0] = weight_matrix[0] + learning_rate * (float(1)/train_sample_num) * train_input_matrix[:, 0].transpose() * error
            # [6, 1] = [6, 1] + 1 * \
            #           ( 1 / 1 * [891, 6].T * [891, 1]
            #               )
            weight_matrix[1:] = weight_matrix[1:] + learning_rate * \
                                                    ( (float(1)/train_sample_num) * train_input_matrix[:, 1::].transpose() * error - \
                                                      float(lambda_regularization) / train_sample_num * weight_matrix[1:] \
                                                      )
            #weight_matrix = weight_matrix + learning_rate * train_input_matrix.transpose() * error
        #"""
            # find optimal solution
            if cur_iter == 0:
                optimal_solution['cur_iter'] = cur_iter
                optimal_solution['cost'] = cost
                optimal_solution['abs(error.sum())'] = abs(error.sum())
                optimal_solution['weight_matrix'] = weight_matrix
            elif cur_iter != 0 and optimal_solution['abs(error.sum())'] > abs(error.sum()):
                optimal_solution['cur_iter'] = cur_iter
                optimal_solution['cost'] = cost
                optimal_solution['abs(error.sum())'] = abs(error.sum())
                optimal_solution['weight_matrix'] = weight_matrix

        logging.info("optimal_solution['cur_iter']:{0}".format(optimal_solution['cur_iter']))
        logging.info("optimal_solution['cost':{0}".format(optimal_solution['cost']))
        logging.info("optimal_solution['abs(error.sum())']:{0}".format(optimal_solution['abs(error.sum())']))
        logging.info("optimal_solution['weight_matrix'].tolist():{0}".format(optimal_solution['weight_matrix'].tolist()))
        #"""
        pylab.plot(cost_list)
        pylab.show()
        return weight_matrix
        #return optimal_solution['weight_matrix']



    @Decorator.log_of_function
    def predict(self, train_feature_tuple_list, weight_matrix):
        '''
        train_feature_tuple_list_without_PassengerId = map(lambda (PassengerId, InterceptTerm, Pclass, Sex, Age, SibSp, Parch, Fare):\
                                                               (InterceptTerm, Pclass, Sex, Age, SibSp, Parch, Fare),\
                                                           train_feature_tuple_list)
        '''
        train_feature_tuple_list_without_PassengerId = map(lambda (PassengerId, InterceptTerm, Pclass, Sex, Age, SibSp, Parch, Fare):\
                                                               (InterceptTerm, Sex, Fare),\
                                                           train_feature_tuple_list)

        train_input_matrix = mat(train_feature_tuple_list_without_PassengerId)

        predict_prob_matrix = self.sigmoid_function(train_input_matrix * weight_matrix)
        '''
        row, col = shape(predict_label_matrix)
        for i in xrange(row):
            print i+1, predict_label_matrix[i][0]
        '''
        predict_prob_list = predict_prob_matrix.transpose().tolist()[0]
        predict_label_list = []
        for prob_idx in xrange(len(predict_prob_list)):
            predict_prob = predict_prob_list[prob_idx]
            if predict_prob > 0.5:
                predict_label_list.append(1)
            else:
                predict_label_list.append(0)
        return predict_label_list


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


    @Decorator.log_of_function
    def plot_decision_bondary(self, weight_matrix):
        pass


################################### PART3 CLASS TEST ##################################
"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"

LRModel = CreateLogisticRegressionModel()

"""