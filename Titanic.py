# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: spider.py
# Description:

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2016-01-21 20:56:10
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
from Titanic.class_initialization_and_load_parameter import *
from Titanic.class_create_database_table import *
from Titanic.class_import_data_from_csv import *
from Titanic.class_create_spark import *
from Titanic.class_data_pre_processing import *
from Titanic.class_create_model_of_logistic_regression import *

################################ PART3 MAIN ###########################################
def main():
    # Step1: class_initialization_and_load_parameter
    #######################################
    # Initialization
    config_data_dir = "./config.ini"
    #######################################
    # load parameters
    ParameterLoader = InitializationAndLoadParameter()
    train_data_dir, test_data_dir,\
    database_name, passenger_table_name,\
    test_data_start_id,\
    pyspark_app_name = ParameterLoader.load_parameter(config_data_dir = config_data_dir)




    # Step2: class_create_database_table
    '''
    #######################################
    # Initial parameters
    database_name = "TitanicDB"
    passenger_table_name = "passenger_table"
    #######################################
    Creater = CreateDatabaseTable()
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         passenger_table_name = passenger_table_name)
    '''




    '''
    # class_create_spark
    SparkCreator = CreateSpark(pyspark_app_name = pyspark_app_name)
    pyspark_sc = SparkCreator.return_spark_context()
    '''




    # Step3: class_import_data_from_csv
    '''
    #######################################
    # Initial parameters
    database_name = "TitanicDB"
    passenger_table_name = "passenger_table"
    train_data_dir = "../data/input/train.csv"
    test_data_dir = "../data/input/test.csv"
    #######################################

    Importer = ImportDataToDB()
    train_list, test_list = Importer.read_data_from_csv(train_data_dir = train_data_dir,\
                                                        test_data_dir = test_data_dir)

    train_sql_list, test_sql_list = Importer.sql_generator(train_list = train_list,\
                                                           test_list = test_list,\
                                                           database_name = database_name,\
                                                           passenger_table_name = passenger_table_name)
    Importer.save_data_to_database(sql_list = train_sql_list)
    Importer.save_data_to_database(sql_list = test_sql_list)
    '''



    # Step4: class_data_pre_processing
    '''
    #######################################
    # Initial parameters
    database_name = "TitanicDB"
    passenger_table_name = "passenger_table"
    #######################################
    '''
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

    train_normalized_feature_2d_list,\
    test_normalized_feature_2d_list = DataPreProcessing.feature_normalization(train_transformed_feature_tuple_list = train_feature_tuple_list,\
                                                                              test_transformed_feature_tuple_list = test_feature_tuple_list)




    # Step5: class_create_model_of_logistic_regression
    """
    #######################################
    # Initial parameters
    # database_name = "TitanicDB"
    # passenger_table_name = "passenger_table"
    #######################################

    LRModel = CreateLogisticRegressionModel()
    train_feature_intercept_term_added_tuple_list,\
    test_feature_intercept_term_added_tuple_list = LRModel.add_intercept_term(train_feature_tuple_list = train_normalized_feature_2d_list,\
                                                                              test_feature_tuple_list = test_normalized_feature_2d_list)
    weight_matrix = LRModel.gradient_descent(train_feature_tuple_list = train_feature_intercept_term_added_tuple_list,\
                                             train_label_list = train_label_list,\
                                             learning_rate = 0.1,\
                                             max_iteration_time = 250,\
                                             lambda_regularization = 0.1)
    logging.info("weight_matrix:{0}".format(weight_matrix.tolist()))
    train_predict_label_list = LRModel.predict(train_feature_tuple_list = train_feature_intercept_term_added_tuple_list,\
                                               weight_matrix = weight_matrix)
    LRModel.accuracy(train_label_list = train_label_list,\
                     predict_label_list = train_predict_label_list)

    test_predicted_label_list = LRModel.predict(train_feature_tuple_list = test_feature_intercept_term_added_tuple_list,\
                                                weight_matrix = weight_matrix)
    LRModel.write_csv_file(start_id = test_data_start_id,\
                           predict_label_list = test_predicted_label_list,\
                           result_csv_dir = "./data/output/LRModel.csv")
    """



################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()