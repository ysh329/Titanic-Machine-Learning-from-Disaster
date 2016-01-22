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

################################ PART3 MAIN ###########################################
def main():
    # class_initialization_and_load_parameter
    # Initialization
    config_data_dir = "./config.ini"
    # load parameters
    ParameterLoader = InitializationAndLoadParameter()
    train_data_dir, test_data_dir,\
    database_name, passenger_table_name,\
    pyspark_app_name = ParameterLoader.load_parameter(config_data_dir = config_data_dir)




    # class_create_database_table
    # Initial parameters
    '''
    database_name = "TitanicDB"
    passenger_table_name = "passenger_table"
    '''
    Creater = CreateDatabaseTable()
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         passenger_table_name = passenger_table_name)




    '''
    # class_create_spark
    SparkCreator = CreateSpark(pyspark_app_name = pyspark_app_name)
    pyspark_sc = SparkCreator.return_spark_context()
    '''




    # class_import_data_from_csv
    # Initial parameters
    '''
    database_name = "TitanicDB"
    passenger_table_name = "passenger_table"
    train_data_dir = "../data/input/train.csv"
    test_data_dir = "../data/input/test.csv"
    '''
    Importer = ImportDataToDB()
    train_list, test_list = Importer.read_data_from_csv(train_data_dir = train_data_dir,\
                                                        test_data_dir = test_data_dir)

    train_sql_list, test_sql_list = Importer.sql_generator(train_list = train_list,\
                                                           test_list = test_list,\
                                                           database_name = database_name,\
                                                           passenger_table_name = passenger_table_name)
    Importer.save_data_to_database(sql_list = train_sql_list)
    Importer.save_data_to_database(sql_list = test_sql_list)

################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()