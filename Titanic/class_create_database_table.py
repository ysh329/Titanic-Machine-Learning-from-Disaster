# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_database_table.py
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
import decorator_of_function

################################### PART2 CLASS && FUNCTION ###########################
class CreateDatabaseTable(object):

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
        logging.info("START CLASS {class_name}.".format(class_name = CreateDatabaseTable.__name__))

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
        logging.info("END CLASS {class_name}.".format(class_name = CreateDatabaseTable.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateDatabaseTable.__name__, delta_time = self.end - self.start))



    @Decorator.log_of_function
    def create_database(self, database_name):
        logging.info("database name: {database_name}".format(database_name = database_name))

        cursor = self.con.cursor()
        sqls = ['SET NAMES UTF8', 'SELECT VERSION()', "CREATE DATABASE {database_name}".format(database_name = database_name)]

        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                if sql_idx == 1:
                    result = cursor.fetchall()[0]
                    mysql_version = result[0]
                    logging.info("MySQL VERSION: {mysql_version}".format(mysql_version = mysql_version))
                self.con.commit()
                logging.info("Success in creating database {database_name}.".format(database_name = database_name))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in creating database {database_name}.".format(database_name = database_name))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()


    @Decorator.log_of_function
    def create_table(self, database_name, passenger_table_name):
        cursor = self.con.cursor()
        sqls = ["USE {database_name}".format(database_name = database_name), 'SET NAMES UTF8']

        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))

        # Create passenger_table_name
        #,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
        sqls.append("""CREATE TABLE IF NOT EXISTS {passenger_table_name}(
                                PassengerId INT(11) PRIMARY KEY,
                                Is_train INT(11),
                                Survived INT(11),
                                Pclass INT(11),
                                Name TEXT,
                                Sex VARCHAR(10),
                                Age FLOAT,
                                SibSp INT(11),
                                Parch INT(11),
                                Ticket TEXT,
                                Fare FLOAT,
                                Cabin TEXT,
                                Embarked VARCHAR(4),
                                UNIQUE (PassengerId))""".format(passenger_table_name = passenger_table_name))

        for sql_idx in range(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                self.con.commit()
                logging.info("Success in creating table.")
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in creating table.")
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                logging.error("Error SQL:{sql}".format(sql = sql))
        cursor.close()


################################### PART3 CLASS TEST ##################################
"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"
Creater = CreateDatabaseTable()
Creater.create_database(database_name = database_name)
Creater.create_table(database_name = database_name,\
                     passenger_table_name = passenger_table_name)
"""