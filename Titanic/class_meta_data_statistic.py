# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_meta_data_statistic.py
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
import pandas as pd
import pylab as pl

################################### PART2 CLASS && FUNCTION ###########################
class MetaDataStatistic(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = MetaDataStatistic.__name__))

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
        logging.info("END CLASS {class_name}.".format(class_name = MetaDataStatistic.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = MetaDataStatistic.__name__, delta_time = self.end - self.start))



    def data_summay_from_csv(self, train_data_dir, summary_pic_save_dir, summary_txt_save_dir):
        dataframe = pd.read_csv(train_data_dir)
        # 浏览数据集
        logging.info("dataframe.head():{0}".format(dataframe.head()))
        data_describe =  str(dataframe.describe())
        summary_txt_handle = open(summary_txt_save_dir, "wb")
        summary_txt_handle.write(data_describe)
        summary_txt_handle.close()

        dataframe.hist()
        #print type(dataframe)
        pl.show()

################################### PART3 CLASS TEST ##################################
#"""
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"
train_data_dir = "../data/input/train.csv"
summary_pic_save_dir = "../data/output/summary.jpg"
summary_txt_save_dir = "../data/output/summary.txt"

MetaData = MetaDataStatistic()
MetaData.data_summay_from_csv(train_data_dir = train_data_dir,\
                              summary_pic_save_dir = summary_pic_save_dir,\
                              summary_txt_save_dir = summary_txt_save_dir)
#"""