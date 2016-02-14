# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: decorator_of_function.py
# Description:
#

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2016-02-14 15:35:17
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import time

################################### PART2 CLASS && FUNCTION ###########################
class CreateDecorator(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateDecorator.__name__))



    def __del__(self):
        logging.info("END CLASS {class_name}.".format(class_name = CreateDecorator.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateDecorator.__name__, delta_time = self.end - self.start))


    def log_of_function(self, undecorated_function):
        def record_start_and_end_log(args, **kw):
            logging.info("Function {0} start.".format(undecorated_function.__name__))
            n = undecorated_function(args, **kw)
            logging.info("Function {0} end.".format(undecorated_function.__name__))
            return n
        return record_start_and_end_log

'''
Decorator = CreateDecorator()
@Decorator.log_of_function
def fun_a(n):
    logging.info("This is {0} from function a.".format(n))
    return n
'''''
################################### PART3 CLASS TEST ##################################
'''
n = fun_a(11)
logging.info("n is {0}".format(n))
'''