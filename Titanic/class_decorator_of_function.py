# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_decorator_of_function.py
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
        def record_start_and_end(args, **kw):
            logging.info("Function {0} start.".format(undecorated_function.__name__))
            undecorated_function(args, **kw)
            logging.info("Function {0} end.".format(undecorated_function.__name__))
        return undecorated_function


Decorator = CreateDecorator()
@Decorator.log_of_function
def fun_a():
    for i in [1,10]:
        logging.info(i)
    return i

@Decorator.log_of_function
def fun_b():
    logging.info("This is function b.")

################################### PART3 CLASS TEST ##################################

fun_a()
fun_b()