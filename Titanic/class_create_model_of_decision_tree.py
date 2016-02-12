# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_model_of_decision_tree.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2016-02-11 10:46:42
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time
from math import log
import operator
#from numpy import *
import csv


################################### PART2 CLASS && FUNCTION ###########################
class CreateDecisionTreeModel(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateDecisionTreeModel.__name__))

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
        logging.info("END CLASS {class_name}.".format(class_name = CreateDecisionTreeModel.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateDecisionTreeModel.__name__, delta_time = self.end - self.start))



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
                              1 if Sex == u'male' else -1,\
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
                              1 if Sex == u'male' else -1,\
                              int(Age),\
                              int(SibSp),\
                              int(Parch)\
                              ),\
                         test_data)
        logging.info("len(test_data):{0}".format(len(test_data)))
        logging.info("test_data[0]:{0}".format(test_data[0]))
        logging.info("type(test_data[0]):{0}".format(type(test_data[0])))
        return train_data, test_data



    def create_training_data_set(self, train_data, feature_name_list):
        # (PassengerId), Survived, Pclass, Sex, Age, SibSp, Parch
        train_data_tuple_list = map(lambda (PassengerId, Survived, Pclass, Sex, Age, SibSp, Parch):\
                                 (Pclass, Sex, Age, SibSp, Parch, Survived),\
                             train_data)
        #train_data_tuple_list.sort()
        logging.info("train_data_tuple_list[0]:{0}".format(train_data_tuple_list[0]))
        return train_data_tuple_list, feature_name_list



    def tree_growth(self, train_data_tuple_list, feature_name_list):

        logging.info("len(train_data_tuple_list):{0}".format(len(train_data_tuple_list)))
        logging.info("train_data_tuple_list[0]:{0}".format(train_data_tuple_list[0]))
        logging.info("feature_name_list:{0}".format(feature_name_list))

        # 从训练数据集train_data_tuple_list中取出每个元素最后一个(也就是类class)
        train_data_class_list = [example[-1] for example in train_data_tuple_list]
        # 如果每个样本为同一类，则直接返回类名
        if train_data_class_list.count(train_data_class_list[0]) == len(train_data_class_list):
           # 返回第一类的类称，因为没啥可分的，所有样本都是一种类型
            logging.info("All data are one same class.")
            return train_data_class_list[0]
        # 样本的特征个数如果为1
        if len(train_data_tuple_list[0]) == 1:# no more features
           # 统计各类出现的次数，返回次数最高的那个类名
            logging.info("Feature number is 1. Return the class name of the most show times.")
            return self.classify(train_data_class_list)
     
        # 计算并返回最佳特征的下标
        best_feature_index = self.find_best_split(train_data_tuple_list)#bestFeat is the index of best feature
        logging.info("best_feature_index:{0}".format(best_feature_index))

        # 最佳特征名 = 特征名列表中下标为bestFeat的元素
        best_feature_name = feature_name_list[best_feature_index]
        logging.info("best_feature_name:{0}".format(best_feature_name))
        # 构造树的根节点
        my_tree_dict = {best_feature_name: {}}
        # 取出所有训练样本最佳特征的值形成一个list
        best_feature_value_list = [example[best_feature_index] for example in train_data_tuple_list]
        # 最佳特征值的种数
        best_feature_unique_value_set = set(best_feature_value_list)
        # 删除del列表features中的最佳特征(就在features变量上操作)
        del (feature_name_list[best_feature_index])
        # 遍历最佳特征里的不同值
        # 对同一个特征里的不同值进行树分叉
        for cur_unique_value in best_feature_unique_value_set:
        # 去掉原数据集train_data_tuple_list里的第bestFeat特征，值为values的
        # 换言之，得到的sub_train_data_tuple_list为这种特征的其中一种值(后续操作做同特征下的分叉处理)
            sub_train_data_tuple_list = self.split_data_set(train_data_tuple_list, best_feature_index, cur_unique_value)
        # 二叉树, 先生成一侧的, 再生成另一侧的
            my_tree_dict[best_feature_name][cur_unique_value] = self.tree_growth(sub_train_data_tuple_list, feature_name_list)
        return my_tree_dict



    def classify(self, train_data_class_list):
        '''
        find the most in the set
        '''
        # 类计数，这是一个字典类型(键是类名，对应的值是该类出现的次数)
        class_count_dict = {}
        # 遍历所有样本的类
        for vote in train_data_class_list:
        # 若不在classCount字典的keys里
            if vote not in class_count_dict.keys():
            # 则创建该key并初始化赋值为0
                class_count_dict[vote] = 0
        # 对当前类vote自增1
            class_count_dict[vote] += 1

        logging.info("class_count_dict:{0}".format(class_count_dict))
        # 排序函数，sorted不在原列表或字典进行排序
        # sorted参数:第一个是迭代类型
        # key = operator.itemgetter(1)这句执行前必须导入operator库
        # 据说有operator可以加快排序速度
        # reverse = True, 降序排列
        sorted_class_count_dict = sorted(class_count_dict.iteritems(), key = operator.itemgetter(1), reverse = True)
        # 返回出现次数最多的那一类的类名
        return sorted_class_count_dict[0][0]



    def find_best_split(self, train_data_tuple_list):
        # 特征个数 = 第一个样本的长度减去类
        feature_num = len(train_data_tuple_list[0]) - 1
        # 基本(初始)信息熵 = 对训练数据集使用计算信息熵的函数
        base_entropy = self.calculate_shannon_entropy(train_data_tuple_list)
        # 最大信息熵增益 = 初始化为0
        best_info_gain = 0.0
        # 最佳分类特征的下标(从0开始) = 初始化为-1
        bestFeat = -1
        # 遍历numFeatures个特征中的第i个特征
        for i in range(feature_num):
        # 对于所有训练集中的第i个特征
            featValues = [example[i] for example in train_data_tuple_list]
        # 用set方法得到第i个特征的该特征有哪些取值
            uniqueFeatValues = set(featValues)
        # 初始化新信息shang
            newEntropy = 0.0
            # 遍历第i个特征[集合]里的元素
        # 计算第i个特征的信息熵(计算该特征下的所有取值的信息熵的和，才是该特征的信息熵)
            for val in uniqueFeatValues:
                subDataSet = self.split_data_set(train_data_tuple_list, i, val)
            # 概率 = 子数据训练样本的个数 / 所有训练样本的个数
                prob = len(subDataSet) / float(len(train_data_tuple_list))
            # 新的信息熵 += 子数据训练样本的个数 / 所有训练样本的个数
                newEntropy += prob * self.calculate_shannon_entropy(subDataSet)
            # 若 当前的基础信息熵 与 当前新的信息熵的差值 大于 先前计算的信息增益
            if(base_entropy - newEntropy)>best_info_gain:
            # 最大信息增益 = 基础信息熵 + 新的信息熵
                best_info_gain = base_entropy - newEntropy
            # 最佳特征的下标 = i
                bestFeat = i
        # 返回最佳特征的下标
        return bestFeat



    def split_data_set(self, train_data_tuple_list, best_feature_index, cur_unique_value):
        # 用于训练子树的的子训练集
        retDataSet = []
        # 遍历取得当前训练集的训练样本(含类标号)
        for featVec in train_data_tuple_list:
        # 如果该条训练样本的第feat个特征 与 该特征里的指定的非重复值value相同
        # 取出除了这个特征以外的特征值(含类标号)
            if featVec[best_feature_index] == cur_unique_value:
                reducedFeatVec = list(featVec[:best_feature_index])
                reducedFeatVec.extend(featVec[best_feature_index + 1:])
                retDataSet.append(reducedFeatVec)
        # 返回子训练数据集
        return retDataSet



    def calculate_shannon_entropy(self, train_data_tuple_list):

        # 这一小段代码和classify函数里计算classCount的函数一样
        # 用字典来统计各个类的个数
        # 实体(训练样本)个数 = dataset的最大维度的元素个数
        example_num = len(train_data_tuple_list)
        # 生成类计数的字典
        label_count_dict = {}
        # 遍历dataset里的训练样本
        for cur_example in train_data_tuple_list:
           # 当前类名 = 取出featVec这条样本的类名
            cur_label = cur_example[-1]
           # 如果 当前样本的类名 不在labelCounts的keys里
            if cur_label not in label_count_dict.keys():
            # 在labelCounts的列表里新建该类的键
            # 并初始化该类名个数为0
                label_count_dict[cur_label] = 0
            # 对当前样本的键(类名)自增1
            label_count_dict[cur_label] += 1
        # 初始化当前的信息熵值为0
        shannon_entropy = 0.0

        # 遍历字典labelCounts的键名key
        # 用所有不同类的个数信息来计算信息熵
        for key in label_count_dict:
               # 当前类key出现的概率 = 当前类key在训练样本里的个数 / 所有训练样本数目
            prob = float(label_count_dict[key]) / example_num
               # 如果当前类出现的概率不等于0
               # 则计算信息熵为shannonEnt = - probablity * log2(probability)
            logging.info("prob:{0}".format(prob))
            if prob != 0:
                shannon_entropy -= prob * log(prob, 2)
                logging.info("shannon_entropy:{0}".format(shannon_entropy))
        # 返回信息熵计算结果
        return shannon_entropy



    def predict(tree, newObject):
        # 判断tree是不是dict的实例
        while isinstance(tree, dict):
            key = tree.keys()[0]
            tree = tree[key][newObject[key]]
        return tree
################################### PART3 CLASS TEST ##################################
# Initial parameters
database_name = "TitanicDB"
passenger_table_name = "passenger_table"
feature_name_list = ['Pclass', 'Sex', 'Age', 'SibSp', 'Parch']

TreeModel = CreateDecisionTreeModel()
train_data, test_data = TreeModel.get_data_from_database(database_name = database_name,\
                                                         passenger_table_name = passenger_table_name)
train_data_tuple_list, feature_name_list = TreeModel.create_training_data_set(train_data = train_data,\
                                                  feature_name_list = feature_name_list)
my_tree_dict = TreeModel.tree_growth(train_data_tuple_list = train_data_tuple_list,\
                                     feature_name_list = feature_name_list)
logging.info("my_tree_dict:{0}".format(my_tree_dict))