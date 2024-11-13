import math
import sys
import csv
import datetime
import io
import os
import numpy as np
import pyspark
from pyspark import SparkConf, SparkContext
import geopandas as gpd
import pandas as pd
import pygplates
import logging
import requests
from rotation_utils import get_reconstruction_model_dict
from extensibility_utils import stages_dict, age_list

os.environ['SPARK_HOME'] = "D:/LenovoQMDownload/SoftMgr/spark/spark-3.3.0-bin-hadoop3"
sys.path.append("D:/LenovoQMDownload/SoftMgr/spark/spark-3.3.0-bin-hadoop3/python")
sys.path.append("D:/LenovoQMDownload/SoftMgr/spark/spark-3.3.0-bin-hadoop3/python/lib")


def get_macro(intervals):
    base_url = "https://macrostrat.org/api/units?interval_name=$interval&response=long&format=csv"
    df_all = pd.DataFrame()
    count = 0
    for interval in intervals:
        # key, value = interval
        res = requests.get(base_url.replace("$interval", interval))
        df = pd.read_csv(io.BytesIO(res.content))
        if count == 0:
            df_all = df
        else:
            df_all.append(df)
        count = count + 1
    return df_all.values


def extend_macro():
    conf = SparkConf().setAppName("Reconstruct with Spark").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    start = datetime.datetime.now()
    ages = ["Cambrian", "Ordovician", "Silurian", "Devonian", "Carboniferous", "Permian", "Triassic", "Jurassic",
                "Cretaceous", "Paleogene", "Neogene", "Quaternary"]
    # divide into different groups by age / repartition
    num = len(ages)
    stageRDD = sc.parallelize(ages, num)
    dataRDD = stageRDD.mapPartitions(lambda x: get_macro(x))
    result = dataRDD.collect()
    res = pd.DataFrame(result)
    # print(res)
    res.to_csv("E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/points/data-7.csv")
    end = datetime.datetime.now()
    time = (end - start).seconds
    print('extend_time:', time)


def macro_period_data():
    base_url = "https://macrostrat.org/api/units?interval_name=$interval&response=long&format=csv"
    start = datetime.datetime.now()
    df_all = pd.DataFrame()
    for age in ["Cambrian", "Ordovician", "Silurian", "Devonian", "Carboniferous", "Permian", "Triassic", "Jurassic",
                "Cretaceous", "Paleogene", "Neogene", "Quaternary"]:
        res = requests.get(base_url.replace("$interval", age))
        df = pd.read_csv(io.BytesIO(res.content))
        if age == 'Cambrian':
            df_all = df
        else:
            df_all = df_all.append(df)
    df_all.to_csv("E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/points/data-5.csv")
    end = datetime.datetime.now()
    time = (end - start).seconds
    print('period_time:', time)


def stage_data():
    base_url = "https://paleobiodb.org/data1.2/occs/list.csv?interval=$stage&show=phylo,class,genus,coords"
    df_all = pd.DataFrame()
    start = datetime.datetime.now()
    for age in age_list:
        for stage in stages_dict[age]:
            res = requests.get(base_url.replace("$stage", stage))
            df = pd.read_csv(io.BytesIO(res.content))
            if stage == 'Fortunian':
                df_all = df
            else:
                df_all.append(df)
    df_all.to_csv("E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/points/data-2.csv")
    end = datetime.datetime.now()
    time = (end - start).seconds
    print('stage_time:', time)


def period_data():
    base_url = "https://paleobiodb.org/data1.2/occs/list.csv?interval=$period&show=phylo,class,genus,coords"
    df_all = pd.DataFrame()
    start = datetime.datetime.now()
    for age in age_list:
        res = requests.get(base_url.replace("$period", age))
        df = pd.read_csv(io.BytesIO(res.content))
        if age == 'Cambrian':
            df_all = df
        else:
            df_all = df_all.append(df)
    df_all.to_csv("E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/points/data-3.csv")
    end = datetime.datetime.now()
    time = (end - start).seconds
    print('period_time:', time)


def all_data():
    url = "https://paleobiodb.org/data1.2/occs/list.csv?interval=Phanerozoic&show=phylo,class,genus,coords"
    start = datetime.datetime.now()
    res = requests.get(url)
    df = pd.read_csv(io.BytesIO(res.content))
    pd.DataFrame(df).to_csv("E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/points/data-4.csv")
    end = datetime.datetime.now()
    time = (end - start).seconds
    print('all_time:', time)


def get_occurrences(intervals):
    # df_all = []
    df_all = pd.DataFrame()
    count = 0
    for interval in intervals:
        key, value = interval
        base_url = "https://paleobiodb.org/data1.2/occs/list.csv?interval=$stage&show=phylo,class,genus,coords"
        res = requests.get(base_url.replace("$stage", value))
        df = pd.read_csv(io.BytesIO(res.content))
        if count == 0:
            df_all = df
        else:
            df_all.append(df)
        count = count + 1
        # df_all.append(df)
    return df_all


def get_partition(key):
    for i in range(0, len(age_list)):
        if age_list[i] == key:
            return i


def extend_data():
    conf = SparkConf().setAppName("Reconstruct with Spark").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    stages = []
    for age in age_list:
        for stage in stages_dict[age]:
            stages.append((age, stage))
    start = datetime.datetime.now()
    stageRDD = sc.parallelize(stages)
    # divide into different groups by age / repartition
    num = len(age_list)
    restageRDD = stageRDD.map(lambda x: (x[0], x[1])).partitionBy(num, get_partition)
    dataRDD = restageRDD.mapPartitions(lambda x: get_occurrences(x))
    result = dataRDD.collect()
    res = pd.DataFrame(result)
    res.to_csv("E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/points/data-1.csv")
    end = datetime.datetime.now()
    time = (end - start).seconds
    print('extend_time:', time)


if __name__ == '__main__':
    # extend_data()
    # stage_data()
    # period_data()
    # all_data()
    # macro_all_data()
    extend_macro() # 15
    # macro_period_data()  # 38
    # print(restageRDD.getNumPartitions())
    # for i, partition in enumerate(restageRDD.glom().collect()):
    #     print("Partition {}: {}".format(i+1, partition))
    # print(restageRDD.keys().take(2))

