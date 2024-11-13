import math
import sys
import csv
import datetime
import os
import numpy as np
import pyspark
from pyspark import SparkConf, SparkContext
import geopandas as gpd
import pandas as pd
import pygplates
import logging
from rotation_utils import get_reconstruction_model_dict

os.environ['SPARK_HOME'] = "D:/LenovoQMDownload/SoftMgr/spark/spark-3.3.0-bin-hadoop3"
sys.path.append("D:/LenovoQMDownload/SoftMgr/spark/spark-3.3.0-bin-hadoop3/python")
sys.path.append("D:/LenovoQMDownload/SoftMgr/spark/spark-3.3.0-bin-hadoop3/python/lib")


# ----- tool functions -----
# read points from csv/xlsx/xls/txt/shp
def read_points(file_path, lon_name, lat_name):
    return {
        'csv': lambda: read_points_from_csv(file_path, lon_name, lat_name),
        'xlsx': lambda: read_points_from_excel(file_path, lon_name, lat_name),
        'xls': lambda: read_points_from_excel(file_path, lon_name, lat_name),
        'txt': lambda: read_points_from_txt(file_path, lon_name, lat_name),
        'shp': lambda: read_points_from_shp(file_path, lon_name, lat_name)
    }.get(file_path.split(".")[1].lower(), lambda: None)()


# read points from csv
def read_points_from_csv(file_path, lon_name, lat_name):
    # specific encoding
    df = pd.read_csv(file_path, encoding='ISO-8859-1')
    df.dropna(subset=[lon_name, lat_name])
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# read points from excel
def read_points_from_excel(file_path, lon_name, lat_name):
    df = pd.read_excel(file_path)
    df.dropna(subset=[lon_name, lat_name])
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# read points from txt
def read_points_from_txt(file_path, lon_name, lat_name):
    df = pd.read_table(file_path, delimiter=",")
    df.dropna(subset=[lon_name, lat_name])
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# read points from shp
def read_points_from_shp(file_path, lon_name, lat_name):
    df = gpd.read_file(file_path, driver='SHP')
    df.dropna(subset=[lon_name, lat_name])
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# ----- core functions -----
# points: result of read_points
def numPartitions(points, age_name):
    # return number of partitions
    return len(points[age_name].unique())


def getPartition(key):
    return key


# key steps of BPPR
def rotate_BPPR(point_features_str, model):
    model_dir = 'data/PALEOGEOGRAPHY_MODELS'
    model_dict = get_reconstruction_model_dict(MODEL_NAME=model)
    static_polygon_filename = str('%s/%s/%s' % (model_dir, model, model_dict['StaticPolygons']))
    rotation_model = pygplates.RotationModel([str('%s/%s/%s' %
                                                  (model_dir, model, rot_file)) for rot_file in
                                              model_dict['RotationFile']])
    partition_time = 0.
    age = 0.

    point_features = []
    for point_feature_str in point_features_str:
        # age, lat, lon, p_index = point_feature_str.split(",")
        age = point_feature_str[0]
        lat, lon, p_index = point_feature_str[1]
        point_feature = pygplates.Feature()
        point_feature.set_geometry(pygplates.PointOnSphere(float(lat), float(lon)))
        point_feature.set_name(str(p_index))
        point_features.append(point_feature)

    # if reconstruct points from past to the present
    # the partition_time will not be 0
    assigned_point_features = pygplates.partition_into_plates(
        static_polygon_filename,
        rotation_model,
        point_features,
        properties_to_copy=[
            pygplates.PartitionProperty.reconstruction_plate_id,
            pygplates.PartitionProperty.valid_time_period],
        reconstruction_time=partition_time
    )

    assigned_point_feature_collection = pygplates.FeatureCollection(assigned_point_features)
    reconstructed_feature_geometries = []
    pygplates.reconstruct(
        assigned_point_feature_collection,
        rotation_model,
        reconstructed_feature_geometries,
        age,
        anchor_plate_id=0)
    result = []
    for reconstructed_feature_geometry in reconstructed_feature_geometries:
        index = reconstructed_feature_geometry.get_feature().get_name()
        rlat, rlon = reconstructed_feature_geometry.get_reconstructed_geometry().to_lat_lon()
        # plat, plon = reconstructed_feature_geometry.get_present_day_geometry().to_lat_lon()
        result.append([index, rlat, rlon])

    return result


# perform paleogeographic point rotation
def rotate_points(points_file, lon_name, lat_name, age_name, model, output_file):
    points = read_points(points_file, lon_name, lat_name)
    data = []
    for index, row in points.iterrows():
        lat = row[lat_name]
        lon = row[lon_name]
        age = row[age_name]
        if lat is None or lon is None or lat == "" or lon == "":
            continue
        # data.append(str.join(',', [str(age), str(lat), str(lon), str(index)]))
        data.append([age, lat, lon, index])
    logging.info("Start reconstruct")
    conf = SparkConf().setAppName("Reconstruct with Spark").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    numPart = numPartitions(points, age_name)
    pointRDD = sc.parallelize(data)
    # repartition
    pointRDD = pointRDD.map(lambda x: (x[0], x[1:])).partitionBy(numPart, getPartition)
    #.reduceByKey(lambda x, y: x + y)

    r_start = datetime.datetime.now()
    # reconstruct data in split
    reconstructedPointRDD = pointRDD.mapPartitions(lambda x: rotate_BPPR(point_features_str=x, model=model))
    result = reconstructedPointRDD.collect()

    r_end = datetime.datetime.now()
    rotation_time = (r_end - r_start).seconds
    print('rotation_time:', rotation_time)

    # output
    new_lon = "lon_paleo"
    new_lat = "lat_paleo"
    new = pd.DataFrame(result, columns=['index', new_lat, new_lon])
    new['index'] = new['index'].astype('int64')
    new.set_index('index', inplace=True)
    # print(new.index)
    out = points.merge(new, how='left', left_index=True, right_index=True)
    # print(out)
    out.to_csv(output_file)


if __name__ == '__main__':
    # define parameters
    model = 'SCOTESE&WRIGHT2018'
    anchor_plate_id = 0
    lon_name = 'lng'
    lat_name = 'lat'
    age_name = 'age'
    points_file = 'E:/ZJU/GitLab/Paper/BatchPointRotation/EX/data/realData/pbdb_data_occs_Cretaceous_more_0220.csv'
    # points_file = 'E:/ZJU/GitLab/Paper/BatchPointRotation/EX/data/realData/CASE/shp/pbdb_data_occs_Maastrichtian.shp'
    output_file = 'data/points/out_point_0220.csv'

    # start to use BPPR
    start = datetime.datetime.now()
    print("Start reconstruct")
    rotate_points(points_file=points_file, lon_name=lon_name, lat_name=lat_name,
                  age_name=age_name, model=model, output_file=output_file)
    end = datetime.datetime.now()
    exetime = (end - start).seconds
    print("total_time:" + str(exetime))
