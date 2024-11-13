import math
import sys
import csv
import datetime
import os
import numpy as np
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
def read_points(file_path):
    return {
        'csv': lambda: read_points_from_csv(file_path),
        'xlsx': lambda: read_points_from_excel(file_path),
        'xls': lambda: read_points_from_excel(file_path),
        'txt': lambda: read_points_from_txt(file_path),
        'shp': lambda: read_points_from_shp(file_path)
    }.get(file_path.split(".")[1].lower(), lambda: None)()


# read points from csv
def read_points_from_csv(file_path):
    # decide if use df.drop() by data
    df = pd.read_csv(file_path)
    # df.drop(df.columns[0], axis=1, inplace=True)
    return df


# read points from excel
def read_points_from_excel(file_path):
    df = pd.read_excel(file_path)
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# read points from txt
def read_points_from_txt(file_path):
    df = pd.read_table(file_path, delimiter=",")
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# read points from shp
def read_points_from_shp(file_path):
    df = gpd.read_file(file_path, driver='SHP')
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


# ----- core functions -----
# key steps of BPPR
def rotate_BPPR(point_features_str, model, time):
    model_dir = 'data/PALEOGEOGRAPHY_MODELS'
    model_dict = get_reconstruction_model_dict(MODEL_NAME=model)
    static_polygon_filename = str('%s/%s/%s' % (model_dir, model, model_dict['StaticPolygons']))
    rotation_model = pygplates.RotationModel([str('%s/%s/%s' %
                                                  (model_dir, model, rot_file)) for rot_file in
                                              model_dict['RotationFile']])
    point_features = []
    for point_feature_str in point_features_str:
        lat, lon, p_index = point_feature_str.split(",")
        point_feature = pygplates.Feature()
        point_feature.set_geometry(pygplates.PointOnSphere(float(lat), float(lon)))
        point_feature.set_name(p_index)
        point_features.append(point_feature)

    partition_time = time

    # it seems when the partition_time is not 0
    # the returned assigned_point_features contains reverse reconstructed present-day geometries.
    # so, when doing reverse reconstruct, do not reverse reconstruct again later.
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
        time,
        anchor_plate_id=0)
    result = []
    for reconstructed_feature_geometry in reconstructed_feature_geometries:
        index = reconstructed_feature_geometry.get_feature().get_name()
        # rlat, rlon = reconstructed_feature_geometry.get_reconstructed_geometry().to_lat_lon()
        plat, plon = reconstructed_feature_geometry.get_present_day_geometry().to_lat_lon()
        result.append([index, plat, plon])

    return result


# perform paleogeographic point rotation
def rotate_points(points_file, lon_name, lat_name, time, model, output_file):
    points = read_points(points_file)
    data = []
    for index, row in points.iterrows():
        lat = row[lat_name]
        lon = row[lon_name]
        if lat is None or lon is None or lat == "" or lon == "":
            continue
        data.append(str.join(',', [str(lat), str(lon), str(index)]))

    logging.info("Start reconstruct")
    conf = SparkConf().setAppName("Reconstruct with Spark").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    # define partition number
    pointRDD = sc.parallelize(data, 3)
    # reconstruct data in split
    r_start = datetime.datetime.now()
    reconstructedPointRDD = pointRDD.mapPartitions(lambda x: rotate_BPPR(point_features_str=x, model=model, time=time))
    result = reconstructedPointRDD.collect()
    r_end = datetime.datetime.now()
    rotation_time = (r_end - r_start).seconds
    print('rotation_time:', rotation_time)

    # output
    new_lon = "lon_present"
    new_lat = "lat_present"
    new = pd.DataFrame(result, columns=['index', new_lat, new_lon])
    new['index'] = new['index'].astype('int64')
    new.set_index('index', inplace=True)
    # print(new.index)
    out = points.merge(new, how='left', left_index=True, right_index=True)
    # print(out)
    out.to_csv(output_file)


def rotate_pygplates(points_file, lon_name, lat_name, time, model, output_file):
    points = read_points(points_file)
    data = []
    for index, row in points.iterrows():
        lat = row[lat_name]
        lon = row[lon_name]
        if lat is None or lon is None or lat == "" or lon == "":
            continue
        data.append(str.join(',', [str(lat), str(lon), str(index)]))

    r_start = datetime.datetime.now()
    model_dir = 'data/PALEOGEOGRAPHY_MODELS'
    model_dict = get_reconstruction_model_dict(MODEL_NAME=model)
    static_polygon_filename = str('%s/%s/%s' % (model_dir, model, model_dict['StaticPolygons']))
    rotation_model = pygplates.RotationModel([str('%s/%s/%s' %
                                                  (model_dir, model, rot_file)) for rot_file in
                                              model_dict['RotationFile']])
    partition_time = time

    point_features = []
    for point_feature_str in data:
        lat, lon, p_index = point_feature_str.split(",")
        point_feature = pygplates.Feature()
        point_feature.set_geometry(pygplates.PointOnSphere(float(lat), float(lon)))
        point_feature.set_name(p_index)
        point_features.append(point_feature)

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
        time,
        anchor_plate_id=0)
    result = []
    for reconstructed_feature_geometry in reconstructed_feature_geometries:
        index = reconstructed_feature_geometry.get_feature().get_name()
        # rlat, rlon = reconstructed_feature_geometry.get_reconstructed_geometry().to_lat_lon()
        plat, plon = reconstructed_feature_geometry.get_present_day_geometry().to_lat_lon()
        result.append([index, plat, plon])

    r_end = datetime.datetime.now()
    rotation_time = (r_end - r_start).seconds
    print('rotation_time:', rotation_time)

    # output
    new_lon = "lon_present"
    new_lat = "lat_present"
    new = pd.DataFrame(result, columns=['index', new_lat, new_lon])
    new['index'] = new['index'].astype('int64')
    new.set_index('index', inplace=True)
    # print(new.index)
    out = points.merge(new, how='left', left_index=True, right_index=True)
    # print(out)
    out.to_csv(output_file)


if __name__ == '__main__':
    # define parameters
    time = 125
    model = 'SCOTESE&WRIGHT2018'
    anchor_plate_id = 0
    lon_name = 'longitude'
    lat_name = 'latitude'
    points_file = 'E:/ZJU/GitLab/Paper/BatchPointRotation/BPPR/data/data/syntheticData/point2000000.csv'
    output_file = 'data/points/out_point-2.csv'

    # start to use BPPR
    print("Start reconstruct")
    start = datetime.datetime.now()
    rotate_points(points_file=points_file, lon_name=lon_name, lat_name=lat_name,
                  time=time, model=model, output_file=output_file)
    end = datetime.datetime.now()
    exetime = (end - start).seconds
    print("BPPR_total_time:" + str(exetime))

    # start = datetime.datetime.now()
    # rotate_pygplates(points_file=points_file, lon_name=lon_name, lat_name=lat_name,
    #                  time=time, model=model, output_file=output_file)
    # end = datetime.datetime.now()
    # exetime = (end - start).seconds
    # print("PyGPlates_total_time:" + str(exetime))
