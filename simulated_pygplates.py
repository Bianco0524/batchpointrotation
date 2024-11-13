import csv
import pygplates
import logging
import os
import pandas as pd
import datetime
from rotation_utils import get_reconstruction_model_dict


def read_points(file_path):
    df = pd.read_csv(file_path)
    df.drop(df.columns[0], axis=1, inplace=True)
    return df


def rotate_points(points_file, lon_name, lat_name, time, model, output_file):
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
    partition_time = 0.

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
        rlat, rlon = reconstructed_feature_geometry.get_reconstructed_geometry().to_lat_lon()
        # plat, plon = reconstructed_feature_geometry.get_present_day_geometry().to_lat_lon()
        result.append([index, rlat, rlon])

    r_end = datetime.datetime.now()
    rotation_time = (r_end - r_start).seconds
    print('rotation_time:', rotation_time)

    # output
    new_lon = "lon_" + str(time) + "MA"
    new_lat = "lat_" + str(time) + "MA"
    new = pd.DataFrame(result, columns=['index', new_lat, new_lon])
    new['index'] = new['index'].astype('int64')
    new.set_index('index', inplace=True)
    # print(new.index)
    out = points.merge(new, how='left', left_index=True, right_index=True)
    # print(out)
    out.to_csv(output_file)


if __name__ == '__main__':
    time = 70
    anchor_plate_id = 0
    lon_name = 'lng'
    lat_name = 'lat'
    model = "SCOTESE&WRIGHT2018"
    # points_file = 'E:/ZJU/GitLab/Paper/BatchPointRotation/EX/data/syntheticData/point2750000.csv'
    points_file = 'E:/ZJU/GitLab/Paper/BatchPointRotation/EX/data/realData/CASE/occurrence/pbdb_data_occs_Maastrichtian.csv'
    output_file = 'data/points/out_point-1.csv'

    start = datetime.datetime.now()
    print("Start reconstruct")
    rotate_points(points_file=points_file, lon_name=lon_name, lat_name=lat_name,
                  time=time, model=model, output_file=output_file)
    end = datetime.datetime.now()
    exetime = (end - start).seconds
    print("total_time:" + str(exetime))
