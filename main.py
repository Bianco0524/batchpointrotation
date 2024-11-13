# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from fossil_pygplates import GPlatesToShp
import datetime


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    folder = "E:/ZJU/GitLab/Paper/BatchPointRotation/EX/data/realData/Cretaceous/"
    name = "pbdb_data_occurance_Cretaceous_"
    time_list = [70.0, 80.0, 90.0, 100.0, 110.0, 120.0, 130.0, 140.0]
    start = datetime.datetime.now()
    for time in time_list:
        file_path = folder + name + str(time) + '.csv'
        out_path = folder + name + str(time) + '.shp'
        GPlatesToShp(file_path, 'lat', 'lng', out_path)
    end = datetime.datetime.now()
    exetime = (end - start).seconds
    print("total_time:" + str(exetime))
