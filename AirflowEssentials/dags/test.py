import os


def get_path():
    x = os.getcwd() + '/dags/bash_scripts'
    print('path is: ', x)


get_path()
