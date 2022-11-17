#!/usr/bin/env python3
import glob
import os
from collections import Counter
from datetime import datetime
import requests
import helpers
import json
from functools import wraps
import traceback
import tqdm

"""
Instruction:
crontab -e (how to change editor https://www.howtogeek.com/410995/how-to-change-the-default-crontab-editor/)
1 * * * * cd path/to/files && python3 airAPI.py &>> airAPI.log
"""
variables = helpers.read_project_variables()

functions_to_paths = {
    'get_sensors_measurements': variables['measurements_dir'],
    'get_measurement_sensors_data': variables['sensors_dir'],
    'get_stations': variables['stations_path']
}


def error_handler(func):
    @wraps(func)
    def with_except(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.ConnectionError as e:
            print(f'Function {func.__name__} called with args: {args} thrown requests.exceptions.ConnectionError')
            print(e)
            print(traceback.format_exc())
            path = functions_to_paths[func.__name__]
            path = os.path.join(path, str(args[0])) if args else path
            try:
                with open(path, 'r') as file:
                    return json.load(file)
            except FileNotFoundError:
                return []

    return with_except


@error_handler
def get_stations():
    response = requests.get(variables['stations_url'])
    return response.json()


def save_stations(stations):
    if not os.path.exists(variables['stations_path']):
        os.mkdir(os.path.dirname(variables['stations_path']))
    with open(variables['stations_path'], 'w') as file:
        json.dump(stations, file)


@error_handler
def get_sensors_measurements(sensors_id):
    station_url = os.path.join(variables['sensors_measurements_url'], str(sensors_id))
    response = requests.get(station_url)
    return response.json()


@error_handler
def get_measurement_sensors_data(station_id):
    station_url = os.path.join(variables['sensors_data_url'], str(station_id))
    response = requests.get(station_url)
    return response.json()


def save_sensors(sensors, station_id):
    if not os.path.exists(variables['sensors_dir']):
        os.mkdir(variables['sensors_dir'])
    with open(os.path.join(variables['sensors_dir'], f'{station_id}.json'), 'w') as file:
        json.dump(sensors, file)


def save_measurements(measurements, sensor_idx):
    if not os.path.exists(variables['measurements_dir']):
        os.mkdir(variables['measurements_dir'])
    with open(os.path.join(variables['measurements_dir'], f'{sensor_idx}.json'), 'w') as file:
        json.dump(measurements, file)


def check_update():
    maxes = Counter()
    for file in glob.glob(os.path.join(variables['measurements_dir'], '*json')):
        with open(file, 'r') as f:
            data = json.load(f)
        if data and data['values']:
            dates = [datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S') for row in data['values']]
            maxes[max(dates)] += 1
        else:
            maxes[datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')] += 1
    now = datetime.now()
    dates_flag = (now - maxes.most_common()[0][0]).seconds / 3600 > 1.002  # hour and 7.2seconds
    return dates_flag and not read_save_control()['saving']


def perform_update_if_needed():
    if check_update():
        save_save_control({'saving': True})
        save_all()
        save_save_control({'saving': False})


def read_save_control():
    try:
        with open(variables['save_control_path'], 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {'saving': False}


def save_save_control(save_control):
    with open(variables['save_control_path'], 'w') as file:
        return json.dump(save_control, file)


def save_all():
    stations = get_stations()
    save_stations(stations)
    for station in tqdm.tqdm(stations):
        # time.sleep(0.1)
        sensors = get_measurement_sensors_data(station['id'])
        save_sensors(sensors, station['id'])
        for sensor in sensors:
            # time.sleep(0.1)
            measurements = get_sensors_measurements(sensor['id'])
            save_measurements(measurements, sensor['id'])


if __name__ == '__main__':
    perform_update_if_needed()
