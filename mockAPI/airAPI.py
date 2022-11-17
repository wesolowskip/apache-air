#!/usr/bin/env python3
import os
from datetime import datetime
import requests
import helpers
import json
"""
Instruction:
crontab -e (how to change editor https://www.howtogeek.com/410995/how-to-change-the-default-crontab-editor/)
1 * * * * cd path/to/files && python3 airAPI.py &>> airAPI.log
"""
variables = helpers.read_project_variables()


def get_stations():
    response = requests.get(variables['stations_url'])
    return response.json()


def save_stations(stations):
    if not os.path.exists(variables['stations_path']):
        os.mkdir(os.path.dirname(variables['stations_path']))
    with open(variables['stations_path'], 'w') as file:
        json.dump(stations, file)


def get_sensors_measurements(sensors_id):
    station_url = os.path.join(variables['sensors_measurements_url'], str(sensors_id))
    response = requests.get(station_url)
    return response.json()


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


def save_all():
    stations = get_stations()
    save_stations(stations)
    for station in stations:
        sensors = get_measurement_sensors_data(station['id'])
        save_sensors(sensors, station['id'])
        for sensor in sensors:
            measurements = get_sensors_measurements(sensor['id'])
            save_measurements(measurements, sensor['id'])


if __name__ == '__main__':
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f'{now} - Data saving start')
    save_all()
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f'{now} - Data saved')
