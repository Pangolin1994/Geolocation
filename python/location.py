import os
import timeit
import pandas as pd
from multiprocessing.dummy import Pool
from geopy.geocoders.yandex import Yandex
from geopy.exc import GeocoderTimedOut
import googlemaps
from googlemaps.geocoding import geocode


def extract_location(response):
    return response[0]['geometry']['location']


def geocode_by_yandex(address: str):
    yandex = Yandex('74a9b0c6-e01e-4115-81be-dd4e3ea30c93')
    try:
        return yandex.geocode(address)
    except GeocoderTimedOut:
        return geocode_by_yandex(address)


def main():
    df = pd.read_excel('../data/test.xlsx', sheet_name=1)
    address_series = df.loc[:, 'адрес']
    address_list = [address for address in address_series]
    locations = process_in_parallel(address_list)
    filename = '../data/output.txt'
    if os.path.exists(filename):
        open(filename, mode='w').close()
    output = open(filename, mode='a')
    for (address, location) in zip(address_list, locations):
        output.write('{} {}\n'.format(address, location))
    output.close()


def process_address(address: str):
    # yandex = Yandex('74a9b0c6-e01e-4115-81be-dd4e3ea30c93')
    response = geocode_by_yandex(address)
    if response is not None:
        lat, long = response.latitude, response.longitude
    else:
        maps = googlemaps.Client('AIzaSyBBJyrkuTdzQGG30_Dc4kboECFjP6bM43I')
        response = geocode(maps, address)
        coords = extract_location(response)
        lat, long = coords['lat'], coords['lng']
    return lat, long


def process_in_parallel(addresses: list):
    pool = Pool(10)
    locations = pool.map(process_address, addresses)
    pool.close()
    return locations


if __name__ == '__main__':
    exec_time = timeit.timeit(main, number=1)
    print("Execution time is {}".format(exec_time))
