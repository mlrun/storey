from dtypes import *
from datetime import datetime, timedelta
from http.client import HTTPSConnection, HTTPConnection
import base64
import ssl
from utils import convert_array_tlv

# a class that accepts - window, (data, key, timestamp)


class WindowedStoreElement:
    def __init__(self, key, window):
        self.key = key

        self.window = window
        self.features = {}
        self.first_bucket_start_time = datetime.now()
        self.last_bucket_start_time = self.first_bucket_start_time + \
                                      timedelta(
                                          milliseconds=(
                                                               window.get_total_number_of_buckets() - 1) * window.period_millis)

        # for feature in aggregations:
        #     for aggr in aggregations[feature]:
        #         self.features[self.get_column_name(feature, aggr)] = \
        #             [0.0] * window.get_total_number_of_buckets()


    def add(self, data, timestamp):
        # add a new point and aggregate
        for column_name in data:
            if column_name not in self.features:
                self.initialize_column(column_name)
            index = self.get_or_advance_bucket_index_by_timestamp(timestamp)
            self.features[column_name][index].extend([data[column_name]])

    def get_column_name(self, column, aggregation):
        return f'{column}_{aggregation}_{self.window.window_str}'

    def initialize_column(self, column):
        self.features[column] = []
        for i in range(self.window.get_total_number_of_buckets()):
            self.features[column].append([])

    def get_or_advance_bucket_index_by_timestamp(self, timestamp):
        if timestamp < self.last_bucket_start_time + timedelta(milliseconds=self.window.period_millis):
            bucket_index = int(
                (timestamp - self.first_bucket_start_time).total_seconds() * 1000 / self.window.period_millis)
            return bucket_index
        else:
            self.advance_window_period(timestamp)
            return self.window.get_total_number_of_buckets() - 1  # return last index

    def advance_window_period(self, advance_to=datetime.now()):
        desired_bucket_index = int(
            (advance_to - self.first_bucket_start_time).total_seconds() * 1000 / self.window.period_millis)
        buckets_to_advnace = desired_bucket_index - (self.window.get_total_number_of_buckets() - 1)

        if buckets_to_advnace > 0:
            if buckets_to_advnace > self.window.get_total_number_of_buckets():
                for column in self.features:
                    self.initialize_column(column)
            else:
                for column in self.features:
                    self.features[column] = self.features[column][buckets_to_advnace:]
                    for i in range(buckets_to_advnace):
                        self.features[column].extend([[]])

            self.first_bucket_start_time = \
                self.first_bucket_start_time + timedelta(milliseconds=buckets_to_advnace * self.window.period_millis)
            self.last_bucket_start_time = \
                self.last_bucket_start_time + timedelta(milliseconds=buckets_to_advnace * self.window.period_millis)


def aggregate(self, aggregation, old_value, new_value):
    if aggregation == 'min':
        return min(old_value, new_value)
    elif aggregation == 'max':
        return max(old_value, new_value)
    elif aggregation == 'sum':
        return old_value + new_value
    elif aggregation == 'count':
        return old_value + 1
    elif aggregation == 'last':
        return new_value
    elif aggregation == 'first':
        return old_value


class WindowedStore:
    def __init__(self, window):
        self.window = window
        self.cache = {}

    def add(self, key, data, timestamp):
        if key not in self.cache:
            self.cache[key] = WindowedStoreElement(key, self.window)

        self.cache[key].add(data, timestamp)
