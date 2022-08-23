# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import math

_aggrTypeNone = 0
_aggrTypeCount = 1
_aggrTypeSum = 2
_aggrTypeSqr = 4
_aggrTypeMax = 8
_aggrTypeMin = 16
_aggrTypeLast = 32
_aggrTypeFirst = 64

# Derived aggregates
_aggrTypeAvg = _aggrTypeCount | _aggrTypeSum
_aggrTypeRate = _aggrTypeLast | 0x8000
_aggrTypeStddev = _aggrTypeCount | _aggrTypeSum | _aggrTypeSqr
_aggrTypeStdvar = _aggrTypeCount | _aggrTypeSum | _aggrTypeSqr | 0x8000
_aggrTypeAll = 0xffff

_raw_aggregates = [_aggrTypeCount, _aggrTypeSum, _aggrTypeSqr, _aggrTypeMax, _aggrTypeMin, _aggrTypeLast]
_raw_aggregates_by_name = {'count': _aggrTypeCount,
                           'sum': _aggrTypeSum,
                           'sqr': _aggrTypeSqr,
                           'max': _aggrTypeMax,
                           'min': _aggrTypeMin,
                           'first': _aggrTypeFirst,
                           'last': _aggrTypeLast}
_all_aggregates_by_name = {'count': _aggrTypeCount,
                           'sum': _aggrTypeSum,
                           'sqr': _aggrTypeSqr,
                           'max': _aggrTypeMax,
                           'min': _aggrTypeMin,
                           'first': _aggrTypeFirst,
                           'last': _aggrTypeLast,
                           'avg': _aggrTypeAvg,
                           'stdvar': _aggrTypeStdvar,
                           'stddev': _aggrTypeStddev}
_all_aggregates_to_name = {_aggrTypeCount: 'count',
                           _aggrTypeSum: 'sum',
                           _aggrTypeSqr: 'sqr',
                           _aggrTypeMax: 'max',
                           _aggrTypeMin: 'min',
                           _aggrTypeFirst: 'first',
                           _aggrTypeLast: 'last',
                           _aggrTypeAvg: 'avg',
                           _aggrTypeStdvar: 'stdvar',
                           _aggrTypeStddev: 'stddev'}


def is_raw_aggregate(aggregate):
    return aggregate in _raw_aggregates_by_name


def _avg(args):
    count = args[0]
    sum = args[1]
    if count == 0:
        return math.nan
    return sum / count


def _stddev(args):
    count = args[0]
    if count == 0 or count == 1:
        return math.nan
    sum = args[1]
    sqr = args[2]

    return math.sqrt((count * sqr - sum * sum) / (count * (count - 1)))


def _stdvar(args):
    count = args[0]
    if count == 0 or count == 1:
        return math.nan
    sum = args[1]
    sqr = args[2]
    return (count * sqr - sum * sum) / (count * (count - 1))


def get_virtual_aggregation_func(aggregation):
    if aggregation == 'avg':
        return _avg
    if aggregation == 'stdvar':
        return _stdvar
    if aggregation == 'stddev':
        return _stddev

    raise TypeError(f'"{aggregation}" aggregator is not defined')


def get_implied_aggregates(aggregate):
    aggrs = []
    aggr_bits = _all_aggregates_by_name[aggregate]
    for raw_aggr in _raw_aggregates:
        if aggr_bits & raw_aggr == raw_aggr:
            aggrs.append(_all_aggregates_to_name[raw_aggr])
    return aggrs


def get_all_raw_aggregates_with_hidden(aggregates):
    raw_aggregates = {}

    for aggregate in aggregates:
        if is_raw_aggregate(aggregate):
            raw_aggregates[aggregate] = False
        else:
            for dependant_aggr in get_implied_aggregates(aggregate):
                if dependant_aggr not in raw_aggregates:
                    raw_aggregates[dependant_aggr] = True

    return raw_aggregates


def get_all_raw_aggregates(aggregates):
    return set(get_all_raw_aggregates_with_hidden(aggregates).keys())
