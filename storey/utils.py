import base64
import hashlib
import os
import struct
from array import array
from urllib.parse import urlparse
import fsspec

bucketPerWindow = 2
schema_file_name = '.schema'


def parse_duration(string_time):
    unit = string_time[-1]

    if unit == 's':
        multiplier = 1000
    elif unit == 'm':
        multiplier = 60 * 1000
    elif unit == 'h':
        multiplier = 60 * 60 * 1000
    elif unit == 'd':
        multiplier = 24 * 60 * 60 * 1000
    else:
        raise ValueError(f'Failed to parse time "{string_time}"')

    return int(string_time[:-1]) * multiplier


def get_one_unit_of_duration(string_time):
    unit = string_time[-1]

    if unit == 's':
        multiplier = 1000
    elif unit == 'm':
        multiplier = 60 * 1000
    elif unit == 'h':
        multiplier = 60 * 60 * 1000
    elif unit == 'd':
        multiplier = 24 * 60 * 60 * 1000
    else:
        raise ValueError(f'Failed to parse time "{string_time}"')

    return multiplier


def convert_array_tlv(a):
    """
    get's the array typed array to convert to a blob value of an array, encode it to base64 from base10 with the following format-
        struct vn_object_item_array_md {
        uint32_t magic_no; #define MAGIC_NO 11223344
        uint16_t version_no; #define ARRAY_VERSION 1
        uint32_t array_size_in_bytes; # 8 x element num (8x10 = 80)
        enum node_query_filter_operand_type type; # int=11 (260), double=12 (261)
        };
    :param a: array type (e.g -  array('i', [1, 2, 3, 4, 5])
    :return: blob value of an array
    """
    array_type = 259 if a.typecode == 'l' else 261
    size = len(a)
    if a.typecode == 'l':
        values = struct.pack("l" * size, *a)
    else:
        values = struct.pack("d" * size, *a)
    structure = struct.pack("IhII", 11223344, 1, size * 8, array_type)
    converted_blob = base64.b64encode(structure + values)
    return converted_blob


def extract_array_tlv(b):
    """
    get's the blob value of an array, decode it from base64 to base10 and extract the type, length and value based
    on the structure -
        struct vn_object_item_array_md {
        uint32_t magic_no; #define MAGIC_NO 11223344
        uint16_t version_no; #define ARRAY_VERSION 1
        uint32_t array_size_in_bytes; # 8 x element num (8x10 = 80)
        enum node_query_filter_operand_type type; # int=11 (260), double=12 (261)
        };
    :param b: blob value
    :return: array type array
    """
    converted_blob = base64.b64decode(b)
    tl = converted_blob[:16]
    v = converted_blob[16:]
    structure = struct.unpack("IhII", tl)  # I=unsigned_int, h=short
    size = int(structure[2] / 8)
    array_type = 'l' if structure[3] == 259 else 'd'
    if array_type == 'l':
        values = [v for v in struct.unpack("{}".format("l" * size), v)]
    else:
        values = [v for v in struct.unpack("{}".format("d" * size), v)]
    return array(array_type[0], values)


def _split_path(path):
    while path.startswith('/'):
        path = path[1:]

    parts = path.split('/', 1)
    if len(parts) == 1:
        return parts[0], '/'
    else:
        return parts[0], f'/{parts[1]}'


def url_to_file_system(url, storage_options):
    remaining_path = url
    scheme = ""
    if "://" in url:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        load_fs_dependencies(scheme)
        if scheme == 'v3io':
            remaining_path = parsed_url.path
        else:
            remaining_path = f'{parsed_url.netloc}{parsed_url.path}'

    if storage_options:
        return fsspec.filesystem(scheme, **storage_options), remaining_path
    else:
        return fsspec.filesystem(scheme), remaining_path


def load_fs_dependencies(schema):
    if schema == "s3":
        try:
            import s3fs  # noqa: F401
        except ImportError:
            raise StoreyMissingDependencyError(
                "s3 packages are missing, use pip install storey[s3]"
            )
    if schema == "az":
        try:
            import adlfs  # noqa: F401
        except ImportError:
            raise StoreyMissingDependencyError(
                "azure packages are missing, use pip install storey[az]"
            )


class StoreyMissingDependencyError(Exception):
    pass


def get_in(obj, keys, default=None):
    """
    >>> get_in({'a': {'b': 1}}, 'a.b')
    1
    """
    if isinstance(keys, str):
        keys = keys.split(".")

    for key in keys:
        if not obj or key not in obj:
            return default
        obj = obj[key]
    return obj


def update_in(obj, key, value):
    parts = key.split(".") if isinstance(key, str) else key
    for part in parts[:-1]:
        sub = obj.get(part, None)
        if sub is None:
            sub = obj[part] = {}
        obj = sub

    last_key = parts[-1]
    obj[last_key] = value


def hash_list(list_to_hash):
    list_to_hash = [str(element) for element in list_to_hash]
    str_concatted = ''.join(list_to_hash)
    sha1 = hashlib.sha1()
    sha1.update(str_concatted.encode('utf8'))
    return sha1.hexdigest()


def stringify_key(key_list):
    if isinstance(key_list, list):
        if len(key_list) >= 3:
            return str(key_list[0]) + "." + hash_list(key_list[1:])
        if len(key_list) == 2:
            return str(key_list[0]) + "." + str(key_list[1])
        return key_list[0]
    else:
        return key_list


def _create_filter_tuple(dtime, attr, sign, list_tuples):
    if attr:
        value = getattr(dtime, attr, None)
        tuple1 = (attr, sign, value)
        list_tuples.append(tuple1)


def _find_filter_helper(list_partitions, dtime, sign, first_sign, first_uncommon, filters, filter_column=None):
    single_filter = []
    if len(list_partitions) == 0 or first_uncommon is None:
        return
    last_partition = list_partitions[-1]
    if len(list_partitions) == 1 or last_partition == first_uncommon:
        return
    list_partitions_without_last_element = list_partitions[:-1]
    for partition in list_partitions_without_last_element:
        _create_filter_tuple(dtime, partition, "=", single_filter)
    if first_sign:
        # only for the first iteration we need to have ">="/"<=" instead of ">"/"<"
        _create_filter_tuple(dtime, last_partition, first_sign, single_filter)
        # start needs to be > and end needs to be "<="
        if first_sign == "<=":
            tuple_last_range = (filter_column, first_sign, dtime)
        else:
            tuple_last_range = (filter_column, sign, dtime)
        single_filter.append(tuple_last_range)
    else:
        _create_filter_tuple(dtime, last_partition, sign, single_filter)
    _find_filter_helper(list_partitions_without_last_element, dtime, sign, None, first_uncommon, filters)
    filters.append(single_filter)


def _get_filters_for_filter_column(start, end, filter_column, side_range):
    lower_limit_tuple = (filter_column, ">", start)
    upper_limit_tuple = (filter_column, "<=", end)
    side_range.append(lower_limit_tuple)
    side_range.append(upper_limit_tuple)


def find_partitions(url, fs):
    # ML-1365. assuming the partitioning is symmetrical (for example both year=2020 and year=2021 directories will have
    # inner month partitions).

    partitions = []

    def _is_private(path):
        _, tail = os.path.split(path)
        return (tail.startswith('_') or tail.startswith('.')) and '=' not in tail

    def find_partition_helper(url, fs, partitions):
        content = fs.ls(url, detail=True)
        if len(content) == 0:
            return partitions
        # https://issues.apache.org/jira/browse/ARROW-1079 there could be some private dirs
        filtered_dirs = [x for x in content if not _is_private(x["name"])]
        if len(filtered_dirs) == 0:
            return partitions

        inner_dir = filtered_dirs[0]["name"]
        if fs.isfile(inner_dir):
            return partitions
        part = inner_dir.split("/")[-1].split("=")
        partitions.append(part[0])
        find_partition_helper(inner_dir, fs, partitions)

    if fs.isfile(url):
        return partitions
    find_partition_helper(url, fs, partitions)

    legal_time_units = ['year', 'month', 'day', 'hour', 'minute', 'second']

    partitions_time_attributes = [j for j in legal_time_units if j in partitions]

    return partitions_time_attributes


def find_filters(partitions_time_attributes, start, end, filters, filter_column):
    # this method build filters to be used by
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
    common_partitions = []
    first_uncommon = None
    # finding the common attributes. for example for start=1.2.2018 08:53:15, end=5.2.2018 16:24:31, partitioned by
    # year, month, day, hour. common_partions=[year, month], first_uncommon=day
    for part in partitions_time_attributes:
        value_start = getattr(start, part, None)
        value_end = getattr(end, part, None)
        if value_end == value_start:
            common_partitions.append(part)
        else:
            first_uncommon = part
            break

    # for start=1.2.2018 08:53:15, end=5.2.2018 16:24:31, this method will append to filters
    # [(year=2018, month=2,day>=1, filter_column>1.2.2018 08:53:15)]
    _find_filter_helper(partitions_time_attributes, start, ">", ">=", first_uncommon, filters, filter_column)

    middle_range_filter = []
    for partition in common_partitions:
        _create_filter_tuple(start, partition, "=", middle_range_filter)

    if len(filters) == 0:
        # creating only the middle range
        _create_filter_tuple(start, first_uncommon, ">=", middle_range_filter)
        _create_filter_tuple(end, first_uncommon, "<=", middle_range_filter)
        _get_filters_for_filter_column(start, end, filter_column, middle_range_filter)
    else:
        _create_filter_tuple(start, first_uncommon, ">", middle_range_filter)
        _create_filter_tuple(end, first_uncommon, "<", middle_range_filter)
    # for start=1.2.2018 08:53:15, end=5.2.2018 16:24:31, this will append to filters
    # [(year=2018, month=2, 1<day<5)]
    filters.append(middle_range_filter)

    # for start=1.2.2018 08:53:15, end=5.2.2018 16:24:31, this method will append to filters
    # [(year=2018, month=2,day<=5, filter_column<5.2.2018 16:24:31)]
    _find_filter_helper(partitions_time_attributes, end, "<", "<=", first_uncommon, filters, filter_column)


def drop_reserved_columns(df):
    cols_to_drop = []
    for col in df.columns:
        if col.startswith('igzpart_'):
            cols_to_drop.append(col)
    df.drop(labels=cols_to_drop, axis=1, inplace=True, errors='ignore')
