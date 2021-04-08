import base64
import struct
from array import array
from urllib.parse import urlparse
import fsspec

bucketPerWindow = 10
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
    schema = ""
    if "://" in url:
        parsed_url = urlparse(url)
        schema = parsed_url.scheme.lower()
        load_fs_dependencies(schema)
        url = parsed_url.path
    if storage_options:
        return fsspec.filesystem(schema, **storage_options), url
    else:
        return fsspec.filesystem(schema), url


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
    str_concatted = ''.join(list_to_hash)
    hash_value = hash(str_concatted)
    return hash_value


def get_hashed_key(key_list):
    if isinstance(key_list, list):
        if len(key_list) >= 3:
            return str(key_list[0]) + "." + str(hash_list(key_list[1:]))
        if len(key_list) == 2:
            return str(key_list[0]) + "." + str(key_list[1])
        return key_list[0]
    else:
        return key_list
