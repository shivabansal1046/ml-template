import os
import json


def get_sys_variable(key: str, default_val: str) -> str:
    a = ''
    if os.getenv(key) is None:
        a = default_val
    else:
        a = os.getenv(key)
    return a


def parse_json(file: str) -> dict:
    with open(file) as json_file:
        output = json.load(json_file)
    return output
