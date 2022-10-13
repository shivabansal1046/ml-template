#### utils test cases ########

from source.utils.Utils import get_sys_variable, parse_json

def test_get_sys_variable():
    assert get_sys_variable("PYTHONUNBUFFERED", "") == "1"