# Databricks notebook source
import os
from source.utils.Utils import get_sys_variable, parse_json


class EnvConfig:

    env = get_sys_variable('env','local')
    base_path = get_sys_variable('base_path','')
    silver_data = get_sys_variable('HIVE_SILVER_SCHEMA','')
    gold_data = get_sys_variable('HIVE_GOLD_SCHEMA','')
    logs = get_sys_variable('HIVE_LOGS_SCHEMA','')
    hostname = get_sys_variable('REDSHIFT_URL','')
    dbname = get_sys_variable('REDSHIFT_DATABASE','')
    jdbcport = get_sys_variable('REDSHIFT_PORT','')
    username = get_sys_variable('REDSHIFT_WRITE_USER','')
    driver = get_sys_variable('REDSHIFT_DRIVER','')
    tempdir = get_sys_variable('tempdir','')

class FileConfig:

    def __init__(self, env: str):
        if env.lower() == "local":
            self.config_dict = parse_json("./resources/local_configs.json")
        else:
            self.config_dict = parse_json("./resources/server_config.json")


