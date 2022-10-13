from source.spark.readers.reader import Reader
from source.spark.standard_spark import Spark
from source.config.configurations import EnvConfig, FileConfig


def test_s3reader():
    env_config = EnvConfig()
    file_config = FileConfig(env_config.env)
    config_dict = file_config.config_dict

    src_configs = config_dict['battery_monitor']
    battery_monitor = Reader(env_config.base_path + src_configs['path'], src_configs['format'],
                             src_configs['options']).s3_reader()

    battery_monitor_actual = Spark.spark.read.option("header", True).csv(env_config.base_path + src_configs['path'])
    assert battery_monitor_actual.exceptAll(battery_monitor).count() == 0


