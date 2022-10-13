from source.spark.readers.reader import Reader
from source.config.configurations import EnvConfig, FileConfig
from source.spark.transformers.join_expressions import JoinExpressions

env_config = EnvConfig()
file_config = FileConfig(env_config.env)
config_dict = file_config.config_dict

src_configs = config_dict['dataset1']
dataset1 = Reader(env_config.base_path + src_configs['path'], src_configs['format'],
                         src_configs['options']).s3_reader()

src_configs = config_dict['dataset2']

dataset2 = Reader(env_config.base_path + src_configs['path'], src_configs['format'],
                    src_configs['options']).s3_reader()

dataset1.show()

dataset2.show()

joinedDF = JoinExpressions.join_dfs(battery_monitor, dataset2, ["id1"], "inner")

joinedDF.show()