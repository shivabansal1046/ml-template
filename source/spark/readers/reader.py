from pyspark.sql import dataframe
from source.spark.standard_spark import Spark
from source.config.configurations import EnvConfig


class Reader(Spark, EnvConfig):

    def __init__(self, file_path: str, data_format: str = "parquet", options: dict = {}):
        self.file_path = file_path
        self.format = data_format.lower()
        self.options = options

    def s3_reader(self) -> dataframe:

        if format == "table":
            _df = self.spark.table(self.file_path)
        elif format == "query":
            _df = self.spark.sql(self.file_path)
        else:
            _df = self.spark.read.options(**self.options).format(self.format).load(self.file_path)
        return _df

    def rs_reader(self) -> dataframe:

        _df = self.spark.read.format("jdbc").options(**self.options).load()
        return _df

