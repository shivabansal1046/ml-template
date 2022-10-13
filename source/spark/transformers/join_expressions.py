from source.spark.standard_spark import Spark
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


class JoinExpressions(Spark):

    @staticmethod
    def join_dfs(df1: DataFrame, df2: DataFrame, join_cols: [str]
                 , join_type: str = "inner") -> DataFrame:
        return df1.join(df2, join_cols, join_type)
