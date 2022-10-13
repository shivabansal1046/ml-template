from source.spark.standard_spark import Spark
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


class ColumnExpression(Spark):
    def __init__(self,columns: [str], expression: str):
        self.columns = columns
        self.expression = expression

    def gen_exp(self, df: DataFrame) -> DataFrame:
        expressions = []
        for i in self.columns:
            expressions += [F.expr("{0}({1})".format(self.expression, i)).alias("{0}_{1}".format(self.expression, i))]
        return df.select(expressions)
