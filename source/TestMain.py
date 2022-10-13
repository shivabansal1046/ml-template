'''from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

#spark code test
spark = SparkSession.builder.appName('abc') \
    .getOrCreate()
# List
data = [('Category A', 100, "This is category A"),
        ('Category B', 120, "This is category B"),
        ('Category C', 150, "This is category C")]

# Create a schema for the dataframe
schema = StructType([
    StructField('Category', StringType(), True),
    StructField('Count', IntegerType(), True),
    StructField('Description', StringType(), True)
])

df = spark.createDataFrame(data, schema)
print(df.schema)
df.show()'''

## Reader
from source.spark.readers.reader import Reader
df = Reader("./data_scenarios/test_file.csv", "csv", {"header": True}).s3_reader()
#df = LocalReader("./resources/test_file.csv", "csv").reader()
df.show()

##writer
from source.spark.writers.writer import Writer
import yaml

Writer(df.union(df), "./data_scenarios/test_writer", "parquet", "overwrite").s3_writer()

df = Reader("./data_scenarios/test_writer", "parquet").s3_reader()
df.show()

## transformers

from source.spark.transformers.column_exp_generators import ColumnExpression


expressions = ColumnExpression(["id1", "id2"], "max")

expressions.gen_exp(df).show()

# utils code test
from source.config.configurations import EnvConfig

config = EnvConfig()

print(config.base_path)




