from pyspark.sql import SparkSession


class Spark:

    spark = SparkSession.builder.appName("reader-support").enableHiveSupport().getOrCreate()