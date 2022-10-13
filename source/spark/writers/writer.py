from pyspark.sql import dataframe
from source.spark.standard_spark import Spark
from source.config.configurations import EnvConfig
import boto3
import psycopg2


class Writer(Spark, EnvConfig):

    def __init__(self, df: dataframe, data_path: str, data_format: str = "parquet", mode: str = "overwrite", options: dict = {}):
        self.df = df
        self.data_path = data_path
        self.format = data_format.lower()
        self.mode = mode
        self.options = options

    def s3_writer(self):

        if format == "table":
            self.df.write.saveAsTable(self.file_path)
        else:
            self.df.write.mode(self.mode).options(**self.options).format(self.format).save(self.data_path)


    def rs_writer(self) -> dataframe:

        self.df.write.mode("overwrite").format("avro").save(EnvConfig.tmp_rs_path)

        client = boto3.client('redshift', region_name=regionName)
        cluster_creds = client.get_cluster_credentials(DbUser=EnvConfig.username,
                                                       DbName=EnvConfig.dbname,
                                                       ClusterIdentifier=EnvConfig.clusterId,
                                                       AutoCreate=False)
        conn = psycopg2.connect(
            host=EnvConfig.hostname,
            port=EnvConfig.jdbcPort,
            user=cluster_creds['DbUser'],
            password=cluster_creds['DbPassword'],
            database=EnvConfig.dbname
        )
        cursor = conn.cursor()
        if(self.mode == "overwrite"):
            cursor.execute("""BEGIN;drop table if exists {0}_tmp;
            create table commercialpc.easy_clean_emr_tmp (like {0});
            ;COPY commercialpc.easy_clean_emr_tmp
                FROM {1}
                iam_role {2}
                format as avro 'auto ignorecase'
                ACCEPTINVCHARS
                TRUNCATECOLUMNS
                ACCEPTANYDATE; 
            drop table commercialpc.easy_clean_emr; 
            alter table commercialpc.easy_clean_emr_tmp rename to easy_clean_emr;
            END;""".format(self.data_path, EnvConfig.tmp_rs_path,
                            EnvConfig.s3_copy_role))
        else:
            cursor.execute("""BEGIN;
            create temporary table {0}_tmp (like {0});
            COPY {0}_tmp
                FROM {1}
                iam_role {2}
                format as csv
                ACCEPTINVCHARS
                TRUNCATECOLUMNS
                ACCEPTANYDATE;     
            insert into commercialpc.easy_clean_emr select * from  commercialpc.easy_clean_emr;
            commit;
            END;""".format(self.data_path, EnvConfig.tmp_rs_path,
                           EnvConfig.s3_copy_role))

