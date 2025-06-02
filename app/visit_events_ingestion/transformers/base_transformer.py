from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
import os
import psycopg2
from pyspark.sql.functions import col

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PW = os.getenv("POSTGRES_PW")


class BaseTransformer(ABC):
    def __init__(self, spark, date, source):
        self.spark = spark
        self.date = date
        self.source = source

    @staticmethod
    def truncate_partition(table, partition, subpartition):
        sql = f"TRUNCATE TABLE {table}_{partition}_{subpartition}"
        conn = psycopg2.connect(
            dbname="demo",
            user=POSTGRES_USER,
            password=POSTGRES_PW,
            host="postgres",
            port=5432,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()

    @staticmethod
    def common_write_function(df, table):
        df.write.format("jdbc").option("url", POSTGRES_URL).option(
            "dbtable", table
        ).option("user", POSTGRES_USER).option("password", POSTGRES_PW).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            "append"
        ).save()

    @staticmethod
    def remove_bad_quality_data(df: DataFrame) -> DataFrame:
        return df.where(col("patient_id").isNotNull())

    @abstractmethod
    def transform_patients(self) -> DataFrame:
        pass

    @abstractmethod
    def transform_visits(self) -> DataFrame:
        pass

    @abstractmethod
    def transform_diagnoses(self) -> DataFrame:
        pass

    @abstractmethod
    def transform_treatments(self) -> DataFrame:
        pass
