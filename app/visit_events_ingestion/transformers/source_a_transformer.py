from datetime import datetime, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import to_date, lit, current_date, col, row_number, explode

from .base_transformer import BaseTransformer,POSTGRES_URL,POSTGRES_USER,POSTGRES_PW

class SourceATransformer(BaseTransformer):
    def __init__(self, spark, date, source):
        super().__init__(spark, date, source)
        self.raw_df = self.spark.read.option("multiline", "true").json(f"input_data/{source}/{date}/visit_events.json")

    def transform_patients(self) -> DataFrame:

        latest_day = datetime.strptime(self.date, "%Y%m%d") - timedelta(days=1)
        latest_day = latest_day.strftime("%Y-%m-%d")

        test_df = self.spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "patients") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PW) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        latest_data = test_df.where(col("source") == lit(self.source)).where(col("snapshot_date") == latest_day)

        df: DataFrame = self.raw_df
        new_data = df.select("patient_id","name") \
            .withColumn("snapshot_date", to_date(lit(self.date), "yyyyMMdd")) \
            .withColumn("source", lit(self.source)) \
            .withColumn("created_date", current_date()) \
            .withColumn("updated_date", current_date())

        combined_df = latest_data.unionByName(new_data) \
            .withColumn("snapshot_date", to_date(lit(self.date), "yyyyMMdd"))

        window = Window.partitionBy("patient_id").orderBy(col("updated_date").desc())

        result = combined_df.withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn")

        return result

    def transform_visits(self) -> DataFrame:
        df: DataFrame = self.raw_df

        result = df.select(
            col("patient_id"),
            explode("visits").alias("visit")
        ).select(col("patient_id"), col("visit.visit_id").alias("visit_id"),
    to_date("visit.date").alias("visit_date"),
    col("visit.provider_notes.text").alias("provider_notes_text"),
    col("visit.provider_notes.author").alias("provider_notes_author")
    ).withColumn("source", lit(self.source)) #.show()

        return result

    def transform_diagnoses(self) -> DataFrame:
        df: DataFrame = self.raw_df

        result = df.select(
            col("patient_id"),
            explode("visits").alias("visit")
        ).select(
            col("visit.date").alias("visit_date"),
            col("visit.visit_id").alias("visit_id"),
            explode("visit.diagnoses").alias("diagnosis")
        ).select(
            to_date("visit_date", "yyyy-MM-dd").alias("visit_date"),
            lit(self.source).alias("source"),
            col("visit_id"),
            col("diagnosis.code").alias("diagnosis_code"),
            col("diagnosis.description").alias("diagnosis_description")
        )


        return result

    def transform_treatments(self) -> DataFrame:
        df: DataFrame = self.raw_df

        result = df.select(
            explode("visits").alias("visit")
        ).select(
            col("visit.date").alias("visit_date"),
            col("visit.visit_id").alias("visit_id"),
            explode("visit.diagnoses").alias("diagnosis")
        ).select(
            col("visit_date"),
            col("visit_id"),
            col("diagnosis.code").alias("diagnosis_code"),
            explode("diagnosis.treatments").alias("treatment")
        ).select(
            to_date("visit_date", "yyyy-MM-dd").alias("visit_date"), # maybe not date format
            lit(self.source).alias("source"),
            col("visit_id"),
            col("diagnosis_code"),
            col("treatment.drug").alias("treatments_drug"),
            col("treatment.dose").alias("treatments_dose")
        )

        return result

