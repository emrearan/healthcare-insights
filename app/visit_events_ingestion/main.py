import sys

from pyspark.sql import SparkSession

from pipeline import run_pipeline


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python your_script.py <source> <date>")
        sys.exit(1)

    source = sys.argv[1]
    date = sys.argv[2]

    spark = SparkSession.builder.appName("Visit Events Ingestion").getOrCreate()

    run_pipeline(source, date, spark)
