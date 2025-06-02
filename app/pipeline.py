from source_a_transformer import SourceATransformer
from source_b_transformer import SourceBTransformer
from base_transformer import BaseTransformer

def run_pipeline(source: str, date: str, spark):
    transformer_class = {
        "source_a": SourceATransformer,
        "source_b": SourceBTransformer
    }[source]

    transformer = transformer_class(spark, date, source)

    transformer_table_map = {
        "patients": transformer.transform_patients(),
        "visits": transformer.transform_visits(),
        "diagnoses": transformer.transform_diagnoses(),
        "treatments": transformer.transform_treatments()
    }

    for table in transformer_table_map.keys():
        BaseTransformer.truncate_partition(table, date, source) # Truncate Partition
        result_data_frame = transformer_table_map[table] # Transformed Data
        BaseTransformer.common_write_function(result_data_frame, table) # Write Data into PostgreSQL DB
