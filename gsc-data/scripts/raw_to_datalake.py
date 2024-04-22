from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, DateType
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--assume_role', help='Datalake role', required=True)
    parser.add_argument('--table_name', help='Table name', required=True)
    parser.add_argument('--partition', help='Partition column', required=True)
    parser.add_argument('--src_location', help='Raw location', required=True)
    parser.add_argument('--dest_location', help='Datalake location', required=True)

    args = parser.parse_args()
    table_name = args.table_name
    table_name = """{pref}""".format(pref="gsc_") + table_name.replace("-", "_")
    partition = args.partition
    src_location = args.src_location
    dest_location = args.dest_location
    assume_role = args.assume_role

    spark = SparkSession \
        .builder \
        .appName("Serverless gsc data ingestor: Raw to Datalake") \
        .enableHiveSupport() \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("amz-assume-role-arn", assume_role)

    raw_gsc_data = spark.read.option("header", True).csv(src_location)

    transformed_data = raw_gsc_data.withColumn("date_pt_timezone", raw_gsc_data["date_pt_timezone"].cast(DateType()))
    transformed_data = transformed_data.withColumn("clicks", transformed_data["clicks"].cast(IntegerType()))
    transformed_data = transformed_data.withColumn("impressions", transformed_data["impressions"].cast(IntegerType()))
    transformed_data = transformed_data.withColumn("ctr", transformed_data["ctr"].cast(DoubleType()))
    transformed_data = transformed_data.withColumn("position", transformed_data["position"].cast(DoubleType()))

    transformed_data = transformed_data.coalesce(2)
    transformed_data.write.mode('overwrite').format("parquet"). \
        option("compression", "snappy"). \
        save(dest_location)

    sql_string = """
            ALTER TABLE warehouse.{table_name} ADD IF NOT EXISTS PARTITION (snapshotdate='{partition}') location '{dest_location}'
            """.format(table_name=table_name, partition=partition, dest_location=dest_location)

    spark.sql(sql_string)

    spark.stop()
