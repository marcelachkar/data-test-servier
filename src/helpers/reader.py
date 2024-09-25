from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def get_spark_session(app: str):
    return SparkSession.builder.appName(app).getOrCreate()


def csv_reader(spark: SparkSession, filepath: str, schema: StructType, separator: str = ","):
    return (
        spark.read.schema(schema)
        .format("csv")
        .option("sep", separator)
        .option("header", "true")
        .load(filepath)
    )


def json_reader(spark: SparkSession, filepath: str, schema: StructType):
    return spark.read.option("multiline", "true").schema(schema).json(filepath)
