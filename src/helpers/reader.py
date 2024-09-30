from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def get_spark_session(app: str):
    return SparkSession.builder.appName(app).getOrCreate()


def csv_reader(spark: SparkSession, filepath: str, schema: StructType, separator: str = ","):
    """
    CSV reader for the input files

    :param separator: delimiter used in the input file
    :param schema: structure definition of the object
    :param filepath: location of the object
    :param spark: SparkSession object
    :return: DataFrame that was read
    """
    return (
        spark.read.schema(schema)
        .format("csv")
        .option("sep", separator)
        .option("header", "true")
        .load(filepath)
    )


def json_reader(spark: SparkSession, filepath: str, schema: StructType):
    """
    JSON reader for the input files

    :param schema: structure definition of the object
    :param filepath: location of the object
    :param spark: SparkSession object
    :return: DataFrame that was read
    """
    return spark.read.option("multiline", "true").schema(schema).json(filepath)
