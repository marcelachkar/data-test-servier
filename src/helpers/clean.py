from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date, unix_timestamp, when
from pyspark.sql.types import StringType

from src.helpers import constants


def clean_clinical_trial(df: DataFrame):
    # align all files with the same column name: title
    df = df.withColumnRenamed("scientific_title", "title")

    # align all files with the same date format
    return fix_dates(df)


def clean_pubmed(df: DataFrame):
    # align all files with the same date format
    return fix_dates(df)


def fix_dates(df: DataFrame):
    """
    :param df: inout DataFrame
    :return:  Dataframe transformed to the desired date format
    """
    # Convert the date column to StringType
    df = df.withColumn(constants.DATE, col(constants.DATE).cast(StringType()))

    possible_formats = [
        ("d MMMM yyyy", "yyyy-MM-dd"),
        ("dd MMMM yyyy", "yyyy-MM-dd"),
        ("dd/MM/yyyy", "yyyy-MM-dd"),
        ("yyyy-MM-dd", "yyyy-MM-dd")
    ]

    date_col = col(constants.DATE)

    # Process each format
    for input_format, output_format in possible_formats:
        date_col = when(
            unix_timestamp(date_col, input_format).isNotNull(),
            to_date(unix_timestamp(date_col, input_format).cast("timestamp"))
        ).otherwise(date_col)

    # Apply the transformations and set the date column
    return df.withColumn(constants.DATE, date_col)
