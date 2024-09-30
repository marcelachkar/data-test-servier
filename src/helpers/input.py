from pyspark.sql import SparkSession, DataFrame

from src.helpers import reader, schemas

"""
input
~~~~~~~

This module contains classes that defines the input objects
and uses a SparkSession and a the filepath in order to read
the input object and parse them.
"""


class Drug(object):
    def __init__(self, spark: SparkSession, path: str, df: DataFrame = None):
        self.spark = spark
        self.path = path
        self.df = df
        self.schema = schemas.drug_schema

    def read(self):
        if self.df is not None:
            return self.df
        if self.path:
            return reader.csv_reader(spark=self.spark, filepath=self.path, schema=self.schema)
        raise ValueError("Either path or df must be provided.")


class ClinicalTrial(object):
    def __init__(self, spark: SparkSession, path: str = None, df: DataFrame = None):
        self.spark = spark
        self.path = path
        self.df = df
        self.schema = schemas.clinical_trial_schema

    def read(self):
        if self.df is not None:
            return self.df
        if self.path:
            return reader.csv_reader(spark=self.spark, filepath=self.path, schema=self.schema)
        raise ValueError("Either path or df must be provided.")


class Pubmed(object):
    def __init__(self, spark: SparkSession, csv_path: str, json_path: str, df: DataFrame = None):
        self.spark = spark
        self.csv_path = csv_path
        self.json_path = json_path
        self.df = df
        self.schema = schemas.pubmed_schema

    def read(self):
        if self.df is not None:
            return self.df
        if self.csv_path and self.json_path:
            return reader.csv_reader(spark=self.spark, filepath=self.csv_path, schema=self.schema).union(
                reader.json_reader(spark=self.spark, filepath=self.json_path, schema=self.schema)
            )
        raise ValueError("Either path or df must be provided.")
