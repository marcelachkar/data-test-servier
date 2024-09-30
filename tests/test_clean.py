import unittest

from pyspark.sql import Row
from pyspark.sql import SparkSession

from src.helpers import clean
from src.helpers.input import ClinicalTrial


class TestInput(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-app").getOrCreate()

    def test_clean_clinical_trial(self):
        # create the mock data to be used in this test
        data = [
            Row(id="1", scientific_title="Study A", date="2020-01-01", journal="Journal of emergency nursing"),
            Row(id="2", scientific_title="Study B", date="27 April 2020", journal="Journal of emergency nursing")
        ]
        clinical_trial_df = self.spark.createDataFrame(data)

        # call the method to be tested
        clinical_trial = ClinicalTrial(spark=self.spark, df=clinical_trial_df)
        cleaned_df = clean.clean_clinical_trial(clinical_trial.read())

        # check the expected result
        self.assertTrue("title" in cleaned_df.columns)
        self.assertFalse("scientific_title" in cleaned_df.columns)
        self.assertEqual("2020-04-27", cleaned_df.collect()[1]["date"])

    def test_clean_pubmed(self):
        # create the mock data to be used in this test
        data = [
            Row(id="1", title="Study A", date="2020-01-01", journal="Journal of emergency nursing"),
            Row(id="2", title="Study B", date="27 April 2020", journal="Journal of emergency nursing")
        ]
        pubmed_df = self.spark.createDataFrame(data)

        # call the method to be tested
        clinical_trial = ClinicalTrial(spark=self.spark, df=pubmed_df)
        cleaned_df = clean.clean_clinical_trial(clinical_trial.read())

        # check the expected result
        self.assertEqual(cleaned_df.count(), 2)
        self.assertEqual("2020-04-27", cleaned_df.collect()[1]["date"])


def tearDown(self):
    self.spark.stop()
