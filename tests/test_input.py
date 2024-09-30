import unittest

from pyspark.sql import SparkSession

from src.helpers.input import Drug, ClinicalTrial, Pubmed


class TestInput(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-app").getOrCreate()

    def test_read_drug(self):
        drug_df = Drug(self.spark, "data/input/drugs.csv").read()
        self.assertEqual(drug_df.count(), 7)

    def test_read_clinical_trial(self):
        clinical_trial_df = ClinicalTrial(self.spark, "data/input/clinical_trials.csv").read()
        self.assertEqual(clinical_trial_df.count(), 8)

    def test_read_pubmed(self):
        pubmed_df = Pubmed(self.spark, "data/input/pubmed.csv", "data/input/pubmed.json").read()
        self.assertEqual(pubmed_df.count(), 13)

    def tearDown(self):
        self.spark.stop()
