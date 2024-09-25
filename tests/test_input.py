import unittest

from pyspark.sql import SparkSession

from src.helpers.input import Drug, ClinicalTrial, Pubmed


class TestInput(unittest.TestCase):
    def test_read_drug(self):
        spark_sess = SparkSession.builder.appName("test-app").getOrCreate()
        drug_df = Drug(spark_sess, "data/input/drugs.csv").read()
        self.assertEqual(drug_df.count(), 7)

    def test_read_clinical_trial(self):
        spark_sess = SparkSession.builder.appName("test-app").getOrCreate()
        clinical_trial_df = ClinicalTrial(spark_sess, "data/input/clinical_trials.csv").read()
        self.assertEqual(clinical_trial_df.count(), 8)

    def test_read_pubmed(self):
        spark_sess = SparkSession.builder.appName("test-app").getOrCreate()
        pubmed_df = Pubmed(spark_sess, "data/input/pubmed.csv", "data/input/pubmed.json").read()
        self.assertEqual(pubmed_df.count(), 13)