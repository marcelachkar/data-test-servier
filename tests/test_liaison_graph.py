import unittest

from pyspark.sql import Row
from pyspark.sql import SparkSession

from src.helpers.clean import clean_clinical_trial, clean_pubmed
from src.helpers.job_helper_sql import liaison_graph


class TestLiaisonGraph(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-app").getOrCreate()

    def test_generate_liaison_graph(self):
        # create the mock data to be used in this test
        drug_data = [
            Row(atccode="123", drug="ETHANOL"),
            Row(atccode="213AQ", drug="TETRACYCLINE"),
            Row(atccode="423A", drug="ETHANOL")
        ]
        drug_df = self.spark.createDataFrame(drug_data)

        clinical_data = [
            Row(id="1", scientific_title="Study A ETHANOL", date="2020-01-01", journal="Journal of test 1"),
            Row(id="2", scientific_title="Study ETHANOL B", date="27 April 2020", journal=""),
            Row(id="2", scientific_title="Study ETHANOL C", date="27 April 2020", journal="Journal of test 1")

        ]
        clinical_trial_df = self.spark.createDataFrame(clinical_data)

        pubmed_data = [
            Row(id="1", title="Study A ETHANOL", date="2020-01-01", journal="Journal of test 1"),
            Row(id="2", title="TETRACYCLINE Study B", date="27 April 2020", journal="Journal of test 2")
        ]
        pubmed_df = self.spark.createDataFrame(pubmed_data)

        # call the method to be tested
        cleaned_cct_df = clean_clinical_trial(clinical_trial_df)
        cleaned_p_df = clean_pubmed(pubmed_df)
        result_df = liaison_graph(drug_df, cleaned_cct_df, cleaned_p_df)

        # check the expected result
        self.assertEqual(result_df.count(), 4)

    def tearDown(self):
        self.spark.stop()
