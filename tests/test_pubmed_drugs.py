import unittest

from pyspark.sql import Row
from pyspark.sql import SparkSession

from src.helpers.job_helper_sql import pubmed_drugs


class TestPubmedDrugs(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-app").getOrCreate()

    def test_generate_pubmed_drugs(self):
        # create the mock data to be used in this test
        data = [
            Row(drug="BETAMETHASONE", title="title of BETAMETHASONE", date="2020-03-01", journal="The journal test 1",
                type="pubmed"),
            Row(drug="ISOPRENALINE", title="title of ISOPRENALINE", date="2020-03-01", journal="The journal test 1",
                type="clinical"),
            Row(drug="ATROPINE", title="title of ATROPINE", date="2020-03-01", journal="The journal test 2",
                type="pubmed"),
            Row(drug="ETHANOL", title="title of BETAMETHASONE", date="2020-03-01", journal="The journal test 1",
                type="pubmed"),
            Row(drug="DIPHENHYDRAMINE", title="title of DIPHENHYDRAMINE", date="2020-03-01", journal="The journal test 1",
                type="clinical"),
        ]
        df = self.spark.createDataFrame(data)

        # call the method to be tested
        result_df = pubmed_drugs(df, "BETAMETHASONE")

        # check the expected result
        self.assertEqual(result_df.count(), 2)
        self.assertEqual("BETAMETHASONE", result_df.collect()[0]["drug"])
        self.assertEqual("ETHANOL", result_df.collect()[1]["drug"])

    def tearDown(self):
        self.spark.stop()
