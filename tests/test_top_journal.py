import unittest

from pyspark.sql import Row
from pyspark.sql import SparkSession

from src.helpers.job_helper_sql import top_journals


class TestTopJournal(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test-app").getOrCreate()

    def test_generate_top_journal(self):
        # create the mock data to be used in this test
        data = [
            Row(drug="BETAMETHASONE", title="title of BETAMETHASONE", date="2020-03-01", journal="The journal test 1", type="pubmed"),
            Row(drug="BETAMETHASONE", title="title of BETAMETHASONE", date="2020-03-01", journal="The journal test 1", type="clinical"),
            Row(drug="ATROPINE", title="title of ATROPINE", date="2020-03-01", journal="The journal test 2", type="pubmed")
        ]
        df = self.spark.createDataFrame(data)

        # call the method to be tested
        result_df = top_journals(df)

        # check the expected result
        self.assertEqual(result_df.count(), 1)
        self.assertEqual("The journal test 1", result_df.collect()[0]["journal"])
        self.assertEqual(2, result_df.collect()[0]["count"])

    def tearDown(self):
        self.spark.stop()

