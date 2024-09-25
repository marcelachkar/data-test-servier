import sys

from src.dependencies import spark
from src.jobs import job_liaison_graph, job_top_journal, job_pubmed_drugs

if __name__ == "__main__":
    spark_sess, spark_logger, config_dict = spark.start_spark()
    spark_sess.sparkContext.setLogLevel('info')

    if len(sys.argv) > 1:
        if sys.argv[1] == "liaison_graph":
            job_liaison_graph.start_job(spark)
        elif sys.argv[1] == "top_journals":
            job_top_journal.start_job(spark)
        elif len(sys.argv) > 2 and sys.argv[1] == "pubmed_drugs":
            job_pubmed_drugs.start_job(spark, sys.argv[2])

    else:
        spark_logger.error("No arguments specified")

    spark_sess.stop()
