import os

from src.helpers.job_helper_sql import pubmed_drugs


def start_job(spark, drug):
    """
    :param spark: spark dependencies
    :param drug: drug name to search similar
    :param df: transformed dataframe from job drug graph
    :return:
    """
    spark_sess, spark_logger, config_dict = spark.start_spark()
    spark_logger.info("Start job pubmed drugs")

    # EXTRACT
    df = spark_sess.read.format("json").load(
        os.path.join(config_dict['result']['path'], config_dict['result']['liaison_graph_folder']))

    # TRANSFORM
    pubmed_drugs_df = pubmed_drugs(df, drug)
    pubmed_drugs_df.show()

    # LOAD
    pubmed_drugs_df.write.json(os.path.join(config_dict['result']['path'], config_dict['result']['pubmed_drugs_folder']),
                               mode="overwrite")
    spark_logger.info(f"File saved in {config_dict['result']['path']}")

    spark_logger.info("End job pubmed drugs")
