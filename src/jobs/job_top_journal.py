import os

from src.helpers.job_helper_sql import top_journals


def start_job(spark):
    """
    :param spark: spark dependencies
    :param df: transformed dataframe from job drug graph
    :return:
    """
    spark_sess, spark_logger, config_dict = spark.start_spark()
    spark_logger.info("Start job top journals")

    # EXTRACT
    df = spark_sess.read.format("json").load(
        os.path.join(config_dict['result']['path'], config_dict['result']['liaison_graph_folder']))

    # TRANSFORM
    top_journals_df = top_journals(df)
    top_journals_df.show()

    # LOAD
    top_journals_df.write.json(os.path.join(config_dict['result']['path'], config_dict['result']['top_journal_folder']),
                               mode="overwrite")
    spark_logger.info(f"File saved in {config_dict['result']['path']}")

    spark_logger.info("End job top journals")
