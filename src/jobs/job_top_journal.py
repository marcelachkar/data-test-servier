import os

from src.helpers.job_helper_sql import top_journals


def start_job(spark):
    """
    find the journal name that has the most different drugs mentioned in his title,
    along with the number of different drugs that where mentioned

    :param spark: spark dependencies
    :return: transformed dataframe
    """
    spark_sess, spark_logger, config_dict = spark.start_spark()
    spark_logger.info("Start job top journals")

    # extract the liaison graph
    df = spark_sess.read.format("json").load(
        os.path.join(config_dict['result']['path'], config_dict['result']['liaison_graph_folder']))

    # call the helper to find the corresponding journal name
    top_journals_df = top_journals(df)
    top_journals_df.show()

    # write the result in a json file in the result directory
    top_journals_df.write.json(os.path.join(config_dict['result']['path'], config_dict['result']['top_journal_folder']),
                               mode="overwrite")
    spark_logger.info(f"File saved in {config_dict['result']['path']}")

    spark_logger.info("End job top journals")
