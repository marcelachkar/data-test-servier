import os

from src.helpers.job_helper_sql import pubmed_drugs


def start_job(spark, drug):
    """
    For a given drug, find the set of drugs mentioned by the same journals referenced
     by scientific publications but not clinical trials

    :param spark: spark dependencies
    :param drug: drug name
    :return: transformed dataframe
    """
    spark_sess, spark_logger, config_dict = spark.start_spark()
    spark_logger.info("Start job pubmed drugs")

    # extract the liaison graph
    df = spark_sess.read.format("json").load(
        os.path.join(config_dict['result']['path'], config_dict['result']['liaison_graph_folder']))

    # call the helper to find the name of the different drugs mentioned by the same article
    pubmed_drugs_df = pubmed_drugs(df, drug)
    pubmed_drugs_df.show()

    # write the result in a json file in the result directory
    pubmed_drugs_df.write.json(os.path.join(config_dict['result']['path'], config_dict['result']['pubmed_drugs_folder']),
                               mode="overwrite")
    spark_logger.info(f"File saved in {config_dict['result']['path']}")

    spark_logger.info("End job pubmed drugs")
