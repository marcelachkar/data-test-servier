import os

from src.helpers.input import Drug, ClinicalTrial, Pubmed
from src.helpers.job_helper_sql import liaison_graph


def start_job(spark):
    """
    :param spark: spark dependencies
    :return: transformed dataframe
    """

    spark_sess, spark_logger, config_dict = spark.start_spark()

    spark_logger.info("Start job liaison graph")
    # INPUT FILES
    input_path = config_dict['input']['path']
    drug_path = os.path.join(input_path, config_dict['input']['drug_file'])
    clinical_path = os.path.join(input_path, config_dict['input']['clinical_trial_file'])
    pubmed_csv_path = os.path.join(input_path, config_dict['input']['pubmed_csv_file'])
    pubmed_json_path = os.path.join(input_path, config_dict['input']['pubmed_json_file'])

    # EXTRACT
    spark_logger.info("Extraction Drug")
    drug_df = Drug(spark_sess, drug_path).read()
    spark_logger.info("Extraction Clinical trials")
    clinical_trial_df = ClinicalTrial(spark_sess, clinical_path).read()
    spark_logger.info("Extraction PubMed")
    pubmed_df = Pubmed(spark_sess, pubmed_csv_path, pubmed_json_path).read()
    
    spark_logger.info("Extraction of input files is Done")

    # TRANSFORM
    transformed_df = liaison_graph(drug_df, clinical_trial_df, pubmed_df)
    spark_logger.info("Transformation of input files is Done")

    # LOAD
    transformed_df.write.json(os.path.join(config_dict['result']['path'], config_dict['result']['liaison_graph_folder']),
                              mode="overwrite")

    spark_logger.info(f"File saved in {config_dict['result']['path']}")
    spark_logger.info("End job liaison graph")

    return transformed_df
