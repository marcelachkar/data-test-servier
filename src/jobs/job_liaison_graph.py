import os

from src.helpers.clean import clean_pubmed, clean_clinical_trial
from src.helpers.input import Drug, ClinicalTrial, Pubmed
from src.helpers.job_helper_sql import liaison_graph


def start_job(spark):
    """
    Build a liaison graph that represents different drugs and their respective mentions in various journals cited
     in scientific publications and clinical trials, along with the date associated with each of these mentions.

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

    # EXTRACT AND CLEAN
    spark_logger.info("Extraction Drug")
    drug_df = Drug(spark_sess, drug_path).read()

    spark_logger.info("Extraction Clinical trials")
    clinical_trial_df = ClinicalTrial(spark_sess, clinical_path).read()
    cleaned_ct_df = clean_clinical_trial(clinical_trial_df)

    spark_logger.info("Extraction PubMed")
    pubmed_df = Pubmed(spark_sess, pubmed_csv_path, pubmed_json_path).read()
    cleaned_p_df = clean_pubmed(pubmed_df)

    spark_logger.info("Extraction of input files is Done")

    # TRANSFORM
    transformed_df = liaison_graph(drug_df, cleaned_ct_df, cleaned_p_df)
    spark_logger.info("Transformation of input files is Done")

    # LOAD
    transformed_df.write.json(os.path.join(config_dict['result']['path'], config_dict['result']['liaison_graph_folder']),
                              mode="overwrite")

    spark_logger.info(f"File saved in {config_dict['result']['path']}")
    spark_logger.info("End job liaison graph")

    return transformed_df
