from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, upper, col, count

from src.helpers import constants


def liaison_graph(drug_df: DataFrame, clinical_trial_df: DataFrame, pubmed_df: DataFrame):
    """
    :param drug_df: Drug Dataframe
    :param clinical_trial_df: Clinical Trial Dataframe
    :param pubmed_df: Pubmed Dataframe
    :return: Dataframe liaison graph
    """
    join_drug_clinical = drug_df.join(clinical_trial_df,
                                      upper(clinical_trial_df[constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE]).contains(upper(drug_df[constants.DRUG_NAME])),
                                      "left").withColumn(constants.TYPE, lit("clinical"))

    join_drug_pubmed = drug_df.join(pubmed_df,
                                    upper(pubmed_df[constants.PUBMED_TITLE]).contains(upper(drug_df[constants.DRUG_NAME])),
                                    "left").withColumn(constants.TYPE, lit("pubmed"))

    union_df = join_drug_clinical.union(join_drug_pubmed).filter("journal is not null").drop(constants.DRUG_ATCCODE,
                                                                                             constants.CLINICAL_TRIAL_ID).withColumnRenamed(
        constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE, constants.PUBMED_TITLE).sort(constants.DRUG_NAME, constants.TYPE)
    return union_df

def top_journals(df: DataFrame):
    """
    :param df: Dataframe transformed
    :return: Dataframe top journal
    """
    return (
        df.select(col(constants.DRUG_NAME), col(constants.PUBMED_JOURNAL))
            .groupBy(col(constants.PUBMED_JOURNAL))
            .agg(count("*").alias("count"))
            .sort(col("count").desc()).limit(1)
    )


def pubmed_drugs(df: DataFrame, drug: str):
    """
    :param df: DataFrame transformed
    :param drug: drug name
    :return: DataFrame pubmed drugs
    """
    clinical_drugs = df.filter(col(constants.TYPE) == "clinical")\
        .select(constants.DRUG_NAME) \
        .drop_duplicates()

    journals_of_drug = df.filter((col(constants.DRUG_NAME) == drug) &
                                 (col(constants.TYPE) == "pubmed")) \
        .select(constants.PUBMED_JOURNAL) \
        .drop_duplicates()

    drug_names = df.filter(col(constants.PUBMED_JOURNAL).isin(journals_of_drug[constants.PUBMED_JOURNAL])) \
        .select(constants.DRUG_NAME) \
        .drop_duplicates()

    return drug_names.exceptAll(clinical_drugs)