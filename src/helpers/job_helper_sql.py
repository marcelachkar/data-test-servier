from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, upper, col, count

from src.helpers import constants

"""
job_helper_sql
~~~~~~~

This module contains methods that execute queries in order to extract and transform
data from the different input objects into the desired output.  
"""

def liaison_graph(drug_df: DataFrame, clinical_trial_df: DataFrame, pubmed_df: DataFrame):
    """
    Builds a liaison graph of drugs from the drug, clinical trial and pubmed data
    we take into account:
    - a drug is considered in a PubMed article or clinical trial if it is mentioned in the title of the publication
    - a drug is considered mentioned by a journal if it is mentioned in a publication issued by that journal
    we will add in the graph structure the TYPE of the journal (Pubmed or clinical trial)

    :param drug_df: Drug Dataframe
    :param clinical_trial_df: Clinical Trial Dataframe
    :param pubmed_df: Pubmed Dataframe
    :return: Dataframe liaison graph
    """

    # extract the drugs in clinical trial
    join_drug_clinical = drug_df.join(clinical_trial_df,
                                      upper(clinical_trial_df[constants.TITLE]).contains(upper(drug_df[constants.DRUG_NAME])),
                                      "left").withColumn(constants.TYPE, lit("clinical"))

    # extract the drugs in pubmed
    join_drug_pubmed = drug_df.join(pubmed_df,
                                    upper(pubmed_df[constants.TITLE]).contains(upper(drug_df[constants.DRUG_NAME])),
                                    "left").withColumn(constants.TYPE, lit("pubmed"))

    # build de liaison graph
    union_df = join_drug_clinical.union(join_drug_pubmed).filter("journal != ''").drop(constants.DRUG_ATCCODE,
                                                                                             constants.ID).withColumnRenamed(
        constants.TITLE, constants.TITLE).sort(constants.DRUG_NAME, constants.TYPE)
    return union_df.drop_duplicates()

def top_journals(df: DataFrame):
    """
    Using the liaison graph, returns the journal name that has the
    most different drugs mentioned in his title,
    along with the number of different drugs that where mentioned

    :param df: Dataframe transformed
    :return: Dataframe top journal
    """
    return (
        df.select(col(constants.DRUG_NAME), col(constants.JOURNAL))
            .groupBy(col(constants.JOURNAL))
            .agg(count("*").alias("count"))
            .sort(col("count").desc()).limit(1)
    )


def pubmed_drugs(df: DataFrame, drug: str):
    """
    Using the liaison graph and the given drug name,
    return the set of drugs mentioned by the same journals referenced
     by scientific publications but not clinical trials

    :param df: DataFrame transformed
    :param drug: drug name
    :return: DataFrame pubmed drugs
    """

    # extract all the drug names that are mentioned in clinical trials
    clinical_drugs = df.filter(col(constants.TYPE) == "clinical")\
        .select(constants.DRUG_NAME) \
        .drop_duplicates()

    # extract all the pubmed journals that mention the given drug name in input
    journals_of_drug = df.filter((col(constants.DRUG_NAME) == drug) &
                                 (col(constants.TYPE) == "pubmed")) \
        .select(constants.JOURNAL) \
        .distinct().rdd.flatMap(lambda x: x).collect()

    # extract all the drug names that are mentioned by the previous journals
    drug_names = df.filter(col(constants.JOURNAL).isin(journals_of_drug)) \
        .select(constants.DRUG_NAME) \
        .drop_duplicates()

    # return all the extracted drug names, without the ones that are also mentioned in the clinical trial journals
    return drug_names.exceptAll(clinical_drugs)