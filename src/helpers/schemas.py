from pyspark.sql.types import StructField, StructType, StringType

import src.helpers.constants as constants

drug_schema = StructType(
    [
        StructField(constants.DRUG_ATCCODE, StringType()),
        StructField(constants.DRUG_NAME, StringType()),
    ]
)

clinical_trial_schema = StructType(
    [
        StructField(constants.ID, StringType()),
        StructField(constants.CLINICAL_TRIAL_SCIENTIFIC_TITLE, StringType()),
        StructField(constants.DATE, StringType()),
        StructField(constants.JOURNAL, StringType()),
    ]
)

pubmed_schema = StructType(
    [
        StructField(constants.ID, StringType()),
        StructField(constants.PUBMED_TITLE, StringType()),
        StructField(constants.DATE, StringType()),
        StructField(constants.JOURNAL, StringType()),
    ]
)
