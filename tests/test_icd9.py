import pytest
from pyspark.sql import functions as F

from claims_etl import icd9


@pytest.mark.parametrize("raw, expected", [
    (" 428.0 ", "4280"),
    ("v58.41", "V5841"),
    ("", None),
    ("   ", None),
])
def test_normalize(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "code string")
    assert df.select(icd9.normalize(F.col("code"))).first()[0] == expected


@pytest.mark.parametrize("code, display, chapter", [
    ("4280", "428.0", "Circulatory system"),
    ("25000", "250.00", "Endocrine, nutritional, metabolic and immunity disorders"),
    ("0389", "038.9", "Infectious and parasitic diseases"),
    ("401", "401", "Circulatory system"),
    ("V5841", "V58.41", icd9.SUPPLEMENTARY),
    ("E8889", "E888.9", icd9.EXTERNAL),
    ("OTHER", "OTH.ER", icd9.UNCLASSIFIED),
])
def test_format_and_chapter(spark, code, display, chapter):
    df = spark.createDataFrame([(code,)], "icd9_code string")
    row = df.select(icd9.formatted("icd9_code").alias("d"), icd9.chapter(F.col("icd9_code")).alias("c")).first()
    assert (row["d"], row["c"]) == (display, chapter)
