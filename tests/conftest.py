from datetime import datetime, timezone

import pytest

from claims_etl.spark_session import get_spark
from onprem.load_source import BENEFICIARY_COLUMNS, INPATIENT_COLUMNS, OUTPATIENT_COLUMNS

INTEGER_COLUMNS = {"segment", "clm_utlztn_day_cnt", "coverage_year", "bene_hi_cvrage_tot_mons",
                   "bene_smi_cvrage_tot_mons", "bene_hmo_cvrage_tot_mons", "plan_cvrg_mos_num"}


@pytest.fixture(scope="session")
def spark():
    session = get_spark("claims-etl-tests", shuffle_partitions=2)
    yield session
    session.stop()


def ts(day: int) -> datetime:
    return datetime(2026, 1, day, tzinfo=timezone.utc)


def _source_df(spark, columns, rows):
    """Rows are dicts with only the interesting columns; everything else is null."""
    all_columns = [*columns, "updated_at", "_batch_id"]
    schema = ", ".join(
        f"{c} {'int' if c in INTEGER_COLUMNS else 'timestamp' if c == 'updated_at' else 'string'}"
        for c in all_columns
    )
    return spark.createDataFrame([tuple(r.get(c) for c in all_columns) for r in rows], schema)


@pytest.fixture
def inpatient_df(spark):
    return lambda rows: _source_df(spark, INPATIENT_COLUMNS, rows)


@pytest.fixture
def outpatient_df(spark):
    return lambda rows: _source_df(spark, OUTPATIENT_COLUMNS, rows)


@pytest.fixture
def beneficiary_df(spark):
    return lambda rows: _source_df(spark, BENEFICIARY_COLUMNS, rows)
