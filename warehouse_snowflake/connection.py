"""Snowflake connection for the CLAIMS_SVC service user (key-pair authentication).

Settings come from environment variables (.env):
    SNOWFLAKE_ACCOUNT           account identifier, e.g. abc12345.central-india.azure or orgname-accountname
    SNOWFLAKE_USER              default CLAIMS_SVC
    SNOWFLAKE_ROLE              default CLAIMS_ENGINEER
    SNOWFLAKE_WAREHOUSE         default CLAIMS_WH_XS
    SNOWFLAKE_DATABASE          default CLAIMS_DB
    SNOWFLAKE_PRIVATE_KEY_FILE  default warehouse_snowflake/keys/claims_svc_rsa_key.p8
"""

import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization

SQL_DIR = Path(__file__).parent / "sql"
WAREHOUSES = {"CLAIMS_WH_XS": 1, "CLAIMS_WH_S": 2, "CLAIMS_WH_M": 4}   # credits per hour


def connect(warehouse: str | None = None) -> snowflake.connector.SnowflakeConnection:
    key_path = Path(os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE", Path(__file__).parent / "keys" / "claims_svc_rsa_key.p8"))
    private_key = serialization.load_pem_private_key(key_path.read_bytes(), password=None).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.getenv("SNOWFLAKE_USER", "CLAIMS_SVC"),
        private_key=private_key,
        role=os.getenv("SNOWFLAKE_ROLE", "CLAIMS_ENGINEER"),
        warehouse=warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "CLAIMS_WH_XS"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CLAIMS_DB"),
        session_parameters={"QUERY_TAG": "healthcare-claims-etl"},
    )


def run_script(conn, file_name: str) -> None:
    """Run a .sql file statement by statement."""
    for cursor in conn.execute_string((SQL_DIR / file_name).read_text(), remove_comments=True):
        cursor.close()
