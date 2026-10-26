"""Connection helper for the on-prem PostgreSQL source."""

import os

import psycopg


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "claims_onprem"),
        user=os.getenv("POSTGRES_USER", "claims"),
        password=os.getenv("POSTGRES_PASSWORD", "change_me"),
    )
