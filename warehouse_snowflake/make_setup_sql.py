"""Fill the service user's public key into the account setup SQL.

Generate the key pair first (keys/ is git-ignored):
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out warehouse_snowflake/keys/claims_svc_rsa_key.p8 -nocrypt
    openssl rsa -in warehouse_snowflake/keys/claims_svc_rsa_key.p8 -pubout -out warehouse_snowflake/keys/claims_svc_rsa_key.pub

Then:
    python -m warehouse_snowflake.make_setup_sql
and run sql/00_account_setup.local.sql in Snowsight as ACCOUNTADMIN.
"""

from pathlib import Path

HERE = Path(__file__).parent


def main() -> None:
    pem = (HERE / "keys" / "claims_svc_rsa_key.pub").read_text()
    key = "".join(line for line in pem.splitlines() if "PUBLIC KEY" not in line)
    template = (HERE / "sql" / "00_account_setup.sql").read_text()
    out = HERE / "sql" / "00_account_setup.local.sql"
    out.write_text(template.replace("{{PUBLIC_KEY}}", key))
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
