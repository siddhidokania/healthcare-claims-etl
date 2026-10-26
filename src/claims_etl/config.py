"""Lake layout. The same code runs on a local path or on ADLS Gen2 (abfss://...)."""

from dataclasses import dataclass

CLAIM_SOURCES = {"inpatient_claims": "IP", "outpatient_claims": "OP"}
BRONZE_TABLES = ["beneficiary", *CLAIM_SOURCES]
LEGACY_SUMMARY = "claims_monthly_summary"

UNKNOWN_KEY = -1


@dataclass(frozen=True)
class LakeConfig:
    lake_root: str

    def _path(self, *parts: str) -> str:
        return "/".join([self.lake_root.rstrip("/"), *parts])

    def raw(self, table: str, batch_id: str) -> str:
        return self._path("raw", table, f"batch_id={batch_id}")

    def manifests(self, batch_id: str) -> str:
        return self._path("raw", "_manifests", f"batch_id={batch_id}")

    def bronze(self, table: str) -> str:
        return self._path("bronze", table)

    def silver(self, table: str) -> str:
        return self._path("silver", table)

    def gold(self, table: str) -> str:
        return self._path("gold", table)

    def ops(self, table: str) -> str:
        return self._path("ops", table)
