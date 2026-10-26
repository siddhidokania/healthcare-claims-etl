"""ICD-9-CM helpers: code normalisation, display format and chapter."""

from pyspark.sql import Column
from pyspark.sql import functions as F

# (first 3 digits from, to, chapter) for numeric diagnosis codes
CHAPTERS = [
    (1, 139, "Infectious and parasitic diseases"),
    (140, 239, "Neoplasms"),
    (240, 279, "Endocrine, nutritional, metabolic and immunity disorders"),
    (280, 289, "Diseases of the blood"),
    (290, 319, "Mental disorders"),
    (320, 389, "Nervous system and sense organs"),
    (390, 459, "Circulatory system"),
    (460, 519, "Respiratory system"),
    (520, 579, "Digestive system"),
    (580, 629, "Genitourinary system"),
    (630, 679, "Pregnancy and childbirth"),
    (680, 709, "Skin and subcutaneous tissue"),
    (710, 739, "Musculoskeletal system"),
    (740, 759, "Congenital anomalies"),
    (760, 779, "Perinatal conditions"),
    (780, 799, "Symptoms, signs and ill-defined conditions"),
    (800, 999, "Injury and poisoning"),
]
SUPPLEMENTARY = "Supplementary factors (V codes)"
EXTERNAL = "External causes of injury (E codes)"
UNCLASSIFIED = "Unclassified"


def normalize(code: Column) -> Column:
    """Upper-case, strip dots and spaces; empty strings become null."""
    cleaned = F.regexp_replace(F.upper(F.trim(code)), r"[^A-Z0-9]", "")
    return F.when(cleaned == "", None).otherwise(cleaned)


def formatted(code_col: str) -> Column:
    """Display format with the decimal point: 4280 -> 428.0, V5841 -> V58.41, E8889 -> E888.9."""
    head = f"CASE WHEN {code_col} LIKE 'E%' THEN 4 ELSE 3 END"
    return F.expr(
        f"CASE WHEN length({code_col}) > {head} "
        f"THEN concat(substring({code_col}, 1, {head}), '.', substring({code_col}, {head} + 1)) "
        f"ELSE {code_col} END"
    )


def chapter(code: Column) -> Column:
    prefix = F.substring(code, 1, 3)
    numeric = F.when(prefix.rlike(r"^[0-9]{3}$"), prefix.cast("int"))
    result = F.when(code.startswith("V"), SUPPLEMENTARY).when(code.startswith("E"), EXTERNAL)
    for low, high, name in CHAPTERS:
        result = result.when(numeric.between(low, high), name)
    return result.otherwise(UNCLASSIFIED)
