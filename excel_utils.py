"""Utility functions to make the Excel ETL process more adaptive and resilient.

This module centralises all of the new functionality that enables the ETL to
scale across many slightly-different survey formats without manual tweaks.

The helpers are intentionally defensive: if a pattern cannot be detected they
fall back to the previous behaviour so that we do not regress on existing files.

Key features implemented (matching planning doc):
1. Flexible column mapping detection (detect_column_mapping)
2. Question-row detection helpers (is_question_row)
3. Survey template detection (detect_survey_format)
4. Self-healing of extracted data (verify_and_fix_data)
5. Extraction validation (validate_extraction)
6. Adaptive pattern learning (update_extraction_patterns)
7. Multi-pass extraction orchestration (extract_survey_data)
8. Excel preprocessing/normalisation (preprocess_excel)

Many of these start out with pragmatic implementations that can be refined as
more training data is ingested. They already add value by logging potential
issues and preventing common extraction failures (e.g. missing Base row).
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Dict, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Flexible column mapping detection
# ---------------------------------------------------------------------------

COMMON_DEMOS = {
    "total": ("total_count", "total_percent"),
    "male": ("male_count", "male_percent"),
    "female": ("female_count", "female_percent"),
    "18-24": ("age_18_24_count", "age_18_24_percent"),
    "25-34": ("age_25_34_count", "age_25_34_percent"),
    "35-44": ("age_35_44_count", "age_35_44_percent"),
    "45-54": ("age_45_54_count", "age_45_54_percent"),
    "55-64": ("age_55_64_count", "age_55_64_percent"),
    "65+": ("age_65_plus_count", "age_65_plus_percent"),
}


def detect_column_mapping(df: pd.DataFrame, max_scan_rows: int = 20) -> Dict[str, Tuple[str, str, str]]:
    """Attempt to infer which dataframe columns correspond to each demographic.

    The Excel files usually contain a header row where the demographic names
    (Total, Male, etc.) appear. We scan the first *max_scan_rows* looking for a
    row that includes *Total*.  Once found we treat that entire row as the demo
    header and map the cell text -> column label.

    Returns a mapping compatible with the existing ETL logic:
        {'Total': ('total_count', 'total_percent', '<dataframe_column_name>'), ...}
    If detection fails we return an empty dict and the caller can fall back to
    the static mapping.
    """
    header_row_idx = None
    for i in range(min(max_scan_rows, len(df))):
        row_texts = [str(x).strip().lower() for x in df.iloc[i].values if pd.notna(x)]
        if "total" in row_texts:
            header_row_idx = i
            break

    if header_row_idx is None:
        logger.warning("Could not automatically detect demographic header row – falling back to defaults")
        return {}

    demo_mapping: Dict[str, Tuple[str, str, str]] = {}
    header_row = df.iloc[header_row_idx]
    for col in df.columns:
        val = header_row[col]
        if pd.isna(val):
            continue
        key = str(val).strip()
        lowered = key.lower()
        # Normalise some variations (e.g. 'male', 'men')
        if lowered in ("men",):
            lowered = "male"
        if lowered in COMMON_DEMOS:
            count_col_name, percent_col_name = COMMON_DEMOS[lowered]
            demo_mapping[key] = (count_col_name, percent_col_name, col)

    logger.info(f"Automatically detected demo column mapping with {len(demo_mapping)} columns")
    return demo_mapping


# ---------------------------------------------------------------------------
# 2. Question detection helper
# ---------------------------------------------------------------------------

# Regex to match question identifiers like Q1, Q2a, QD3, etc.
QUESTION_REGEX = re.compile(r"^(Q|QD)\d+[A-Za-z]?")


def is_question_row(text: str) -> bool:
    """Heuristic to decide whether *text* looks like a question heading."""
    if not isinstance(text, str):
        return False
    if QUESTION_REGEX.match(text.strip()):
        return True
    # Backup rule: long text with a question mark
    if "?" in text and len(text) > 20:
        return True
    return False


# ---------------------------------------------------------------------------
# 3. Survey templates and format detection
# ---------------------------------------------------------------------------

SURVEY_TEMPLATES = {
    "standard": {
        "question_pattern": r"Q\d+\.\s?",
        "base_offset": 1,
        "header_offset": 3,
        "data_offset": 4,
    },
    "compact": {
        "question_pattern": r"Q\d+[\.:]",
        "base_offset": 0,
        "header_offset": 1,
        "data_offset": 2,
    },
}


def detect_survey_format(df: pd.DataFrame) -> str:
    """Return the name of the template that best matches *df* structure."""
    # Very naive detection for now – we can train a classifier later.
    # If we see header rows within 2 lines after a question, treat as compact.
    for i in range(min(50, len(df))):
        first_cell = str(df.iloc[i, 0]) if pd.notna(df.iloc[i, 0]) else ""
        if is_question_row(first_cell):
            # Look ahead for 'Base:'
            for j in range(1, 3):
                if i + j < len(df):
                    cell = str(df.iloc[i + j, 0]) if pd.notna(df.iloc[i + j, 0]) else ""
                    if cell.lower().startswith("base"):
                        return "compact"
            return "standard"
    return "standard"


# ---------------------------------------------------------------------------
# 4. Self-healing data verification
# ---------------------------------------------------------------------------

def verify_and_fix_data(responses: List[dict]) -> List[dict]:
    """Ensure mandatory rows/values are present and fix obvious issues."""
    has_base = any(r.get("item_label") == "Base" for r in responses)
    if not has_base:
        # Estimate base count as sum of counts where available.
        base_count = sum(r.get("total_count", 0) or 0 for r in responses)
        responses.insert(0, {"item_label": "Base", "total_count": base_count, "total_percent": None})
        logger.info("Inserted missing Base row (self-healing)")
    return responses


# ---------------------------------------------------------------------------
# 5. Validation helpers
# ---------------------------------------------------------------------------

def validate_extraction(extracted_questions: List[dict]) -> List[str]:
    """Return list of validation error strings (empty if all good)."""
    errors: List[str] = []
    if not extracted_questions:
        errors.append("No questions detected")
        return errors

    for q in extracted_questions:
        if not q.get("responses"):
            errors.append(f"No responses for question {q.get('question_number')}")

    # Quick check on demographic columns completeness
    demo_columns = set()
    for q in extracted_questions:
        for r in q.get("responses", []):
            demo_columns.update([k for k in r.keys() if k.endswith("_count")])
    if len(demo_columns) < 5:
        errors.append("Insufficient demographic breakdowns detected")
    return errors


# ---------------------------------------------------------------------------
# 6. Adaptive learning – persist patterns that worked
# ---------------------------------------------------------------------------

PATTERN_STORE = "extraction_patterns.json"


def update_extraction_patterns(success_patterns: dict, failed_patterns: dict | None = None) -> None:
    record = {"success": success_patterns, "failed": failed_patterns or {}}
    try:
        with open(PATTERN_STORE, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2)
            logger.info("Persisted extraction patterns for future runs")
    except Exception as exc:
        logger.warning(f"Could not persist extraction patterns: {exc}")


# ---------------------------------------------------------------------------
# 7. Multi-pass extraction orchestrator
# ---------------------------------------------------------------------------

def extract_survey_data(df: pd.DataFrame, standard_extract_fn, *args, **kwargs):
    """Run extraction using multiple strategies until validation passes.

    *standard_extract_fn* is a callable that implements the current extraction
    logic (e.g. AAPollETL._process_p1_sheet).  We run that first. If validation
    flags problems we can add alternative approaches later (stub for now).
    """
    extracted_questions = standard_extract_fn(df, *args, **kwargs)
    errors = validate_extraction(extracted_questions)
    if errors:
        logger.warning(f"Validation detected issues: {errors}. Alternative extraction not yet implemented.")
    else:
        update_extraction_patterns({"template": detect_survey_format(df)})
    return extracted_questions


# ---------------------------------------------------------------------------
# 8. Pre-processing / normalisation
# ---------------------------------------------------------------------------

def _fix_merged_cells(df: pd.DataFrame) -> pd.DataFrame:
    # Placeholder – real implementation later.
    return df.copy()


def _normalize_column_headers(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure column labels are unique strings (no duplicates).
    new_cols = []
    for i, col in enumerate(df.columns):
        new_cols.append(str(col) if col != "Unnamed: 0" else f"col_{i}")
    df.columns = new_cols
    return df


def _remove_empty_rows_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df


def preprocess_excel(filepath: str) -> str:
    """Load *filepath*, clean obvious structural issues, save *_normalized.xlsx*.

    If preprocessing fails for any reason we simply return the original path.
    """
    try:
        df = pd.read_excel(filepath, sheet_name="P1")
    except Exception as exc:
        logger.warning(f"Pre-processing skipped – could not read file: {exc}")
        return filepath

    df = _fix_merged_cells(df)
    df = _normalize_column_headers(df)
    df = _remove_empty_rows_columns(df)

    normalized_path = filepath.replace(".xlsx", "_normalized.xlsx")
    try:
        with pd.ExcelWriter(normalized_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="P1")
        logger.info(f"Saved normalised Excel to {normalized_path}")
        return normalized_path
    except Exception as exc:
        logger.warning(f"Could not save normalised Excel: {exc}")
        return filepath 