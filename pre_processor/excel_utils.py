"""This file has functions that preprocess the excel files and create a normalised version.

1. Provide default demographic mappings
2. Preprocess Excel files for normalization
"""
from __future__ import annotations

import logging
from typing import Dict, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def default_column_mapping() -> Dict[str, Tuple[str, str, str]]:

    return {
        'Total': ('total_count', 'total_percent', 'Unnamed: 2'),
        'Male': ('male_count', 'male_percent', 'Unnamed: 3'),
        'Female': ('female_count', 'female_percent', 'Unnamed: 4'),
        '18-24': ('age_18_24_count', 'age_18_24_percent', 'Unnamed: 5'),
        '25-34': ('age_25_34_count', 'age_25_34_percent', 'Unnamed: 6'),
        '35-44': ('age_35_44_count', 'age_35_44_percent', 'Unnamed: 7'),
        '45-54': ('age_45_54_count', 'age_45_54_percent', 'Unnamed: 8'),
        '55-64': ('age_55_64_count', 'age_55_64_percent', 'Unnamed: 9'),
        '65+': ('age_65_plus_count', 'age_65_plus_percent', 'Unnamed: 10'),
        'Scotland': ('region_scotland_count', 'region_scotland_percent', 'Unnamed: 11'),
        'North East': ('region_north_east_count', 'region_north_east_percent', 'Unnamed: 12'),
        'North West': ('region_north_west_count', 'region_north_west_percent', 'Unnamed: 13'),
        'Yorkshire & Humberside': ('region_yorkshire_humberside_count', 'region_yorkshire_humberside_percent', 'Unnamed: 14'),
        'West Midlands': ('region_west_midlands_count', 'region_west_midlands_percent', 'Unnamed: 15'),
        'East Midlands': ('region_east_midlands_count', 'region_east_midlands_percent', 'Unnamed: 16'),
        'Wales': ('region_wales_count', 'region_wales_percent', 'Unnamed: 17'),
        'Eastern': ('region_eastern_count', 'region_eastern_percent', 'Unnamed: 18'),
        'London': ('region_london_count', 'region_london_percent', 'Unnamed: 19'),
        'South East': ('region_south_east_count', 'region_south_east_percent', 'Unnamed: 20'),
        'South West': ('region_south_west_count', 'region_south_west_percent', 'Unnamed: 21'),
        'Northern Ireland': ('region_northern_ireland_count', 'region_northern_ireland_percent', 'Unnamed: 22'),
        'AB': ('seg_ab_count', 'seg_ab_percent', 'Unnamed: 23'),
        'C1': ('seg_c1_count', 'seg_c1_percent', 'Unnamed: 24'),
        'C2': ('seg_c2_count', 'seg_c2_percent', 'Unnamed: 25'),
        'DE': ('seg_de_count', 'seg_de_percent', 'Unnamed: 26'),
    }


# 
# Excel preprocessing and normalisation
# 

def _normalise_column_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure column labels are unique strings (no duplicates)."""
    new_cols = []
    for i, col in enumerate(df.columns):
        new_cols.append(str(col) if col != "Unnamed: 0" else f"col_{i}")
    df.columns = new_cols
    return df


def _remove_empty_rows_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove completely empty rows and columns."""
    return df.dropna(axis=0, how='all').dropna(axis=1, how='all')


def preprocess_excel(filepath: str) -> str:
    """Load *filepath*, clean obvious structural issues, save *_normalized.xlsx*.

    If preprocessing fails for any reason we return the original path.
    """
    try:
        df = pd.read_excel(filepath, sheet_name="P1")
    except Exception as exc:
        logger.warning(f"Pre-processing skipped – could not read file: {exc}")
        return filepath

    df = _normalise_column_headers(df)
    df = _remove_empty_rows_columns(df)

    normalised_path = filepath.replace(".xlsx", "_normalized.xlsx")
    try:
        with pd.ExcelWriter(normalised_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="P1")
        logger.info(f"Saved normalised Excel to {normalised_path}")
        return normalised_path
    except Exception as exc:
        logger.warning(f"Could not save normalised Excel: {exc}")
        return filepath 