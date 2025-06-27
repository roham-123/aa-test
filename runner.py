from __future__ import annotations

import logging
import os
import re
import sys

import pandas as pd

import excel_utils as eu
from dao import AAPollDAO
from db_config import DB_CONFIG
from p1_processor import process_p1_sheet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("runner")

# helpers
# 
MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}

FILENAME_RE = re.compile(r"AA_([A-Za-z]+)(\d{2})(?:-[A-Za-z0-9_]+)?\.xlsx$")


def extract_survey_metadata(filename: str):
    m = FILENAME_RE.search(filename)
    if not m:
        raise ValueError(f"Invalid filename: {filename}")
    month_str, year_str = m.groups()
    month = MONTH_MAP[month_str]
    year = 2000 + int(year_str)
    survey_id = f"AA-{month:02d}{year}"
    return survey_id, month, year


def main():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "excel_files")
    dao = AAPollDAO(DB_CONFIG)

    try:
        dao.connect()

        for filename in os.listdir(data_dir):
            if "_normalized" in filename:
                continue  # skip generated files
            if not FILENAME_RE.match(filename):
                continue

            if dao.is_file_processed(filename):
                logger.info("Skipping already-processed file %s", filename)
                continue

            filepath = os.path.join(data_dir, filename)
            logger.info("Processing %s", filepath)

            survey_id, month, year = extract_survey_metadata(filename)
            dao.insert_survey(survey_id, month, year, filename)

            # preprocess and read sheet
            normalised = eu.preprocess_excel(filepath)
            p1_df = pd.read_excel(normalised, sheet_name="P1")

            # run processor
            process_p1_sheet(dao, p1_df, survey_id)

            dao.mark_file_processed(filename)
            logger.info("Finished %s", filename)

    finally:
        dao.close()


if __name__ == "__main__":
    main() 