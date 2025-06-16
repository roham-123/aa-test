"""Data-access layer for AA Poll ETL.

This class owns the MySQL connection and provides insert / update helpers that the
sheet-processors call.  The implementation is copied from the original monolithic
etl_script.py so behaviour stays identical.
"""
from __future__ import annotations

import logging
import math
import os
import re
from datetime import datetime

import mysql.connector
import pandas as pd  # only needed for type hints in a couple of helpers

from db_config import DB_CONFIG

logger = logging.getLogger(__name__)


class AAPollDAO:
    """Thin DAO around the aa_poll_demo schema."""

    def __init__(self, db_config: dict | None = None):
        self.db_config = db_config or DB_CONFIG
        self.conn = None
        self.cursor = None

    # ------------------------------------------------------------------
    # connection helpers
    # ------------------------------------------------------------------
    def connect(self):
        self.conn = mysql.connector.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        logger.info("Connected to database")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    # ------------------------------------------------------------------
    # processed-files helper
    # ------------------------------------------------------------------
    def is_file_processed(self, filename: str) -> bool:
        self.cursor.execute("SELECT filename FROM processed_files WHERE filename=%s", (filename,))
        return self.cursor.fetchone() is not None

    def mark_file_processed(self, filename: str):
        self.cursor.execute("INSERT INTO processed_files (filename) VALUES (%s)", (filename,))
        self.conn.commit()

    # ------------------------------------------------------------------
    # survey / questions / demographics
    # ------------------------------------------------------------------
    def insert_survey(self, survey_id: str, month: int, year: int, filename: str):
        self.cursor.execute(
            """
            INSERT IGNORE INTO surveys (survey_id, month, year, filename)
            VALUES (%s, %s, %s, %s)
            """,
            (survey_id, month, year, filename),
        )
        self.conn.commit()

    def insert_question(
        self,
        survey_id: str,
        question_number: str,
        part: int,
        question_text: str,
        is_demographic: bool,
        base_description: str | None,
    ) -> int:
        self.cursor.execute(
            """
            INSERT INTO questions (survey_id, question_number, question_part, question_text, is_demographic, base_description)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                survey_id,
                question_number,
                part,
                question_text,
                is_demographic,
                base_description,
            ),
        )
        self.conn.commit()
        return self.cursor.lastrowid

    # ---------- demographics -------------------------------------------------
    def insert_demographic(self, demo_code: str, demo_description: str) -> int:
        self.cursor.execute(
            """
            INSERT INTO demographics (demo_code, demo_description)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE demo_description = VALUES(demo_description)
            """,
            (demo_code, demo_description),
        )
        self.conn.commit()
        demo_id = self.cursor.lastrowid
        if demo_id == 0:
            self.cursor.execute("SELECT demo_id FROM demographics WHERE demo_code=%s", (demo_code,))
            demo_id = self.cursor.fetchone()[0]
        return demo_id

    def insert_demographic_response(
        self,
        question_id: int,
        survey_id: str,
        demo_id: int,
        item_label: str,
        count: float | int | None,
        percent: float | int | None,
    ):
        def clean_num(v):
            if v is None:
                return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v

        self.cursor.execute(
            """
            INSERT INTO demographic_responses (question_id, survey_id, demo_id, item_label, count, percent)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                question_id,
                survey_id,
                demo_id,
                item_label,
                clean_num(count),
                clean_num(percent),
            ),
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # answer options + P1 fact rows
    # ------------------------------------------------------------------
    def insert_answer_option(self, question_id: int, option_text: str, option_order: int | None = None) -> int:
        # try lookup first
        self.cursor.execute(
            "SELECT option_id FROM answer_options WHERE question_id=%s AND option_text=%s",
            (question_id, option_text),
        )
        row = self.cursor.fetchone()
        if row:
            return row[0]

        self.cursor.execute(
            """
            INSERT INTO answer_options (question_id, option_text, option_order)
            VALUES (%s, %s, %s)
            """,
            (question_id, option_text, option_order),
        )
        self.conn.commit()
        return self.cursor.lastrowid

    def insert_p1_fact(
        self,
        question_id: int,
        survey_id: str,
        option_id: int,
        demo_id: int | None,
        item_label: str,
        count_value,
        percent_value=None,
    ):
        def clean(v):
            if v is None:
                return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            return v

        self.cursor.execute(
            """
            INSERT INTO p1_responses
            (question_id, survey_id, option_id, demo_id, item_label, cnt, pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                question_id,
                survey_id,
                option_id,
                demo_id,
                item_label,
                clean(count_value),
                clean(percent_value),
            ),
        )
        self.conn.commit() 