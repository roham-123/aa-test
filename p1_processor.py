"""P-1 sheet extraction logic extracted from etl_script.py to keep the core ETL small.

The public entry-point is `process_p1_sheet(etl, data, survey_id)` where:
    etl  – instance of AAPollETL (provides DAO methods such as insert_question, insert_p1_fact …)
    data – pandas.DataFrame of the P1 sheet (already read by the caller)
    survey_id – AA-MMYYYY identifier

This file contains **no** direct DB calls; it delegates everything via the passed-in `etl` object, so it can
be unit-tested with a mock.
"""
from __future__ import annotations

import logging
import re
from typing import List

import pandas as pd

import excel_utils as eu  # renamed helper module

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# public API
# ----------------------------------------------------------------------

def process_p1_sheet(etl, data: pd.DataFrame, survey_id: str) -> List[int]:
    """Parse the normalised P1 sheet and write rows via *etl* DAO methods.

    Returns a list of question_ids that were successfully extracted (useful for stats / validation).
    """

    current_question = None
    current_question_part = None
    current_base = None
    question_id = None

    # Track if the stem of a question (part=1) has already been written for this survey
    seen_main_questions: dict[str, int] = {}
    # Track the current max part number for each question number so we can continue counting across tables
    part_counters: dict[str, int] = {}

    extracted_questions: List[int] = []

    main_col = 'Return to Index'

    logger.info(f"Column names in Excel: {data.columns.tolist()}")

    demo_mapping = eu.detect_column_mapping(data)
    if not demo_mapping:
        demo_mapping = eu.default_column_mapping()

    def get_cell_value(row_dict, col_name):
        return row_dict.get(col_name) if pd.notna(row_dict.get(col_name)) else None

    current_table = None
    row_index = 0

    # pre-compute list of excel columns that hold counts (helps numeric-row detection)
    demo_excel_cols = [tpl[2] for tpl in demo_mapping.values() if tpl[2] is not None]

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def is_new_block(value):
        return isinstance(value, str) and (value.startswith('Table') or value.startswith('Q'))

    # ------------------------------------------------------------------
    # main row loop
    # ------------------------------------------------------------------
    while row_index < len(data):
        row_values = data.iloc[row_index].to_dict()
        main_value = get_cell_value(row_values, main_col)

        if isinstance(main_value, str):
            if 'Table' in main_value:
                logger.info(f"Found table row: {row_index}: {main_value}")
            elif main_value.startswith('Q') and ('?' in main_value or '.' in main_value):
                logger.info(f"Found question row: {row_index}: {main_value}")
            elif main_value.startswith('Base:'):
                logger.info(f"Found base row: {row_index}: {main_value}")

        # ------- Table header -------------------------------------------------
        if isinstance(main_value, str) and 'Table' in main_value:
            current_table = main_value

            # The question is usually the next row
            if row_index + 1 >= len(data):
                row_index += 1
                continue

            question_row = data.iloc[row_index + 1]
            question_text_raw = get_cell_value(question_row, main_col)
            if not isinstance(question_text_raw, str):
                row_index += 1
                continue

            clean_question_text = question_text_raw.strip()

            q_match  = re.match(r'^Q(\d+)([a-d])?\.?', clean_question_text)
            qd_match = re.match(r'^QD(\d+)\.?', clean_question_text)

            if q_match:
                question_number = f"Q{q_match.group(1)}"
                is_demographic = False
            elif qd_match:
                question_number = f"QD{qd_match.group(1)}"
                is_demographic = True
            else:
                table_num = re.search(r'Table (\d+)', current_table)
                question_number = f"T{table_num.group(1)}" if table_num else "Unknown"
                is_demographic = False

            # concatenate with preceding prefix if the current row lacks Q/QD prefix
            if not (clean_question_text.startswith('Q') or clean_question_text.startswith('QD')):
                for lookback in range(1, 10):
                    if row_index - lookback < 0:
                        break
                    prev_text = get_cell_value(data.iloc[row_index - lookback], main_col)
                    if isinstance(prev_text, str) and (prev_text.startswith('Q') or prev_text.startswith('QD')):
                        if len(prev_text) < 150:
                            clean_question_text = prev_text.strip() + " " + clean_question_text
                            break

            # If we've already inserted the stem (part 1) for this question_number in this survey,
            # we should NOT insert it again – subsequent tables will only add variants.
            is_new_question_number = question_number not in seen_main_questions

            if is_new_question_number:
                current_question = question_number
                current_question_part = 1
            else:
                current_question = question_number
                current_question_part = part_counters[question_number]

            # locate Base description (within next 3 rows)
            base_description = None
            for i in range(row_index + 2, min(row_index + 5, len(data))):
                base_text = get_cell_value(data.iloc[i].to_dict(), main_col)
                if isinstance(base_text, str) and base_text.startswith('Base:'):
                    base_description = base_text
                    current_base = base_text
                    break

            if is_new_question_number:
                # Insert the stem only once per survey
                question_id = etl.insert_question(
                    survey_id,
                    question_number,
                    1,
                    clean_question_text,
                    is_demographic,
                    current_base or base_description,
                )
                extracted_questions.append(question_id)

                seen_main_questions[question_number] = question_id
                part_counters[question_number] = 1
            else:
                # Do not reinsert; just reuse existing stem id
                question_id = seen_main_questions[question_number]

            # ------------- Non-demographic questions → option rows -------------
            if not is_demographic:
                data_start_idx = row_index + 2  # skip question row + possible base row
                option_order = 1

                # active_question_id is the question (core or variant) we are currently populating
                active_question_id = question_id

                while data_start_idx < len(data):
                    option_row_series = data.iloc[data_start_idx]
                    option_main_val   = get_cell_value(option_row_series.to_dict(), main_col)

                    # When we encounter another Q/Table row -> break out to outer loop
                    if is_new_block(option_main_val) and option_order > 1:
                        break

                    # Determine if the row is a bullet/variant header
                    text_val = "" if option_main_val is None else str(option_main_val).strip()
                    is_bullet = text_val.startswith('-')

                    # Does this row have at least one numeric cell?
                    numeric_found = any(
                        pd.notna(pd.to_numeric(option_row_series.get(col), errors='coerce'))
                        for col in demo_excel_cols
                    )

                    # ------------------- handle bullet rows --------------------------------
                    if is_bullet:
                        bullet_label = text_val.lstrip('-').strip()

                        # Skip "- Summary" rows entirely (no question, no responses)
                        if bullet_label.lower().startswith('summary'):
                            data_start_idx += 1
                            continue  # move to next row without inserting anything

                        # create new variant question (increment part)
                        current_question_part = part_counters.get(current_question, 1) + 1
                        part_counters[current_question] = current_question_part

                        active_question_id = etl.insert_question(
                            survey_id,
                            current_question,
                            current_question_part,
                            bullet_label,
                            False,
                            current_base,
                        )
                        extracted_questions.append(active_question_id)

                        # reset option ordering for this variant
                        option_order = 1

                        # proceed to treat *this same row* as numeric row if numbers are present

                    # ------------------------------------------------------------------
                    # Rows with numeric values → treat as answer options for *active_question_id*
                    # ------------------------------------------------------------------
                    if numeric_found:
                        # Normalise option text – handle blanks / NaNs
                        if is_bullet:
                            # bullet rows themselves are not answer options; treat label as "(overall)"
                            option_text = "(overall)"
                        else:
                            if text_val.lower() in {"", "nan", "none"}:
                                option_text = "(blank)"
                            else:
                                option_text = text_val

                        option_id = etl.insert_answer_option(active_question_id, option_text, option_order)

                        for col_name, (_, _, excel_col) in demo_mapping.items():
                            count_value = option_row_series.get(excel_col)
                            if pd.isna(pd.to_numeric(count_value, errors='coerce')):
                                continue

                            demo_code = _infer_demo_code(col_name)
                            demo_id   = etl.insert_demographic(demo_code, demo_code) if demo_code else None

                            etl.insert_p1_fact(
                                active_question_id,
                                survey_id,
                                option_id,
                                demo_id,
                                col_name,
                                count_value,
                                None,
                            )

                        option_order += 1

                        data_start_idx += 1
                        continue  # finished numeric processing for this row

                    # ------------------------------------------------------------------
                    # Non-numeric & non-bullet rows → skip
                    # ------------------------------------------------------------------
                    data_start_idx += 1
                    continue

            # ------------- Demographic questions (QD) --------------------------
            else:
                demo_id = etl.insert_demographic(question_number, clean_question_text)

                data_idx = row_index + 1
                while data_idx < len(data):
                    series = data.iloc[data_idx]
                    main_val_ = get_cell_value(series.to_dict(), main_col)

                    if is_new_block(main_val_) and data_idx != row_index + 1:
                        break

                    numeric_found = any(
                        pd.notna(pd.to_numeric(series.get(col), errors='coerce'))
                        for col in demo_excel_cols
                    )
                    if not numeric_found:
                        data_idx += 1
                        continue

                    item_label_raw = "" if main_val_ is None else str(main_val_).strip()
                    item_label = item_label_raw if item_label_raw else "(blank)"

                    for col_name, (_, _, excel_col) in demo_mapping.items():
                        cnt_val = series.get(excel_col)
                        if pd.isna(pd.to_numeric(cnt_val, errors='coerce')):
                            continue
                        etl.insert_demographic_response(
                            question_id, survey_id, demo_id, item_label, cnt_val, None
                        )

                    data_idx += 1

                row_index = data_idx - 1

        # ------- QD row outside of Table context --------------------------------
        elif isinstance(main_value, str) and main_value.startswith('QD'):
            # handled similarly to original logic – demographic question row
            clean_q = main_value.strip()
            qd_match = re.match(r'^QD(\d+)', clean_q)
            if qd_match:
                question_number = f"QD{qd_match.group(1)}"
                question_id = etl.insert_question(
                    survey_id, question_number, 1, clean_q, True, None
                )
                demo_id = etl.insert_demographic(question_number, clean_q)

                data_idx = row_index + 1
                while data_idx < len(data):
                    series = data.iloc[data_idx]
                    main_val_ = get_cell_value(series.to_dict(), main_col)

                    if is_new_block(main_val_) and data_idx != row_index + 1:
                        break

                    numeric_found = any(
                        pd.notna(pd.to_numeric(series.get(col), errors='coerce'))
                        for col in demo_excel_cols
                    )
                    if not numeric_found:
                        data_idx += 1
                        continue

                    item_label_raw = "" if main_val_ is None else str(main_val_).strip()
                    item_label = item_label_raw if item_label_raw else "(blank)"

                    for col_name, (_, _, excel_col) in demo_mapping.items():
                        cnt_val = series.get(excel_col)
                        if pd.isna(pd.to_numeric(cnt_val, errors='coerce')):
                            continue
                        etl.insert_demographic_response(
                            question_id, survey_id, demo_id, item_label, cnt_val, None
                        )

                    data_idx += 1

                row_index = data_idx - 1

        row_index += 1

    return extracted_questions

# ----------------------------------------------------------------------
# internal helpers
# ----------------------------------------------------------------------

def _infer_demo_code(col_name: str) -> str | None:
    """Rough mapping from column header → QD code."""
    if col_name in {'Male', 'Female'}:
        return 'QD2'
    if col_name in {'18-24', '25-34', '35-44', '45-54', '55-64', '65+'}:
        return 'QD1'
    if col_name in {
        'Scotland', 'North East', 'North West', 'Yorkshire & Humberside', 'West Midlands',
        'East Midlands', 'Wales', 'Eastern', 'London', 'South East', 'South West', 'Northern Ireland',
    }:
        return 'QD3'
    if col_name in {'AB', 'C1', 'C2', 'DE'}:
        return 'QD4'
    return None 