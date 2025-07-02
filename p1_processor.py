"""
This file parses the P1 sheet of the Excel file and writes the data to the database using the dao object.
"""
from __future__ import annotations

import logging
import re
from typing import List

import pandas as pd

import excel_utils as eu  

logger = logging.getLogger(__name__)


def process_p1_sheet(dao, data: pd.DataFrame, survey_id: str) -> List[int]:
    """Parses the the excel sheet and returns a list of question_ids.
    """
    current_question = None
    current_question_part = None
    current_base = None
    question_id = None

    # Track if the stem of a question (part=1) has already been written for this survey
    seen_main_questions: dict[str, int] = {}
    # Track the current max part number for each question number so we can continue counting across tables
    part_counters: dict[str, int] = {}
    # For every question_number keep a part_label: question_id
    # we do not insert the same variant multiple times 
    seen_variants: dict[str, dict[str, int]] = {}
    # Track which questions are in variant mode (have summary tables)
    variant_mode_questions: dict[str, bool] = {}

    extracted_questions: List[int] = []

    main_col = 'Return to Index' # name of the column that contains everything; question number, text, base etc

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

    
    # helpers
    # 
    def is_new_block(value):
        return isinstance(value, str) and (value.startswith('Table') or value.startswith('Q'))

    def is_summary_table(question_text):
        """Check if this table contains 'summary' indicating it's a summary table to skip."""
        return 'summary' in question_text.lower()

    def extract_question_number_from_text(text):
        """Extract question number (e.g., 'Q2') from question text."""
        q_match = re.search(r'\bQ(\d+)([a-d])?\.?\s', text + ' ')
        qd_match = re.search(r'\bQD(\d+)\.?\s', text + ' ')
        
        if q_match:
            return f"Q{q_match.group(1)}", False
        elif qd_match:
            return f"QD{qd_match.group(1)}", True
        return None, False

    def check_for_summary_after_question(data, question_row_idx, main_col):
        """Check if there's a 'Summary' row within the next few rows after a question."""
        for i in range(question_row_idx + 1, min(question_row_idx + 4, len(data))):
            try:
                val = get_cell_value(data.iloc[i].to_dict(), main_col)
                if isinstance(val, str) and val.strip().lower() == 'summary':
                    return True
            except:
                continue
        return False

    # main loop
    # 
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

        # Table header 
        if isinstance(main_value, str) and 'Table' in main_value:
            current_table = main_value

            # The question is always the next row
            if row_index + 1 >= len(data):
                row_index += 1
                continue

            question_row = data.iloc[row_index + 1]
            question_text_raw = get_cell_value(question_row, main_col)
            if not isinstance(question_text_raw, str):
                row_index += 1
                continue

            clean_question_text = question_text_raw.strip()

            # Try to extract Q/QD number 
            question_number, is_demographic = extract_question_number_from_text(clean_question_text)
            
            if not question_number:
                # Skip this table entirely if we can't identify the question
                logger.warning(f"Could not identify question number in table '{current_table}' with text: '{clean_question_text[:100]}...'")
                row_index += 1
                continue

            # Check if this is a summary table that should be skipped (for 2024+ surveys with "- Summary")
            if is_summary_table(clean_question_text):
                logger.info(f"Found summary table for {question_number}, marking for variant mode")
                variant_mode_questions[question_number] = True
                row_index += 1
                continue

            # For older surveys: Check if there's a "Summary" row after this question
            has_summary_after = check_for_summary_after_question(data, row_index + 1, main_col)
            has_summary_table = has_summary_after
            
            if not variant_mode_questions.get(question_number, False) and has_summary_after:
                logger.info(f"Found Summary row after {question_number}, marking for variant mode")
                variant_mode_questions[question_number] = True

            # Check if we're processing variants for this question
            is_variant_mode = variant_mode_questions.get(question_number, False)
            is_new_question_number = question_number not in seen_main_questions

            # For summary tables in variant mode: extract stem/base but process data as variants
            if has_summary_table and is_variant_mode and is_new_question_number:
                # This is the first summary table - extract the stem question and base
                logger.info(f"Processing first summary table for {question_number} - extracting stem")
                process_as_stem = True
            elif has_summary_table and is_variant_mode and not is_new_question_number:
                # This is a subsequent summary table - skip it entirely (different base)
                logger.info(f"Skipping subsequent summary table for {question_number}")
                row_index += 1
                continue
            else:
                process_as_stem = is_new_question_number

            if process_as_stem:
                current_question = question_number
                current_question_part = 1
            else:
                current_question = question_number
                # For variants, increment the part counter
                if is_variant_mode:
                    current_question_part = part_counters.get(question_number, 1) + 1
                else:
                    current_question_part = part_counters[question_number]

            # locate Base description (within next 3 rows)
            base_description = None
            for i in range(row_index + 2, min(row_index + 5, len(data))):
                base_text = get_cell_value(data.iloc[i].to_dict(), main_col)
                if isinstance(base_text, str) and base_text.startswith('Base:'):
                    base_description = base_text
                    current_base = base_text
                    break

            if process_as_stem:
                # Insert the stem only once per survey
                question_id = dao.insert_question(
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
            elif is_variant_mode:
                # Create a new variant question
                current_question_part = part_counters.get(question_number, 1) + 1
                part_counters[question_number] = current_question_part
                
                # For variants, look for meaningful text in the next few rows after the Q row
                variant_text = None
                search_idx = row_index + 2  # Skip the Q row
                
                while search_idx < min(row_index + 8, len(data)):
                    search_row = data.iloc[search_idx]
                    search_text = get_cell_value(search_row.to_dict(), main_col)
                    
                    if isinstance(search_text, str) and search_text.strip():
                        search_text = search_text.strip()
                        # Skip Base rows, Summary rows, and Table rows - look for actual content
                        if (not search_text.startswith('Base:') and 
                            not search_text.startswith('Table') and 
                            search_text.lower() != 'summary' and
                            len(search_text) > 5):  # Must be meaningful content
                            variant_text = search_text
                            break
                    search_idx += 1
                
                # If no specific variant text found, use a generic description
                if not variant_text:
                    # Generate a variant name based on the table or position
                    table_num = current_table.split()[-1] if current_table else "Unknown"
                    variant_text = f"Variant {table_num}"
                
                question_id = dao.insert_question(
                    survey_id,
                    question_number,
                    current_question_part,
                    variant_text,
                    is_demographic,
                    current_base or base_description,
                )
                extracted_questions.append(question_id)
                logger.info(f"Creating variant for {question_number}: {variant_text}")
            else:
                # Do not reinsert. just reuse existing stem id
                question_id = seen_main_questions[question_number]

            if not is_demographic:
                data_start_idx = row_index + 2  # skip question row + possible base row
                option_order = 1

                # active_question_id is the question (stem or variant) we are currently populating
                active_question_id = question_id

                while data_start_idx < len(data):
                    option_row_series = data.iloc[data_start_idx]
                    option_main_val   = get_cell_value(option_row_series.to_dict(), main_col)

                    # When we encounter another Q/Table row, break out to outer loop
                    if is_new_block(option_main_val) and option_order > 1:
                        break

                    # Skip "Summary" rows entirely when processing data
                    text_val = "" if option_main_val is None else str(option_main_val).strip()
                    if text_val.lower() == 'summary':
                        logger.info(f"Skipping Summary row at data processing level")
                        data_start_idx += 1
                        continue

                    # Determine if the row is a variant by checking for "-"
                    is_bullet = text_val.startswith('-')

                    # Does this row have at least one numeric cell?
                    numeric_found = any(
                        pd.notna(pd.to_numeric(option_row_series.get(col), errors='coerce'))
                        for col in demo_excel_cols
                    )

                    # handle bullet question rows 
                    if is_bullet:
                        bullet_label = text_val.lstrip('-').strip()

                        # Skip "- Summary" rows entirely 
                        if bullet_label.lower().startswith('summary'):
                            data_start_idx += 1
                            continue  # move to next row without inserting anything

                        # Have we already created this variant earlier in
                        # this survey for the same question?  If so, re-use
                        # the existing question_id instead of inserting a
                        # duplicate.
                        variant_map = seen_variants.setdefault(current_question, {})

                        if bullet_label in variant_map:
                            active_question_id = variant_map[bullet_label]
                            # do not reset option order 
                            # options appended after the existing ones
                        else:
                            # create new variant question and increment part number
                            current_question_part = part_counters.get(current_question, 1) + 1
                            part_counters[current_question] = current_question_part

                            active_question_id = dao.insert_question(
                                survey_id,
                                current_question,
                                current_question_part,
                                bullet_label,
                                False,
                                current_base,
                            )
                            extracted_questions.append(active_question_id)

                            variant_map[bullet_label] = active_question_id

                            # reset option ordering for this brand-new variant
                            option_order = 1

                    # Rows with numeric values are treated as answer options for active_question_id
                    if numeric_found:
                        if is_bullet:
                            # bullet rows themselves are not answer options. treat label as "(overall)"
                            option_text = "(overall)"
                        else:
                            # Skip rows that are just whitespace
                            if text_val.lower() in {"", "nan", "none"} or text_val.strip() == "":
                                data_start_idx += 1
                                continue  
                            else:
                                option_text = text_val

                        option_id = dao.insert_answer_option(active_question_id, option_text, option_order)

                        for col_name, (_, _, excel_col) in demo_mapping.items():
                            count_value = option_row_series.get(excel_col)
                            if pd.isna(pd.to_numeric(count_value, errors='coerce')):
                                continue

                            demo_code = _infer_demo_code(col_name)
                            demo_id   = dao.insert_demographic(demo_code, demo_code) if demo_code else None

                            dao.insert_p1_fact(
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

                    # Non-numeric & non-bullet rows = skip
                    data_start_idx += 1
                    continue

            # Demographic questions (QD) 
            else:
                demo_id = dao.insert_demographic(question_number, clean_question_text)

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
                        dao.insert_demographic_response(
                            question_id, survey_id, demo_id, item_label, cnt_val, None
                        )

                    data_idx += 1

                row_index = data_idx - 1

        # QD row outside of Table context 
        elif isinstance(main_value, str) and main_value.startswith('QD'):
            # handled similarly to original logic 
            clean_q = main_value.strip()
            qd_match = re.match(r'^QD(\d+)', clean_q)
            if qd_match:
                question_number = f"QD{qd_match.group(1)}"
                question_id = dao.insert_question(
                    survey_id, question_number, 1, clean_q, True, None
                )
                demo_id = dao.insert_demographic(question_number, clean_q)

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
                        dao.insert_demographic_response(
                            question_id, survey_id, demo_id, item_label, cnt_val, None
                        )

                    data_idx += 1

                row_index = data_idx - 1

        row_index += 1

    return extracted_questions

# internal helpers
# 
def _infer_demo_code(col_name: str) -> str | None:
    """Rough mapping from column header to QD code."""
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