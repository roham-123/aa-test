"""
This file parses the P1 sheet of the Excel file and writes the data to the database using the dao object.
Refactored to use separate modules for different responsibilities.
"""
from __future__ import annotations

import logging
import re
from typing import List

import pandas as pd

import excel_utils as eu
from variant_detector import VariantDetector
from question_extractor import QuestionExtractor
from answer_processor import AnswerProcessor

logger = logging.getLogger(__name__)


def process_p1_sheet(dao, data: pd.DataFrame, survey_id: str) -> List[int]:
    """Parses the excel sheet and returns a list of question_ids."""
    
    # Initialize processing components
    variant_detector = VariantDetector()
    question_extractor = QuestionExtractor()
    answer_processor = AnswerProcessor()
    
    # Track questions and state
    current_question = None
    current_question_part = None
    current_base = None
    question_id = None
    seen_main_questions: dict[str, int] = {}
    extracted_questions: List[int] = []

    main_col = 'Return to Index'  # name of the column that contains everything

    logger.info(f"Column names in Excel: {data.columns.tolist()}")

    demo_mapping = eu.detect_column_mapping(data)
    if not demo_mapping:
        demo_mapping = eu.default_column_mapping()

    current_table = None
    row_index = 0

    # Pre-compute list of excel columns that hold counts
    demo_excel_cols = [tpl[2] for tpl in demo_mapping.values() if tpl[2] is not None]

    def get_cell_value(row_dict, col_name):
        return row_dict.get(col_name) if pd.notna(row_dict.get(col_name)) else None

    def is_new_block(value):
        return isinstance(value, str) and (value.startswith('Table') or value.startswith('Q'))

    # Main processing loop
    while row_index < len(data):
        row_values = data.iloc[row_index].to_dict()
        main_value = get_cell_value(row_values, main_col)

        # Log significant rows
        if isinstance(main_value, str):
            if 'Table' in main_value:
                logger.info(f"Found table row: {row_index}: {main_value}")
            elif main_value.startswith('Q') and ('?' in main_value or '.' in main_value):
                logger.info(f"Found question row: {row_index}: {main_value}")
            elif main_value.startswith('Base:'):
                logger.info(f"Found base row: {row_index}: {main_value}")

        # Handle Table headers
        if isinstance(main_value, str) and 'Table' in main_value:
            row_index = _process_table_block(
                data, row_index, main_value, main_col, dao, survey_id,
                variant_detector, question_extractor, answer_processor,
                demo_mapping, demo_excel_cols, seen_main_questions, extracted_questions
            )

        # Handle standalone QD questions (outside Table context)
        elif isinstance(main_value, str) and main_value.startswith('QD'):
            row_index = _process_standalone_qd(
                data, row_index, main_value, main_col, dao, survey_id,
                answer_processor, demo_mapping, demo_excel_cols
            )

        else:
            row_index += 1

    return extracted_questions


def _process_table_block(data: pd.DataFrame, row_index: int, table_header: str, main_col: str,
                        dao, survey_id: str, variant_detector: VariantDetector, 
                        question_extractor: QuestionExtractor, answer_processor: AnswerProcessor,
                        demo_mapping: dict, demo_excel_cols: list, 
                        seen_main_questions: dict, extracted_questions: list) -> int:
    """Process a table block starting with a Table header."""
    
    current_table = table_header

    # The question is always the next row
    if row_index + 1 >= len(data):
        return row_index + 1

    question_row = data.iloc[row_index + 1]
    question_text_raw = question_row.get(main_col) if pd.notna(question_row.get(main_col)) else None
    
    if not isinstance(question_text_raw, str):
        return row_index + 1

    clean_question_text = question_text_raw.strip()

    # Extract question number
    question_number, is_demographic = question_extractor.extract_question_number_from_text(clean_question_text)
    
    if not question_number:
        logger.warning(f"Could not identify question number in table '{current_table}' with text: '{clean_question_text[:100]}...'")
        return row_index + 1

    # Detect variant mode and summary tables
    is_variant_mode, has_summary_after = variant_detector.detect_variant_mode(
        question_number, clean_question_text, data, row_index, main_col
    )

    # Check if this summary table should be skipped
    if variant_detector.should_skip_summary_table(question_number, has_summary_after, is_variant_mode):
        return row_index + 1

    # Determine if processing as stem or variant
    process_as_stem = variant_detector.should_process_as_stem(question_number, has_summary_after, is_variant_mode)

    # Extract base description
    base_description = question_extractor.extract_base_description(data, row_index, main_col)
    current_base = base_description

    # Create question record
    if process_as_stem:
        question_id = question_extractor.create_stem_question(
            dao, survey_id, question_number, clean_question_text, is_demographic, base_description
        )
        extracted_questions.append(question_id)
        seen_main_questions[question_number] = question_id
        variant_detector.set_part_counter(question_number, 1)
        
    elif is_variant_mode:
        # Create variant question
        current_question_part = variant_detector.get_next_part_number(question_number)
        variant_text = question_extractor.extract_variant_text(
            data, row_index, question_number, main_col, current_table
        )
        
        question_id = question_extractor.create_variant_question(
            dao, survey_id, question_number, current_question_part, variant_text, 
            is_demographic, base_description
        )
        extracted_questions.append(question_id)
        
    else:
        # Reuse existing stem question
        question_id = seen_main_questions[question_number]

    # Process answer options or demographic responses
    if not is_demographic:
        data_start_idx, _ = answer_processor.process_answer_options(
            dao, data, row_index + 2, question_id, demo_mapping, demo_excel_cols,
            main_col, variant_detector, question_extractor, survey_id, 
            question_number, current_base
        )
        return data_start_idx
    else:
        # Handle demographic questions
        demo_id = dao.insert_demographic(question_number, clean_question_text)
        next_idx = answer_processor.process_demographic_responses(
            dao, data, row_index + 1, question_id, demo_id, survey_id,
            demo_mapping, demo_excel_cols, main_col
        )
        return next_idx - 1


def _process_standalone_qd(data: pd.DataFrame, row_index: int, main_value: str, main_col: str,
                          dao, survey_id: str, answer_processor: AnswerProcessor,
                          demo_mapping: dict, demo_excel_cols: list) -> int:
    """Process a standalone QD question outside of table context."""
    
    clean_q = main_value.strip()
    qd_match = re.match(r'^QD(\d+)', clean_q)
    
    if not qd_match:
        return row_index + 1

    question_number = f"QD{qd_match.group(1)}"
    question_id = dao.insert_question(survey_id, question_number, 1, clean_q, True, None)
    demo_id = dao.insert_demographic(question_number, clean_q)

    next_idx = answer_processor.process_demographic_responses(
        dao, data, row_index + 1, question_id, demo_id, survey_id,
        demo_mapping, demo_excel_cols, main_col
    )
    
    return next_idx - 1 