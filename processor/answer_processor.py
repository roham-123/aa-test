"""
Handles processing of answer options and demographic responses.
"""
import logging
from typing import List, Dict, Tuple, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class AnswerProcessor:
    """Handles processing of answer options and demographic data."""
    
    def process_answer_options(self, dao, data: pd.DataFrame, start_idx: int,
                             question_id: int, demo_mapping: Dict, demo_excel_cols: List[str],
                             main_col: str, variant_detector, question_extractor, 
                             survey_id: str, current_question: str, current_base: Optional[str]) -> Tuple[int, int]:
        """
        Process answer options for a question, handling bullet variants.
        
        Returns:
            Tuple of (next_row_index, active_question_id)
        """
        data_start_idx = start_idx
        option_order = 1
        active_question_id = question_id

        while data_start_idx < len(data):
            option_row_series = data.iloc[data_start_idx]
            option_main_val = self._get_cell_value(option_row_series.to_dict(), main_col)

            # When we encounter another Q/Table row, break out to outer loop
            if self._is_new_block(option_main_val) and option_order > 1:
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

            # Handle bullet question rows 
            if is_bullet:
                result = self._process_bullet_variant(
                    dao, text_val, variant_detector, question_extractor, 
                    survey_id, current_question, current_base, option_order
                )
                if result[0] is not None:  # Only update if not skipping
                    active_question_id, option_order = result
                data_start_idx += 1
                continue

            # Rows with numeric values are treated as answer options for active_question_id
            if numeric_found:
                option_order = self._process_numeric_row(
                    dao, option_row_series, text_val, is_bullet, active_question_id,
                    demo_mapping, option_order, survey_id
                )

            data_start_idx += 1

        return data_start_idx, active_question_id

    def process_demographic_responses(self, dao, data: pd.DataFrame, start_idx: int,
                                    question_id: int, demo_id: int, survey_id: str,
                                    demo_mapping: Dict, demo_excel_cols: List[str],
                                    main_col: str) -> int:
        """
        Process demographic responses for a QD question.
        
        Returns:
            Next row index to process
        """
        data_idx = start_idx
        
        while data_idx < len(data):
            series = data.iloc[data_idx]
            main_val = self._get_cell_value(series.to_dict(), main_col)

            if self._is_new_block(main_val) and data_idx != start_idx:
                break

            numeric_found = any(
                pd.notna(pd.to_numeric(series.get(col), errors='coerce'))
                for col in demo_excel_cols
            )
            if not numeric_found:
                data_idx += 1
                continue

            item_label_raw = "" if main_val is None else str(main_val).strip()
            item_label = item_label_raw if item_label_raw else "(blank)"

            for col_name, (_, _, excel_col) in demo_mapping.items():
                cnt_val = series.get(excel_col)
                if pd.isna(pd.to_numeric(cnt_val, errors='coerce')):
                    continue
                dao.insert_demographic_response(
                    question_id, survey_id, demo_id, item_label, cnt_val, None
                )

            data_idx += 1

        return data_idx

    def _process_bullet_variant(self, dao, text_val: str, variant_detector, question_extractor,
                              survey_id: str, current_question: str, current_base: Optional[str],
                              option_order: int) -> Tuple[Optional[int], int]:
        """Process a bullet point variant row."""
        bullet_label = text_val.lstrip('-').strip()

        # Skip "- Summary" rows entirely 
        if bullet_label.lower().startswith('summary'):
            return None, option_order  # Return None to indicate no active question change

        # Check if we already created this variant
        existing_question_id = variant_detector.get_variant_question_id(current_question, bullet_label)
        
        if existing_question_id:
            active_question_id = existing_question_id
            # Do not reset option order - options appended after existing ones
        else:
            # Create new variant question and increment part number
            current_question_part = variant_detector.get_next_part_number(current_question)

            active_question_id = question_extractor.create_bullet_variant_question(
                dao, survey_id, current_question, current_question_part, 
                bullet_label, current_base
            )

            variant_detector.register_variant(current_question, bullet_label, active_question_id)
            # Reset option ordering for this brand-new variant
            option_order = 1

        return active_question_id, option_order

    def _process_numeric_row(self, dao, option_row_series: pd.Series, text_val: str,
                           is_bullet: bool, active_question_id: int, demo_mapping: Dict,
                           option_order: int, survey_id: str) -> int:
        """Process a row with numeric data as an answer option."""
        if is_bullet:
            # Bullet rows themselves are not answer options. treat label as "(overall)"
            option_text = "(overall)"
        else:
            # Skip rows that are just whitespace
            if text_val.lower() in {"", "nan", "none"} or text_val.strip() == "":
                return option_order
            else:
                option_text = text_val

        option_id = dao.insert_answer_option(active_question_id, option_text, option_order)

        for col_name, (_, _, excel_col) in demo_mapping.items():
            count_value = option_row_series.get(excel_col)
            if pd.isna(pd.to_numeric(count_value, errors='coerce')):
                continue

            demo_code = self._infer_demo_code(col_name)
            demo_id = dao.insert_demographic(demo_code, demo_code) if demo_code else None

            dao.insert_p1_fact(
                active_question_id,
                survey_id,  # Use the passed survey_id parameter
                option_id,
                demo_id,
                col_name,
                count_value,
                None,
            )

        return option_order + 1

    def _is_new_block(self, value: Any) -> bool:
        """Check if a value indicates a new question/table block."""
        return isinstance(value, str) and (value.startswith('Table') or value.startswith('Q'))

    def _get_cell_value(self, row_dict, col_name):
        """Helper to get cell value from row dictionary."""
        return row_dict.get(col_name) if pd.notna(row_dict.get(col_name)) else None

    def _infer_demo_code(self, col_name: str) -> Optional[str]:
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