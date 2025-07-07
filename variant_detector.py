"""
Handles detection and management of question variants in survey data.
"""
import logging
import re
from typing import Dict, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class VariantDetector:
    
    def __init__(self):
        # Track which questions are in variant mode (have summary tables)
        self.variant_mode_questions: Dict[str, bool] = {}
        # Track the current max part number for each question number
        self.part_counters: Dict[str, int] = {}
        # For every question_number keep a part_label: question_id mapping
        self.seen_variants: Dict[str, Dict[str, int]] = {}
    
    def is_summary_table(self, question_text: str) -> bool:
        """Check if this table contains 'summary' indicating it's a summary table."""
        return 'summary' in question_text.lower()
    
    def check_for_summary_after_question(self, data: pd.DataFrame, question_row_idx: int, main_col: str) -> bool:
        """Check if there's a 'Summary' or 'Summary Table' row within the next few rows after a question."""
        for i in range(question_row_idx + 1, min(question_row_idx + 4, len(data))):
            try:
                val = self._get_cell_value(data.iloc[i].to_dict(), main_col)
                if isinstance(val, str):
                    val_lower = val.strip().lower()
                    if val_lower == 'summary' or 'summary table' in val_lower:
                        return True
            except:
                continue
        return False
    
    def detect_variant_mode(self, question_number: str, question_text: str, 
                          data: pd.DataFrame, row_idx: int, main_col: str) -> Tuple[bool, bool]:
        """
        Detect if a question should be processed in variant mode.
        
        Returns:
            Tuple of (is_variant_mode, has_summary_after)
        """
        # Check if this is a summary table that should be skipped (for 2024+ surveys with "- Summary")
        if self.is_summary_table(question_text):
            logger.info(f"Found summary table for {question_number}, marking for variant mode")
            self.variant_mode_questions[question_number] = True
            return True, False

        # For older surveys: Check if there's a "Summary" row after this question
        has_summary_after = self.check_for_summary_after_question(data, row_idx + 1, main_col)
        
        if not self.variant_mode_questions.get(question_number, False) and has_summary_after:
            logger.info(f"Found Summary row after {question_number}, marking for variant mode")
            self.variant_mode_questions[question_number] = True

        is_variant_mode = self.variant_mode_questions.get(question_number, False)
        return is_variant_mode, has_summary_after
    
    def should_skip_summary_table(self, question_number: str, has_summary_table: bool, 
                                is_variant_mode: bool) -> bool:
        """
        Determine if a summary table should be skipped entirely.
        
        Returns True if this is a repeated summary table that should be skipped.
        """
        is_new_question_number = question_number not in self.part_counters
        
        if has_summary_table and is_variant_mode and not is_new_question_number:
            logger.info(f"Skipping subsequent summary table for {question_number}")
            return True
        return False
    
    def should_process_as_stem(self, question_number: str, has_summary_table: bool, 
                             is_variant_mode: bool) -> bool:
        is_new_question_number = question_number not in self.part_counters
        
        if has_summary_table and is_variant_mode and is_new_question_number:
            # This is the first summary table - extract the stem question
            logger.info(f"Processing first summary table for {question_number} - extracting stem")
            return True
        else:
            return is_new_question_number
    
    def get_next_part_number(self, question_number: str) -> int:
        """Get the next part number for a question and update the counter."""
        current_part = self.part_counters.get(question_number, 1) + 1
        self.part_counters[question_number] = current_part
        return current_part
    
    def set_part_counter(self, question_number: str, part_number: int):
        """Set the part counter for a question."""
        self.part_counters[question_number] = part_number
    
    def is_variant_mode_question(self, question_number: str) -> bool:
        """Check if a question is in variant mode."""
        return self.variant_mode_questions.get(question_number, False)
    
    def get_variant_question_id(self, question_number: str, bullet_label: str) -> int:
        """Get existing variant question ID if it exists."""
        variant_map = self.seen_variants.get(question_number, {})
        return variant_map.get(bullet_label)
    
    def register_variant(self, question_number: str, bullet_label: str, question_id: int):
        """Register a new variant question."""
        variant_map = self.seen_variants.setdefault(question_number, {})
        variant_map[bullet_label] = question_id
    
    def _get_cell_value(self, row_dict, col_name):
        """Helper to get cell value from row dictionary."""
        return row_dict.get(col_name) if pd.notna(row_dict.get(col_name)) else None 