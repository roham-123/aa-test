"""
Handles extraction and parsing of questions from survey data.
"""
import logging
import re
from typing import Tuple, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class QuestionExtractor:
    """Handles parsing and creation of survey questions."""
    
    def extract_question_number_from_text(self, text: str) -> Tuple[Optional[str], bool]:
        """Extract question number (e.g., 'Q2') from question text."""
        q_match = re.search(r'\bQ\.?(\d+)([a-d])?\.?\s', text + ' ')
        qd_match = re.search(r'\bQ\.?D(\d+)\.?\s', text + ' ')
        
        if q_match:
            return f"Q{q_match.group(1)}", False
        elif qd_match:
            return f"QD{qd_match.group(1)}", True
        return None, False
    
    def extract_variant_text(self, data: pd.DataFrame, row_idx: int, 
                           question_number: str, main_col: str, current_table: str) -> str:
        """Extract meaningful variant text for a question variant."""
        variant_text = None
        search_idx = row_idx + 2  # Skip the Q row
        
        while search_idx < min(row_idx + 8, len(data)):
            search_row = data.iloc[search_idx]
            search_text = self._get_cell_value(search_row.to_dict(), main_col)
            
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
        
        return variant_text
    
    def extract_base_description(self, data: pd.DataFrame, row_idx: int, main_col: str) -> Optional[str]:
        """Extract base description from the next few rows after a question."""
        base_description = None
        for i in range(row_idx + 2, min(row_idx + 5, len(data))):
            base_text = self._get_cell_value(data.iloc[i].to_dict(), main_col)
            if isinstance(base_text, str) and base_text.startswith('Base:'):
                base_description = base_text
                break
        return base_description
    
    def create_stem_question(self, dao, survey_id: str, question_number: str, 
                           question_text: str, is_demographic: bool, 
                           base_description: Optional[str]) -> int:
        """Create the stem (part 1) question."""
        question_id = dao.insert_question(
            survey_id,
            question_number,
            1,
            question_text,
            is_demographic,
            base_description,
        )
        logger.info(f"Created stem question {question_number} part 1")
        return question_id
    
    def create_variant_question(self, dao, survey_id: str, question_number: str,
                              part_number: int, variant_text: str, 
                              is_demographic: bool, base_description: Optional[str]) -> int:
        """Create a variant question with the specified part number."""
        question_id = dao.insert_question(
            survey_id,
            question_number,
            part_number,
            variant_text,
            is_demographic,
            base_description,
        )
        logger.info(f"Created variant question {question_number} part {part_number}: {variant_text}")
        return question_id
    
    def create_bullet_variant_question(self, dao, survey_id: str, question_number: str,
                                     part_number: int, bullet_label: str, 
                                     current_base: Optional[str]) -> int:
        """Create a bullet point variant question."""
        question_id = dao.insert_question(
            survey_id,
            question_number,
            part_number,
            bullet_label,
            False,  # bullet variants are not demographic
            current_base,
        )
        logger.info(f"Created bullet variant {question_number} part {part_number}: {bullet_label}")
        return question_id
    
    def _get_cell_value(self, row_dict, col_name):
        """Helper to get cell value from row dictionary."""
        return row_dict.get(col_name) if pd.notna(row_dict.get(col_name)) else None 