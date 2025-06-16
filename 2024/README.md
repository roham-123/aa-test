# AA Poll ETL Pipeline

This project provides an Extract-Transform-Load (ETL) pipeline for processing AA polling data from Excel files into a structured MySQL database.

## Database Schema

The database (`aa_poll_demo`) consists of the following tables:

- **processed_files**: Tracks which Excel files have been processed
- **surveys**: Metadata about surveys (ID, month, year)
- **questions**: Survey questions with metadata (text, type, etc.)
- **p1_responses**: Response data with demographic breakdowns

## ETL Process

The ETL pipeline:

1. Reads Excel files in the format `AA_MMMyy.xlsx` (e.g., AA_Jun24.xlsx)
2. Extracts survey metadata from filename
3. Processes the P1 sheet to extract questions and response data
4. Stores all data in the MySQL database

## Scripts

- **etl_script.py**: Main ETL script for processing Excel files
- **fix_question_text.py**: Helper script to fix question text issues
- **run_etl.sh**: Shell script to run the ETL process
- **analyze_excel.py**: Helper script to analyze Excel structure

## Usage

1. Place your Excel files (AA_MMMyy.xlsx format) in the project directory
2. Ensure MySQL is running with the correct configuration
3. Run the ETL script:
   ```
   ./run_etl.sh
   ```

## Database Configuration

The database connection is configured as follows:

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Roham137789+',
    'database': 'aa_poll_demo'
}
```

## Future Improvements

- Better handling of partial question texts
- Automatic deduplication of repeated questions
- Support for more demographic breakdowns
- PDF format support as an alternative to Excel
