# AA Poll Data ETL Processing

This repository contains three different ETL (Extract-Transform-Load) approaches for processing AA Poll survey data:

1. Excel-based ETL (`etl_script.py`)
2. PDF-based ETL (`pdf_etl_script.py`)
3. Improved PDF-based ETL (`improved_pdf_etl_script.py`)

All scripts import survey data into the same database schema, but they use different source formats and extraction techniques.

## Database Schema

The database schema consists of the following tables:

- `surveys`: Stores metadata about each survey
- `processed_files`: Tracks which files have been processed
- `questions`: Contains survey questions
- `p1_responses`: Stores survey responses with demographic breakdowns

## Excel-based ETL (etl_script.py)

The Excel-based ETL approach:

- Processes survey data from Excel (.xlsx) files
- Navigates complex Excel structure with unnamed columns
- Maps Excel columns to database fields
- Handles special cases like decimal range limits for percentages

### Usage:

```bash
python etl_script.py
```

## PDF-based ETL (pdf_etl_script.py)

The PDF-based ETL approach:

- Processes survey data from PDF files
- Uses tabula-py and pdfplumber for table extraction
- Works with visual table layout directly from the PDF
- May provide more consistent results across different survey formats

### Dependencies:

- tabula-py
- pdfplumber

### Usage:

```bash
python pdf_etl_script.py
```

## Improved PDF-based ETL (improved_pdf_etl_script.py)

This enhanced version of the PDF-based ETL offers:

- Better question and table extraction from PDFs
- Robust survey metadata extraction from filenames
- Improved pattern recognition for questions and demographic data
- Better error handling and reporting
- Topic-based survey ID generation for better organization

### Usage:

```bash
python improved_pdf_etl_script.py
```

## PDF Analysis Tool (analyze_pdf.py)

A dedicated analysis tool for PDF files:

- Examines PDF structure and content
- Extracts tables using both pdfplumber and tabula
- Helps identify patterns in survey data layout
- Useful for debugging extraction issues

### Usage:

```bash
python analyze_pdf.py <path_to_pdf_file>
```

## Comparison

**Excel-based ETL:**

- Pros: Direct access to raw data, potentially faster
- Cons: Complex Excel structure requires careful navigation, sensitive to format changes

**PDF-based ETL:**

- Pros: Works with rendered tables as they appear visually, more resilient to structural changes
- Cons: PDF extraction can be less precise, requires Java for tabula-py

**Improved PDF-based ETL:**

- Pros: Better pattern recognition, more reliable extraction, handles multiple PDF formats
- Cons: Slower than Excel-based approach, may still miss some complex table structures

## Data Fixing Tools

The repository also contains tools to fix specific issues:

- `fix_question_text.py`: Corrects incomplete question text
- `fix_q1_base.py`: Adds missing Base rows for Q1 responses
- `analyze_q1.py`: Diagnoses issues with the Q1 data

## Maintenance

To maintain these ETL processes:

1. For new survey formats, check if extraction patterns need adjustment
2. If percentage values exceed decimal limits, all scripts handle this automatically
3. Test with small samples before running on the full dataset
4. Check logs for debugging information:
   - Excel ETL: `etl_log.log`
   - PDF ETL: `pdf_etl_log.log`
   - Improved PDF ETL: `improved_pdf_etl_log.log`

## Notes and Further Improvements

See `PDF_ETL_NOTES.md` for detailed information about the PDF extraction process, current limitations, and suggestions for future improvements.
