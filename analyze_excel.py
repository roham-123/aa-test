#!/usr/bin/env python3
import pandas as pd
import os
import sys

def analyze_excel(filepath):
    """Analyze the structure of an Excel file and print out key information"""
    print(f"\nAnalyzing Excel file: {filepath}")
    
    try:
        # List all sheets in the workbook
        xl = pd.ExcelFile(filepath)
        sheets = xl.sheet_names
        print(f"Sheets in the workbook: {sheets}")
        
        # Focus on P1 sheet
        if 'P1' in sheets:
            print("\nAnalyzing P1 sheet...")
            df = pd.read_excel(filepath, sheet_name='P1')
            
            # Basic info
            print(f"Shape: {df.shape}")
            print(f"Column names: {df.columns.tolist()}")
            
            # Print first column name specifically
            first_col = df.columns[0]
            print(f"First column name: '{first_col}'")
            
            # Look for tables and questions
            print("\nScanning for tables and questions:")
            table_rows = []
            question_rows = []
            base_rows = []
            
            for i, row in df.iterrows():
                # First column might have a different name than 'A'
                col_val = row.iloc[0]  # Get first column value
                row_str = str(col_val) if pd.notna(col_val) else ''
                
                if 'Table' in row_str:
                    table_rows.append((i, row_str))
                    print(f"Row {i}: TABLE - {row_str}")
                elif ('Q1' in row_str or 'Q2' in row_str or 'Q3' in row_str or 'QD' in row_str) and ('?' in row_str or '.' in row_str):
                    question_rows.append((i, row_str))
                    print(f"Row {i}: QUESTION - {row_str}")
                elif row_str.startswith('Base:'):
                    base_rows.append((i, row_str))
                    print(f"Row {i}: BASE - {row_str}")
            
            # Print a few sample rows
            print("\nSample rows (first 10):")
            for i in range(min(10, len(df))):
                row_dict = {str(col): str(val) if pd.notna(val) else 'NaN' for col, val in zip(df.columns, df.iloc[i])}
                print(f"Row {i}: {row_dict}")
                
            # Show some data rows (if any base rows found)
            if base_rows:
                print("\nExample data rows after base rows:")
                for base_row_idx, _ in base_rows:
                    print(f"\nData after base row {base_row_idx}:")
                    # Look at up to 5 rows after each base row
                    for i in range(base_row_idx + 1, min(base_row_idx + 6, len(df))):
                        row_dict = {str(col): str(val) if pd.notna(val) else 'NaN' for col, val in zip(df.columns, df.iloc[i])}
                        print(f"Row {i}: {row_dict}")
        else:
            print("P1 sheet not found!")
            
    except Exception as e:
        import traceback
        print(f"Error analyzing Excel file: {e}")
        print(traceback.format_exc())

def main():
    # Directory containing Excel files
    data_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Find all Excel files with AA_ prefix
    excel_files = [f for f in os.listdir(data_dir) 
                  if f.endswith('.xlsx') and f.startswith('AA_')]
    
    if not excel_files:
        print("No Excel files matching pattern 'AA_*.xlsx' found in current directory.")
        sys.exit(1)
        
    print(f"Found Excel files: {excel_files}")
    
    for file in excel_files:
        analyze_excel(os.path.join(data_dir, file))

if __name__ == "__main__":
    main() 