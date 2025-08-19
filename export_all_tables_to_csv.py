#!/usr/bin/env python3
"""
Script to export all database tables to CSV files.

This script connects to the MySQL database using the existing configuration
and exports all 6 tables in the aa_poll_demo schema to separate CSV files.
"""

import csv
import mysql.connector
from datetime import datetime
import logging
import sys
import os

from db_config import DB_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define all tables to export
TABLES_CONFIG = {
    'surveys': {
        'query': 'SELECT survey_id, year, month, filename, processed FROM surveys ORDER BY year DESC, month DESC, survey_id',
        'description': 'Survey metadata and processing status'
    },
    'survey_questions': {
        'query': '''SELECT question_id, survey_id, question_number, question_part, 
                           question_text, is_demographic, base_description 
                    FROM survey_questions 
                    ORDER BY survey_id, question_number, question_part''',
        'description': 'Survey questions with metadata'
    },
    'demographics': {
        'query': 'SELECT demo_id, demo_code, demo_description FROM demographics ORDER BY demo_id',
        'description': 'Demographic categories'
    },
    'demographic_responses': {
        'query': '''SELECT id, question_id, survey_id, demo_id, item_label, count, percent 
                    FROM demographic_responses 
                    ORDER BY survey_id, question_id, demo_id''',
        'description': 'Demographic response data'
    },
    'answer_options': {
        'query': '''SELECT option_id, question_id, option_text, option_order 
                    FROM answer_options 
                    ORDER BY question_id, option_order, option_id''',
        'description': 'Answer options for survey questions'
    },
    'p1_responses': {
        'query': '''SELECT id, question_id, survey_id, option_id, demo_id, item_label, cnt, pct 
                    FROM p1_responses 
                    ORDER BY survey_id, question_id, option_id, demo_id''',
        'description': 'Primary response data (main fact table)'
    }
}


def export_table_to_csv(cursor, table_name, table_config, output_dir, timestamp):
    """
    Export a single table to CSV.
    
    Args:
        cursor: Database cursor
        table_name (str): Name of the table
        table_config (dict): Configuration for the table (query, description)
        output_dir (str): Output directory for CSV files
        timestamp (str): Timestamp for filename
    
    Returns:
        tuple: (filename, record_count)
    """
    try:
        # Generate filename
        filename = os.path.join(output_dir, f"{table_name}_{timestamp}.csv")
        
        logger.info(f"Exporting {table_name}...")
        logger.info(f"  Description: {table_config['description']}")
        
        # Execute query
        cursor.execute(table_config['query'])
        
        # Fetch all results
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        logger.info(f"  Found {len(rows):,} records")
        
        # Write to CSV file
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(column_names)
            
            # Write data rows
            writer.writerows(rows)
        
        logger.info(f"  ✅ Exported to {os.path.basename(filename)}")
        return filename, len(rows)
        
    except Exception as e:
        logger.error(f"  ❌ Error exporting {table_name}: {e}")
        raise


def export_all_tables_to_csv(output_dir=None):
    """
    Export all database tables to CSV files.
    
    Args:
        output_dir (str, optional): Output directory for CSV files. 
                                   If None, creates timestamped directory.
    
    Returns:
        dict: Dictionary with export results
    """
    # Generate timestamp and output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if output_dir is None:
        output_dir = f"database_export_{timestamp}"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    conn = None
    cursor = None
    results = {}
    
    try:
        # Connect to database
        logger.info("Connecting to database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("Connected successfully")
        logger.info(f"Export directory: {os.path.abspath(output_dir)}\n")
        
        total_records = 0
        
        # Export each table
        for table_name, table_config in TABLES_CONFIG.items():
            try:
                filename, record_count = export_table_to_csv(
                    cursor, table_name, table_config, output_dir, timestamp
                )
                results[table_name] = {
                    'filename': filename,
                    'record_count': record_count,
                    'status': 'success'
                }
                total_records += record_count
                
            except Exception as e:
                results[table_name] = {
                    'filename': None,
                    'record_count': 0,
                    'status': 'failed',
                    'error': str(e)
                }
            
            print()  # Add spacing between tables
        
        # Create summary report
        summary_filename = os.path.join(output_dir, f"export_summary_{timestamp}.txt")
        create_summary_report(results, summary_filename, total_records)
        
        logger.info(f"🎉 Export completed! Total records: {total_records:,}")
        logger.info(f"📁 All files saved to: {os.path.abspath(output_dir)}")
        
        return results
        
    except mysql.connector.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
        
    finally:
        # Clean up database connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed")


def create_summary_report(results, summary_filename, total_records):
    """Create a summary report of the export process."""
    
    with open(summary_filename, 'w') as f:
        f.write("DATABASE EXPORT SUMMARY\n")
        f.write("=" * 50 + "\n")
        f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Records Exported: {total_records:,}\n\n")
        
        f.write("TABLE EXPORT DETAILS:\n")
        f.write("-" * 30 + "\n")
        
        for table_name, result in results.items():
            status = "✅ SUCCESS" if result['status'] == 'success' else "❌ FAILED"
            f.write(f"{table_name}: {status}\n")
            
            if result['status'] == 'success':
                f.write(f"  Records: {result['record_count']:,}\n")
                f.write(f"  File: {os.path.basename(result['filename'])}\n")
            else:
                f.write(f"  Error: {result.get('error', 'Unknown error')}\n")
            f.write("\n")
        
        f.write("\nFILE DESCRIPTIONS:\n")
        f.write("-" * 20 + "\n")
        for table_name, config in TABLES_CONFIG.items():
            f.write(f"{table_name}: {config['description']}\n")
    
    logger.info(f"📋 Summary report created: {os.path.basename(summary_filename)}")


def main():
    """Main function to run the export script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export all database tables to CSV')
    parser.add_argument('-o', '--output-dir', 
                       help='Output directory for CSV files (default: database_export_TIMESTAMP)',
                       default=None)
    
    args = parser.parse_args()
    
    try:
        results = export_all_tables_to_csv(args.output_dir)
        
        # Display final summary
        successful_exports = sum(1 for r in results.values() if r['status'] == 'success')
        failed_exports = len(results) - successful_exports
        
        print(f"\n{'='*60}")
        print(f"📊 EXPORT SUMMARY:")
        print(f"   Successful: {successful_exports}/{len(results)} tables")
        
        if failed_exports > 0:
            print(f"   Failed: {failed_exports} tables")
            print("   Check the log for error details")
        
        total_records = sum(r['record_count'] for r in results.values() if r['status'] == 'success')
        print(f"   Total records: {total_records:,}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\n❌ Export cancelled by user")
        sys.exit(1)


if __name__ == "__main__":
    main() 