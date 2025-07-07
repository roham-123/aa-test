#!/bin/bash

# Install required packages if not already installed
pip_install() {
  package=$1
  python3 -c "import $package" 2>/dev/null || pip3 install $package
}

echo "Checking and installing required packages..."
pip_install pandas
pip_install mysql-connector-python
pip_install openpyxl

echo "Running ETL pipeline..."
python3 runner.py

echo "ETL process completed. Check etl_log.log for details." 