#!/bin/bash

# Check if python3 and required packages are installed
command -v python3 >/dev/null 2>&1 || { echo "Python3 is required but not installed. Aborting."; exit 1; }

# Install required packages if not already installed
pip_install() {
  package=$1
  python3 -c "import $package" 2>/dev/null || pip3 install $package
}

echo "Checking and installing required packages..."
pip_install pandas
pip_install mysql-connector-python
pip_install openpyxl

echo "Running ETL script..."
python3 etl_script.py

echo "ETL process completed. Check etl_log.log for details." 