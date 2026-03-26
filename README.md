# Referral Data Pipeline

## Overview
This project processes referral data and detects fraud using business logic.

## Steps
1. Load CSV files
2. Clean data
3. Join tables
4. Apply business logic
5. Generate final report

## How to Run

### Local
pip install pandas
python main.py

### Docker
docker build -t referral-app .
docker run referral-app

## Output
final_report.csv (46 rows)