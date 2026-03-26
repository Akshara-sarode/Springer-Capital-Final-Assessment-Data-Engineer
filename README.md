# Springer Capital – Data Engineer Assessment

## Overview
This project builds a data pipeline to process referral data and detect potential fraud based on business rules.

## Features
- Data profiling (null counts, distinct counts)
- Data cleaning and transformation
- Table joins across multiple datasets
- Business logic validation for referral rewards
- Final report generation (46 rows)

## Project Structure
- main.py → pipeline script
- data_profiling.csv → profiling output
- final_report.csv → final result
- Dockerfile → containerized setup

## How to Run

### Local
pip install pandas numpy
python main.py

### Docker
docker build -t referral-app .

docker run -v $(pwd):/app referral-app

## Output
- final_report.csv (46 rows)
- data_profiling.csv

## Notes
- Handled duplicate joins using deduplication strategy
- Ensured 1-to-1 mapping for referral records
