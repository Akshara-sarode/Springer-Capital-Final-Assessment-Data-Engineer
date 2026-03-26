# Springer Capital – Data Engineer Assessment

## Overview
This project builds a data pipeline to process referral data and detect potential fraud based on defined business rules.  
The pipeline integrates multiple data sources, performs cleaning and transformations, and generates a final validated report.

---

## Features
- Data profiling (null counts and distinct counts)
- Data cleaning and transformation
- Handling one-to-many joins and removing duplicates
- Table joins across multiple datasets
- Business logic validation for referral rewards
- Final report generation with correct output (46 rows)

---

## Project Structure
- `main.py` → Main pipeline script
- `data_profiling.csv` → Profiling output for all tables
- `final_report.csv` → Final validated dataset (46 rows)
- `Dockerfile` → Containerized setup
- `data_dictionary.xlsx` → Business-friendly column definitions

---

## Data Processing Steps
1. Load all CSV data sources
2. Perform data profiling (null & distinct counts)
3. Clean and standardize data types
4. Handle missing values and duplicates
5. Join all relevant tables
6. Apply transformations:
   - Time conversions
   - String formatting
   - Source category mapping
7. Apply business logic to detect valid/invalid referrals
8. Generate final report

---

## Data Quality Handling
- Removed duplicate records caused by one-to-many joins
- Ensured one record per `referral_id`
- Standardized inconsistent string values (e.g., transaction status)
- Handled null values appropriately

---

## How to Run

### Run Locally
```bash
pip install pandas numpy
python main.py
