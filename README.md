# Medallion Lakehouse Pipeline

A production-style data pipeline built with PySpark and Delta Lake, implementing the Medallion architecture (Bronze → Silver → Gold) for insurance analytics.

## Architecture
- **Bronze**: Raw data ingestion from source, saved as Delta Lake table
- **Silver**: Data cleaning, deduplication, type casting, derived columns (age group, BMI category)
- **Gold**: Aggregated analytics tables ready for BI consumption

## Tech Stack

- PySpark 3.3
- Delta Lake 2.3
- Python 3.11
- Apache Spark (local mode)

## Gold Layer Outputs

- `charges_by_region` — Average, min and max insurance charges by region
- `charges_by_smoker_agegroup` — Charge breakdown by smoking status and age group

## How to Run

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate

# Install dependencies
pip install pyspark==3.3.2 delta-spark==2.3.0 pandas requests

# Run the pipeline
python main.py
```

## Results Sample

| Region    | Avg Charge | 
|-----------|------------|
| Southeast | £18,770    |
| Northwest | £12,082    |
| Southwest | £11,643    |
| Northeast | £10,675    |
