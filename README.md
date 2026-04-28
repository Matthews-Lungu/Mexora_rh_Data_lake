# Mexora RH Intelligence — Data Lake IT Job Market Morocco

**Mini-Project 2 — Data Engineering Course**

## Project Overview
Data Lake built on 5,000 Moroccan IT job offers (Rekrute, MarocAnnonce, LinkedIn Maroc).
Bronze / Silver / Gold architecture. DuckDB analytics. Power BI dashboard.

## Stack
- Python 3.11+
- pandas, pyarrow, duckdb
- Jupyter Notebook
- Power BI Desktop

## How to Reproduce

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate the raw data
```bash
python data_generation/generate_data.py
```

### 3. Run the full pipeline
```bash
python main.py
```

### 4. Open the analysis notebook
```bash
jupyter notebook analysis/analyse_marche_it_maroc.ipynb
```

## Project Structure
```
mexora_rh_lake/
├── pipeline/          # Bronze → Silver → Gold transformations
├── analysis/          # DuckDB queries and visualizations
├── data_generation/   # Script to generate raw input data
├── data_lake/         # Data Lake root (not committed to Git)
├── main.py            # Pipeline orchestrator
└── requirements.txt
```