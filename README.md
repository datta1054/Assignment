# Kaggle → Bronze/Silver → SQLite (Notebook-first)

This repo implements a small, notebook-first data pipeline:
- Extract **latest** dataset from Kaggle via Kaggle API
- Load raw data into a **staging (bronze)** table in SQLite
- Transform into **2 dimensions** (`silver_dim_product_line`, `silver_dim_branch` SCD2) and **1 fact** table (`silver_fact_sales`)
- Run an example analytical SQL report (joins + window functions)

Start here:
- Primary notebook: [notebooks/supermarket_sales_pipeline.ipynb](notebooks/supermarket_sales_pipeline.ipynb)
- Walkthrough notebook: [notebooks/end_to_end_codebase.ipynb](notebooks/end_to_end_codebase.ipynb)
- Architecture diagram: [docs/architecture_diagram.md](docs/architecture_diagram.md)
- Cloud architecture (draft): [docs/GCP High Level Architecture Diagram.png](docs/GCP%20High%20Level%20Architecture%20Diagram.png)

## Setup
1) Create `.env` at the repo root and populate:
- `KAGGLE_USERNAME`
- `KAGGLE_KEY`

Where to get these values:
- Kaggle → **Account** → **API** → download `kaggle.json`
- Copy `username` → `KAGGLE_USERNAME`
- Copy `key` → `KAGGLE_KEY`

Note:
- Do not commit `.env`.
- You do not need to keep a `kaggle.json` file in this repo.
- Use `.env.example` as a template.

2) Install deps:
- `pip install -r requirements.txt`

3) Run:
- Open the notebook and execute cells top-to-bottom.

## Outputs
- Local SQLite DB: `./db/supermarket_sales.sqlite` (ignored by git)
- Raw extracted files: `./data/` (ignored by git)
- SQL scripts: `./sql/`

