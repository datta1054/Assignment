# Supermarket Sales (Kaggle → Bronze/Silver → SQLite)

## 1) What this repository is

This repository implements a small, local data pipeline that:

- Downloads the **latest** “Supermarket Sales” dataset from Kaggle.
- Stores the raw rows in a **Bronze** (staging) table in SQLite.
- Builds a simple **Silver** model:
  - 2 dimensions: `silver_dim_product_line` and `silver_dim_branch` (SCD Type 2)
  - 1 fact: `silver_fact_sales`
- Runs an example **analytical report** query (joins + window functions).
- Runs **data quality / validation checks** to make sure the DB is usable.

The pipeline is implemented as Python modules under `src/` and is typically executed from the notebooks.

---

## 2) Quick start (how to run)

### 2.1 Prerequisites

- Python installed
- Kaggle API credentials available as environment variables

### 2.2 Required `.env`

Create a `.env` file in the repo root and set:

- `KAGGLE_USERNAME=...`
- `KAGGLE_KEY=...`


### 2.3 Install dependencies

Run from repo root:

- `pip install -r requirements.txt`

### 2.4 Run the pipeline

Recommended (notebook-first):
- Open [notebooks/supermarket_sales_pipeline.ipynb](../notebooks/supermarket_sales_pipeline.ipynb) and run top-to-bottom.

Alternative (script/module):
- Run the runner as a module from repo root:
  - `python -m src.runner`

Why `-m` matters:
- `src/runner.py` uses package-relative imports like `from .config import ...`, which require running it as a module.

### 2.5 Outputs (what gets created)

- Raw extracted dataset files: `data/raw/`
- SQLite database: `db/supermarket_sales.sqlite` (default)

---

## 3) Repository layout (what each folder is for)

- `src/`: the pipeline “library” (extract, transform, load, validate)
- `notebooks/`: notebook orchestrators / walkthroughs
- `sql/`: SQL assets
  - `00sqlite_ddl.sql`: DDL reference (same structure as `src/schema_sql.py`)
  - Numbered analysis queries (examples):
    - `03.KPI Dashboard (5 Tiles).sql`
    - `11.Running Revenue by Branch (Daily).sql`
    - `14.Top 3 Product Lines per Branch (Revenue Rank).sql`
- `docs/`: architecture diagram(s) and documentation
- `data/`: runtime data outputs (raw files)
- `db/`: runtime database outputs (SQLite)

---

## 4) End-to-end pipeline flow (simple step-by-step)

The main orchestration entrypoint is:

- `src.runner.run_pipeline()`

At a high level:

1) **Load settings** (paths, dataset name, log level)
2) **Configure logging**
3) **Extract** latest dataset from Kaggle to `data/raw/`
4) **Choose CSV** (largest CSV found)
5) **Create SQLite DB + tables** (DDL)
6) **Read + normalize CSV** into a stable schema
7) **Load Bronze** (`bronze_sales_raw`)
8) **Build/Update dimensions**
9) **Load Fact** (`silver_fact_sales`) in an idempotent way
10) **Commit** and **validate** the resulting database

A helpful visual is in: [docs/architecture_diagram.md](architecture_diagram.md)

---

## 5) Data model (Bronze/Silver)

### 5.1 Bronze table: `bronze_sales_raw`

Purpose:
- Store landed data with **minimal changes**.

What the pipeline adds/standardizes:
- Normalized column names (snake_case)
- Parsed `date` into `YYYY-MM-DD`
- A deterministic `row_hash`
- `extracted_at` timestamp (when we loaded into bronze)

Important behavior:
- The runner recreates the tables each run (it drops `bronze_sales_raw` before creating it).

### 5.2 Silver dimensions

#### `silver_dim_product_line` (Type 1)

- One row per product line.
- No history tracking.
- Inserts new product lines if they appear in Bronze.

#### `silver_dim_branch` (SCD Type 2)

- Natural key: `branch_code` (source column `Branch`)
- Tracked attribute: `city`

If the city changes for a branch:
- Expire the current row (`valid_to` set, `is_current = 0`)
- Insert a new current row (`valid_from` set, `is_current = 1`)

### 5.3 Silver fact: `silver_fact_sales`

Grain:
- One row per transaction-like record from the source (based on Bronze rows).

Keys:
- Uses foreign keys into dimensions:
  - `product_line_key` → `silver_dim_product_line`
  - `branch_key` → `silver_dim_branch`

Idempotency:
- The fact table has `row_hash` with a `UNIQUE` constraint.
- Inserts use `INSERT OR IGNORE` so reruns do not duplicate rows.

---

## 6) Configuration (env vars you can set)

### 6.1 Pipeline settings

Loaded by `src.config.load_settings()`:

- `KAGGLE_DATASET`
  - Default: `faresashraf1001/supermarket-sales`
- `DATA_DIR`
  - Default: `./data`
- `SQLITE_DB_PATH`
  - Default: `./db/supermarket_sales.sqlite`
- `LOG_LEVEL`
  - Default: `INFO`

### 6.2 Data quality / validation settings

Used by `src.validate.validate_sqlite_db()`:

- `DQ_FAIL_ON_WARNINGS` (default `false`)
  - If true, warnings cause the pipeline to fail.
- `DQ_MIN_FACT_COVERAGE` (default `0.98`)
  - Minimum acceptable ratio of eligible bronze rows that make it into the fact table.
---

## 7) Module-by-module and function-by-function documentation

This section explains what each file does and what each function is responsible for.

### 7.1 [src/runner.py](../src/runner.py) — pipeline orchestrator

#### `run_pipeline() -> None`

Purpose:
- Runs the entire pipeline end-to-end.

Flow (in order):
1) `settings = load_settings()`
2) `configure_logging(settings.log_level)`
3) Determine `raw_dir = settings.data_dir / "raw"`
4) `extract_latest_dataset(dataset=settings.kaggle_dataset, output_dir=raw_dir)`
5) `csv_path = find_first_csv(extracted_dir)`
6) `conn = db.connect(settings.sqlite_db_path)`
7) `db.execute_script(conn, DDL_SQLITE)` (creates all tables)
8) `frames = read_raw_csv(csv_path)`
9) `load_staging(conn, frames)`
10) `ensure_dim_product_line(conn)`
11) `scd2_upsert_dim_branch(conn)`
12) `load_fact_sales(conn)`
13) `conn.commit()`
14) `validate_sqlite_db(settings.sqlite_db_path)`

Things to be careful about:
- This runner recreates the schema each time (drops and creates Bronze; drops legacy tables).
- If Kaggle auth fails, the run fails early.
- If the fact load skips too many rows due to missing dimension keys, validation may fail due to low coverage.

---

### 7.2 [src/config.py](../src/config.py) — settings and path resolution

#### `Settings` (dataclass)

Fields:
- `kaggle_dataset: str`
- `data_dir: Path`
- `sqlite_db_path: Path`
- `log_level: str`

#### `load_settings() -> Settings`

Purpose:
- Loads configuration from environment (and optionally `.env`).

Behavior:
- Calls `dotenv.load_dotenv(override=False)` so values from the real environment win.
- Calculates the repo root as the parent of `src/`.
- Resolves `DATA_DIR` and `SQLITE_DB_PATH`:
  - If you pass a relative path, it is interpreted relative to repo root.
  - If you pass an absolute path, it is used as-is.

---

### 7.3 [src/logging_utils.py](../src/logging_utils.py) — logging setup

#### `configure_logging(level: str) -> None`

Purpose:
- Sets Python logging defaults.

Behavior:
- Uses `logging.basicConfig()` with a consistent timestamped format.
- If an unknown log level is provided, falls back to `INFO`.

---

### 7.4 [src/extract.py](../src/extract.py) — Kaggle download and file selection

This module is responsible for:
- Ensuring Kaggle credentials exist
- Creating a temporary Kaggle config folder for the Kaggle API client
- Downloading and unzipping the dataset
- Picking which CSV to use

#### `_ensure_kaggle_env_credentials_present() -> None`

Purpose:
- Fail fast if `KAGGLE_USERNAME` or `KAGGLE_KEY` is missing.

Why:
- Without this, the Kaggle API client will fail later with less clear errors.

#### `_temporary_kaggle_config_dir() -> Iterator[Path]` (context manager)

Purpose:
- Creates a temporary directory that contains a generated `kaggle.json` for this run.

Why:
- The Kaggle API looks for credentials via a config directory.
- This approach avoids committing/storing a real `kaggle.json` in the repo.

Important behavior:
- Temporarily sets `KAGGLE_CONFIG_DIR` and restores it afterwards.

#### `extract_latest_dataset(*, dataset: str, output_dir: Path) -> Path`

Purpose:
- Uses Kaggle API to download and unzip the dataset into `output_dir`.

Key steps:
- Ensures `output_dir` exists.
- Authenticates using `KaggleApi().authenticate()`.
- Calls `dataset_download_files(..., unzip=True)`.

Output:
- Returns `output_dir`.

#### `find_first_csv(extracted_dir: Path) -> Path`

Purpose:
- Locates CSV files under the extracted dataset directory and selects one.

Selection logic:
- Finds all `*.csv` recursively.
- Chooses the **largest** by file size.

Why:
- Kaggle datasets can contain multiple CSVs; choosing the largest is a simple “best guess”.

---

### 7.5 [src/db.py](../src/db.py) — SQLite helper wrapper

This module intentionally stays small and straightforward.

#### `connect(db_path: Path) -> sqlite3.Connection`

Purpose:
- Creates the DB folder if needed and opens a SQLite connection.

Important behavior:
- Enables FK enforcement: `PRAGMA foreign_keys = ON`.

#### `execute_script(conn, sql: str) -> None`

Purpose:
- Executes multi-statement SQL via `conn.executescript()`.

Typical use:
- Running the full DDL string.

#### `executemany(conn, sql: str, rows: Iterable[tuple]) -> None`

Purpose:
- Batch insert/update many rows.

#### `fetch_all(conn, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]`

Purpose:
- Convenience method to run a query and return all results.

---

### 7.6 [src/schema_sql.py](../src/schema_sql.py) — DDL (table definitions)

#### `DDL_SQLITE: str`

Purpose:
- A single SQL string that defines the pipeline tables.

What it does:
- Drops older legacy tables if present.
- Drops and recreates `bronze_sales_raw`.
- Creates `silver_dim_product_line`, `silver_dim_branch`, and `silver_fact_sales`.

Note:
- A similar DDL is also in `sql/00sqlite_ddl.sql` for reference.

---

### 7.7 [src/transform_load.py](../src/transform_load.py) — normalize, stage, dimension builds, fact load

This module contains most of the “data work”. It:
- Reads CSV via pandas
- Normalizes schema
- Loads Bronze
- Builds dimensions
- Loads facts idempotently

#### `NormalizedFrames` (dataclass)

- `raw: pd.DataFrame`

This is a small wrapper to keep the API extensible (you could add more frames later).

#### `utc_now_iso() -> str`

Purpose:
- Returns the current UTC timestamp as ISO string with no microseconds.

Used for:
- `extracted_at`, `created_at`, `valid_from`, `loaded_at` timestamps.

#### `_normalize_columns(df: pd.DataFrame) -> pd.DataFrame`

Purpose:
- Renames columns from the source dataset into stable snake_case column names.

Important mapping examples:
- `Product line` → `product_line`
- `Tax 5%` → `tax_5_percent`
- Supports variation: `Sales` is treated as `total` (same as `Total`).

Behavior:
- Warns if expected columns are missing.

#### `_parse_date_iso(date_series: pd.Series) -> pd.Series`

Purpose:
- Converts source date values to ISO date strings (`YYYY-MM-DD`).

Behavior:
- Uses `pd.to_datetime(..., errors="coerce")` so bad values become null.

#### `_row_hash(row: pd.Series) -> str`

Purpose:
- Creates a deterministic SHA-256 hash used as a stable identifier.

Inputs used:
- `invoice_id`, `branch`, `product_line`, `date`, `time`, `total`

Why it matters:
- This enables idempotent loads: the same logical row produces the same `row_hash`.

#### `read_raw_csv(csv_path: Path) -> NormalizedFrames`

Purpose:
- Reads the raw CSV and returns a normalized DataFrame.

Key steps:
- `pd.read_csv()`
- `_normalize_columns()`
- Parse `date` if present
- Compute `row_hash`
- Coerce numeric fields (`unit_price`, `total`, `rating`, etc.)
- Coerce `quantity` into nullable integer type (`Int64`)

Output:
- Returns `NormalizedFrames(raw=df)`.

#### `load_staging(conn, frames: NormalizedFrames) -> None`

Purpose:
- Loads the normalized DataFrame into `bronze_sales_raw`.

Behavior:
- Adds `extracted_at` as a single timestamp value for all rows in the load.
- Inserts every row into the Bronze table.

Things to be careful about:
- Bronze uses `row_hash` as the primary key. If duplicates exist in the incoming data, inserts will fail.

#### `ensure_dim_product_line(conn) -> None`

Purpose:
- Type 1 dimension loader for product line.

Behavior:
- Reads existing `product_line_name` values.
- Finds distinct `product_line` values in Bronze.
- Inserts missing ones with `created_at` timestamp.

#### `scd2_upsert_dim_branch(conn) -> None`

Purpose:
- SCD Type 2 loader for the branch dimension.

Logic:
- For each distinct `(branch_code, city)` in Bronze:
  - If no current record exists for the branch, insert a current row.
  - If a current record exists and the city is unchanged, do nothing.
  - If a current record exists and the city changed:
    - expire the current row
    - insert a new current row

#### `_lookup_product_line_keys(conn) -> dict[str, int]`

Purpose:
- Builds a dictionary of `product_line_name -> product_line_key` from the dimension.

#### `_lookup_current_branch_keys(conn) -> dict[str, int]`

Purpose:
- Builds a dictionary of `branch_code -> branch_key` for only current SCD2 rows.

#### `load_fact_sales(conn) -> None`

Purpose:
- Loads the fact table `silver_fact_sales`.

Behavior:
- Reads eligible Bronze rows (`date IS NOT NULL`).
- Looks up dimension keys.
- Skips rows when required dimension keys are missing.
- Inserts into fact with `INSERT OR IGNORE` to keep it idempotent.

Important detail:
- Facts use the **current** branch dimension record at load time.
  - In a more advanced warehouse, you might instead link facts to the correct historical dimension record based on the transaction date. This repo keeps it simple.

---

### 7.8 [src/validate.py](../src/validate.py) — validation and data quality checks

This module runs “lightweight data quality checks” after the pipeline completes.

#### `env_bool(name: str, default: bool) -> bool`

Purpose:
- Reads a boolean from environment using common truthy strings (`1`, `true`, `yes`, etc.).

#### `env_float(name: str, default: float) -> float`

Purpose:
- Reads a float from environment; logs a warning and uses default if invalid.

#### `validate_sqlite_db(db_path: Path) -> None`

Purpose:
- Validates that the SQLite DB is present, populated, and consistent.

Main checks performed:
- DB file exists
- Expected tables exist
- Non-zero row counts for Bronze and Fact
- Fact uniqueness (`row_hash` must be unique)
- Fact coverage check (expected eligible Bronze rows vs actual fact rows)
- Date formatting (`txn_date` should look like `YYYY-MM-DD`)
- SCD2 integrity:
  - at least one current row
  - exactly one current row per branch_code
- Null checks for critical fact columns
- Referential integrity checks (fact foreign keys match dimensions)
- Basic numeric sanity checks (no negative money, no non-positive quantity)
- Rating range check ([0,10] by default)

Failure behavior:
- Some issues are warnings; others are errors.
- If `DQ_FAIL_ON_WARNINGS=true`, warnings also fail the run.

---

### 7.9 [src/__init__.py](../src/__init__.py)

Purpose:
- Marks `src` as a package.
- Contains a short comment indicating it is the canonical pipeline source package.

---

## 8) SQL assets (reporting + DDL)

### 8.1 SQLite DDL

- [sql/00sqlite_ddl.sql](../sql/00sqlite_ddl.sql)

Note:
- The runner uses the embedded DDL in `src/schema_sql.py` (`DDL_SQLITE`).
- The SQL file is mainly for review and reference.

### 8.2 Example analysis queries

This repository includes multiple SQL files under `sql/` that demonstrate:
- joins to dimensions for readable attributes
- aggregations
- window functions (rankings, running totals, MoM)

Examples:
- [sql/11.Running Revenue by Branch (Daily).sql](../sql/11.Running%20Revenue%20by%20Branch%20(Daily).sql)
- [sql/13.Month-over-month revenue by branch.sql](../sql/13.Month-over-month%20revenue%20by%20branch.sql)
- [sql/14.Top 3 Product Lines per Branch (Revenue Rank).sql](../sql/14.Top%203%20Product%20Lines%20per%20Branch%20(Revenue%20Rank).sql)

---

## 9) Notebooks (how the repo is meant to be used)

### 9.1 [notebooks/supermarket_sales_pipeline.ipynb](../notebooks/supermarket_sales_pipeline.ipynb)

This is the “main” notebook experience:
- Loads settings
- Runs `src.runner.run_pipeline()`
- Runs quick row-count checks
- Executes the report SQL and displays results

### 9.2 [notebooks/end_to_end_codebase.ipynb](../notebooks/end_to_end_codebase.ipynb)

This is a step-by-step walkthrough:
- Explains each stage (extract → normalize → model → validate)
- Runs the stages explicitly

Function defined in this notebook:

#### `find_repo_root(start: Path) -> Path`

Purpose:
- Makes notebook imports reliable by locating the repo root and adding it to `sys.path`.

How it works:
- Walks upward from the current directory looking for markers:
  - a `src/` folder and a `requirements.txt` file

---

## 10) Operational notes (what to take care of)

### 10.1 Credentials and secrets

- Keep `KAGGLE_KEY` out of git.
- Use `.env` locally; in cloud, use a secret manager.

### 10.2 Idempotency and reruns

- Facts are idempotent due to `row_hash` uniqueness + `INSERT OR IGNORE`.
- Bronze is recreated and loaded each run.

### 10.3 Schema drift

- `_normalize_columns()` is designed to handle small source variations.
- Missing columns are logged as warnings.

### 10.4 Data quality expectations

- Validation aims to catch common failure modes quickly.
- Tune thresholds using the `DQ_*` env vars if needed.

---

## 11) Cloud deployment (draft guidance)

This repo includes a local architecture diagram, and the assignment asks for a cloud design where SQLite is not used.

Cloud diagram asset in this repo:
- [GCP High Level Architecture Diagram.png](../GCP%20High%20Level%20Architecture%20Diagram.png)

A simple cloud version usually includes:

- **Orchestration**: Airflow (e.g., Cloud Composer) or a workflow engine
- **Secrets**: Secret Manager (store Kaggle credentials)
- **Landing zone**: Object storage (e.g., GCS bucket) for raw files
- **Compute**: Containerized Python pipeline (Cloud Run / Kubernetes / VM)
- **Warehouse**: BigQuery / Snowflake / Postgres (instead of SQLite)
- **Transform modeling**: dbt/Dataform (optional), or keep Python SQL-based transforms
- **Reporting**: scheduled query / BI tool (Looker/Power BI/Tableau)

Keep the same logical stages:
- Extract → Bronze (raw) → Silver (dims/fact) → Validate → Report

---

## 12) Where to look first (for new readers)

- Main runner: [src/runner.py](../src/runner.py)
- Transform/load logic: [src/transform_load.py](../src/transform_load.py)
- Validation checks: [src/validate.py](../src/validate.py)
- Walkthrough: [notebooks/end_to_end_codebase.ipynb](../notebooks/end_to_end_codebase.ipynb)
