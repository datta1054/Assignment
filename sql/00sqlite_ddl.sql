-- SQLite DDL for staging (bronze), dimensions, and fact
-- Source-of-truth for DDL is also embedded in src/schema_sql.py

-- Staging (bronze)

-- Legacy tables (pre-rename). Drop if present to keep the DB clean.
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_branch;
DROP TABLE IF EXISTS dim_product_line;
DROP TABLE IF EXISTS stg_sales_raw;

DROP TABLE IF EXISTS bronze_sales_raw;
CREATE TABLE bronze_sales_raw (
    row_hash TEXT PRIMARY KEY,
    invoice_id TEXT,
    branch TEXT,
    city TEXT,
    customer_type TEXT,
    gender TEXT,
    product_line TEXT,
    unit_price REAL,
    quantity INTEGER,
    tax_5_percent REAL,
    total REAL,
    date TEXT,
    time TEXT,
    payment TEXT,
    cogs REAL,
    gross_margin_percentage REAL,
    gross_income REAL,
    rating REAL,
    extracted_at TEXT NOT NULL
);

-- Dimension: Product Line (Type 1)
CREATE TABLE IF NOT EXISTS silver_dim_product_line (
    product_line_key INTEGER PRIMARY KEY,
    product_line_name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

-- Dimension: Branch (SCD Type 2)
CREATE TABLE IF NOT EXISTS silver_dim_branch (
    branch_key INTEGER PRIMARY KEY,
    branch_code TEXT NOT NULL,
    city TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    is_current INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(branch_code, valid_from)
);

-- Fact: Sales (transaction grain)
CREATE TABLE IF NOT EXISTS silver_fact_sales (
    sales_key INTEGER PRIMARY KEY,
    row_hash TEXT NOT NULL UNIQUE,
    invoice_id TEXT,
    product_line_key INTEGER NOT NULL,
    branch_key INTEGER NOT NULL,
    txn_date TEXT NOT NULL,
    txn_time TEXT,
    unit_price REAL,
    quantity INTEGER,
    tax_5_percent REAL,
    total REAL,
    cogs REAL,
    gross_income REAL,
    rating REAL,
    payment TEXT,
    customer_type TEXT,
    gender TEXT,
    loaded_at TEXT NOT NULL,
    FOREIGN KEY(product_line_key) REFERENCES silver_dim_product_line(product_line_key),
    FOREIGN KEY(branch_key) REFERENCES silver_dim_branch(branch_key)
);

