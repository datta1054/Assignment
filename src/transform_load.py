import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from . import db

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class NormalizedFrames:
    raw: pd.DataFrame


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# Standardize source columns into a stable, snake_case schema
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "Invoice ID": "invoice_id",
        "Branch": "branch",
        "City": "city",
        "Customer type": "customer_type",
        "Gender": "gender",
        "Product line": "product_line",
        "Unit price": "unit_price",
        "Quantity": "quantity",
        "Tax 5%": "tax_5_percent",
        "Total": "total",
        "Sales": "total",
        "Date": "date",
        "Time": "time",
        "Payment": "payment",
        "cogs": "cogs",
        "gross margin percentage": "gross_margin_percentage",
        "gross income": "gross_income",
        "Rating": "rating",
    }

    df2 = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns}).copy()
    missing = [v for v in mapping.values() if v not in df2.columns]
    if missing:
        logger.warning("Some expected columns are missing: %s", missing)

    return df2


# Parse dates and return ISO strings (YYYY-MM-DD)
def _parse_date_iso(date_series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(date_series, errors="coerce")
    return dt.dt.date.astype("string")


# Deterministic hash used for idempotent fact loads
def _row_hash(row: pd.Series) -> str:
    parts = [
        str(row.get("invoice_id", "")),
        str(row.get("branch", "")),
        str(row.get("product_line", "")),
        str(row.get("date", "")),
        str(row.get("time", "")),
        str(row.get("total", "")),
    ]
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def read_raw_csv(csv_path: Path) -> NormalizedFrames:
    logger.info("Reading raw CSV: %s", csv_path)
    df = pd.read_csv(csv_path)
    df = _normalize_columns(df)

    if "date" in df.columns:
        df["date"] = _parse_date_iso(df["date"])

    df["row_hash"] = df.apply(_row_hash, axis=1)

    for col in ["unit_price", "tax_5_percent", "total", "cogs", "gross_margin_percentage", "gross_income", "rating"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return NormalizedFrames(raw=df)


def load_staging(conn, frames: NormalizedFrames) -> None:
    extracted_at = utc_now_iso()
    df = frames.raw.copy()
    df["extracted_at"] = extracted_at

    logger.info("Loading %d rows into staging", len(df))

    cols = [
        "row_hash",
        "invoice_id",
        "branch",
        "city",
        "customer_type",
        "gender",
        "product_line",
        "unit_price",
        "quantity",
        "tax_5_percent",
        "total",
        "date",
        "time",
        "payment",
        "cogs",
        "gross_margin_percentage",
        "gross_income",
        "rating",
        "extracted_at",
    ]

    rows = []
    for _, r in df[cols].iterrows():
        rows.append(tuple(None if pd.isna(v) else v for v in r.to_list()))

    db.executemany(
        conn,
        """
        INSERT INTO bronze_sales_raw (
            row_hash, invoice_id, branch, city, customer_type, gender, product_line,
            unit_price, quantity, tax_5_percent, total, date, time, payment,
            cogs, gross_margin_percentage, gross_income, rating, extracted_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )


# Type 1 dim: insert missing product lines
def ensure_dim_product_line(conn) -> None:
    now = utc_now_iso()

    existing = {r[0] for r in db.fetch_all(conn, "SELECT product_line_name FROM silver_dim_product_line")}
    missing = db.fetch_all(
        conn,
        """
        SELECT DISTINCT product_line
        FROM bronze_sales_raw
        WHERE product_line IS NOT NULL
        """,
    )

    to_insert = [(name, now) for (name,) in missing if name not in existing]
    if not to_insert:
        logger.info("dim_product_line is up to date")
        return

    logger.info("Inserting %d new product lines", len(to_insert))
    db.executemany(
        conn,
        "INSERT INTO silver_dim_product_line(product_line_name, created_at) VALUES (?,?)",
        to_insert,
    )


# SCD Type 2 upsert for branch (natural key: branch_code; tracked: city)
def scd2_upsert_dim_branch(conn) -> None:
    now = utc_now_iso()

    incoming = db.fetch_all(
        conn,
        """
        SELECT DISTINCT branch AS branch_code, city
        FROM bronze_sales_raw
        WHERE branch IS NOT NULL AND city IS NOT NULL
        """,
    )

    for branch_code, city in incoming:
        current = db.fetch_all(
            conn,
            """
            SELECT branch_key, city, valid_from
            FROM silver_dim_branch
            WHERE branch_code = ? AND is_current = 1
            """,
            (branch_code,),
        )

        if not current:
            db.executemany(
                conn,
                """
                INSERT INTO silver_dim_branch(branch_code, city, valid_from, valid_to, is_current, created_at)
                VALUES (?,?,?,?,?,?)
                """,
                [(branch_code, city, now, None, 1, now)],
            )
            continue

        branch_key, current_city, _valid_from = current[0]
        if current_city == city:
            continue

        logger.info("Branch %s changed city %s -> %s (SCD2)", branch_code, current_city, city)

        db.executemany(
            conn,
            "UPDATE silver_dim_branch SET valid_to = ?, is_current = 0 WHERE branch_key = ?",
            [(now, branch_key)],
        )

        db.executemany(
            conn,
            """
            INSERT INTO silver_dim_branch(branch_code, city, valid_from, valid_to, is_current, created_at)
            VALUES (?,?,?,?,?,?)
            """,
            [(branch_code, city, now, None, 1, now)],
        )


def _lookup_product_line_keys(conn) -> dict[str, int]:
    rows = db.fetch_all(conn, "SELECT product_line_key, product_line_name FROM silver_dim_product_line")
    return {name: int(key) for key, name in rows}


def _lookup_current_branch_keys(conn) -> dict[str, int]:
    rows = db.fetch_all(
        conn,
        "SELECT branch_key, branch_code FROM silver_dim_branch WHERE is_current = 1",
    )
    return {code: int(key) for key, code in rows}


# Load facts idempotently using row_hash uniqueness
def load_fact_sales(conn) -> None:
    now = utc_now_iso()

    product_keys = _lookup_product_line_keys(conn)
    branch_keys = _lookup_current_branch_keys(conn)

    stg_rows = db.fetch_all(
        conn,
        """
        SELECT
            row_hash, invoice_id, product_line, branch, date, time,
            unit_price, quantity, tax_5_percent, total, cogs, gross_income, rating,
            payment, customer_type, gender
        FROM bronze_sales_raw
        WHERE date IS NOT NULL
        """,
    )

    rows_to_insert: list[tuple] = []
    skipped_missing_dim = 0

    for (
        row_hash,
        invoice_id,
        product_line,
        branch,
        txn_date,
        txn_time,
        unit_price,
        quantity,
        tax_5_percent,
        total,
        cogs,
        gross_income,
        rating,
        payment,
        customer_type,
        gender,
    ) in stg_rows:
        if product_line not in product_keys or branch not in branch_keys:
            skipped_missing_dim += 1
            continue

        rows_to_insert.append(
            (
                row_hash,
                invoice_id,
                product_keys[product_line],
                branch_keys[branch],
                txn_date,
                txn_time,
                unit_price,
                quantity,
                tax_5_percent,
                total,
                cogs,
                gross_income,
                rating,
                payment,
                customer_type,
                gender,
                now,
            )
        )

    if skipped_missing_dim:
        logger.warning("Skipped %d rows due to missing dimension keys", skipped_missing_dim)

    if not rows_to_insert:
        logger.info("No fact rows to insert")
        return

    logger.info("Inserting %d fact rows (idempotent)", len(rows_to_insert))
    db.executemany(
        conn,
        """
        INSERT OR IGNORE INTO silver_fact_sales(
            row_hash, invoice_id, product_line_key, branch_key, txn_date, txn_time,
            unit_price, quantity, tax_5_percent, total, cogs, gross_income, rating,
            payment, customer_type, gender, loaded_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows_to_insert,
    )
