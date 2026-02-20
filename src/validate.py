import logging
import sqlite3
import os
from pathlib import Path

logger = logging.getLogger(__name__)


EXPECTED_TABLES = {
    "bronze_sales_raw",
    "silver_dim_product_line",
    "silver_dim_branch",
    "silver_fact_sales",
}


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default


# Lightweight validation checks for the generated SQLite DB
def validate_sqlite_db(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found at: {db_path}")

    fail_on_warnings = env_bool("DQ_FAIL_ON_WARNINGS", False)
    min_fact_coverage = env_float("DQ_MIN_FACT_COVERAGE", 0.98)
    min_fact_coverage = max(0.0, min(1.0, float(min_fact_coverage)))

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        errors: list[str] = []
        warnings: list[str] = []

        def warn(msg: str) -> None:
            warnings.append(msg)
            logger.warning(msg)

        def err(msg: str) -> None:
            errors.append(msg)
            logger.error(msg)

        def count(sql: str, params: tuple | None = None) -> int:
            row = conn.execute(sql, params or ()).fetchone()
            return int(row[0]) if row and row[0] is not None else 0

        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        }
        missing = EXPECTED_TABLES - tables
        if missing:
            raise RuntimeError(f"Missing expected tables: {sorted(missing)}")

        bronze_rows = count("SELECT COUNT(*) FROM bronze_sales_raw")
        fact_rows = count("SELECT COUNT(*) FROM silver_fact_sales")
        dim_pl_rows = count("SELECT COUNT(*) FROM silver_dim_product_line")
        dim_branch_rows = count("SELECT COUNT(*) FROM silver_dim_branch")
        logger.info(
            "Row counts: bronze=%d fact=%d dim_product_line=%d dim_branch=%d",
            bronze_rows,
            fact_rows,
            dim_pl_rows,
            dim_branch_rows,
        )

        if bronze_rows == 0:
            raise RuntimeError("bronze_sales_raw has 0 rows — extraction/load likely failed")
        if fact_rows == 0:
            raise RuntimeError("silver_fact_sales has 0 rows — dim lookups or fact load likely failed")

        if dim_pl_rows == 0:
            raise RuntimeError("silver_dim_product_line has 0 rows — dimension load likely failed")

        fact_dupes = count(
            """
            SELECT COUNT(*)
            FROM (
                SELECT row_hash
                FROM silver_fact_sales
                GROUP BY row_hash
                HAVING COUNT(*) > 1
            )
            """,
        )
        if fact_dupes:
            raise RuntimeError("silver_fact_sales contains duplicate row_hash values (should be UNIQUE)")

        # Coverage: how many distinct eligible bronze rows made it into the fact table.
        expected_fact = count(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT row_hash
                FROM bronze_sales_raw
                WHERE date IS NOT NULL
                  AND product_line IS NOT NULL
                  AND branch IS NOT NULL
                  AND city IS NOT NULL
            )
            """,
        )
        actual_fact = fact_rows
        if expected_fact > 0:
            coverage = actual_fact / float(expected_fact)
            logger.info("Fact coverage: %.3f (%d/%d)", coverage, actual_fact, expected_fact)
            if coverage < min_fact_coverage:
                err(
                    "Fact coverage below threshold: "
                    f"{coverage:.3f} ({actual_fact}/{expected_fact}) < {min_fact_coverage:.3f}. "
                    "This can indicate missing dimension keys, bad parsing, or load errors."
                )
        else:
            warn("No eligible bronze rows found for coverage check (date/product_line/branch/city all required)")

        bad_txn_dates = conn.execute(
            """
            SELECT txn_date
            FROM silver_fact_sales
            WHERE txn_date IS NOT NULL
              AND txn_date NOT GLOB '????-??-??'
            LIMIT 5
            """
        ).fetchall()
        if bad_txn_dates:
            err(f"Found non-ISO txn_date values (sample): {[r[0] for r in bad_txn_dates]}")

        current_branch = count("SELECT COUNT(*) FROM silver_dim_branch WHERE is_current = 1")
        if current_branch == 0:
            err("silver_dim_branch has no current records (is_current=1)")

        multi_current = count(
            """
            SELECT COUNT(*)
            FROM (
                SELECT branch_code
                FROM silver_dim_branch
                WHERE is_current = 1
                GROUP BY branch_code
                HAVING COUNT(*) != 1
            )
            """,
        )
        if multi_current:
            err(f"Found {multi_current} branch_code values with != 1 current record (SCD2 integrity issue)")

        # Null checks (should be zero for NOT NULL columns; invoice_id is allowed but useful to know)
        null_fact_critical = count(
            """
            SELECT COUNT(*)
            FROM silver_fact_sales
            WHERE row_hash IS NULL
               OR product_line_key IS NULL
               OR branch_key IS NULL
               OR txn_date IS NULL
               OR loaded_at IS NULL
            """,
        )
        if null_fact_critical:
            err(f"silver_fact_sales has {null_fact_critical} rows with NULLs in critical columns")

        # Referential integrity (should be enforced by FK constraints, but validate anyway)
        unmatched_product = count(
            """
            SELECT COUNT(*)
            FROM silver_fact_sales f
            LEFT JOIN silver_dim_product_line d
              ON f.product_line_key = d.product_line_key
            WHERE d.product_line_key IS NULL
            """,
        )
        if unmatched_product:
            err(f"Found {unmatched_product} fact rows with missing product_line_key in dim")

        unmatched_branch = count(
            """
            SELECT COUNT(*)
            FROM silver_fact_sales f
            LEFT JOIN silver_dim_branch b
              ON f.branch_key = b.branch_key
            WHERE b.branch_key IS NULL
            """,
        )
        if unmatched_branch:
            err(f"Found {unmatched_branch} fact rows with missing branch_key in dim")

        # Basic numeric sanity checks
        negative_money = count(
            """
            SELECT COUNT(*)
            FROM silver_fact_sales
            WHERE (unit_price IS NOT NULL AND unit_price < 0)
               OR (tax_5_percent IS NOT NULL AND tax_5_percent < 0)
               OR (total IS NOT NULL AND total < 0)
               OR (cogs IS NOT NULL AND cogs < 0)
               OR (gross_income IS NOT NULL AND gross_income < 0)
            """,
        )
        if negative_money:
            err(f"Found {negative_money} fact rows with negative monetary values")

        nonpositive_qty = count(
            """
            SELECT COUNT(*)
            FROM silver_fact_sales
            WHERE quantity IS NOT NULL AND quantity <= 0
            """,
        )
        if nonpositive_qty:
            err(f"Found {nonpositive_qty} fact rows with non-positive quantity")

        rating_out_of_range = count(
            """
            SELECT COUNT(*)
            FROM silver_fact_sales
            WHERE rating IS NOT NULL AND (rating < 0 OR rating > 10)
            """,
        )
        if rating_out_of_range:
            warn(f"Found {rating_out_of_range} fact rows with rating outside [0,10]")

        if errors or (fail_on_warnings and warnings):
            parts: list[str] = []
            if errors:
                parts.append("Data quality errors:\n- " + "\n- ".join(errors))
            if fail_on_warnings and warnings:
                parts.append("Warnings treated as errors:\n- " + "\n- ".join(warnings))
            raise RuntimeError("\n\n".join(parts))

        logger.info("Validation passed (%d warnings)", len(warnings))
    finally:
        conn.close()
