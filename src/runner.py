import logging

from .config import load_settings
from .extract import extract_latest_dataset, find_first_csv
from .logging_utils import configure_logging
from .schema_sql import DDL_SQLITE
from .transform_load import (
    ensure_dim_product_line,
    load_fact_sales,
    load_staging,
    read_raw_csv,
    scd2_upsert_dim_branch,
)
from .validate import validate_sqlite_db
from . import db

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)

    raw_dir = settings.data_dir / "raw"
    extracted_dir = extract_latest_dataset(dataset=settings.kaggle_dataset, output_dir=raw_dir)
    csv_path = find_first_csv(extracted_dir)

    conn = db.connect(settings.sqlite_db_path)
    try:
        logger.info("Creating (or recreating) tables")
        db.execute_script(conn, DDL_SQLITE)

        frames = read_raw_csv(csv_path)
        load_staging(conn, frames)

        ensure_dim_product_line(conn)
        scd2_upsert_dim_branch(conn)
        load_fact_sales(conn)

        conn.commit()
        logger.info("Pipeline complete. SQLite DB at %s", settings.sqlite_db_path)

        validate_sqlite_db(settings.sqlite_db_path)
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
