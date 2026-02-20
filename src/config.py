import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    kaggle_dataset: str
    data_dir: Path
    sqlite_db_path: Path
    log_level: str


# Load settings from environment (optionally via .env)
def load_settings() -> Settings:
    load_dotenv(override=False)

    repo_root = Path(__file__).resolve().parents[1]

    kaggle_dataset = os.getenv("KAGGLE_DATASET", "faresashraf1001/supermarket-sales")

    data_dir_env = os.getenv("DATA_DIR")
    if data_dir_env:
        data_dir_candidate = Path(data_dir_env).expanduser()
        data_dir = (
            data_dir_candidate.resolve()
            if data_dir_candidate.is_absolute()
            else (repo_root / data_dir_candidate).resolve()
        )
    else:
        data_dir = (repo_root / "data").resolve()

    sqlite_db_path_env = os.getenv("SQLITE_DB_PATH")
    if sqlite_db_path_env:
        sqlite_candidate = Path(sqlite_db_path_env).expanduser()
        sqlite_db_path = (
            sqlite_candidate.resolve()
            if sqlite_candidate.is_absolute()
            else (repo_root / sqlite_candidate).resolve()
        )
    else:
        sqlite_db_path = (repo_root / "db" / "supermarket_sales.sqlite").resolve()

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    return Settings(
        kaggle_dataset=kaggle_dataset,
        data_dir=data_dir,
        sqlite_db_path=sqlite_db_path,
        log_level=log_level,
    )
