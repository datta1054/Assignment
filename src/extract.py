import logging
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


# Fail fast if Kaggle credentials are missing
def _ensure_kaggle_env_credentials_present() -> None:
    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY")
    if username and key:
        return

    raise RuntimeError(
        "Kaggle credentials not found. "
        "Set KAGGLE_USERNAME and KAGGLE_KEY (for example in a .env at the repo root)."
    )
@contextmanager
# Create a temporary kaggle.json for the Kaggle client
def _temporary_kaggle_config_dir() -> Iterator[Path]:
    previous_config_dir = os.environ.get("KAGGLE_CONFIG_DIR")

    with tempfile.TemporaryDirectory(prefix="kaggle-") as tmp_dir:
        tmp_path = Path(tmp_dir)

        payload = {
            "username": os.environ["KAGGLE_USERNAME"].strip(),
            "key": os.environ["KAGGLE_KEY"].strip(),
        }
        (tmp_path / "kaggle.json").write_text(json.dumps(payload), encoding="utf-8")

        os.environ["KAGGLE_CONFIG_DIR"] = str(tmp_path)
        try:
            yield tmp_path
        finally:
            if previous_config_dir is None:
                os.environ.pop("KAGGLE_CONFIG_DIR", None)
            else:
                os.environ["KAGGLE_CONFIG_DIR"] = previous_config_dir
# Download and unzip the latest version of a Kaggle dataset
def extract_latest_dataset(*, dataset: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    _ensure_kaggle_env_credentials_present()



@contextmanager

    """Create a temporary kaggle.json for the Kaggle client.

    The Kaggle API reads credentials from $KAGGLE_CONFIG_DIR/kaggle.json.
    This keeps credentials out of the repo while still enabling API usage.
    """
        api = KaggleApi()
        try:
            api.authenticate()
        except Exception as e:
            raise RuntimeError(
                "Failed to authenticate to Kaggle API. "
                "Verify KAGGLE_USERNAME/KAGGLE_KEY in .env, and that your Kaggle account API access is enabled."
            ) from e

        logger.info("Downloading Kaggle dataset: %s", dataset)
        api.dataset_download_files(dataset, path=str(output_dir), unzip=True, quiet=False)

    logger.info("Dataset extracted to: %s", output_dir)
    return output_dir
# Find the primary CSV under the extracted dataset directory
def find_first_csv(extracted_dir: Path) -> Path:
    csvs = list(extracted_dir.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found under {extracted_dir}")

    chosen = max(csvs, key=lambda p: p.stat().st_size)
    logger.info("Using CSV file: %s", chosen)
    return chosen
