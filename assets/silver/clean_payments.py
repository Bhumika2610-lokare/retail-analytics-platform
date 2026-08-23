"""
clean_payments.py

Silver layer cleaning for the `payments` dataset.

Source : data/bronze/payments/YYYY-MM-DD.parquet
Output : data/silver/payments/YYYY-MM-DD.parquet

Cleaning rules:
  - Remove duplicate payment_id
  - Remove null payment_id
  - Validate amount > 0
  - Standardize payment_method
  - Standardize payment_status
  - Remove invalid records
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------

DATASET = "payments"
REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / DATASET
SILVER_DIR = REPO_ROOT / "data" / "silver" / DATASET

PARTITION_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\.parquet$")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(DATASET)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Reusable helper functions
# ---------------------------------------------------------------------------

def get_latest_bronze_partition() -> Path:
    """Return the path of the most recent dated Bronze parquet file."""
    if not BRONZE_DIR.exists():
        raise FileNotFoundError(f"Bronze folder not found: {BRONZE_DIR}")

    partitions = sorted(
        f for f in BRONZE_DIR.glob("*.parquet") if PARTITION_PATTERN.match(f.name)
    )
    if not partitions:
        raise FileNotFoundError(f"No dated Bronze partitions found in {BRONZE_DIR}")
    return partitions[-1]


def get_unprocessed_partitions() -> list[Path]:
    """Return Bronze partitions that haven't been written to Silver yet."""
    if not BRONZE_DIR.exists():
        raise FileNotFoundError(f"Bronze folder not found: {BRONZE_DIR}")

    bronze_partitions = sorted(
        f for f in BRONZE_DIR.glob("*.parquet") if PARTITION_PATTERN.match(f.name)
    )
    processed_dates = set()
    if SILVER_DIR.exists():
        processed_dates = {
            f.stem for f in SILVER_DIR.glob("*.parquet") if PARTITION_PATTERN.match(f.name)
        }
    return [f for f in bronze_partitions if f.stem not in processed_dates]


def standardize_text(series: pd.Series, title_case: bool = True) -> pd.Series:
    """Trim whitespace, collapse internal whitespace, and normalize casing."""
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.str.replace(r"\s+", " ", regex=True)
    if title_case:
        cleaned = cleaned.str.title()
    return cleaned


def write_silver_partition(df: pd.DataFrame, partition_date: str) -> Path:
    """Write a cleaned DataFrame to the Silver layer, preserving the partition date."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SILVER_DIR / f"{partition_date}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


# ---------------------------------------------------------------------------
# Cleaning logic
# ---------------------------------------------------------------------------

def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all Silver-layer cleaning rules to a raw Bronze payments DataFrame."""
    row_count_before = len(df)
    logger.info("Row count before cleaning: %d", row_count_before)

    # Remove null / duplicate payment_id
    df = df.dropna(subset=["payment_id"])
    df = df.drop_duplicates(subset=["payment_id"], keep="last")

    # Validate amount > 0
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        before = len(df)
        df = df[df["amount"] > 0]
        logger.info("  - dropped %d rows with amount <= 0 or invalid", before - len(df))

    # Standardize text fields
    if "payment_method" in df.columns:
        df["payment_method"] = standardize_text(df["payment_method"])
    if "payment_status" in df.columns:
        df["payment_status"] = standardize_text(df["payment_status"])

    # Remove any remaining invalid/incomplete records
    required_cols = [c for c in ["payment_id", "payment_method", "payment_status"] if c in df.columns]
    if required_cols:
        df = df.dropna(subset=required_cols)
        df = df[~(df[required_cols].astype("string").apply(lambda s: s.str.strip()) == "").any(axis=1)]

    df = df.reset_index(drop=True)

    row_count_after = len(df)
    logger.info("Row count after cleaning: %d", row_count_after)
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def process_partition(partition_path: Path) -> None:
    partition_date = partition_path.stem
    logger.info("Processing payments partition: %s", partition_date)

    df = pd.read_parquet(partition_path)
    cleaned_df = clean_payments(df)
    out_path = write_silver_partition(cleaned_df, partition_date)

    logger.info("Wrote cleaned partition to %s", out_path)


def main() -> None:
    partitions = get_unprocessed_partitions()
    if not partitions:
        logger.info("No new payments partitions to process.")
        return

    logger.info("Found %d new payments partition(s) to process.", len(partitions))
    for partition_path in partitions:
        process_partition(partition_path)


if __name__ == "__main__":
    main()
