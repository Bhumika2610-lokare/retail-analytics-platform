"""
clean_customers.py

Silver layer cleaning for the `customers` dataset.

Source : data/bronze/customers/YYYY-MM-DD.parquet
Output : data/silver/customers/YYYY-MM-DD.parquet

Cleaning rules:
  - Remove duplicate customer_id
  - Remove null customer_id
  - Standardize customer_name
  - Standardize city
  - Standardize state
  - Validate email format
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

DATASET = "customers"
REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / DATASET
SILVER_DIR = REPO_ROOT / "data" / "silver" / DATASET

PARTITION_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\.parquet$")
EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

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


def is_valid_email(series: pd.Series) -> pd.Series:
    """Return a boolean mask of syntactically valid email addresses."""
    return series.astype("string").str.match(EMAIL_PATTERN, na=False)


def write_silver_partition(df: pd.DataFrame, partition_date: str) -> Path:
    """Write a cleaned DataFrame to the Silver layer, preserving the partition date."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SILVER_DIR / f"{partition_date}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


# ---------------------------------------------------------------------------
# Cleaning logic
# ---------------------------------------------------------------------------

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all Silver-layer cleaning rules to a raw Bronze customers DataFrame."""
    row_count_before = len(df)
    logger.info("Row count before cleaning: %d", row_count_before)

    # Remove null / duplicate customer_id
    df = df.dropna(subset=["customer_id"])
    df = df.drop_duplicates(subset=["customer_id"], keep="last")

    # Standardize text fields
    if "customer_name" in df.columns:
        df["customer_name"] = standardize_text(df["customer_name"])
    if "city" in df.columns:
        df["city"] = standardize_text(df["city"])
    if "state" in df.columns:
        df["state"] = standardize_text(df["state"])

    # Validate email format
    if "email" in df.columns:
        valid_email_mask = is_valid_email(df["email"])
        invalid_count = (~valid_email_mask).sum()
        logger.info("  - dropped %d rows with invalid email format", invalid_count)
        df = df[valid_email_mask]

    # Remove any remaining invalid/incomplete records
    required_cols = [c for c in ["customer_id", "customer_name", "email"] if c in df.columns]
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
    logger.info("Processing customers partition: %s", partition_date)

    df = pd.read_parquet(partition_path)
    cleaned_df = clean_customers(df)
    out_path = write_silver_partition(cleaned_df, partition_date)

    logger.info("Wrote cleaned partition to %s", out_path)


def main() -> None:
    partitions = get_unprocessed_partitions()
    if not partitions:
        logger.info("No new customers partitions to process.")
        return

    logger.info("Found %d new customers partition(s) to process.", len(partitions))
    for partition_path in partitions:
        process_partition(partition_path)


if __name__ == "__main__":
    main()
