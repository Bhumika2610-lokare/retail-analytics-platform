"""
clean_products.py

Silver layer cleaning for the `products` dataset.

Source : data/bronze/products/YYYY-MM-DD.parquet
Output : data/silver/products/YYYY-MM-DD.parquet

Cleaning rules:
  - Remove duplicate product_id
  - Remove null product_id
  - Validate selling_price > 0
  - Validate cost_price > 0
  - Standardize category names
  - Standardize brand names
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

DATASET = "products"
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

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all Silver-layer cleaning rules to a raw Bronze products DataFrame."""
    row_count_before = len(df)
    logger.info("Row count before cleaning: %d", row_count_before)

    # Remove null / duplicate product_id
    df = df.dropna(subset=["product_id"])
    df = df.drop_duplicates(subset=["product_id"], keep="last")

    # Standardize text fields
    if "category" in df.columns:
        df["category"] = standardize_text(df["category"])
    if "brand" in df.columns:
        df["brand"] = standardize_text(df["brand"])

    # Validate selling_price > 0
    if "selling_price" in df.columns:
        df["selling_price"] = pd.to_numeric(df["selling_price"], errors="coerce")
        before = len(df)
        df = df[df["selling_price"] > 0]
        logger.info("  - dropped %d rows with selling_price <= 0 or invalid", before - len(df))

    # Validate cost_price > 0
    if "cost_price" in df.columns:
        df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
        before = len(df)
        df = df[df["cost_price"] > 0]
        logger.info("  - dropped %d rows with cost_price <= 0 or invalid", before - len(df))

    # Remove any remaining invalid/incomplete records
    required_cols = [c for c in ["product_id", "category", "brand"] if c in df.columns]
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
    logger.info("Processing products partition: %s", partition_date)

    df = pd.read_parquet(partition_path)
    cleaned_df = clean_products(df)
    out_path = write_silver_partition(cleaned_df, partition_date)

    logger.info("Wrote cleaned partition to %s", out_path)


def main() -> None:
    partitions = get_unprocessed_partitions()
    if not partitions:
        logger.info("No new products partitions to process.")
        return

    logger.info("Found %d new products partition(s) to process.", len(partitions))
    for partition_path in partitions:
        process_partition(partition_path)


if __name__ == "__main__":
    main()
