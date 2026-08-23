"""
clean_orders.py

Silver layer cleaning for the `orders` dataset.

Source : data/bronze/orders/YYYY-MM-DD.parquet
Output : data/silver/orders/YYYY-MM-DD.parquet

Cleaning rules:
  - Remove duplicate order_id
  - Remove null order_id
  - Remove quantity <= 0
  - Remove total_amount <= 0
  - Standardize order_status
  - Validate customer_id exists
  - Validate product_id exists

Referential validation reads the already-cleaned Silver customers/products
tables (all partitions). If Silver customers/products haven't been built
yet, the corresponding check is skipped with a warning instead of failing.
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

DATASET = "orders"
REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / DATASET
SILVER_DIR = REPO_ROOT / "data" / "silver" / DATASET
SILVER_ROOT = REPO_ROOT / "data" / "silver"

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


def load_known_ids(reference_dataset: str, id_column: str) -> set | None:
    """Load the full set of known ids from all Silver partitions of a reference dataset."""
    ref_dir = SILVER_ROOT / reference_dataset
    if not ref_dir.exists():
        logger.warning(
            "Silver dataset '%s' not found; skipping %s referential check.",
            reference_dataset, id_column,
        )
        return None

    files = list(ref_dir.glob("*.parquet"))
    if not files:
        logger.warning(
            "No Silver partitions found for '%s'; skipping %s referential check.",
            reference_dataset, id_column,
        )
        return None

    frames = [pd.read_parquet(f, columns=[id_column]) for f in files]
    return set(pd.concat(frames)[id_column].dropna().unique())


def write_silver_partition(df: pd.DataFrame, partition_date: str) -> Path:
    """Write a cleaned DataFrame to the Silver layer, preserving the partition date."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SILVER_DIR / f"{partition_date}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


# ---------------------------------------------------------------------------
# Cleaning logic
# ---------------------------------------------------------------------------

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all Silver-layer cleaning rules to a raw Bronze orders DataFrame."""
    row_count_before = len(df)
    logger.info("Row count before cleaning: %d", row_count_before)

    # Remove null / duplicate order_id
    df = df.dropna(subset=["order_id"])
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    # Remove quantity <= 0
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        before = len(df)
        df = df[df["quantity"] > 0]
        logger.info("  - dropped %d rows with quantity <= 0 or invalid", before - len(df))

    # Remove total_amount <= 0
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
        before = len(df)
        df = df[df["total_amount"] > 0]
        logger.info("  - dropped %d rows with total_amount <= 0 or invalid", before - len(df))

    # Standardize order_status
    if "order_status" in df.columns:
        df["order_status"] = standardize_text(df["order_status"])

    # Validate customer_id exists
    if "customer_id" in df.columns:
        known_customers = load_known_ids("customers", "customer_id")
        if known_customers is not None:
            before = len(df)
            df = df[df["customer_id"].isin(known_customers)]
            logger.info("  - dropped %d rows with unknown customer_id", before - len(df))

    # Validate product_id exists
    if "product_id" in df.columns:
        known_products = load_known_ids("products", "product_id")
        if known_products is not None:
            before = len(df)
            df = df[df["product_id"].isin(known_products)]
            logger.info("  - dropped %d rows with unknown product_id", before - len(df))

    df = df.reset_index(drop=True)

    row_count_after = len(df)
    logger.info("Row count after cleaning: %d", row_count_after)
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def process_partition(partition_path: Path) -> None:
    partition_date = partition_path.stem
    logger.info("Processing orders partition: %s", partition_date)

    df = pd.read_parquet(partition_path)
    cleaned_df = clean_orders(df)
    out_path = write_silver_partition(cleaned_df, partition_date)

    logger.info("Wrote cleaned partition to %s", out_path)


def main() -> None:
    partitions = get_unprocessed_partitions()
    if not partitions:
        logger.info("No new orders partitions to process.")
        return

    logger.info("Found %d new orders partition(s) to process.", len(partitions))
    for partition_path in partitions:
        process_partition(partition_path)


if __name__ == "__main__":
    main()
