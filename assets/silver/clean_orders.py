"""
Silver Layer: Order Cleaning
--------------------------------
Reads the latest Bronze orders partition, applies cleaning rules
(including referential validation against Silver customers/products),
and writes the result to the Silver layer as Parquet.

Source : data/bronze/orders/YYYY-MM-DD.parquet
Output : data/silver/orders/YYYY-MM-DD.parquet
"""

import glob
import logging
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
BRONZE_DIR = Path("data/bronze/orders")
SILVER_DIR = Path("data/silver/orders")

SILVER_CUSTOMERS_DIR = Path("data/silver/customers")
SILVER_PRODUCTS_DIR = Path("data/silver/products")
BRONZE_CUSTOMERS_DIR = Path("data/bronze/customers")
BRONZE_PRODUCTS_DIR = Path("data/bronze/products")

VALID_ORDER_STATUSES = {"pending", "confirmed", "shipped", "delivered", "cancelled", "returned"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver.orders")


# --------------------------------------------------------------------------
# Reusable helpers
# --------------------------------------------------------------------------
def get_latest_partition(directory: Path) -> Path:
    files = sorted(glob.glob(str(directory / "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No partitions found in {directory}")
    return Path(files[-1])


def read_parquet(path: Path) -> pd.DataFrame:
    logger.info("Reading file: %s", path)
    df = pd.read_parquet(path)
    logger.info("Rows read: %d", len(df))
    return df


def load_reference_ids(silver_dir: Path, bronze_dir: Path, id_column: str) -> set:
    """Prefer the Silver reference table; fall back to Bronze if Silver isn't built yet."""
    try:
        ref_path = get_latest_partition(silver_dir)
        ref_df = read_parquet(ref_path)
        logger.info("Using Silver reference table for %s: %s", id_column, ref_path)
    except FileNotFoundError:
        logger.warning("Silver reference for %s not found, falling back to Bronze", id_column)
        ref_path = get_latest_partition(bronze_dir)
        ref_df = read_parquet(ref_path)
    return set(ref_df[id_column].dropna().unique())


def standardize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.lower()


# --------------------------------------------------------------------------
# Cleaning logic
# --------------------------------------------------------------------------
def clean_orders(df: pd.DataFrame, valid_customer_ids: set, valid_product_ids: set) -> pd.DataFrame:
    initial_count = len(df)
    logger.info("Starting order cleaning. Initial row count: %d", initial_count)

    df = df.copy()

    # Remove null order_id
    df = df[df["order_id"].notna()]
    logger.info("After removing null order_id: %d rows", len(df))

    # Remove duplicate order_id
    df = df.drop_duplicates(subset="order_id", keep="first")
    logger.info("After removing duplicate order_id: %d rows", len(df))

    # Remove quantity <= 0
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df = df[df["quantity"] > 0]
        logger.info("After removing quantity <= 0: %d rows", len(df))

    # Remove total_amount <= 0
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
        df = df[df["total_amount"] > 0]
        logger.info("After removing total_amount <= 0: %d rows", len(df))

    # Standardize order_status
    if "order_status" in df.columns:
        df["order_status"] = standardize_text(df["order_status"])
        valid_status_mask = df["order_status"].isin(VALID_ORDER_STATUSES)
        logger.info("Invalid order_status records found: %d", (~valid_status_mask).sum())
        df = df[valid_status_mask]

    # Validate customer_id exists
    if "customer_id" in df.columns:
        valid_customer_mask = df["customer_id"].isin(valid_customer_ids)
        logger.info("Orders with unknown customer_id: %d", (~valid_customer_mask).sum())
        df = df[valid_customer_mask]

    # Validate product_id exists
    if "product_id" in df.columns:
        valid_product_mask = df["product_id"].isin(valid_product_ids)
        logger.info("Orders with unknown product_id: %d", (~valid_product_mask).sum())
        df = df[valid_product_mask]

    final_count = len(df)
    logger.info(
        "Order cleaning complete. Rows before: %d | Rows after: %d | Removed: %d",
        initial_count, final_count, initial_count - final_count,
    )
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------
# Write
# --------------------------------------------------------------------------
def write_silver(df: pd.DataFrame, output_dir: Path, partition_date: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{partition_date}.parquet"
    df.to_parquet(output_path, index=False)
    logger.info("Silver file written: %s (%d rows)", output_path, len(df))
    return output_path


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    latest_bronze_file = get_latest_partition(BRONZE_DIR)
    partition_date = latest_bronze_file.stem

    raw_df = read_parquet(latest_bronze_file)

    valid_customer_ids = load_reference_ids(SILVER_CUSTOMERS_DIR, BRONZE_CUSTOMERS_DIR, "customer_id")
    valid_product_ids = load_reference_ids(SILVER_PRODUCTS_DIR, BRONZE_PRODUCTS_DIR, "product_id")

    cleaned_df = clean_orders(raw_df, valid_customer_ids, valid_product_ids)
    write_silver(cleaned_df, SILVER_DIR, partition_date)


if __name__ == "__main__":
    main()
