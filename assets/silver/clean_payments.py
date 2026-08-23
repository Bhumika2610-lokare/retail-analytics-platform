"""
Silver Layer: Payment Cleaning
--------------------------------
Reads the latest Bronze payments partition, applies cleaning rules,
and writes the result to the Silver layer as Parquet.

Source : data/bronze/payments/YYYY-MM-DD.parquet
Output : data/silver/payments/YYYY-MM-DD.parquet
"""

import glob
import logging
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
BRONZE_DIR = Path("data/bronze/payments")
SILVER_DIR = Path("data/silver/payments")

VALID_PAYMENT_METHODS = {"credit_card", "debit_card", "upi", "net_banking", "cash_on_delivery", "wallet"}
VALID_PAYMENT_STATUSES = {"success", "failed", "pending", "refunded"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver.payments")


# --------------------------------------------------------------------------
# Reusable helpers
# --------------------------------------------------------------------------
def get_latest_bronze_partition(bronze_dir: Path) -> Path:
    files = sorted(glob.glob(str(bronze_dir / "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No bronze partitions found in {bronze_dir}")
    latest = Path(files[-1])
    logger.info("Latest bronze partition detected: %s", latest)
    return latest


def read_bronze(path: Path) -> pd.DataFrame:
    logger.info("Reading bronze file: %s", path)
    df = pd.read_parquet(path)
    logger.info("Rows read: %d", len(df))
    return df


def standardize_category(series: pd.Series, valid_set: set) -> tuple[pd.Series, pd.Series]:
    """Normalize a category-like column and return (normalized_series, valid_mask)."""
    normalized = (
        series.astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"[\s-]+", "_", regex=True)
    )
    mask = normalized.isin(valid_set)
    return normalized, mask


# --------------------------------------------------------------------------
# Cleaning logic
# --------------------------------------------------------------------------
def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    logger.info("Starting payment cleaning. Initial row count: %d", initial_count)

    df = df.copy()

    # Remove null payment_id
    df = df[df["payment_id"].notna()]
    logger.info("After removing null payment_id: %d rows", len(df))

    # Remove duplicate payment_id
    df = df.drop_duplicates(subset="payment_id", keep="first")
    logger.info("After removing duplicate payment_id: %d rows", len(df))

    # Validate amount > 0
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df = df[df["amount"] > 0]
        logger.info("After validating amount > 0: %d rows", len(df))

    # Standardize payment_method
    if "payment_method" in df.columns:
        df["payment_method"], valid_method_mask = standardize_category(
            df["payment_method"], VALID_PAYMENT_METHODS
        )
        logger.info("Invalid payment_method records found: %d", (~valid_method_mask).sum())
        df = df[valid_method_mask]

    # Standardize payment_status
    if "payment_status" in df.columns:
        df["payment_status"], valid_status_mask = standardize_category(
            df["payment_status"], VALID_PAYMENT_STATUSES
        )
        logger.info("Invalid payment_status records found: %d", (~valid_status_mask).sum())
        df = df[valid_status_mask]

    # Remove any remaining invalid records
    required_cols = [c for c in ["payment_id", "amount"] if c in df.columns]
    df = df.dropna(subset=required_cols)

    final_count = len(df)
    logger.info(
        "Payment cleaning complete. Rows before: %d | Rows after: %d | Removed: %d",
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
    latest_bronze_file = get_latest_bronze_partition(BRONZE_DIR)
    partition_date = latest_bronze_file.stem

    raw_df = read_bronze(latest_bronze_file)
    cleaned_df = clean_payments(raw_df)
    write_silver(cleaned_df, SILVER_DIR, partition_date)


if __name__ == "__main__":
    main()
