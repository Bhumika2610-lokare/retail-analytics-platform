"""
Silver Layer: Returns Cleaning
--------------------------------
Reads the latest Bronze returns partition, applies cleaning rules,
and writes the result to the Silver layer as Parquet.

Source : data/bronze/returns/YYYY-MM-DD.parquet
Output : data/silver/returns/YYYY-MM-DD.parquet
"""

import glob
import logging
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
BRONZE_DIR = Path("data/bronze/returns")
SILVER_DIR = Path("data/silver/returns")

VALID_RETURN_REASONS = {
    "damaged", "defective", "wrong_item", "size_issue",
    "not_as_described", "changed_mind", "late_delivery", "other",
}
VALID_REFUND_STATUSES = {"pending", "approved", "rejected", "completed"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver.returns")


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
def clean_returns(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    logger.info("Starting returns cleaning. Initial row count: %d", initial_count)

    df = df.copy()

    # Remove null return_id
    df = df[df["return_id"].notna()]
    logger.info("After removing null return_id: %d rows", len(df))

    # Remove duplicate return_id
    df = df.drop_duplicates(subset="return_id", keep="first")
    logger.info("After removing duplicate return_id: %d rows", len(df))

    # Validate refund_amount > 0
    if "refund_amount" in df.columns:
        df["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="coerce")
        df = df[df["refund_amount"] > 0]
        logger.info("After validating refund_amount > 0: %d rows", len(df))

    # Standardize return_reason
    if "return_reason" in df.columns:
        df["return_reason"], valid_reason_mask = standardize_category(
            df["return_reason"], VALID_RETURN_REASONS
        )
        logger.info("Invalid return_reason records found: %d", (~valid_reason_mask).sum())
        df = df[valid_reason_mask]

    # Standardize refund_status
    if "refund_status" in df.columns:
        df["refund_status"], valid_status_mask = standardize_category(
            df["refund_status"], VALID_REFUND_STATUSES
        )
        logger.info("Invalid refund_status records found: %d", (~valid_status_mask).sum())
        df = df[valid_status_mask]

    # Remove any remaining invalid records
    required_cols = [c for c in ["return_id", "refund_amount"] if c in df.columns]
    df = df.dropna(subset=required_cols)

    final_count = len(df)
    logger.info(
        "Returns cleaning complete. Rows before: %d | Rows after: %d | Removed: %d",
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
    cleaned_df = clean_returns(raw_df)
    write_silver(cleaned_df, SILVER_DIR, partition_date)


if __name__ == "__main__":
    main()
