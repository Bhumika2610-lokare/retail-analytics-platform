"""
Silver Layer: Customer Cleaning
--------------------------------
Reads the latest Bronze customers partition, applies cleaning rules,
and writes the result to the Silver layer as Parquet.

Source : data/bronze/customers/YYYY-MM-DD.parquet
Output : data/silver/customers/YYYY-MM-DD.parquet
"""

import re
import glob
import logging
from pathlib import Path
from datetime import date

import pandas as pd

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
BRONZE_DIR = Path("data/bronze/customers")
SILVER_DIR = Path("data/silver/customers")

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver.customers")


# --------------------------------------------------------------------------
# Reusable helpers
# --------------------------------------------------------------------------
def get_latest_bronze_partition(bronze_dir: Path) -> Path:
    """Return the most recent YYYY-MM-DD.parquet file in the bronze folder."""
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


def standardize_text(series: pd.Series) -> pd.Series:
    """Trim whitespace and title-case a text column, handling nulls safely."""
    return (
        series.astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )


def is_valid_email(series: pd.Series) -> pd.Series:
    return series.astype("string").str.match(EMAIL_REGEX, na=False)


# --------------------------------------------------------------------------
# Cleaning logic
# --------------------------------------------------------------------------
def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    logger.info("Starting customer cleaning. Initial row count: %d", initial_count)

    df = df.copy()

    # Remove null customer_id
    df = df[df["customer_id"].notna()]
    logger.info("After removing null customer_id: %d rows", len(df))

    # Remove duplicate customer_id (keep first occurrence)
    df = df.drop_duplicates(subset="customer_id", keep="first")
    logger.info("After removing duplicate customer_id: %d rows", len(df))

    # Standardize customer_name
    if "customer_name" in df.columns:
        df["customer_name"] = standardize_text(df["customer_name"])

    # Standardize city
    if "city" in df.columns:
        df["city"] = standardize_text(df["city"])

    # Standardize state
    if "state" in df.columns:
        df["state"] = standardize_text(df["state"])

    # Validate email format
    if "email" in df.columns:
        df["email"] = df["email"].astype("string").str.strip().str.lower()
        valid_email_mask = is_valid_email(df["email"])
        invalid_emails = (~valid_email_mask).sum()
        logger.info("Invalid email records found: %d", invalid_emails)
        df = df[valid_email_mask]

    # Remove any remaining invalid records (missing required fields)
    required_cols = [c for c in ["customer_id", "customer_name", "email"] if c in df.columns]
    df = df.dropna(subset=required_cols)

    final_count = len(df)
    logger.info(
        "Customer cleaning complete. Rows before: %d | Rows after: %d | Removed: %d",
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
    partition_date = latest_bronze_file.stem  # e.g. "2026-08-23"

    raw_df = read_bronze(latest_bronze_file)
    cleaned_df = clean_customers(raw_df)
    write_silver(cleaned_df, SILVER_DIR, partition_date)


if __name__ == "__main__":
    main()
