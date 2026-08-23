"""
Silver Layer: Product Cleaning
--------------------------------
Reads the latest Bronze products partition, applies cleaning rules,
and writes the result to the Silver layer as Parquet.

Source : data/bronze/products/YYYY-MM-DD.parquet
Output : data/silver/products/YYYY-MM-DD.parquet
"""

import glob
import logging
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
BRONZE_DIR = Path("data/bronze/products")
SILVER_DIR = Path("data/silver/products")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver.products")


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


def standardize_text(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )


# --------------------------------------------------------------------------
# Cleaning logic
# --------------------------------------------------------------------------
def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    initial_count = len(df)
    logger.info("Starting product cleaning. Initial row count: %d", initial_count)

    df = df.copy()

    # Remove null product_id
    df = df[df["product_id"].notna()]
    logger.info("After removing null product_id: %d rows", len(df))

    # Remove duplicate product_id
    df = df.drop_duplicates(subset="product_id", keep="first")
    logger.info("After removing duplicate product_id: %d rows", len(df))

    # Validate selling_price > 0
    if "selling_price" in df.columns:
        df["selling_price"] = pd.to_numeric(df["selling_price"], errors="coerce")
        df = df[df["selling_price"] > 0]
        logger.info("After validating selling_price > 0: %d rows", len(df))

    # Validate cost_price > 0
    if "cost_price" in df.columns:
        df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
        df = df[df["cost_price"] > 0]
        logger.info("After validating cost_price > 0: %d rows", len(df))

    # Standardize category names
    if "category" in df.columns:
        df["category"] = standardize_text(df["category"])

    # Standardize brand names
    if "brand" in df.columns:
        df["brand"] = standardize_text(df["brand"])

    # Remove any remaining invalid records
    required_cols = [c for c in ["product_id", "selling_price", "cost_price"] if c in df.columns]
    df = df.dropna(subset=required_cols)

    final_count = len(df)
    logger.info(
        "Product cleaning complete. Rows before: %d | Rows after: %d | Removed: %d",
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
    cleaned_df = clean_products(raw_df)
    write_silver(cleaned_df, SILVER_DIR, partition_date)


if __name__ == "__main__":
    main()
