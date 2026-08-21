"""
Retail Analytics Platform
Returns Generator

Generates daily incremental return data
using delivered orders.
"""

from pathlib import Path
from datetime import datetime
from faker import Faker

import pandas as pd
import random
import uuid


fake = Faker("en_IN")


ORDER_PATH = "data/bronze/orders"

RETURN_PATH = Path(
    "data/bronze/returns"
)

RETURN_PATH.mkdir(
    parents=True,
    exist_ok=True
)


RETURN_REASONS = [
    "Damaged Product",
    "Wrong Item",
    "Defective Product",
    "Customer Changed Mind",
    "Late Delivery",
    "Quality Issue",
    "Size Issue"
]


REFUND_STATUS = [
    "Pending",
    "Approved",
    "Processed",
    "Completed"
]


def get_latest_file(folder):

    files = sorted(
        Path(folder).glob("*.parquet")
    )

    if not files:

        raise FileNotFoundError(
            f"No parquet files found in {folder}"
        )

    return files[-1]


def load_orders():

    latest_file = get_latest_file(
        ORDER_PATH
    )

    return pd.read_parquet(
        latest_file
    )


def generate_returns():

    orders = load_orders()

    delivered_orders = orders[
        orders["order_status"]
        == "Delivered"
    ]

    return_rate = random.uniform(
        0.02,
        0.08
    )

    return_orders = delivered_orders.sample(
        frac=return_rate,
        random_state=42
    )

    returns = []

    for _, order in return_orders.iterrows():

        returns.append(
            {
                "return_id":
                str(uuid.uuid4()),

                "order_id":
                order["order_id"],

                "customer_id":
                order["customer_id"],

                "product_id":
                order["product_id"],

                "return_reason":
                random.choice(
                    RETURN_REASONS
                ),

                "refund_amount":
                round(
                    order["total_amount"],
                    2
                ),

                "refund_status":
                random.choice(
                    REFUND_STATUS
                ),

                "return_date":
                fake.date_this_month(),

                "created_at":
                datetime.now()
            }
        )

    return pd.DataFrame(
        returns
    )


def save_partition(df):

    partition_date = (
        datetime.now()
        .strftime("%Y-%m-%d")
    )

    output_file = (
        RETURN_PATH /
        f"{partition_date}.parquet"
    )

    if output_file.exists():

        existing_df = (
            pd.read_parquet(
                output_file
            )
        )

        df = pd.concat(
            [existing_df, df],
            ignore_index=True
        )

    df.to_parquet(
        output_file,
        index=False
    )

    return (
        output_file,
        len(df)
    )


def main():

    print(
        "\nGenerating Returns..."
    )

    returns_df = (
        generate_returns()
    )

    file_path, total_rows = (
        save_partition(
            returns_df
        )
    )

    print(
        "\nReturns Generation Complete"
    )

    print(
        f"File Saved : {file_path}"
    )

    print(
        f"Total Returns : {total_rows}"
    )


if __name__ == "__main__":
    main()