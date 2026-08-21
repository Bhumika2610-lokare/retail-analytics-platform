"""
Retail Analytics Platform
Payment Generator

Generates daily incremental payment data
using existing order data.
"""

from pathlib import Path
from datetime import datetime
from faker import Faker

import pandas as pd
import random
import uuid


fake = Faker("en_IN")


ORDER_PATH = "data/bronze/orders"

PAYMENT_PATH = Path(
    "data/bronze/payments"
)

PAYMENT_PATH.mkdir(
    parents=True,
    exist_ok=True
)


PAYMENT_METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet"
]


PAYMENT_STATUS = [
    "Success",
    "Pending",
    "Failed",
    "Refunded"
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


def generate_payments():

    orders = load_orders()

    payments = []

    for _, order in orders.iterrows():

        payment_status = random.choices(
            PAYMENT_STATUS,
            weights=[
                85,
                7,
                5,
                3
            ]
        )[0]

        payments.append(
            {
                "payment_id":
                str(uuid.uuid4()),

                "transaction_id":
                fake.bothify(
                    text="TXN########"
                ),

                "order_id":
                order["order_id"],

                "customer_id":
                order["customer_id"],

                "payment_method":
                random.choice(
                    PAYMENT_METHODS
                ),

                "payment_status":
                payment_status,

                "amount":
                order["total_amount"],

                "payment_date":
                order["order_date"],

                "created_at":
                datetime.now()
            }
        )

    return pd.DataFrame(
        payments
    )


def save_partition(df):

    partition_date = (
        datetime.now()
        .strftime("%Y-%m-%d")
    )

    output_file = (
        PAYMENT_PATH /
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
        "\nGenerating Payments..."
    )

    payment_df = (
        generate_payments()
    )

    file_path, total_rows = (
        save_partition(
            payment_df
        )
    )

    print(
        "\nPayment Generation Complete"
    )

    print(
        f"File Saved : {file_path}"
    )

    print(
        f"Total Payments : {total_rows}"
    )


if __name__ == "__main__":
    main()