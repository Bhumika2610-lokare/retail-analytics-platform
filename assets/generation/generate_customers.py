"""
Retail Analytics Platform
Customer Data Generator

Generates daily incremental customer data
and stores it inside Bronze Layer.
"""

from faker import Faker
import pandas as pd
from pathlib import Path
from datetime import datetime
import uuid
import random


fake = Faker("en_IN")


BRONZE_PATH = Path(
    "data/bronze/customers"
)

BRONZE_PATH.mkdir(
    parents=True,
    exist_ok=True
)


def generate_customers(
    num_records: int = 1000
) -> pd.DataFrame:

    customers = []

    loyalty_levels = [
        "Bronze",
        "Silver",
        "Gold",
        "Platinum"
    ]

    for _ in range(num_records):

        customer = {
            "customer_id":
            str(uuid.uuid4()),

            "customer_name":
            fake.name(),

            "email":
            fake.email(),

            "phone":
            fake.phone_number(),

            "city":
            fake.city(),

            "state":
            fake.state(),

            "country":
            "India",

            "loyalty_level":
            random.choice(
                loyalty_levels
            ),

            "signup_date":
            fake.date_between(
                start_date="-2y",
                end_date="today"
            ),

            "created_at":
            datetime.now()
        }

        customers.append(customer)

    return pd.DataFrame(
        customers
    )


def save_partition(df):

    partition_date = (
        datetime.now()
        .strftime("%Y-%m-%d")
    )

    output_file = (
        BRONZE_PATH /
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

    return output_file, len(df)


def main():

    print(
        "\nGenerating customer data..."
    )

    customer_df = (
        generate_customers(
            num_records=1000
        )
    )

    file_path, total_rows = (
        save_partition(customer_df)
    )

    print(
        "\nCustomer Generation Complete"
    )

    print(
        f"File: {file_path}"
    )

    print(
        f"Total Rows: {total_rows}"
    )


if __name__ == "__main__":
    main()