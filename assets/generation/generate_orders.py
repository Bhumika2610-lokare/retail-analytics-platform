"""
Retail Analytics Platform
Order Generator

Generates daily incremental order data
using existing customers and products.
"""

from pathlib import Path
from datetime import datetime
from faker import Faker

import pandas as pd
import random
import uuid


fake = Faker("en_IN")


CUSTOMER_PATH = "data/bronze/customers"
PRODUCT_PATH = "data/bronze/products"

ORDER_PATH = Path(
    "data/bronze/orders"
)

ORDER_PATH.mkdir(
    parents=True,
    exist_ok=True
)


ORDER_STATUS = [
    "Placed",
    "Confirmed",
    "Packed",
    "Shipped",
    "Delivered",
    "Cancelled"
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


def load_customers():

    latest_file = get_latest_file(
        CUSTOMER_PATH
    )

    return pd.read_parquet(
        latest_file
    )


def load_products():

    latest_file = get_latest_file(
        PRODUCT_PATH
    )

    return pd.read_parquet(
        latest_file
    )


def generate_orders(
    num_orders: int = 5000
):

    customers = load_customers()

    products = load_products()

    orders = []

    for _ in range(num_orders):

        customer = (
            customers.sample(1)
            .iloc[0]
        )

        product = (
            products.sample(1)
            .iloc[0]
        )

        quantity = random.randint(
            1,
            5
        )

        unit_price = float(
            product["selling_price"]
        )

        total_amount = round(
            quantity * unit_price,
            2
        )

        order_status = random.choice(
            ORDER_STATUS
        )

        order = {

            "order_id":
            str(uuid.uuid4()),

            "customer_id":
            customer["customer_id"],

            "product_id":
            product["product_id"],

            "customer_name":
            customer["customer_name"],

            "city":
            customer["city"],

            "state":
            customer["state"],

            "product_name":
            product["product_name"],

            "category":
            product["category"],

            "brand":
            product["brand"],

            "quantity":
            quantity,

            "unit_price":
            unit_price,

            "total_amount":
            total_amount,

            "order_status":
            order_status,

            "payment_status":
            random.choice(
                PAYMENT_STATUS
            ),

            "estimated_delivery_days":
            random.randint(
                1,
                10
            ),

            "order_date":
            fake.date_this_year(),

            "created_at":
            datetime.now()
        }

        orders.append(order)

    return pd.DataFrame(
        orders
    )


def save_partition(df):

    partition_date = (
        datetime.now()
        .strftime("%Y-%m-%d")
    )

    output_file = (
        ORDER_PATH /
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
        "\nGenerating Orders..."
    )

    orders_df = generate_orders(
        num_orders=5000
    )

    file_path, total_rows = (
        save_partition(
            orders_df
        )
    )

    print(
        "\nOrder Generation Complete"
    )

    print(
        f"File Saved : {file_path}"
    )

    print(
        f"Total Orders : {total_rows}"
    )


if __name__ == "__main__":
    main()