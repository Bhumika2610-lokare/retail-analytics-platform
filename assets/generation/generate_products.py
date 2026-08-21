"""
Retail Analytics Platform
Product Catalog Generator

Generates daily incremental product data
for the Bronze Layer.
"""

from faker import Faker
import pandas as pd
from pathlib import Path
from datetime import datetime
import uuid
import random


fake = Faker("en_IN")


BRONZE_PATH = Path(
    "data/bronze/products"
)

BRONZE_PATH.mkdir(
    parents=True,
    exist_ok=True
)


CATEGORIES = {
    "Electronics": [
        "Laptop",
        "Mobile Phone",
        "Smart Watch",
        "Tablet",
        "Headphones",
        "Power Bank"
    ],
    "Home Appliances": [
        "Microwave",
        "Refrigerator",
        "Washing Machine",
        "Mixer",
        "Air Conditioner"
    ],
    "Fashion": [
        "Shirt",
        "Jeans",
        "Jacket",
        "Shoes",
        "T-Shirt"
    ],
    "Books": [
        "Novel",
        "Programming Book",
        "Data Science Book",
        "Biography",
        "Business Book"
    ],
    "Sports": [
        "Cricket Bat",
        "Football",
        "Badminton Racket",
        "Gym Gloves",
        "Yoga Mat"
    ]
}


BRANDS = [
    "TechMax",
    "PrimeTech",
    "EliteStore",
    "FutureTech",
    "SmartCart",
    "MegaBrand",
    "UltraGear",
    "NextGen"
]


SUPPLIERS = [
    "ABC Distributors",
    "Prime Suppliers",
    "National Traders",
    "Tech Wholesale",
    "Global Supply Co"
]


def generate_products(
    num_products: int = 500
) -> pd.DataFrame:

    products = []

    for _ in range(num_products):

        category = random.choice(
            list(CATEGORIES.keys())
        )

        product_name = random.choice(
            CATEGORIES[category]
        )

        cost_price = round(
            random.uniform(
                100,
                10000
            ),
            2
        )

        margin_percent = random.randint(
            10,
            50
        )

        selling_price = round(
            cost_price *
            (
                1 +
                margin_percent / 100
            ),
            2
        )

        product = {

            "product_id":
            str(uuid.uuid4()),

            "sku":
            fake.bothify(
                text="SKU-#####"
            ),

            "product_name":
            product_name,

            "category":
            category,

            "brand":
            random.choice(
                BRANDS
            ),

            "supplier":
            random.choice(
                SUPPLIERS
            ),

            "cost_price":
            cost_price,

            "selling_price":
            selling_price,

            "margin_percent":
            margin_percent,

            "stock_quantity":
            random.randint(
                20,
                1000
            ),

            "rating":
            round(
                random.uniform(
                    3.0,
                    5.0
                ),
                1
            ),

            "launch_date":
            fake.date_between(
                start_date="-2y",
                end_date="today"
            ),

            "created_at":
            datetime.now()
        }

        products.append(
            product
        )

    return pd.DataFrame(
        products
    )


def save_partition(
    df: pd.DataFrame
):

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

    return (
        output_file,
        len(df)
    )


def main():

    print(
        "\nGenerating Product Catalog..."
    )

    product_df = (
        generate_products(
            num_products=500
        )
    )

    file_path, total_products = (
        save_partition(
            product_df
        )
    )

    print(
        "\nProduct Generation Complete"
    )

    print(
        f"File Saved : {file_path}"
    )

    print(
        f"Total Products : {total_products}"
    )


if __name__ == "__main__":
    main()