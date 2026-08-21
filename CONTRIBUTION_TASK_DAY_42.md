# 🤝 Contribution Task: Day 42 – Silver Layer

Thank you for contributing to the **Retail Analytics Platform** project.

---

# 🎯 Objective

The Bronze Layer is already completed.

Current Bronze Datasets:

```text
customers
products
orders
payments
returns
```

stored as:

```text
data/bronze/<dataset>/YYYY-MM-DD.parquet
```

Your task is to build the **Silver Layer**.

---

# 📂 Folder Structure

Create the following files:

```text
assets/silver/

├── clean_customers.py
├── clean_products.py
├── clean_orders.py
├── clean_payments.py
└── clean_returns.py
```

Output should be written to:

```text
data/silver/

├── customers/
├── products/
├── orders/
├── payments/
└── returns/
```

---

# ⚙️ Requirements

## General Rules

- Read the latest Bronze partition.
- Process data incrementally.
- Save cleaned data as Parquet files.
- Preserve partition date.
- Use Pandas.
- Add logging.
- Print row counts before and after cleaning.
- Create reusable functions.
- Write production-style code.

---

# 🧹 Customer Cleaning

Source:

```text
data/bronze/customers/
```

Tasks:

- Remove duplicate customer_id
- Remove null customer_id
- Standardize customer_name
- Standardize city
- Standardize state
- Validate email format
- Remove invalid records

Output:

```text
data/silver/customers/
```

---

# 🧹 Product Cleaning

Source:

```text
data/bronze/products/
```

Tasks:

- Remove duplicate product_id
- Remove null product_id
- Validate selling_price > 0
- Validate cost_price > 0
- Standardize category names
- Standardize brand names
- Remove invalid records

Output:

```text
data/silver/products/
```

---

# 🧹 Order Cleaning

Source:

```text
data/bronze/orders/
```

Tasks:

- Remove duplicate order_id
- Remove null order_id
- Remove quantity <= 0
- Remove total_amount <= 0
- Standardize order_status
- Validate customer_id exists
- Validate product_id exists

Output:

```text
data/silver/orders/
```

---

# 🧹 Payment Cleaning

Source:

```text
data/bronze/payments/
```

Tasks:

- Remove duplicate payment_id
- Remove null payment_id
- Validate amount > 0
- Standardize payment_method
- Standardize payment_status
- Remove invalid records

Output:

```text
data/silver/payments/
```

---

# 🧹 Returns Cleaning

Source:

```text
data/bronze/returns/
```

Tasks:

- Remove duplicate return_id
- Remove null return_id
- Validate refund_amount > 0
- Standardize return_reason
- Standardize refund_status
- Remove invalid records

Output:

```text
data/silver/returns/
```

---

# ✅ Expected Deliverables

Provide:

1. Complete code for all five Silver files.
2. Exact file 