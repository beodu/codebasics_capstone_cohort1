# Dashboard

Local (same Gold tables):

```bash
python3 run.py serve    # http://127.0.0.1:8080
```

Visuals: revenue by month, top products, top cities, new vs returning.
Filters: date from/to and category.

Net revenue = not Cancelled. Returned is a separate number.

## Fabric

Gold path: `abfss://lakehouse@<storage>.dfs.core.windows.net/gold/`

1. Lakehouse → New shortcut → ADLS Gen2 → that `gold` folder
2. Don’t copy the data
3. Relationships: fact_sales → dim_customer / dim_product / dim_date on the keys
4. Mark dim_date as the date table

## Model relationships

| From | To | Cardinality |
|---|---|---|
| `fact_sales[customer_id]` | `dim_customer[customer_id]` | many-to-one, single direction |
| `fact_sales[product_id]` | `dim_product[product_id]` | many-to-one, single direction |
| `fact_sales[date_key]` | `dim_date[date_key]` | many-to-one, single direction |
| `fact_marketing_spend[date_key]` | `dim_date[date_key]` | many-to-one, single direction |

Mark `dim_date` as the date table on `full_date`, so time intelligence works.

## Measures

```dax
Gross Revenue = SUM(fact_sales[gross_revenue])

Net Revenue =
CALCULATE([Gross Revenue], fact_sales[is_revenue] = TRUE)

Returned Revenue =
CALCULATE([Gross Revenue], fact_sales[is_returned] = TRUE)

Orders = DISTINCTCOUNT(fact_sales[order_id])

Units Sold = SUM(fact_sales[quantity])

Average Order Value = DIVIDE([Net Revenue], [Orders])

Gross Profit = SUM(fact_sales[gross_profit])

New Customers =
CALCULATE(
    DISTINCTCOUNT(dim_customer[customer_id]),
    FILTER(
        dim_customer,
        FORMAT(dim_customer[signup_date], "yyyy-MM") = MAX(dim_date[year_month])
    )
)

Returning Customers = [Active Customers] - [New Customers]

Active Customers = DISTINCTCOUNT(fact_sales[customer_id])
```

## Visuals

1. Revenue by month (net)
2. Top products
3. Top cities
4. New vs returning by month

Slicers: date range, category.
- `fact_sales[status]` — dropdown

## Check your tiles against known-good numbers

Before you trust a single visual, compare it to
[`reference_numbers.md`](reference_numbers.md) — figures computed from the same seed
data by `tools/local_pipeline_harness.py`. The three worth checking first:

| Tile | Expected |
|---|---:|
| Net Revenue card | 1,745,772.00 |
| Revenue trend, Jan 2026 | 371,750.00 |
| Top city (Jaipur) | 321,516.00 |

If Net Revenue reads 2,076,041.00, the `is_revenue` filter is not applied and you are
showing gross. That is the single most likely mistake in this build.

## Revenue definition (put this on the canvas as a text box)

> Revenue excludes Cancelled orders. Returned orders are shown separately and are not
> deducted from Net Revenue. Grain: one row per order item. Data refreshed nightly at
> 01:00 IST.

Stating the definition on the dashboard is what stops two people reading the same
number and meaning different things by it.
