import duckdb
import csv
import os

# Write a small local CSV so no internet is needed (offline-first!)
csv_path = "sample_sales.csv"
rows = [
    ["date", "region", "product", "revenue"],
    ["2024-01-01", "Europe", "Analytics Tool", 4200],
    ["2024-01-01", "Asia",   "Analytics Tool", 3100],
    ["2024-01-02", "Europe", "Dashboard",      1800],
    ["2024-01-02", "USA",    "Analytics Tool", 5500],
    ["2024-01-03", "USA",    "Dashboard",      2200],
    ["2024-01-03", "Asia",   "Dashboard",      1400],
    ["2024-01-04", "Europe", "Analytics Tool", 4800],
    ["2024-01-04", "USA",    "Analytics Tool", 6100],
]
with open(csv_path, "w", newline="") as f:
    csv.writer(f).writerows(rows)

conn = duckdb.connect()

result = conn.execute(f"""
    SELECT
        region,
        product,
        SUM(revenue)   AS total_revenue,
        COUNT(*)       AS num_sales,
        AVG(revenue)   AS avg_sale
    FROM read_csv_auto('{csv_path}')
    GROUP BY region, product
    ORDER BY total_revenue DESC
""").fetchdf()

print("Your first DuckDB query — sample sales data:")
print(result.to_string(index=False))
print("\nDuckDB is working. Phase 0 complete.")

os.remove(csv_path)
