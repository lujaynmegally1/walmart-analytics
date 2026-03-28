"""
Walmart Analytics — Python Visualizations

Connects to Snowflake and generates 10 charts using matplotlib, seaborn, and plotly.

NOTE: Uses the pre-joined gold table WALMART.PUBLIC_GOLD.WALMART_JOINED_REDUCED
for performance. Full 3-table join on the raw bronze tables creates billions of rows
and is not feasible to pull into pandas locally.

Run the gold table setup in snowflake/filter-data-for-local-python-querying.sql
before executing this script.
"""

from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import plotly.express as px
import warnings

warnings.filterwarnings("ignore")

# Snowflake Connection

engine = create_engine(
    "snowflake://{user}:{password}@{account}/{database}/{schema}"
    "?warehouse={warehouse}&role={role}".format(
        user="lujymeg",
        password="YOUR_PASSWORD_HERE",   # Replace or use env variable
        account="SIOROVO-WYB67513",
        warehouse="COMPUTE_WH",
        database="WALMART",
        schema="PUBLIC_GOLD",
        role="ACCOUNTADMIN",
    )
)

print("Connecting to Snowflake and loading data...")
query = "SELECT * FROM WALMART.PUBLIC_GOLD.WALMART_JOINED_REDUCED"
df = pd.read_sql(query, engine)
print(f"Loaded {len(df):,} rows.")


# Data Prep


df["store_weekly_sales"] = pd.to_numeric(df["store_weekly_sales"], errors="coerce")
df["store_date"] = pd.to_datetime(df["store_date"])
df["year"] = df["store_date"].dt.year
df["month"] = df["store_date"].dt.month
df["month_name"] = df["store_date"].dt.strftime("%b")
df["isholiday"] = df["isholiday"].astype(str).str.upper().map({"TRUE": "Holiday", "FALSE": "Non-Holiday"})

sns.set_theme(style="whitegrid")
palette = sns.color_palette("Set2")

# Chart 1: Weekly Sales by Store and Holiday (Stacked Bar)

print("Chart 1: Weekly Sales by Store and Holiday...")
sales_holiday = (
    df.groupby(["store_id", "isholiday"])["store_weekly_sales"]
    .sum()
    .unstack(fill_value=0)
)
fig, ax = plt.subplots(figsize=(14, 6))
sales_holiday.plot(kind="bar", stacked=True, ax=ax, color=palette[:2])
ax.set_title("Weekly Sales by Store and Holiday", fontsize=14, fontweight="bold")
ax.set_xlabel("Store ID")
ax.set_ylabel("Total Weekly Sales ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
ax.tick_params(axis="x", rotation=45)
ax.legend(title="Period", bbox_to_anchor=(1.01, 1))
plt.tight_layout()
plt.savefig("chart1_sales_by_store_holiday.png", dpi=150)
plt.show()


# Chart 2: Weekly Sales by Temperature and Year (Scatter)


print("Chart 2: Weekly Sales by Temperature and Year...")
fig, ax = plt.subplots(figsize=(10, 6))
for year, grp in df.groupby("year"):
    ax.scatter(grp["store_temperature"], grp["store_weekly_sales"], alpha=0.3, label=str(year), s=10)
ax.set_title("Weekly Sales by Temperature and Year", fontsize=14, fontweight="bold")
ax.set_xlabel("Temperature (°F)")
ax.set_ylabel("Weekly Sales ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
ax.legend(title="Year")
plt.tight_layout()
plt.savefig("chart2_sales_by_temp_year.png", dpi=150)
plt.show()


# Weekly Sales by Store Size (Scatter)


print("Chart 3: Weekly Sales by Store Size...")
size_sales = df.groupby("store_size")["store_weekly_sales"].mean().reset_index()
fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(size_sales["store_size"], size_sales["store_weekly_sales"], color=palette[2], alpha=0.7, s=60)
ax.set_title("Average Weekly Sales by Store Size", fontsize=14, fontweight="bold")
ax.set_xlabel("Store Size (sq ft)")
ax.set_ylabel("Avg Weekly Sales ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
plt.tight_layout()
plt.savefig("chart3_sales_by_store_size.png", dpi=150)
plt.show()

# Chart 4: Weekly Sales by Store Type and Month (Grouped Bar)

print("Chart 4: Weekly Sales by Store Type and Month...")
type_month = (
    df.groupby(["store_type", "month"])["store_weekly_sales"]
    .sum()
    .reset_index()
)
fig = px.bar(
    type_month,
    x="month",
    y="store_weekly_sales",
    color="store_type",
    barmode="group",
    title="Weekly Sales by Store Type and Month",
    labels={"store_weekly_sales": "Total Weekly Sales ($)", "month": "Month", "store_type": "Store Type"},
)
fig.update_layout(xaxis=dict(tickmode="linear"))
fig.write_html("chart4_sales_by_type_month.html")
fig.show()


# Chart 5: Markdown Sales by Year and Store (Line)


print("Chart 5: Markdown Sales by Year and Store...")
markdown_cols = ["markdown1", "markdown2", "markdown3", "markdown4", "markdown5"]
for col in markdown_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
df["total_markdown"] = df[markdown_cols].sum(axis=1)

md_year_store = (
    df.groupby(["year", "store_id"])["total_markdown"]
    .sum()
    .reset_index()
)
fig, ax = plt.subplots(figsize=(12, 6))
for store_id, grp in md_year_store.groupby("store_id"):
    ax.plot(grp["year"], grp["total_markdown"], marker="o", linewidth=1, alpha=0.6)
ax.set_title("Total Markdown by Year per Store", fontsize=14, fontweight="bold")
ax.set_xlabel("Year")
ax.set_ylabel("Total Markdown ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
plt.tight_layout()
plt.savefig("chart5_markdown_by_year_store.png", dpi=150)
plt.show()


# Chart 6: Weekly Sales by Store Type (Box Plot)


print("Chart 6: Weekly Sales by Store Type...")
fig, ax = plt.subplots(figsize=(8, 6))
sns.boxplot(data=df, x="store_type", y="store_weekly_sales", palette="Set2", ax=ax)
ax.set_title("Weekly Sales Distribution by Store Type", fontsize=14, fontweight="bold")
ax.set_xlabel("Store Type")
ax.set_ylabel("Weekly Sales ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
plt.tight_layout()
plt.savefig("chart6_sales_by_store_type.png", dpi=150)
plt.show()

# Chart 7: Fuel Price by Year (Line)

print("Chart 7: Fuel Price by Year...")
df["fuel_price"] = pd.to_numeric(df["fuel_price"], errors="coerce")
fuel_year = df.groupby("year")["fuel_price"].mean().reset_index()
fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(fuel_year["year"], fuel_year["fuel_price"], marker="o", color=palette[3], linewidth=2)
ax.set_title("Average Fuel Price by Year", fontsize=14, fontweight="bold")
ax.set_xlabel("Year")
ax.set_ylabel("Avg Fuel Price ($)")
plt.tight_layout()
plt.savefig("chart7_fuel_price_by_year.png", dpi=150)
plt.show()


# Chart 8: Weekly Sales Over Time (Time Series)


print("Chart 8: Weekly Sales Over Time...")
time_series = df.groupby("store_date")["store_weekly_sales"].sum().reset_index()
fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(time_series["store_date"], time_series["store_weekly_sales"], linewidth=1.2, color=palette[0])
ax.set_title("Total Weekly Sales Over Time", fontsize=14, fontweight="bold")
ax.set_xlabel("Date")
ax.set_ylabel("Total Weekly Sales ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.0f}M"))
plt.tight_layout()
plt.savefig("chart8_sales_over_time.png", dpi=150)
plt.show()


# Chart 9: Weekly Sales by CPI (Scatter)


print("Chart 9: Weekly Sales by CPI...")
df["cpi"] = pd.to_numeric(df["cpi"], errors="coerce")
cpi_sales = df.groupby("cpi")["store_weekly_sales"].mean().reset_index()
fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(cpi_sales["cpi"], cpi_sales["store_weekly_sales"], alpha=0.5, color=palette[4], s=20)
ax.set_title("Average Weekly Sales by CPI", fontsize=14, fontweight="bold")
ax.set_xlabel("CPI")
ax.set_ylabel("Avg Weekly Sales ($)")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))
plt.tight_layout()
plt.savefig("chart9_sales_by_cpi.png", dpi=150)
plt.show()


# Chart 10: Department-wise Weekly Sales (Horizontal Bar)


print("Chart 10: Department-wise Weekly Sales...")
dept_sales = (
    df.groupby("dept_id")["store_weekly_sales"]
    .sum()
    .sort_values(ascending=True)
    .tail(20)   # Top 20 departments for readability
    .reset_index()
)
fig, ax = plt.subplots(figsize=(10, 10))
ax.barh(dept_sales["dept_id"].astype(str), dept_sales["store_weekly_sales"], color=palette[1])
ax.set_title("Top 20 Departments by Total Weekly Sales", fontsize=14, fontweight="bold")
ax.set_xlabel("Total Weekly Sales ($)")
ax.set_ylabel("Department ID")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.0f}M"))
plt.tight_layout()
plt.savefig("chart10_sales_by_dept.png", dpi=150)
plt.show()

print("\nAll charts saved successfully.")
