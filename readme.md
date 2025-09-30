# Canadian CPI Dashboard – End-to-End Data Engineering Project

A **production-ready data engineering project** showcasing automated ingestion, cleaning, transformation, and loading of Canada's Consumer Price Index (CPI) data into a PostgreSQL database for interactive analysis and analytics.

This project demonstrates the ability to handle **large, messy real-world datasets**, build reproducible pipelines, and prepare data for **dashboards, analytics, or machine learning workflows**.

---

## Project Overview

This project implements a robust **automated pipeline for monthly CPI data** from Statistics Canada:

* Automatically downloads the latest CPI data from [StatsCan](https://www150.statcan.gc.ca/n1/tbl/csv/18100004-eng.zip).
* Extracts, cleans, and transforms raw CSV files into **tidy, analytical tables**.
* Computes key metrics: **Index Value (`VALUE`)**, **Month-over-Month (`MoM`)**, and **Year-over-Year (`YoY`)**.
* Supports filtering and aggregation by geography (province/city) and CPI categories.
* Loads the processed data into **PostgreSQL / Supabase** for efficient querying and dashboard integration.
* Optimized for **reproducibility and scalability** with modular functions, caching, and logging.

---

## Tech Stack

* **Python 3.11+** – scripting and ETL orchestration.
* **Pandas & NumPy** – data cleaning, transformation, and computation.
* **Requests & Zipfile** – automated download and extraction of raw CSVs.
* **SQLAlchemy** – database integration with PostgreSQL/Supabase.
* **Streamlit & Plotly** – interactive dashboards and visualizations.
* **Data Pipeline Best Practices** – modular functions, caching, validation, incremental ingestion.

---

## Data Pipeline Features

1. **Automated Data Ingestion**

   * Downloads the latest StatsCan CPI dataset only if updated.
   * Extracts CSV from ZIP files programmatically.
   * Stores raw CSVs as **immutable raw data** for reproducibility.

2. **Data Cleaning & Transformation**

   * Converts dates to `datetime` and CPI values to numeric types.
   * Replaces placeholder symbols (`..`, `n/a`, etc.) with NaN.
   * Encodes product names for safe storage and computation.
   * Computes **Month-over-Month (MoM)** and **Year-over-Year (YoY)** changes per product and geography.
   * Forward/backward fills small gaps to handle missing values.
   * Extracts **City** and **Province** from the `GEO` column automatically.

3. **Database Loading**

   * Loads processed data into PostgreSQL/Supabase.
   * Supports **incremental ingestion** for efficient updates of new months.
   * Enables downstream consumption by dashboards or analytics scripts.

4. **Flexible Output**

   * Produces tidy, long-format datasets suitable for visualizations, analytics, or ML workflows.
   * Modular code allows user-defined filters and aggregations.

---

## StatsCan CPI Dataset – Column Overview

| Column                          | Description                                      |
| ------------------------------- | ------------------------------------------------ |
| **REF_DATE**                    | Reference period (monthly, YYYY-MM-DD).          |
| **GEO**                         | Geography (province, city, or national).         |
| **Products and product groups** | CPI category.                                    |
| **UOM**                         | Unit of measure (base year index).               |
| **VALUE**                       | CPI value.                                       |
| **MoM**                         | Month-over-Month percentage change.              |
| **YoY**                         | Year-over-Year percentage change.                |
| **City / Province**             | Normalized location fields (extracted from GEO). |
| **Other Metadata**              | Includes `SYMBOL`, `STATUS`, `DGUID`, etc.       |

---

## How to Run

1. Clone the repository:

```bash
git clone https://github.com/yourusername/cpi-dashboard.git
cd cpi-dashboard
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` or `password` file for your **PostgreSQL/Supabase credentials**.

4. Run the data pipeline located at `flows/cpi_pipeline.py`:

```bash
python flows/cpi_pipeline.py
```

> This step downloads, cleans, computes metrics, extracts City/Province, and loads the data into PostgreSQL/Supabase.

5. **Visualize the data** using the Streamlit dashboard:

```bash
streamlit run streamlitapp.py
```

> You must run `streamlitapp.py` to explore and interact with the processed CPI data. The dashboard is designed as the primary interface for analysis.

---

## Data Engineering Skills Highlighted

* **Automated ETL / Data Pipelines** – downloading, transforming, and loading datasets programmatically.
* **Data Cleaning & Validation** – handling messy real-world datasets with missing values and inconsistent metadata.
* **Metric Computation & Aggregation** – MoM/YoY calculations per product and geography.
* **Reproducible Pipelines** – modular, cached, and idempotent functions for production-ready workflows.
* **Database Integration** – PostgreSQL/Supabase storage for scalable analytics.
* **Interactive Visualization** – Streamlit dashboard as the main interface for exploring the data.

---

## Extensions / Next Steps

* Schedule **automated pipeline runs** with Cron, Airflow, Prefect, or Dagster.
* Implement **incremental ingestion** to append only new monthly data.
* Add **data validation** with `pandera` or `great_expectations`.
* Extend to **forecasting inflation trends** using cleaned CPI datasets.
* Enable **downloadable processed datasets** for downstream analytics or ML projects.
