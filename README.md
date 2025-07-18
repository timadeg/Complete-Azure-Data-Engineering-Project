# End-to-End Azure NHS Prescription Data Project

This project demonstrates an end-to-end data engineering pipeline on Azure, focusing on the analysis of **NHS English Prescribing Data (EPD)** from 2020 to 2024. The goal was to extract, transform, and load (ETL) monthly prescribing datasets from the NHS open data API into Azure Data Lake, process and clean the data in Synapse Spark, and build insightful analytics in Power BI.

---

## 🧠 Business Objective

To understand prescribing trends, drug usage patterns, and regional cost distribution of NHS medicines across England using real public health data. The project provides a reproducible framework for handling large-scale healthcare data using modern Azure tools.

---

## 💾 Data Sources

- NHSBSA Open Data Portal:  
  `https://opendata.nhsbsa.net/`  
- Dataset: **English Prescribing Data (EPD)**  
- Time Range: **2020–2024**

---

## 🚀 Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Data Factory | Automated REST API ingestion to ADLS |
| Azure Data Lake Storage Gen2 | Centralized cloud data storage |
| Azure Synapse Analytics | Data cleaning & transformation using Spark notebooks |
| Power BI | Data visualization and interactive dashboard |

---

## 1️⃣ Data Ingestion

Using **Azure Data Factory**, we performed the following:

- Parameterized REST API calls to fetch monthly prescribing datasets via SQL queries
- Looped through resource IDs for all months between 2020–2024
- Saved each dataset as a `.csv` file into ADLS "raw" zone

> Sample API structure used:  
> `https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_202401&sql=SELECT * FROM "EPD_202401" WHERE pco_code='13T00' AND bnf_chemical_substance='0407010H0'`

---

## 2️⃣ Data Cleaning (Synapse Spark)

After ingestion, the data was cleaned in **Synapse Spark Notebooks**:

- Raw CSVs contained all data in a single column
- Used PySpark to split columns, remove nulls, and cast data types
- Extracted `Year` and `Month` from the `YEAR_MONTH` column
- Created derived columns such as `Cost per Item` and `Cost per Quantity`

Final cleaned data was saved into a **clean zone** in ADLS as Parquet files.

---

## 3️⃣ Data Modeling & Analytics (Power BI)

The clean data was connected to Power BI via **Azure Data Lake**.

### 📊 Key KPIs:

- Total Items Prescribed
- Total Net Ingredient Cost (NIC)
- Total Actual Cost
- Cost per Item
- Most Prescribed Chemicals
- Cost by Region, Practice, ICB

### 📈 Visualizations:

- Line charts for prescribing trends over time
- Bar charts for Top 10 BNF Substances by cost
- Tree map of costs by ICB and Regional Office
- Slicers for Year, Month, Practice, Chemical
- Map of prescribing cost by location

---

## 🧪 Final Dashboard Highlights

- Prescribing patterns from 2020 to 2024
- Dynamic insights by region, chemical, or time
- Easily extensible pipeline for future months or filters

---

## 🧩 Challenges & Notes

- NHS API returned raw data in one-column CSVs — required parsing in Spark
- Some columns had inconsistent types (e.g., cost as string)
- Data volume was large but manageable in Spark with optimized file formats (Parquet)

---

## ✅ Conclusion

This project shows how to build a **real-world, scalable healthcare data pipeline on Azure**, using modern data engineering techniques:

- Automated ingestion via REST API
- Spark-based cleaning in Synapse
- Interactive BI via Power BI and ADLS

---

## 📁 Folder Structure (Recommended)
