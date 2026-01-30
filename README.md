# 🚗  Transportation Service company Analytics Data Pipeline

## Project Overview
This project demonstrates an **End-to-End data engineering workflow/Business Intelligence**, covering:  
- **Data Ingestion**  
- **Data Transformation**  
- **Data Modeling**  
- **Data Visualization**  

## It reflects real-world startup needs and shows how data can be **turned into actionable insights** through a well-designed pipeline.

---
### Project Workflow

 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/work%20flow.png)

 
## 🏗 Architecture
- **Medallion Architecture:** Bronze → Silver → Gold  
  - **Bronze:** Raw data ingestion  
  - **Silver:** Cleaned and transformed data  
  - **Gold:** Analytics-ready, aggregated data for BI

---

## 💻 Platform
- **Databricks** with **Delta Lake**

---

## 🛠 Tools
- **PySpark** – Data processing and transformations  
- **dbt** – Data modeling and transformations  
- **Power BI** – Data visualization and dashboards

---

## 📊 Key Features
- End-to-end pipeline from raw data to dashboards  
- Demonstrates best practices in **data engineering and analytics**  
- Supports scalable and maintainable workflows suitable for startups

# 🚚 Transportation Service Data Pipeline

## Project Overview
This project demonstrates an **end-to-end data engineering workflow**, covering:  
- **Data Ingestion**  
- **Data Transformation**  
- **Data Modeling**  
- **Data Visualization**  

It reflects real-world startup needs and shows how data can be **turned into actionable insights** through a well-designed pipeline.

---

## 🏗 Architecture
- **Medallion Architecture:** Bronze → Silver → Gold  
  - **Bronze:** Raw data ingestion  
  - **Silver:** Cleaned and curated datasets  
  - **Gold:** Analytics-ready tables for BI and dashboards

---

## 💻 Platform
- **Databricks** with **Delta Lake**

---

## 🛠 Tools
- **PySpark** – Data processing and transformations  
- **dbt** – Data modeling and transformations  
- **Power BI** – Data visualization and dashboards

---

## 📦 Phase 1: Bronze Layer – Raw Data Ingestion

**Implementation Approach (Spark Structured Streaming Pattern):**  
- Two-step process: **batch schema discovery** → **streaming ingestion**  
- Read sample files in **batch mode** to infer schemas (columns & data types)  
- Configure streaming to continuously monitor **source folders** using predefined schemas  
- Streaming **automatically detects and processes new files** as they arrive  

**Key Features Implemented:**  
1. **Automated File Detection**  
2. **Checkpoint-Based Processing**  
3. **Trigger-Once Pattern**  
4. **Schema Enforcement**

## Bronze Layer Ingestion:

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/Bronze_ingestion_code.png)


---

## 📦 Phase 2: Silver Layer – Data Transformation & Curation

**Objective:**  
Transform raw Bronze data into **clean, deduplicated, business-ready datasets** with consistent quality and change tracking.  

**What Was Built:**  

**Core Infrastructure – Reusable Transformation Class:**  
- Developed a Python class `transformations` with three generic, reusable methods:

1. **Deduplication Method**  
   - Removes duplicate records, keeping only the most recent version  

2. **Timestamp Tracking Method**  
   - Adds metadata for **audit trail** and **incremental processing support**  

3. **Smart Upsert Method**  
   - Intelligently merges **new/updated data** into Silver tables  

**First Run Detection:**  
- Checks if target table exists:  
  - **No →** Creates table with **initial full load**  
  - **Yes →** Performs **incremental merge**

 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/class%201.png)
 
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/class%202.png)
 
  ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/class_implementation.png)
---


### Phase 3: Gold Layer - Analytics & Historical Tracking(DBT)
## dim_customers
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/dim%20cust.png)
 
## dim_date
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/dim%20date.png)
 
 ## dim_drivers
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/dim%20driver.png)
 
 ## din_Vehicles
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/vehicle%20dim.png)
 
 ## fact_payments
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/fact_payments.png)
 
 ## fact_trips
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/fact_trips.png)

### Data Quality Assurance

## dbt tests ensure data quality by automatically validating assumptions, detecting anomalies, and preventing bad data from propagating through the pipeline

## Geneeric Tests
 ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/generic%20tests.png)

## Singular Tests
  ![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/singular_tests1.png)
  
![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/singular_tests2.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/Assigning%20tests%20to%20models.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/testing_in_dbt.png)


### Macros to assign Models to Gold Schema schema
![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/macros%20.png)


### Snapshots to track historical changed (SCD2) 

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/snapshots.png)


### Data Lineage 

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/data%20lineage%201.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/data%20lineage%202.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/data%20lineage%203.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/data%20lineage%204.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/b6edd74fcee61d82ed3cfc6ec70a817064b100da/schreenshots/data%20lineage%205.png)



### Phase 4: Scheduling and Monitoring: 


## Databricks handles scheduling Bronze and Silver layer
![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/f6b4a8f442e4841890af937c90a638aaea341142/schreenshots/databricks%20scheduling.png)

## DBT cloud handles scheduling Gold layer
![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/f6b4a8f442e4841890af937c90a638aaea341142/schreenshots/DBT-schedule-gold_layer.png)



### Phase 5:BI Layer - Business Intelligence & Visualization


## STAR SCHEMA
![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/f6b4a8f442e4841890af937c90a638aaea341142/schreenshots/star%20schema.png)

## DASHBOARDS
![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/f6b4a8f442e4841890af937c90a638aaea341142/power%20bi/Executive%20Overview.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/f6b4a8f442e4841890af937c90a638aaea341142/power%20bi/Operations%20%26%20Driver%20Performance.png)

![image alt](https://github.com/majedabdualla/Transportation-Analytics-Data-Pipeline-Databricks-pyspark-dbt-Power-BI-/blob/f6b4a8f442e4841890af937c90a638aaea341142/power%20bi/Customer%20Behavior%20%26%20Payment%20Health.png)


### Business Insights & Business Impact &  Recommendations :

1. **Payment Crisis**  
   - **Insight:** 55% of payments are failing or pending (industry benchmark: 5–10%)  
   - **Impact:** Major revenue loss; delayed driver payouts; dissatisfaction  
   - **Recommendation:** Implement smart retries for soft declines, especially around peak paydays (1st & 15th)

2. **Sedan Dominance**  
   - **Insight:** Sedans are the most used vehicle type and top revenue contributor  
   - **Impact:** Fleet heavily optimized for commuter demand  
   - **Recommendation:**  
     - **Tiered Pricing:** Economy vs Comfort sedans to increase revenue  
     - **Preventive Maintenance:** Prioritize sedans to reduce downtime

3. **Geography Gap**  
   - **Insight:** "Brandiport" leads revenue; most drivers based in North Shelly Berg  
   - **Impact:** High deadheading as drivers travel empty to demand areas  
   - **Recommendation:** Send demand heatmap alerts before shifts to guide drivers to Brandiport

4. **August Peak & 2024 Surge**  
   - **Insight:** August highest-revenue month; 2024 surge in new signups  
   - **Impact:** Strong seasonality; summer demand spike  
   - **Recommendation:**  
     - **Driver Incentives:** Boost bonuses in July for full fleet capacity in August  
     - **Re-engagement:** Target 2024 signups with 1-year anniversary discounts

5. **User Demographics Signal**  
   - **Insight:** Hotmail/Yahoo users outnumber Gmail by ~50%  
   - **Impact:** User base skews older/traditional  
   - **Recommendation:**  
     - **Accessibility First:** Optimize app and emails for simplicity & older browsers  
     - **Trust Messaging:** Emphasize safety, vetted drivers, fixed pricing

---


## Author : Maged Abdulla - Aspiring data Engineer & BI Developer
## Likedin: https://www.linkedin.com/in/majed-abdulla-39ba2438b/
## Portofolio:https://www.datascienceportfol.io/majedabdu29



