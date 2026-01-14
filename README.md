# 📦 Banking Data Warehouse & Automated Pipeline – Yohan

This project is the final task to build an end-to-end Data Warehouse that includes:
- Processing raw source data (CSV/XLSX)
- ETL pipeline using Python
- Creating staging, warehouse, and data mart layers using SQL
- Workflow automation using Apache Airflow
- Containerization using Docker for consistent and portable deployment

The repository structure is designed to be modular, easy to understand, and reusable.

---

## 📁 Project Folder Overview

- **AIRFLOW/**  
  Contains DAGs and supporting scripts used to orchestrate the ETL workflow in Apache Airflow.

- **DATA SOURCE/**  
  Stores the raw source files used as ETL inputs.

- **PYTHON SCRIPT/**  
  Contains Python scripts for the Extract, Transform, and Load processes into the Data Warehouse.

- **SQL/**  
  Contains SQL scripts for creating staging tables, warehouse tables, data marts, and stored procedures.

---

## 🐳 Docker Implementation

This project uses **Docker** to ensure a consistent development and execution environment, and to simplify setup without manually installing dependencies.

### **What is containerized?**

1. **Apache Airflow**  
   Airflow runs in multiple containers:
   - webserver  
   - scheduler  
   - postgres (metadata database)  
   - airflow-init  

2. **Python ETL Environment**  
   All ETL scripts run inside the Airflow container, ensuring Python dependencies stay isolated from the local OS.

### **Docker Files Used**
- **docker-compose.yml**  
  Manages Airflow services (scheduler, webserver, worker, postgres).

---

## 📊 Data Pipeline Flow

The pipeline processes two types of source data:

1. **Database `db.sample`**  
   → Assumed to be the main transactional database.

2. **CSV and Excel Files**  
   → Supporting data sources that are also processed.

The ETL flow is built using Python and executed through Airflow.  
Below is the full end-to-end process:

### 1. **Extract (Python → Airflow)**
- Pulls data from the `db.sample` database.  
- Reads CSV and Excel files from the `DATA SOURCE/` directory.

### 2. **Load into STAGING**
- Extracted data is loaded into staging tables.  
- This step stores cleaned raw data without any business logic.

### 3. **Transform & Load into CORE (Warehouse)**
- Python scripts process data from staging → core.  
- Data is structured into **dimension** and **fact** tables.

### 4. **Load into MART**
The mart layer contains two main tables:
- `mart.CustomerMart`  
- `mart.DailySummaryMart`

These tables are final outputs used for reporting and analytics.

### 5. **Stored Procedure (SP)**
- SP only retrieves data from the mart layer.  
- No heavy joins or transformations are done here since everything is pre-processed upstream.

---
