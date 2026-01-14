# 🏦 Largest Banks Market Capitalization ETL Pipeline

## 📌 Project Overview
This project is an **ETL (Extract, Transform, Load) pipeline** that collects data on the **world’s largest banks by market capitalization** from Wikipedia, transforms the data using exchange rates, and stores the results in both **CSV files** and a **SQLite database**.

The project demonstrates practical skills in:
- Web scraping
- Data transformation
- Logging
- Modular Python project structure
- SQL querying

---

## 🛠️ Tech Stack
- **Python 3**
- **Requests** – HTTP requests
- **BeautifulSoup (lxml)** – Web scraping
- **Pandas & NumPy** – Data processing
- **SQLite3** – Database storage

---

## 🔄 ETL Workflow

### 1️⃣ Extract
- Scrapes data from archived Wikipedia:
  - Bank rank
  - Country
  - Market capitalization (USD)
- Cleans numeric values and converts them to usable formats

### 2️⃣ Transform
- Reads exchange rates from `exchange_rate.csv`
- Converts market capitalization from USD to:
  - EUR
  - GBP
  - INR
- Rounds values to 2 decimal places

### 3️⃣ Load
- Saves transformed data into:
  - CSV file (`output/Largest_banks_data.csv`)
  - SQLite database (`db/Banks.db`)
- Executes SQL queries for validation and analysis

