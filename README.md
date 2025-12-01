# 📦 Data Warehouse Final Task – Yohan

Proyek ini merupakan final task untuk membangun end-to-end Data Warehouse yang meliputi:
- Pengolahan data sumber (CSV/XLSX)
- Proses ETL dengan Python
- Pembuatan staging, warehouse, dan data mart dengan SQL
- Workflow automation menggunakan Apache Airflow
- Containerization menggunakan Docker agar deployment lebih konsisten dan portable

Struktur repository dibuat modular agar mudah dipahami dan direplikasi.

## 📁 Project Folder Overview

- **AIRFLOW/**  
  Berisi DAG dan script pendukung yang digunakan untuk mengatur workflow ETL di Apache Airflow.

- **DATA SOURCE/**  
  Menyimpan file sumber mentah yang digunakan sebagai input ETL.

- **PYTHON SCRIPT/**  
  Berisi script Python untuk proses Extract, Transform, dan Load menuju Data Warehouse.

- **SQL/**  
  Berisi SQL script untuk membuat tabel staging, warehouse, data mart, serta stored procedure.

## 🐳 Docker Implementation

Proyek ini menggunakan **Docker** agar lingkungan pengembangan dan eksekusi pipeline menjadi konsisten, serta mempermudah setup tanpa harus menginstal dependency secara manual.

### **Apa yang dicontainerize?**
1. **Apache Airflow**  
   Airflow dijalankan dalam beberapa container:
   - webserver
   - scheduler
   - worker (jika menggunakan Celery)
   - postgres (metadata database)
   - airflow-init

2. **Python ETL Environment**  
   Semua script ETL berjalan di dalam container Airflow sehingga dependency Python terisolasi dari OS lokal.

### **File Docker yang Digunakan**
- **docker-compose.yml**  
  Mengatur service Airflow (scheduler, webserver, worker, postgres).
- **Dockerfile (opsional)**  
  Digunakan jika ada dependency tambahan di dalam ETL.

### **Cara Menjalankan Airflow dengan Docker**
1. Clone repository
2. Masuk ke folder project
3. Jalankan perintah:

