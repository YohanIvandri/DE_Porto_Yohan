from datetime import datetime
from src.ingestion_bronze import save_to_bronze
from src.transform_silver import transform_bronze_to_silver
import os

LOG_PATH = "./gcp_logs/gcp_etl_log.txt"

def log_progress(message):
    os.makedirs("./gcp_logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a") as log:
        log.write(f"{timestamp} : {message}\n")

def run_bronze_ingestion():
    log_progress("Ingestion Start")
    
    tickers = ['NVDA', 'GOOGL', 'AAPL']
    bucket_name = 'stock-etl-bronze'  # ← GANTI INI!
    
    for t in tickers:
        save_to_bronze(t, bucket_name)  # ← Passing bucket_name
    
    log_progress("Ingestion End")

def run_silver_transformation():
    """Silver layer transformation"""
    log_progress("Silver Transformation Start")
    
    bucket_name = 'stock-etl-bronze'
    project_id = os.getenv('GCP_PROJECT_ID')  # Auto-detect dari env
    
    result = transform_bronze_to_silver(bucket_name, project_id)
    log_progress(f"Silver Transformation End: {result}")


def main():
    log_progress("ETL Pipeline Start")
    
    # Bronze layer
    run_bronze_ingestion()
    
    # Silver layer
    run_silver_transformation()
    
    log_progress("ETL Pipeline Complete")

if __name__ == "__main__":
    main()
