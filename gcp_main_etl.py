from datetime import datetime
from src.ingestion_bronze import save_to_bronze
import os

LOG_PATH = "./gcp_logs/gcp_etl_log.txt"

def log_progress(message):
    os.makedirs("./gcp_logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a") as log:
        log.write(f"{timestamp} : {message}\n")

def run_ingestion():
    """Bronze layer ingestion"""
    log_progress("Ingestion Start")
    
    tickers = ['NVDA', 'GOOGL', 'AAPL']
    
    for t in tickers:
        save_to_bronze(t)
    
    log_progress("Ingestion End")

def main():
    log_progress("Preliminaries complete. Initiating ETL process")
    
    # Bronze layer
    run_ingestion()
    
    # TODO: Silver layer (nanti)
    # run_transformation()
    
    # TODO: Gold layer (nanti)
    # run_aggregation()
    
    log_progress("Process Complete")

if __name__ == "__main__":
    main()