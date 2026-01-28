import yfinance as yf
import os
from datetime import datetime
# from pathlib import path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def save_to_bronze(company):
    print (f"Ingesting {company} file....")

    try:

        #buat folder baru
        bronze_path = os.path.join(BASE_DIR, "bronze", company)
        os.makedirs(bronze_path, exist_ok = True)

        #ambil data dari yf
        ticker = yf.Ticker(company)
        history = ticker.history(period = "5d")
        
        if history.empty:
            raise ValueError("No data returned from yfinance")
            
        #tambah kolom nama
        history.insert(0,'Company',company)
        

        #buat timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(bronze_path,f'{company}_{timestamp}.parquet')
        
        #save file di directory
        history.to_parquet(filename)
        
        print(f"Data {company} berhasil tersimpan di {filename}")
        
        return history
    except Exception as e:
            print(f" Error ingesting {company}: {e}")
            return None
    
    