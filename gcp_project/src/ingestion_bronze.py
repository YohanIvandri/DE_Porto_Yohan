import os
import yfinance as yf
from datetime import datetime
from google.cloud import storage
import io


def save_to_bronze(company, bucket_name='stock-etl-bronze'):
    print(f"Ingesting {company} file....")

    try:
        # Ambil data dari yf
        ticker = yf.Ticker(company)
        history = ticker.history(period="5d")
        
        if history.empty:
            raise ValueError("No data returned from yfinance")
            
        # Tambah kolom nama
        history.insert(0, 'Company', company)
        
        # Buat timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Path di GCS (mirip struktur folder lo)
        blob_path = f'bronze/{company}/{company}_{timestamp}.parquet'
        
        # Save ke BytesIO (in-memory buffer)
        buffer = io.BytesIO()
        history.to_parquet(buffer)
        buffer.seek(0)  # Reset pointer ke awal
        
        # Upload ke GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        print(f"✅ Data {company} berhasil tersimpan di {gcs_uri}")
        
        return history
        
    except Exception as e:
        print(f"❌ Error ingesting {company}: {e}")
        return None