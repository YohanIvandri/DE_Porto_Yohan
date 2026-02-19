from google.cloud import storage, bigquery
import pandas as pd
import io
from datetime import datetime

def read_parquet_from_gcs(bucket_name, blob_path):
    """
    Read parquet file from GCS
    
    Args:
        bucket_name: GCS bucket name
        blob_path: Path to parquet file (e.g., 'bronze/NVDA/NVDA_xxx.parquet')
    
    Returns:
        pandas DataFrame
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        # Download parquet as bytes
        parquet_bytes = blob.download_as_bytes()
        
        # Read parquet from bytes
        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        
        print(f"Read {len(df)} rows from {blob_path}")
        return df
        
    except Exception as e:
        print(f"Error reading {blob_path}: {e}")
        return None

def clean_data(df):
    """
    Clean data: remove duplicates & null values
    
    Args:
        df: pandas DataFrame
    
    Returns:
        Cleaned pandas DataFrame
    """
    print(f" Rows before cleaning: {len(df)}")
    
    # Remove duplicates based on Company + Date
    initial_rows = len(df)
    df = df.drop_duplicates(subset=['Company', 'Date'], keep='last')
    duplicates_removed = initial_rows - len(df)
    print(f" Removed {duplicates_removed} duplicates")
    
    # Remove rows with null values
    initial_rows = len(df)
    df = df.dropna()
    nulls_removed = initial_rows - len(df)
    print(f" Removed {nulls_removed} rows with null values")
    
    print(f" Rows after cleaning: {len(df)}")
    
    return df

def load_to_bigquery(df, project_id, dataset_id, table_id, write_disposition='APPEND'):
    """
    Load DataFrame to BigQuery
    
    Args:
        df: pandas DataFrame
        project_id: GCP project ID (None for auto-detect)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        write_disposition: 'APPEND' or 'TRUNCATE'
    """
    try:
        # Initialize BigQuery client
        if project_id:
            bq_client = bigquery.Client(project=project_id)
        else:
            bq_client = bigquery.Client()  # Auto-detect project
        
        # Table reference
        table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
        
        # Configure load job
        if write_disposition == 'TRUNCATE':
            write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            write_disp = bigquery.WriteDisposition.WRITE_APPEND
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disp,
            autodetect=True,  # Auto-detect schema
        )
        
        # Load to BigQuery
        print(f"📤 Loading to BigQuery: {table_ref}")
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        
        print(f"✅ Loaded {len(df)} rows to {table_ref}")
        return True
        
    except Exception as e:
        print(f"❌ Error loading to BigQuery: {e}")
        return False

def transform_bronze_to_silver(bucket_name, project_id=None):
    """
    Main ETL function: Bronze → Silver
    
    Process:
    1. Read all parquet files from GCS bronze layer
    2. Clean data (remove duplicates & nulls)
    3. Load to BigQuery silver table
    
    Args:
        bucket_name: GCS bucket name
        project_id: GCP project ID (None for auto-detect)
    
    Returns:
        Result message
    """
    try:
        print(f"\n{'='*60}")
        print("🔄 SILVER LAYER TRANSFORMATION")
        print(f"{'='*60}\n")
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # List all parquet files in bronze/
        print(f"📂 Scanning bronze layer: gs://{bucket_name}/bronze/")
        blobs = list(bucket.list_blobs(prefix='bronze/'))
        
        # Filter only parquet files
        parquet_blobs = [blob for blob in blobs if blob.name.endswith('.parquet')]
        
        if not parquet_blobs:
            return "⚠️  No parquet files found in bronze layer"
        
        print(f"📦 Found {len(parquet_blobs)} parquet files\n")
        
        # Read all parquet files
        all_data = []
        for blob in parquet_blobs:
            df = read_parquet_from_gcs(bucket_name, blob.name)
            if df is not None:
                all_data.append(df)
        
        if not all_data:
            return "❌ Failed to read any parquet files"
        
        # Combine all data
        print(f"\n🔗 Combining {len(all_data)} dataframes...")
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"✅ Combined total: {len(combined_df)} rows\n")
        
        # Clean data
        print("🧹 Cleaning data...")
        cleaned_df = clean_data(combined_df)
        
        if cleaned_df.empty:
            return "⚠️  No data left after cleaning"
        
        # Load to BigQuery
        print("\n📊 Loading to BigQuery...")
        success = load_to_bigquery(
            cleaned_df,
            project_id=project_id,
            dataset_id='stock_data_silver',
            table_id='stock_prices',
            write_disposition='APPEND'  # Change to 'TRUNCATE' to replace all data
        )
        
        if success:
            result = f"✅ Success! Loaded {len(cleaned_df)} rows to BigQuery"
        else:
            result = "❌ Failed to load to BigQuery"
        
        print(f"\n{'='*60}")
        print(result)
        print(f"{'='*60}\n")
        
        return result
        
    except Exception as e:
        error_msg = f"❌ Error in silver transformation: {e}"
        print(error_msg)
        return error_msg

# For testing
if __name__ == "__main__":
    bucket_name = 'stock-etl-bronze'
    project_id = None  # Auto-detect from VM credentials
    
    result = transform_bronze_to_silver(bucket_name, project_id)
    print(f"\nFinal result: {result}")