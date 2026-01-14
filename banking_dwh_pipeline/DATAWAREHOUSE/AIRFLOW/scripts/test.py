import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from datetime import datetime
import os
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
import glob

print(f"Start export - {datetime.now()}")

# ===== CONNECTION =====
engine = create_engine(
    "mssql+pymssql://DB_Yohan:Password10!@host.docker.internal:59237/DW_Yohan"
)

# ===== DIRECTORY =====
export_dir = "data_export"
temp_dir = os.path.join(export_dir, "temp_parquet")
os.makedirs(temp_dir, exist_ok=True)

# ===== TABLE CONFIG =====
tables = [
    {
        "name": "customer",
        "src": "sample.dbo.customer",
        "columns": [
            "customer_id", "customer_name", "address", 
            "city_id", "age", "gender", "email", "dtmcrt"
        ]
    },
    {
        "name": "city",
        "src": "sample.dbo.city",
        "columns": [
            "city_id", "city_name", "state_id", "dtmcrt"
        ]
    },
    {
        "name": "account",
        "src": "sample.dbo.account",
        "columns": [
            "account_id", "customer_id", "account_type", "balance",
            "date_opened","status","dtmcrt"
        ]
    },
    {
        "name": "branch",
        "src": "sample.dbo.branch",
        "columns": [
            "branch_id", "branch_name", "branch_location","dtmcrt"
        ]
    },
    {
        "name": "state",
        "src": "sample.dbo.state",
        "columns": [
            "state_id", "state_name", "dtmcrt"
        ]
    },
    {
        "name": "transaction",
        "src": "sample.dbo.transaction_db",
        "columns": [
            "transaction_id", "account_id", "transaction_date",
            "amount","transaction_type","branch_id","dtmcrt"
        ]
    }
]


# ============================================================
# ========== GET LAST DTMCRT FROM STAGING ====================
# ============================================================
def get_last_date(table_name):
    """Ambil dtmcrt terakhir dari staging"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT MAX(dtmcrt) 
                FROM DW_Yohan.staging.{table_name}_raw
            """)).scalar()
            
            if result is None:
                return datetime(1900, 1, 1)  # Return datetime object, not string
            return result
    except:
        return datetime(1900, 1, 1)


# ============================================================
# ========== EXPORT & LOAD DARI SOURCE DB =====================
# ============================================================
def export_and_load(table):
    table_name = table["name"]
    src_table = table["src"]
    cols = ", ".join(table["columns"])

    last_date = get_last_date(table_name)
    print(f"\n {table_name.upper()} – Last dtmcrt: {last_date}")

    # FIXED: Use direct datetime comparison with proper string format
    # pymssql requires specific datetime string format
    last_date_str = last_date.strftime('%Y%m%d %H:%M:%S')
    
    sql = f"""
        SELECT {cols}, GETDATE() as dtmload
        FROM {src_table} WITH (NOLOCK)
        WHERE dtmcrt > '{last_date_str}'
        ORDER BY dtmcrt
    """

    parquet_files = []
    total_rows = 0

    # --- EXPORT KE PARQUET ---
    # FIXED: Don't use execution_options(autocommit=True) with pymssql
    # Use engine directly or text() wrapper
    try:
        print(f"  ↓ Reading data from source...")
        df = pd.read_sql(sql, engine)  # Direct engine connection works!
        
        if df.empty or len(df) == 0:
            print(f"  ⚠ No data returned from query")
            return []
        
        total_rows = len(df)
        print(f"  ✓ Retrieved {total_rows} rows")
        
        # Split into chunks manually if needed (for very large datasets)
        chunksize = 10000
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            batch = (i // chunksize) + 1
            
            filename = os.path.join(temp_dir, f"{table_name}_part_{batch}.parquet")
            table_pa = pa.Table.from_pandas(chunk, preserve_index=False)
            pq.write_table(table_pa, filename, compression="snappy")
            
            parquet_files.append(filename)
            print(f"  💾 Saved chunk {batch}: {len(chunk)} rows")
        
        print(f"  ✓ Exported {total_rows} rows")
        
        if total_rows == 0:
            print(f"  ⚠ No new data for {table_name}")
            return []

    except Exception as e:
        print(f"  ✗ ERROR during export: {e}")
        import traceback
        traceback.print_exc()
        return []

    # --- LOAD KE STAGING ---
    try:
        loaded_rows = 0
        with engine.begin() as conn:
            for file in parquet_files:
                print(f"  ↑ Inserting {os.path.basename(file)}...")
                df = pq.read_table(file).to_pandas()

                df.to_sql(
                    f"{table_name}_raw",
                    conn,
                    schema="staging",
                    if_exists="append",
                    index=False,
                    chunksize=5000
                )
                loaded_rows += len(df)
        
        print(f"  ✓ Loaded {loaded_rows} rows to staging.{table_name}_raw")

    except Exception as e:
        print(f"  ✗ ERROR during load: {e}")
        import traceback
        traceback.print_exc()
        return []

    return parquet_files


# ============================================================
# ========== LOAD CSV / EXCEL KE TRANSACTION_RAW =============
# ============================================================
def load_transaction_from_files(directory="Data_Transaction"):
    """
    Load transaksi dari CSV/XLSX ke staging.transaction_raw
    """
    if not os.path.exists(directory):
        print(f"\n⚠ Directory {directory} does not exist")
        return
    
    supported_ext = ["*.csv", "*.xlsx", "*.xls"]
    files = []
    for ext in supported_ext:
        files.extend(glob.glob(os.path.join(directory, ext)))
    
    if not files:
        print(f"\n⚠ No CSV/XLSX transaction files found in {directory}")
        return
    
    print(f"\nLoading {len(files)} transaction file(s) ...")
    
    total_loaded = 0
    with engine.begin() as conn:
        for fpath in files:
            print(f"\n  Processing: {os.path.basename(fpath)}")
            
            try:
                # Load file based on extension
                if fpath.endswith(".csv"):
                    df = pd.read_csv(fpath)
                else:
                    df = pd.read_excel(fpath)
                
                # Check required columns
                required_cols = [
                    "transaction_id", "account_id", "transaction_date",
                    "amount", "transaction_type", "branch_id"
                ]
                missing = [c for c in required_cols if c not in df.columns]
                if missing:
                    print(f"    ✗ Missing columns: {missing}")
                    continue
                
                # Convert transaction_date to proper datetime
                df["transaction_date"] = pd.to_datetime(
                    df["transaction_date"], 
                    format='mixed',
                    dayfirst=True,
                    errors='coerce'
                )
                
                # Filter out rows with invalid dates
                invalid_dates = df["transaction_date"].isna().sum()
                if invalid_dates > 0:
                    print(f"    ⚠ {invalid_dates} rows with invalid dates will be skipped")
                    df = df.dropna(subset=["transaction_date"])
                
                if df.empty:
                    print(f"    ⚠ No valid data after date conversion")
                    continue
                
                # Create dtmcrt and dtmload
                df["dtmcrt"] = df["transaction_date"]
                df["dtmload"] = datetime.now()
                
                # Ensure datetime columns are proper datetime dtype
                datetime_cols = ["transaction_date", "dtmcrt", "dtmload"]
                for col in datetime_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col])
                
                # Insert to database
                df.to_sql(
                    "transaction_raw",
                    conn,
                    schema="staging",
                    if_exists="append",
                    index=False,
                    chunksize=5000,
                    method='multi'
                )
                print(f"    ✓ Inserted {len(df)} rows")
                total_loaded += len(df)
                
            except Exception as e:
                print(f"    ✗ ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
    
    print(f"\n✓ Transaction loading completed. Total: {total_loaded} rows")


# ============================================================
# ======================== MAIN LOOP ==========================
# ============================================================
all_files = []

for tbl in tables:
    files = export_and_load(tbl)
    all_files.extend(files)

# ========== LOAD TRANSAKSI DARI CSV/XLSX ===============
load_transaction_from_files("Data_Transaction")

# ========== HAPUS PARQUET TEMP ==========================
if all_files:
    print("\n🧹 Cleaning temp parquet...")
    for f in all_files:
        try:
            os.remove(f)
        except:
            pass

print(f"\n✓ Finish - {datetime.now()}")