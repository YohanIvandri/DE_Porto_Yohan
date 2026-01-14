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
import sys



print(sys.path)

print(f"Start export - {datetime.now()}")

# ===== CONNECTION =====
params_src = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=host.docker.internal,59237;"
    "DATABASE=DW_Yohan;"
    "UID=DB_Yohan;"
    "PWD=Password10!;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)
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
    """Ambil dtmload terakhir dari staging (sampai detik)"""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT MAX(dtmload) 
            FROM DW_Yohan.staging.{table_name}_raw
        """)).scalar()
        # Return string format ISO
        return '1900-01-01 00:00:00' if result is None else result.strftime("%Y-%m-%d %H:%M:%S")


def export_and_load(table):
    table_name = table["name"]
    src_table = table["src"]
    cols = ", ".join(table["columns"])

    last_date = get_last_date(table_name)
    print(f"\n {table_name.upper()} – Last dtmload: {last_date}")

    sql = f"""
        SELECT {cols}, dtmload = GETDATE()
        FROM {src_table} WITH (NOLOCK)
        WHERE dtmcrt > '{last_date}' 
           OR ISNULL(dtmupd, '1900-01-01') > '{last_date}' 
        ORDER BY dtmcrt
    """
    
    df = pd.read_sql(sql, engine)

    chunksize = 10000
    parquet_files = []

    # --- EXPORT KE PARQUET ---
    with engine.connect() as conn:
        chunks = pd.read_sql(sql, conn, chunksize=chunksize)
        batch = 0

        for chunk in tqdm(chunks, desc=f"Exporting {table_name}"):
            batch += 1
            filename = os.path.join(temp_dir, f"{table_name}_part_{batch}.parquet")

            table_pa = pa.Table.from_pandas(chunk, preserve_index=False)
            pq.write_table(table_pa, filename, compression="snappy")

            parquet_files.append(filename)

    # --- DELETE UPDATED RECORDS (NEW LOGIC) ---
    with engine.begin() as conn:
        delete_sql = f"""
            DELETE stg
            FROM DW_Yohan.staging.{table_name}_raw stg
            INNER JOIN {src_table} src
                ON stg.{table_name}_id = src.{table_name}_id
            WHERE src.dtmupd > stg.dtmload;
        """
        print(f"Deleting updated rows from staging.{table_name}_raw...")
        conn.execute(text(delete_sql))

    # --- LOAD KE STAGING ---
    with engine.begin() as conn:
        for file in parquet_files:
            print(f"Insert {file}...")
            df = pq.read_table(file).to_pandas()

            df.to_sql(
                f"{table_name}_raw",
                conn,
                schema="staging",
                if_exists="append",
                index=False,
                chunksize=5000
            )

    return parquet_files


# ============================================================
# ========== LOAD CSV / EXCEL KE TRANSACTION_RAW =============
# ============================================================
def load_transaction_from_files(directory="/opt/airflow/Data_Transaction"):
    """
    Load transaksi dari CSV/XLSX ke staging.transaction_raw
    """
    supported_ext = ["*.csv", "*.xlsx", "*.xls"]
    files = []
    for ext in supported_ext:
        files.extend(glob.glob(os.path.join(directory, ext)))

        print(files)    
    
    if not files:
        print("No CSV/XLSX transaction files found.")
        return
    
    print(f"\nLoading {len(files)} transaction file(s) ...")
    
    with engine.begin() as conn:
        for fpath in files:
            print(f"Loading file: {fpath}")
            
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
                    print(f"ERROR: Missing columns in {fpath}: {missing}")
                    continue
                
                # CRITICAL FIX: Convert transaction_date to proper datetime
                # Try multiple date formats commonly found in data
                df["transaction_date"] = pd.to_datetime(
                    df["transaction_date"], 
                    format='mixed',  # Handles multiple formats
                    dayfirst=True,   # Assumes DD-MM-YYYY format
                    errors='coerce'
                )
                
                # Filter out rows with invalid dates BEFORE creating other columns
                invalid_dates = df["transaction_date"].isna().sum()
                if invalid_dates > 0:
                    print(f"WARNING: {invalid_dates} rows with invalid dates will be skipped")
                    df = df.dropna(subset=["transaction_date"])
                
                if df.empty:
                    print(f"WARNING: No valid data to load from {fpath}")
                    continue
                
                # Create dtmcrt and dtmload AFTER filtering
                df["dtmcrt"] = df["transaction_date"]
                df["dtmload"] = datetime.now()
                
                # Ensure all datetime columns are proper datetime dtype
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
                    method='multi'  # Faster bulk insert
                )
                print(f"✓ Inserted {len(df)} rows from {os.path.basename(fpath)}")
                
            except Exception as e:
                print(f"ERROR loading {fpath}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
    
    print("\nTransaction loading completed.")


# ============================================================
# ======================== MAIN LOOP ==========================
# ============================================================
all_files = []

for tbl in tables:
    files = export_and_load(tbl)
    all_files.extend(files)

# ========== LOAD TRANSAKSI DARI CSV/XLSX ===============
load_transaction_from_files("/opt/airflow/Data_Transaction")

# ========== HAPUS PARQUET TEMP ==========================
print("\n Cleaning temp parquet...")
for f in all_files:
    os.remove(f)

print(f" Finish - {datetime.now()}")