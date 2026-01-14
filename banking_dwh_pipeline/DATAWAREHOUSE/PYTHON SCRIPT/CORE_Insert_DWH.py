import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from datetime import datetime

print(f"Start INCREMENTAL ETL - {datetime.now()}")

# ============================================================
# =============== DATABASE CONNECTION SETUP ==================
# ============================================================
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=host.docker.internal,59237;"
    "DATABASE=DW_Yohan;"
    "UID=DB_Yohan;"
    "PWD=Password10!;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


# ============================================================
# =============== HELPER: GET LAST UPDATE DATE ===============
# ============================================================
# Mapping di awal file
TABLE_MAPPING = {
    'customer': 'dimCustomer',
    'branch': 'dimBranch',
    'account': 'dimAccount',
    'transaction': 'factTransaction',  # Explicitly map transaction ke fact
}

def get_last_update_date(table_name):
    """Ambil dtm_load terakhir untuk tracking update"""
    
    # Cek di mapping dulu
    table_key = table_name.lower().replace('dim', '').replace('fact', '')
    full_table_name = TABLE_MAPPING.get(table_key, table_name)
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT MAX(dtmload) 
            FROM DW_Yohan.core.{full_table_name}
        """)).scalar()
        return '1900-01-01 00:00:00' if result is None else result.strftime("%Y-%m-%d %H:%M:%S")



# ============================================================
# ================== LOAD DIM CUSTOMER ========================
# ============================================================
def load_dim_customer():
    print("\n[1] Loading DimCustomer (Incremental)...")

    sql = """
        SELECT 
            c.customer_id AS CustomerID,
            UPPER(c.customer_name) AS CustomerName,
            UPPER(c.address) AS Address,
            UPPER(ci.city_name) AS CityName,
            UPPER(s.state_name) AS StateName,
            c.age AS Age,
            UPPER(c.gender) AS Gender,
            c.email AS Email
        FROM staging.customer_raw c
        LEFT JOIN staging.city_raw ci ON c.city_id = ci.city_id
        LEFT JOIN staging.state_raw s ON ci.state_id = s.state_id
        WHERE c.customer_id NOT IN (SELECT CustomerID FROM core.DimCustomer)
    """

    df = pd.read_sql(sql, engine)
    df = df.drop_duplicates(subset=['CustomerID'])

    if df.empty:
        print("✓ No new customers.")
        return 0
    
    with engine.begin() as conn:
        df.to_sql(
            "DimCustomer",
            conn,
            schema="core",
            index=False,
            if_exists="append",
            chunksize=5000
        )
    
    print(f"✓ Inserted new DimCustomer rows: {len(df)}")
    return len(df)


def update_dim_customer():
    print("\n[1.1] Updating DimCustomer...")
    
    last_update = get_last_update_date('customer')
    print(f"    Last update check: {last_update}")
    
    sql = f"""
        UPDATE dc
        SET 
            dc.CustomerName = UPPER(c.customer_name),
            dc.Address = UPPER(c.address),
            dc.CityName = UPPER(ci.city_name),
            dc.StateName = UPPER(s.state_name),
            dc.Age = c.age,
            dc.Gender = UPPER(c.gender),
            dc.Email = c.email,
            dc.dtmupd = GETDATE()
        FROM core.DimCustomer dc
        INNER JOIN staging.customer_raw c ON dc.CustomerID = c.customer_id
        LEFT JOIN staging.city_raw ci ON c.city_id = ci.city_id
        LEFT JOIN staging.state_raw s ON ci.state_id = s.state_id
        WHERE c.dtmload > '{last_update}'
          AND c.dtmload IS NOT NULL
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount
    
    if rows > 0:
        print(f"✓ Updated {rows} DimCustomer rows")
    else:
        print("✓ No customer updates needed")
    
    return rows


# ============================================================
# ================== LOAD DIM BRANCH ==========================
# ============================================================
def load_dim_branch():
    print("\n[2] Loading DimBranch (Incremental)...")

    sql = """
        SELECT 
            branch_id AS BranchID,
            UPPER(branch_name) AS BranchName,
            UPPER(branch_location) AS BranchLocation
        FROM staging.branch_raw
        WHERE branch_id NOT IN (SELECT BranchID FROM core.DimBranch)
    """

    df = pd.read_sql(sql, engine)
    df = df.drop_duplicates(subset=['BranchID'])

    if df.empty:
        print("✓ No new branches.")
        return 0
    
    with engine.begin() as conn:
        df.to_sql(
            "DimBranch",
            conn,
            schema="core",
            index=False,
            if_exists="append",
            chunksize=5000
        )
    
    print(f"✓ Inserted new DimBranch rows: {len(df)}")
    return len(df)


def update_dim_branch():
    print("\n[2.1] Updating DimBranch...")
    
    last_update = get_last_update_date('branch')
    print(f"    Last update check: {last_update}")
    
    sql = f"""
        UPDATE db
        SET 
            db.BranchName = UPPER(b.branch_name),
            db.BranchLocation = UPPER(b.branch_location),
            db.dtmupd = GETDATE()
        FROM core.DimBranch db
        INNER JOIN staging.branch_raw b ON db.BranchID = b.branch_id
        WHERE b.dtmload > '{last_update}'
          AND b.dtmload IS NOT NULL
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount
    
    if rows > 0:
        print(f"✓ Updated {rows} DimBranch rows")
    else:
        print("✓ No branch updates needed")
    
    return rows


# ============================================================
# ================== LOAD DIM ACCOUNT =========================
# ============================================================
def load_dim_account():
    print("\n[3] Loading DimAccount (Incremental)...")

    sql = """
        SELECT 
            a.account_id AS AccountID,
            a.customer_id AS CustomerID,
            UPPER(a.account_type) AS AccountType,
            CAST(a.balance AS DECIMAL(18,2)) AS Balance,
            d.DateID AS DateOpenedID,
            UPPER(a.status) AS Status
        FROM staging.account_raw a
        INNER JOIN core.DimDate d 
            ON CAST(a.date_opened AS DATE) = d.FullDate
        WHERE a.account_id NOT IN (SELECT AccountID FROM core.DimAccount)
    """

    df = pd.read_sql(sql, engine)
    df = df.drop_duplicates(subset=['AccountID'])

    if df.empty:
        print("✓ No new accounts.")
        return 0
    
    with engine.begin() as conn:
        df.to_sql(
            "DimAccount",
            conn,
            schema="core",
            index=False,
            if_exists="append",
            chunksize=5000
        )
    
    print(f"✓ Inserted new DimAccount rows: {len(df)}")
    return len(df)


def update_dim_account():
    print("\n[3.1] Updating DimAccount...")
    
    last_update = get_last_update_date('account')
    print(f"    Last update check: {last_update}")
    
    sql = f"""
        UPDATE da
        SET 
            da.AccountType = UPPER(a.account_type),
            da.Balance = CAST(a.balance AS DECIMAL(18,2)),
            da.Status = UPPER(a.status),
            da.dtmupd = GETDATE()
        FROM core.DimAccount da
        INNER JOIN staging.account_raw a ON da.AccountID = a.account_id
        WHERE a.dtmload > '{last_update}'
          AND a.dtmload IS NOT NULL
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount
    
    if rows > 0:
        print(f"✓ Updated {rows} DimAccount rows")
    else:
        print("✓ No account updates needed")
    
    return rows


# ============================================================
# ================== LOAD FACT TRANSACTION ====================
# ============================================================
def load_fact_transaction():
    print("\n[4] Loading FactTransaction (Incremental)...")

    sql = """
        WITH cte AS (
            SELECT
                t.transaction_id AS TransactionID,
                t.account_id AS AccountID,
                d.DateID AS TransactionDateID,
                CAST(t.amount AS DECIMAL(18,2)) AS Amount,
                UPPER(t.transaction_type) AS TransactionType,
                t.branch_id AS BranchID,
                ROW_NUMBER() OVER(PARTITION BY t.transaction_id ORDER BY t.transaction_date DESC) AS rn
            FROM staging.transaction_raw t
            INNER JOIN core.DimAccount da ON t.account_id = da.AccountID
            INNER JOIN core.DimBranch db ON t.branch_id = db.BranchID
            INNER JOIN core.DimDate d ON CAST(t.transaction_date AS DATE) = d.FullDate
        )
        SELECT TransactionID, AccountID, TransactionDateID, Amount, TransactionType, BranchID
        FROM cte
        WHERE rn = 1
          AND TransactionID NOT IN (SELECT TransactionID FROM core.FactTransaction)
    """

    df = pd.read_sql(sql, engine)
    df = df.drop_duplicates(subset=['TransactionID'])

    if df.empty:
        print("✓ No new fact rows.")
        return 0
    
    with engine.begin() as conn:
        df.to_sql(
            "FactTransaction",
            conn,
            schema="core",
            index=False,
            if_exists="append",
            chunksize=5000
        )
    
    print(f"✓ Inserted new FactTransaction rows: {len(df)}")
    return len(df)


def update_fact_transaction():
    print("\n[4.1] Updating FactTransaction...")
    
    last_update = get_last_update_date('transaction')
    print(f"    Last update check: {last_update}")
    
    sql = f"""
        UPDATE ft
        SET 
            ft.Amount = CAST(t.amount AS DECIMAL(18,2)),
            ft.TransactionType = UPPER(t.transaction_type),
            ft.dtmupd = GETDATE()
        FROM core.FactTransaction ft
        INNER JOIN staging.transaction_raw t ON ft.TransactionID = t.transaction_id
        WHERE t.dtmload > '{last_update}'
          AND t.dtmload IS NOT NULL
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount
    
    if rows > 0:
        print(f"✓ Updated {rows} FactTransaction rows")
    else:
        print("✓ No transaction updates needed")
    
    return rows


# ============================================================
# ======================= MAIN PIPELINE ======================
# ============================================================
def run_etl():
    print("\n========== START INCREMENTAL LOADING ==========\n")

    # Track totals
    total_inserted = 0
    total_updated = 0

    # Load new records
    total_inserted += load_dim_customer()
    total_inserted += load_dim_branch()
    total_inserted += load_dim_account()
    total_inserted += load_fact_transaction()
    
    # Update existing records
    total_updated += update_dim_customer()
    total_updated += update_dim_branch()
    total_updated += update_dim_account()
    total_updated += update_fact_transaction()

    print("\n========== ETL SUMMARY ==========")
    print(f"Total Records Inserted: {total_inserted}")
    print(f"Total Records Updated: {total_updated}")
    print(f"Finish - {datetime.now()}")


# ============================================================
# ======================= ENTRY POINT ========================
# ============================================================
if __name__ == "__main__":
    run_etl()