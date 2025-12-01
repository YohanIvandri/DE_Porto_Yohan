import pyodbc

# =========================
# SQL SERVER CONNECTION
# =========================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=host.docker.internal,59237;"
    "DATABASE=DW_Yohan;"
    "UID=DB_Yohan;"
    "PWD=Password10!;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()
print("Connected to SQL Server")

# =========================
# TRUNCATE MART TABLES
# =========================
truncate_queries = [
    "TRUNCATE TABLE mart.CustomerMart;",
    "TRUNCATE TABLE mart.DailySummaryMart;"
]
for q in truncate_queries:
    cursor.execute(q)
    conn.commit()
print("CustomerMart and DailySummaryMart truncated.")

# =========================
# INSERT INTO CustomerMart
# =========================
customer_mart_query = """
INSERT INTO mart.CustomerMart (
    CustomerID, CustomerName, AccountID, AccountType,
    BaseBalance, CurrentBalance
)
  SELECT
		c.CustomerID,
        c.CustomerName,
		a.AccountID,
        a.AccountType,
        a.Balance,
        CurrentBalance =
            a.Balance
            + SUM(
                CASE 
                    WHEN f.TransactionType = 'Deposit' THEN ISNULL(f.Amount,0)
                    ELSE ISNULL(-f.Amount,0)
                END
            )

    FROM core.DimCustomer c
    INNER JOIN core.DimAccount a
        ON c.CustomerID = a.CustomerID
    LEFT JOIN core.FactTransaction f
        ON a.AccountID = f.AccountID
    GROUP BY 
		c.CustomerID,
        c.CustomerName,
		a.AccountID,
        a.AccountType,
        a.Balance
    ORDER BY 
        c.CustomerID

"""
cursor.execute(customer_mart_query)
conn.commit()
print("CustomerMart loaded.")

# =========================
# INSERT INTO DailySummaryMart
# =========================
daily_mart_query = """
INSERT INTO mart.DailySummaryMart (
    TransactionDate, TotalTransactions, TotalTransactionAmount
)
SELECT
    d.FullDate AS TransactionDate,
    COUNT(f.TransactionID) AS TotalTransactions,
    ISNULL(SUM(f.Amount),0) AS TotalTransactionAmount
FROM core.FactTransaction f
INNER JOIN core.DimDate d ON f.TransactionDateID = d.DateID
GROUP BY d.FullDate;
"""
cursor.execute(daily_mart_query)
conn.commit()
print("DailySummaryMart loaded.")

cursor.close()
conn.close()
print("ETL for new mart structure COMPLETED.")