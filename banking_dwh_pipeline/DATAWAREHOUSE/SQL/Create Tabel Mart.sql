/**
CREATE TABLE mart.DailySummaryMart (
    TransactionDate DATE PRIMARY KEY,
    TotalTransactions INT,
    TotalTransactionAmount DECIMAL(18,2)
);


--drop table mart.CustomerMart 
CREATE TABLE mart.CustomerMart (
    CustomerID INT,
    CustomerName VARCHAR(200),
    AccountID INT,
    AccountType VARCHAR(50),
    BaseBalance DECIMAL(18,2),          -- Balance awal dari DimAccount
    CurrentBalance DECIMAL(18,2),
    PRIMARY KEY (CustomerID, AccountID)
);
**/


EXEC [mart].[BalancePerCustomer] 'Shelly'

EXEC [mart].[GetDailySummaryRange] '20240118','20240120'


select * from core.FactTransaction
select * from core.DimAccount
select * from core.DimBranch
select * from core.DimCustomer


select * from mart.CustomerMart
select * from mart.DailySummaryMart 



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


-- Monthly summary example