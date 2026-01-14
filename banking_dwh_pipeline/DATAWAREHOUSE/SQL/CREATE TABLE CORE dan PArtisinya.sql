CREATE TABLE core.DimDate (
    DateID INT PRIMARY KEY,       
    FullDate DATE NOT NULL,
    Day INT,
    Month INT,
    Year INT,
    Quarter INT,
    WeekOfYear INT,
    DayOfWeek INT,
    IsWeekend BIT
);


CREATE TABLE core.DimCustomer (
    CustomerID INT NOT NULL PRIMARY KEY NONCLUSTERED,
    CustomerName VARCHAR(200) NOT NULL,
    Address VARCHAR(300) NOT NULL,
    CityName VARCHAR(200) NOT NULL,
    StateName VARCHAR(200) NOT NULL,
    Age INT NOT NULL,
    Gender VARCHAR(20) NOT NULL,
    Email VARCHAR(200) NOT NULL,
);

CREATE INDEX IX_DimCustomer_CustomerName 
    ON core.DimCustomer (CustomerName);
GO

CREATE TABLE core.DimAccount (
    AccountID INT NOT NULL PRIMARY KEY NONCLUSTERED,
    CustomerID INT NOT NULL,
    AccountType VARCHAR(50) NOT NULL,
    Balance DECIMAL(18,2) NOT NULL,
    DateOpenedID INT NOT NULL,
    Status VARCHAR(50) NOT NULL,

    CONSTRAINT FK_DimAccount_Customer FOREIGN KEY (CustomerID) REFERENCES core.DimCustomer(CustomerID),
    CONSTRAINT FK_DimAccount_DateOpened FOREIGN KEY (DateOpenedID) REFERENCES core.DimDate(DateID)
);

CREATE INDEX IX_DimAccount_CustomerID
    ON core.DimAccount (CustomerID);


CREATE TABLE core.DimBranch (
    BranchID INT NOT NULL PRIMARY KEY NONCLUSTERED,
    BranchName VARCHAR(200) NOT NULL,
    BranchLocation VARCHAR(300) NOT NULL,
);





CREATE TABLE core.FactTransaction (
    TransactionID INT NOT NULL,
    AccountID INT NOT NULL,
    TransactionDate DATE NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    TransactionType VARCHAR(50) NOT NULL,
    BranchID INT NOT NULL,

    CONSTRAINT PK_FactTransaction PRIMARY KEY (TransactionID),

    FOREIGN KEY (AccountID) REFERENCES core.DimAccount(AccountID),
    FOREIGN KEY (BranchID) REFERENCES core.DimBranch(BranchID)
);
GO


select * from core.DimDate
select * from core.DimCustomer
select * from core.DimAccount
select * from core.DimBranch
select * from core.FactTransaction


-- Drop dulu kalau mau recreate
-- DROP TABLE IF EXISTS core.DimDate;
