CREATE TABLE staging.customer_raw (
    customer_id     INT NULL,
    customer_name   VARCHAR(100) NULL,
    address         VARCHAR(200) NULL,
    city_id         INT NULL,
    age             INT NULL,
    gender          VARCHAR(10) NULL,
    email           VARCHAR(100) NULL,
    dtmcrt          DATETIME NULL,
    dtmload         DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE staging.city_raw (
    city_id    INT NULL,
    city_name  VARCHAR(50) NULL,
    state_id   INT NULL,
    dtmcrt     DATETIME NULL,
    dtmload    DATETIME NOT NULL DEFAULT GETDATE()
);

CREATE TABLE staging.account_raw (
    account_id      INT NULL,
    customer_id     INT NULL,
    account_type    VARCHAR(10) NULL,
    balance         INT NULL,
    date_opened     DATETIME2(6) NULL,
    status          VARCHAR(10) NULL,
    dtmcrt          DATETIME NOT NULL DEFAULT GETDATE(),  -- Waktu masuk staging
    dtmload         DATETIME NOT NULL DEFAULT GETDATE()   -- Waktu load ETL
);

CREATE TABLE staging.branch_raw (
    branch_id      INT NULL,
    branch_name    VARCHAR(50) NULL,
	  branch_location    VARCHAR(50) NULL,
    dtmcrt          DATETIME NOT NULL DEFAULT GETDATE(),  -- Waktu masuk staging
    dtmload         DATETIME NOT NULL DEFAULT GETDATE()   -- Waktu load ETL
);

CREATE TABLE staging.state_raw (
    state_id      INT NULL,
    state_name    VARCHAR(50) NULL,
    dtmcrt          DATETIME NOT NULL DEFAULT GETDATE(),  -- Waktu masuk staging
    dtmload         DATETIME NOT NULL DEFAULT GETDATE()   -- Waktu load ETL
);

CREATE TABLE staging.transaction_raw (
    transaction_id   INT NULL,
	account_id       INT NULL,
	transaction_date DATETIME NULL,
	amount			 INT NULL,
    transaction_type VARCHAR(50) NULL,
	branch_id        INT,
    dtmcrt           DATETIME NOT NULL DEFAULT GETDATE(),  -- Waktu masuk staging
    dtmload          DATETIME NOT NULL DEFAULT GETDATE()   -- Waktu load ETL
);






--TRUNCATE TABLE staging.customer_raw 
--TRUNCATE TABLE staging.city_raw
--TRUNCATE TABLE staging.account_raw
--TRUNCATE TABLE staging.branch_raw
--TRUNCATE TABLE staging.state_raw
--TRUNCATE TABLE staging.transaction_raw


select convert(varchar(10),dtmload,112),* FROM staging.customer_raw where customer_id in (4,5,7,8)
select * FROM staging.city_raw
select * FROM staging.account_raw
select * FROM staging.branch_raw
select * FROM staging.state_raw
select * FROM staging.transaction_raw

select * from core.DimCustomer where customerid in (4,5,7,8)
select * FROM staging.customer_raw where customer_id in (4,5,7,8)

SELECT *, dtmload = GETDATE()
        FROM sample.dbo.customer WITH (NOLOCK)
        WHERE CONVERT(VARCHAR(8),dtmcrt,112) > '{last_date}' 
           OR CONVERT(VARCHAR(8),isnull(dtmupd,''),112) > '{last_date}' 
        ORDER BY dtmcrt

--update sample.dbo.customer set age =  25 , dtmupd = getdate() where customer_id in (4,5,7,8)
select * from sample.dbo.account
select * from sample.dbo.branch
select * from sample.dbo.city
select * from sample.dbo.transaction_db
select * from sample.dbo.state


