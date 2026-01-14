-- Buat database
CREATE DATABASE DW_Yohan
ON
(
    NAME = DW_Yohan_data,
    FILENAME = 'D:\Yohan\01.Document\Rakamin\Project Based Internship\Final Task\SQL_DATA\DW_Yohan_data.mdf',
    SIZE = 50MB,
    MAXSIZE = 200MB,
    FILEGROWTH = 10MB
)
LOG ON
(
    NAME = DW_Yohan_log,
    FILENAME = 'D:\Yohan\01.Document\Rakamin\Project Based Internship\Final Task\SQL_DATA\DW_Yohan_log.ldf',
    SIZE = 20MB,
    MAXSIZE = 50MB,
    FILEGROWTH = 5MB
)

ALTER DATABASE DW_Yohan SET RECOVERY SIMPLE;

-- Buat schema sederhana
USE DW_Yohan;

CREATE SCHEMA staging;
CREATE SCHEMA core;


SELECT * 
FROM sys.schemas;