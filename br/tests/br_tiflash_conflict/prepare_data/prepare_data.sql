-- 1. Drop the database if it exists
DROP DATABASE IF EXISTS test;

-- 2. Create the database
CREATE DATABASE test;

-- Switch to the new database
USE test;

-- 3. Create 5 tables with 5 columns of different types

-- Table 1
CREATE TABLE table1 (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    salary DECIMAL(10, 2),
    join_date DATE
);

-- Table 2
CREATE TABLE table2 (
    id INT PRIMARY KEY,
    description TEXT,
    status ENUM('active', 'inactive'),
    created_at DATETIME,
    updated_at DATETIME
);

-- Table 3
CREATE TABLE table3 (
    id INT PRIMARY KEY,
    score FLOAT,
    grade CHAR(1),
    remarks TEXT,
    is_pass BOOLEAN
);

-- 4. Insert some data into these tables

-- Insert data into table1
INSERT INTO table1 (id, name, age, salary, join_date) VALUES
(1, 'John Doe', 30, 50000.00, '2020-01-15'),
(2, 'Jane Smith', 25, 60000.00, '2019-03-22');

-- Insert data into table2
INSERT INTO table2 (id, description, status, created_at, updated_at) VALUES
(1, 'Initial setup', 'active', '2023-01-01 10:00:00', '2023-01-01 10:00:00'),
(2, 'Database migration', 'inactive', '2023-02-01 12:00:00', '2023-02-01 12:00:00');

-- Insert data into table3
INSERT INTO table3 (id, score, grade, remarks, is_pass) VALUES
(1, 85.5, 'B', 'Good performance', TRUE),
(2, 92.0, 'A', 'Excellent performance', TRUE);

-- Enable TiFlash replica for table1
ALTER TABLE table1 SET TIFLASH REPLICA 1;

-- Enable TiFlash replica for table2
ALTER TABLE table2 SET TIFLASH REPLICA 1;

-- Enable TiFlash replica for table3
ALTER TABLE table3 SET TIFLASH REPLICA 1;