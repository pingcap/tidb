-- Test cases for different key types during PITR restore

-- 1. regular table key
CREATE DATABASE IF NOT EXISTS key_types_test;
CREATE TABLE key_types_test.table_key_test (
    id INT PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255),
    value INT
);
INSERT INTO key_types_test.table_key_test VALUES (1, 'test1', 100), (2, 'test2', 200);

-- 2. auto Increment ID Key Test
CREATE TABLE key_types_test.auto_inc_test (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255)
);

INSERT INTO key_types_test.auto_inc_test (name) VALUES ('auto1'), ('auto2'), ('auto3');

-- 3. sequence Key Test
CREATE SEQUENCE key_types_test.seq1 START WITH 1 INCREMENT BY 2 NOCACHE;
CREATE TABLE key_types_test.sequence_test (
    id INT PRIMARY KEY DEFAULT NEXT VALUE FOR key_types_test.seq1,
    name VARCHAR(255)
);
INSERT INTO key_types_test.sequence_test (name) VALUES ('seq1'), ('seq2'), ('seq3');

-- 4. auto Random Table ID Key Test
CREATE TABLE key_types_test.auto_random_test (
    id BIGINT PRIMARY KEY AUTO_RANDOM(5),
    name VARCHAR(255)
);
INSERT INTO key_types_test.auto_random_test (name) VALUES ('rand1'), ('rand2'), ('rand3');