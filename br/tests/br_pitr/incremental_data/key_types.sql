-- incremental changes to test during log backup

-- test existing tables
INSERT INTO key_types_test.table_key_test VALUES (3, 'test3', 300);
INSERT INTO key_types_test.auto_inc_test (name) VALUES ('auto4');
INSERT INTO key_types_test.sequence_test (name) VALUES ('seq4');
INSERT INTO key_types_test.auto_random_test (name) VALUES ('random4');

-- Create new tables during log backup to test table creation with special keys
-- 1. New table with regular key
CREATE TABLE key_types_test.table_key_test2 (
    id INT PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255),
    value INT
);
INSERT INTO key_types_test.table_key_test2 VALUES (1, 'test1', 100);

-- 2. New table with auto increment
CREATE TABLE key_types_test.auto_inc_test2 (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255)
);
INSERT INTO key_types_test.auto_inc_test2 (name) VALUES ('auto1'), ('auto2');

-- 3. New sequence and table using it
CREATE SEQUENCE key_types_test.seq2 START WITH 1 INCREMENT BY 2 NOCACHE;
CREATE TABLE key_types_test.sequence_test2 (
    id INT PRIMARY KEY DEFAULT NEXT VALUE FOR key_types_test.seq2,
    name VARCHAR(255)
);
INSERT INTO key_types_test.sequence_test2 (name) VALUES ('seq1'), ('seq2');

-- 4. New table with auto random
CREATE TABLE key_types_test.auto_random_test2 (
    id BIGINT PRIMARY KEY AUTO_RANDOM(5),
    name VARCHAR(255)
);
INSERT INTO key_types_test.auto_random_test2 (name) VALUES ('rand1'), ('rand2');
