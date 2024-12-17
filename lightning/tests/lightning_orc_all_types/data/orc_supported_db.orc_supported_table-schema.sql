/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET NAMES binary*/;
CREATE TABLE orc_supported_table (
  boolean_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  decimal_col DECIMAL(10,2),
  string_col VARCHAR(255),
  char_col CHAR(10),
  varchar_col VARCHAR(20),
  binary_col VARBINARY(255),
  timestamp_col DATETIME(6),
  date_col DATE,
  struct_col JSON,
  array_col JSON,
  map_col JSON,
  union_col JSON
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
