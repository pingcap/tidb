-- Primary test table: all data-type columns are constants, only id/val come from CSV.
CREATE TABLE `t` (
  `id`            INT              NOT NULL,
  `val`           VARCHAR(100)     NOT NULL,
  -- string types
  `c_varchar`     VARCHAR(64)      NOT NULL,
  `c_text`        TEXT             NOT NULL,
  -- integer types
  `c_tinyint`     TINYINT          NOT NULL,
  `c_bigint`      BIGINT UNSIGNED  NOT NULL,
  `c_bool`        TINYINT(1)       NOT NULL,
  -- fixed/floating point
  `c_decimal`     DECIMAL(10,2)    NOT NULL,
  `c_float`       FLOAT            NOT NULL,
  -- date/time types
  `c_date`        DATE             NOT NULL,
  `c_datetime`    DATETIME         NOT NULL,
  `c_timestamp`   TIMESTAMP        NOT NULL,
  -- complex types
  `c_json`        JSON             NOT NULL,
  `c_enum`        ENUM('a','b','c') NOT NULL
);
