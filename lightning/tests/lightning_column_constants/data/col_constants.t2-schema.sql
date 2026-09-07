-- Used to test:
--   1. table-filter glob matching (col_constants.* matches this table too)
--   2. constant does not override an explicit non-NULL source value
--   3. NULL-healing: source sends empty field for a NOT NULL column → constant applied
CREATE TABLE `t2` (
  `id`      INT          NOT NULL,
  `region`  VARCHAR(64)  NOT NULL,
  `tenant`  VARCHAR(64)  NOT NULL
);
