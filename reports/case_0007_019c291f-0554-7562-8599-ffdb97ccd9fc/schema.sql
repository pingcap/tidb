SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v5;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `k0` int NOT NULL,
  `k1` date NOT NULL,
  `k2` int NOT NULL,
  `k3` varchar(64) NOT NULL,
  `p0` datetime NOT NULL,
  `p1` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` int NOT NULL,
  `d0` float NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` date NOT NULL,
  `k0` int NOT NULL,
  `d0` date NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_d1_1` (`d1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` int NOT NULL,
  `k0` int NOT NULL,
  `d0` int NOT NULL,
  `d1` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` varchar(64) NOT NULL,
  `k0` int NOT NULL,
  `d0` tinyint(1) NOT NULL,
  `d1` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t3`.`d1`) AS `sum1` FROM (((`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t0`.`k0`>`t3`.`k0`) AND (`t0`.`k0`!=`t3`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t0`.`k0`>=`t3`.`k0`) AND (`t0`.`k0`<=`t4`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND ((`t0`.`k0`<=>`t3`.`k0`) OR (`t0`.`k0`<`t3`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`!=`t2`.`k0`)) WHERE (`t4`.`k0`=`t1`.`k0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`) AS SELECT LOWER(`t4`.`d1`) AS `c0`,(`t3`.`d1`-`t0`.`id`) AS `c1` FROM (`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t0`.`k0`>=`t3`.`k0`) AND (`t0`.`k0`<=`t3`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t0`.`k0`!=`t4`.`k0`) OR (`t3`.`k0`>`t4`.`k0`))) WHERE NOT EXISTS (SELECT `t0`.`p0` AS `c0` FROM `shiro_fuzz_r6`.`t0` WHERE (`t0`.`k1`!=`t0`.`p0`) ORDER BY (`t0`.`p1`+`t0`.`k2`) DESC,ABS(`t0`.`k2`) DESC LIMIT 1);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`) AS SELECT DISTINCT ABS(`t3`.`d1`) AS `c0`,_UTF8MB4's73' AS `c1` FROM ((((`shiro_fuzz_r6`.`t0` JOIN `shiro_fuzz_r6`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t0`.`k0`=`t3`.`k0`) OR (`t0`.`k0`<=`t3`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND (`t3`.`k0`>=`t4`.`k0`))) RIGHT JOIN `shiro_fuzz_r6`.`v0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r6`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND (`t0`.`k0`<=`t1`.`k0`))) LEFT JOIN `shiro_fuzz_r6`.`v1` ON (1=0) WHERE ((`t3`.`k0` IN (53,43)) AND EXISTS (SELECT `t4`.`d0` AS `c0` FROM `shiro_fuzz_r6`.`t4` WHERE ((`t4`.`k0`=`t0`.`k0`) AND (`t4`.`k0`>=`t0`.`k0`)))) ORDER BY 1,2 DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t3`.`d1`) AS `sum1` FROM (((`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND ((`t0`.`k0`>`t2`.`k0`) OR (`t0`.`k0`=`t2`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`<=`t2`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND NOT (`t2`.`k0` IN (46)))) RIGHT JOIN `shiro_fuzz_r6`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND (`t2`.`k0`=`t3`.`k0`)) WHERE (`t3`.`k0`!=`t1`.`k0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v5` (`c0`, `c1`, `c2`) AS SELECT `t4`.`d0` AS `c0`,ROUND(`t0`.`p1`) AS `c1`,LOWER(_UTF8MB4's62') AS `c2` FROM (`shiro_fuzz_r6`.`t0` RIGHT JOIN `shiro_fuzz_r6`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`<=`t2`.`k0`)))) LEFT JOIN `shiro_fuzz_r6`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t2`.`k0`<=`t4`.`k0`) AND (`t2`.`k0`!=`t4`.`k0`))) WHERE ((`t2`.`k0`<=>`t4`.`k0`) AND NOT EXISTS (SELECT `t2`.`k1` AS `c0` FROM `shiro_fuzz_r6`.`t2` WHERE ((`t2`.`k0`=`t0`.`k0`) AND (`t2`.`k0`<`t0`.`k0`)))) ORDER BY `t0`.`id`,`t2`.`k1`;

SET FOREIGN_KEY_CHECKS=1;
