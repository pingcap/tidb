SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` int NOT NULL,
  `c3` decimal(12,2) DEFAULT NULL,
  `c4` tinyint(1) NOT NULL,
  `c5` bigint NOT NULL,
  `c6` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c2` (`c2`),
  KEY `idx_c0_0` (`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` int NOT NULL,
  `c1` int NOT NULL,
  `c2` int NOT NULL,
  `c3` bigint NOT NULL,
  `c4` float NOT NULL,
  `c5` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_c3` (`c3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c5`) AS `sum1` FROM `shiro_fuzz_r1`.`t0` LEFT JOIN `shiro_fuzz_r1`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t1` WHERE ((`t1`.`c2`>`t1`.`c1`) AND (`t1`.`c3`<=>`t1`.`c2`)) ORDER BY COUNT(1) LIMIT 5);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`) AS SELECT 0 AS `c0`,`t1`.`c2` AS `c1` FROM (`shiro_fuzz_r1`.`t0` RIGHT JOIN `shiro_fuzz_r1`.`v0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r1`.`t1` ON (1=0) WHERE (EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t0` WHERE ((`t0`.`c2`=`t0`.`id`) AND (`t0`.`c5`>=`t0`.`c3`))) AND ((NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t1` WHERE NOT (`t1`.`c4` IN (85.94,39.79))) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t1` WHERE ((`t1`.`c3`=`t1`.`c2`) AND (`t1`.`c2`!=`t1`.`c3`)))) OR ((`t1`.`c1`<=`v0`.`cnt`) OR NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t0` WHERE (`t0`.`c2`>`t0`.`c1`)))));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c3`) AS `sum1` FROM ((`shiro_fuzz_r1`.`v1` LEFT JOIN `shiro_fuzz_r1`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r1`.`v0` ON (1=0)) JOIN `shiro_fuzz_r1`.`t0` ON (1=0) WHERE (`t0`.`c6`<=`t0`.`c0`) ORDER BY SUM(`shiro_fuzz_r1`.`t0`.`c3`) LIMIT 18;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`c0`, `c1`) AS SELECT SUM(`v1`.`c1`) OVER (ORDER BY `t0`.`c1` DESC,`v2`.`sum1`) AS `c0`,_UTF8MB4'2023-11-25 10:46:16' AS `c1` FROM (`shiro_fuzz_r1`.`t0` RIGHT JOIN `shiro_fuzz_r1`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r1`.`v2` ON (1=0) WHERE ((`t0`.`c1`<=>`t0`.`c2`) OR (EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`v1` WHERE ((`v1`.`c0`<=`v1`.`c1`) AND (`v1`.`c1`!=`v1`.`c0`)) ORDER BY COUNT(1) DESC LIMIT 6) AND (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`v1` WHERE ((`v1`.`c0`=`v1`.`c1`) OR (`v1`.`c1`!=`v1`.`c0`))) AND (`t0`.`c2`<=`v2`.`cnt`))));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`) AS SELECT `v0`.`sum1` AS `c0`,`v1`.`c0` AS `c1` FROM (`shiro_fuzz_r1`.`v0` RIGHT JOIN `shiro_fuzz_r1`.`v3` ON (1=0)) JOIN `shiro_fuzz_r1`.`v1` ON (1=0) WHERE (`v0`.`sum1`!=`v1`.`c1`);

SET FOREIGN_KEY_CHECKS=1;
