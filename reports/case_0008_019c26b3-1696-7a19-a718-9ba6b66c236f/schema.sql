SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v5;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `c0` int NOT NULL,
  `c1` float DEFAULT NULL,
  `c2` float NOT NULL,
  `c3` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` decimal(12,2) NOT NULL,
  `c1` bigint NOT NULL,
  `c2` datetime NOT NULL,
  `c3` date NOT NULL,
  `c4` int NOT NULL,
  `c5` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c5` (`c5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 3;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` varchar(64) NOT NULL,
  `c1` datetime DEFAULT NULL,
  `c2` double NOT NULL,
  `c3` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c0`) AS `sum1` FROM (`shiro_fuzz_r7`.`t0` JOIN `shiro_fuzz_r7`.`t2` ON (1=0)) JOIN `shiro_fuzz_r7`.`t1` ON (1=0) WHERE NOT (`t2`.`c1` IN (_UTF8MB4'2024-01-04 20:43:43')) ORDER BY COUNT(1) DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`) AS SELECT `t0`.`c3` AS `c0` FROM (`shiro_fuzz_r7`.`t0` LEFT JOIN `shiro_fuzz_r7`.`t1` ON (1=0)) JOIN `shiro_fuzz_r7`.`t2` ON (1=0) WHERE EXISTS (SELECT `t1`.`c3` AS `c0` FROM `shiro_fuzz_r7`.`t1` WHERE NOT (`t1`.`c3` IN (_UTF8MB4'2023-04-13',_UTF8MB4'2023-03-18',_UTF8MB4'2026-01-09')) ORDER BY `t1`.`c3` DESC LIMIT 7) ORDER BY (`t0`.`id`+`t2`.`id`),(71+`t1`.`id`) DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`g0`, `cnt`, `sum1`) AS SELECT DISTINCT `t0`.`c2` AS `g0`,COUNT(1) AS `cnt`,SUM(`t1`.`c0`) AS `sum1` FROM ((`shiro_fuzz_r7`.`v1` LEFT JOIN `shiro_fuzz_r7`.`v2` ON (1=0)) RIGHT JOIN `shiro_fuzz_r7`.`t1` ON (1=0)) JOIN `shiro_fuzz_r7`.`t0` ON (1=0) WHERE (EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r7`.`v1` WHERE (`v1`.`cnt`>=`v1`.`sum1`)) OR (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r7`.`v2` WHERE NOT (`v2`.`c0` IN (_UTF8MB4'2025-05-13 16:26:24'))) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r7`.`t0` WHERE ((`t0`.`id`<=`t0`.`c1`) OR (`t0`.`id`>`t0`.`c2`))))) GROUP BY `t0`.`c2` HAVING (SUM(`shiro_fuzz_r7`.`t0`.`c0`)>=5) ORDER BY SUM(`shiro_fuzz_r7`.`t1`.`c0`) DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`g0`, `cnt`, `sum1`) AS SELECT `t1`.`c3` AS `g0`,COUNT(1) AS `cnt`,SUM(`t2`.`c3`) AS `sum1` FROM (((`shiro_fuzz_r7`.`t1` JOIN `shiro_fuzz_r7`.`t2` ON (1=0)) RIGHT JOIN `shiro_fuzz_r7`.`v1` ON (1=0)) LEFT JOIN `shiro_fuzz_r7`.`v2` ON (1=0)) JOIN `shiro_fuzz_r7`.`t0` ON (1=0) WHERE ((`t2`.`c1` IN (SELECT `t1`.`c2` AS `c0` FROM `shiro_fuzz_r7`.`t1` WHERE (`t1`.`id`=`t2`.`c2`))) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r7`.`t2` WHERE NOT (`t2`.`id` IN (39,92)))) GROUP BY `t1`.`c3`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v5` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t1`.`c0`) AS `sum1` FROM (((`shiro_fuzz_r7`.`v4` LEFT JOIN `shiro_fuzz_r7`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r7`.`t1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r7`.`t2` ON (1=0)) RIGHT JOIN `shiro_fuzz_r7`.`v3` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r7`.`t0` WHERE (`t0`.`c0`<=>`t0`.`id`));

SET FOREIGN_KEY_CHECKS=1;
