SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
DROP VIEW IF EXISTS v2;
DROP VIEW IF EXISTS v1;
DROP VIEW IF EXISTS v0;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
CREATE TABLE `t0` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` datetime NOT NULL,
  `c2` datetime NOT NULL,
  `c3` date DEFAULT NULL,
  `c4` datetime DEFAULT NULL,
  `c5` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c0_0` (`c0`),
  KEY `idx_c5_1` (`c5`),
  KEY `idx_c3_3` (`c3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` decimal(12,2) NOT NULL,
  `c1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_id_4` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` timestamp NOT NULL,
  `c1` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c1` (`c1`),
  KEY `idx_id_2` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 3;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` timestamp NOT NULL,
  `c1` timestamp NULL DEFAULT NULL,
  `c2` bigint NOT NULL,
  `c3` bigint NOT NULL,
  `c4` tinyint(1) NOT NULL,
  `c5` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_c5` (`c5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 2;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` float DEFAULT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` double NOT NULL,
  `c3` bigint DEFAULT NULL,
  `c4` tinyint(1) DEFAULT NULL,
  `c5` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c3` (`c3`),
  KEY `idx_c5` (`c5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 3;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` timestamp NOT NULL,
  `c1` int NOT NULL,
  `c2` float NOT NULL,
  `c3` datetime NOT NULL,
  `c4` bigint NOT NULL,
  `c5` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c1_c0_5` (`c1`,`c0`),
  KEY `idx_c4_c3_c1_6` (`c4`,`c3`,`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`g0`, `g1`, `cnt`, `sum1`) AS SELECT `t0`.`c0` AS `g0`,`t1`.`id` AS `g1`,COUNT(1) AS `cnt`,SUM(`t1`.`c0`) AS `sum1` FROM `shiro_fuzz_r3`.`t0` LEFT JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE (`t0`.`c4`!=`t0`.`c1`) GROUP BY `t0`.`c0`,`t1`.`id`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c5`) AS `sum1` FROM (`shiro_fuzz_r3`.`t1` LEFT JOIN `shiro_fuzz_r3`.`t0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r3`.`v0` ON (1=0) WHERE NOT (`t1`.`c1` IN (SELECT `t1`.`c1` AS `c0` FROM `shiro_fuzz_r3`.`t1` WHERE ((`t1`.`id`!=`t1`.`c0`) AND (`t1`.`id`>`t1`.`c0`)) ORDER BY ROUND(`t1`.`id`),`t1`.`c1` LIMIT 8)) ORDER BY 1,2 DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`) AS SELECT ABS(`t1`.`c0`) AS `c0` FROM ((`shiro_fuzz_r3`.`v1` RIGHT JOIN `shiro_fuzz_r3`.`v0` ON (1=0)) JOIN `shiro_fuzz_r3`.`t0` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`t1` ON (1=0) WHERE (EXISTS (SELECT `t1`.`id` AS `c0` FROM `shiro_fuzz_r3`.`t1` WHERE NOT (`t1`.`id` IN (24)) ORDER BY `t1`.`c1` DESC,`t1`.`id` LIMIT 8) AND (`t0`.`c2`>`v0`.`g0`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t1`.`c0`) AS `sum1` FROM ((`shiro_fuzz_r3`.`t1` RIGHT JOIN `shiro_fuzz_r3`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r3`.`v2` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`v0` WHERE ((`v0`.`sum1`<=>`v0`.`cnt`) AND (`v0`.`cnt`>=`v0`.`sum1`)) LIMIT 7);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`, `c2`) AS SELECT ABS(`t1`.`id`) AS `c0`,39 AS `c1`,`v3`.`cnt` AS `c2` FROM (((`shiro_fuzz_r3`.`v0` RIGHT JOIN `shiro_fuzz_r3`.`t1` ON (1=0)) JOIN `shiro_fuzz_r3`.`v2` ON (1=0)) LEFT JOIN `shiro_fuzz_r3`.`v1` ON (1=0)) JOIN `shiro_fuzz_r3`.`v3` ON (1=0) WHERE (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r3`.`t0` WHERE (`t0`.`c0` IN (_UTF8MB4'2024-02-09 02:13:33',_UTF8MB4'2026-02-18 12:55:05',_UTF8MB4'2025-06-19 05:08:33'))) AND (`t1`.`c0` IN (86.28,83.69,15.63)));

SET FOREIGN_KEY_CHECKS=1;
