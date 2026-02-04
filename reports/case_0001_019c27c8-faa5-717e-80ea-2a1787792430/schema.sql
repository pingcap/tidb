SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v4;
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
  `k0` date NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k2` date NOT NULL,
  `k3` date NOT NULL,
  `p0` varchar(64) NOT NULL,
  `p1` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_p0_9` (`p0`),
  KEY `idx_id_15` (`id`),
  KEY `idx_k3_24` (`k3`),
  KEY `idx_k2_27` (`k2`),
  KEY `idx_p1_29` (`p1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` date NOT NULL,
  `d0` varchar(64) NOT NULL,
  `d1` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_d0_13` (`d0`),
  KEY `idx_d1_18` (`d1`),
  KEY `idx_id_20` (`id`),
  KEY `idx_k0_28` (`k0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k0` date NOT NULL,
  `d0` int NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_d0_19` (`d0`),
  KEY `idx_d1_22` (`d1`),
  KEY `idx_k0_23` (`k0`),
  KEY `idx_k1_25` (`k1`),
  KEY `idx_id_26` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` date NOT NULL,
  `k0` date NOT NULL,
  `d0` date NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k2_2` (`k2`),
  KEY `idx_k0_7` (`k0`),
  KEY `idx_id_11` (`id`),
  KEY `idx_d1_12` (`d1`),
  KEY `idx_d0_17` (`d0`),
  KEY `idx_k0_k2_d1_23` (`k0`,`k2`,`d1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` date NOT NULL,
  `k0` date NOT NULL,
  `d0` date NOT NULL,
  `d1` bigint NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k3_8` (`k3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t2`.`d0`) AS `sum1` FROM (`shiro_fuzz`.`t0` JOIN `shiro_fuzz`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND NOT (`t0`.`k0` IN (_UTF8MB4'2024-10-19',_UTF8MB4'2025-11-25',_UTF8MB4'2023-02-12')))) JOIN `shiro_fuzz`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND ((`t0`.`k0`>=`t2`.`k0`) OR (`t0`.`k0`>=`t2`.`k0`))) WHERE (NOT (`t1`.`k0` IN (_UTF8MB4'2025-10-17',_UTF8MB4'2026-06-24')) AND ((NOT EXISTS (SELECT `t4`.`d1` AS `c0` FROM `shiro_fuzz`.`t4` WHERE (`t4`.`k0`=`t0`.`k0`)) AND NOT (`t0`.`k0` IN (SELECT `t4`.`k3` AS `c0` FROM `shiro_fuzz`.`t4` WHERE (`t4`.`k0`<`t4`.`d0`) ORDER BY `t4`.`d0`,`t4`.`id` LIMIT 8))) AND (`t0`.`k0`>`t1`.`k0`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`g0`, `g1`, `cnt`, `sum1`) AS SELECT `t1`.`d0` AS `g0`,`v0`.`cnt` AS `g1`,COUNT(1) AS `cnt`,SUM(`v0`.`sum1`) AS `sum1` FROM ((((`shiro_fuzz`.`t1` JOIN `shiro_fuzz`.`t4` ON ((`t1`.`k0`=`t4`.`k0`) AND (`t1`.`k0` IN (_UTF8MB4'2025-01-19',_UTF8MB4'2025-04-10',_UTF8MB4'2025-01-07')))) RIGHT JOIN `shiro_fuzz`.`t2` ON ((`t1`.`k0`=`t2`.`k0`) AND ((`t1`.`k0`<`t4`.`k0`) AND (`t1`.`k0`>=`t2`.`k0`)))) RIGHT JOIN `shiro_fuzz`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz`.`t0` ON ((`t1`.`k0`=`t0`.`k0`) AND ((`t1`.`k0`>=`t2`.`k0`) AND (`t1`.`k0`<=`t4`.`k0`)))) LEFT JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND (`t4`.`k0`>`t3`.`k0`)) WHERE (`t1`.`k0` IN (_UTF8MB4'2026-04-22')) GROUP BY `t1`.`d0`,`v0`.`cnt` HAVING (`v0`.`cnt`<=56);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`, `c2`) AS SELECT (`t2`.`d1`-`t2`.`id`) AS `c0`,(`t2`.`d0`-`t2`.`d0`) AS `c1`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz`.`v1` WHERE (`v1`.`g0`=`t1`.`k0`)) AS `c2` FROM ((`shiro_fuzz`.`t0` JOIN `shiro_fuzz`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`<=>`t2`.`k0`)))) LEFT JOIN `shiro_fuzz`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND NOT (`t0`.`k0` IN (_UTF8MB4'2026-08-11',_UTF8MB4'2026-05-01')))) LEFT JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t2`.`k0`>`t1`.`k0`) AND (`t2`.`k0`>`t1`.`k0`))) WHERE (`t0`.`k0`<=>`t1`.`k0`) ORDER BY (`t2`.`d0`-`t2`.`d0`),(`t2`.`d1`-`t2`.`id`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`id`) AS `sum1` FROM (`shiro_fuzz`.`t0` JOIN `shiro_fuzz`.`t1` USING (`k0`)) JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t0`.`k0`>=`t3`.`k0`) OR (`t0`.`k0`=`t3`.`k0`))) WHERE (EXISTS (SELECT `t3`.`k0` AS `c0` FROM `shiro_fuzz`.`t3` WHERE (`t3`.`k0`=`t0`.`k0`) LIMIT 10) OR EXISTS (SELECT `v1`.`g0` AS `c0` FROM `shiro_fuzz`.`v1` WHERE ((`v1`.`cnt`!=`v1`.`sum1`) OR (`v1`.`sum1`<=>`v1`.`cnt`)) LIMIT 10));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`) AS SELECT `v3`.`cnt` AS `c0` FROM ((((`shiro_fuzz`.`v1` JOIN `shiro_fuzz`.`t2` ON (1=0)) LEFT JOIN `shiro_fuzz`.`t4` USING (`k0`)) LEFT JOIN `shiro_fuzz`.`v0` ON (1=0)) RIGHT JOIN `shiro_fuzz`.`v3` ON (1=0)) JOIN `shiro_fuzz`.`v2` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz`.`v0` WHERE ((`v0`.`cnt`>`v0`.`sum1`) OR (`v0`.`sum1`<`v0`.`cnt`)));

SET FOREIGN_KEY_CHECKS=1;
