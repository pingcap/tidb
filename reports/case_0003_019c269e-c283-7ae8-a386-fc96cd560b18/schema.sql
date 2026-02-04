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
  `k0` varchar(64) NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k2` int NOT NULL,
  `k3` int NOT NULL,
  `p0` float NOT NULL,
  `p1` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` bigint NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` decimal(12,2) NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` int NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` float NOT NULL,
  `d1` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_d1_0` (`d1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` int NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` date NOT NULL,
  `d1` bigint NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT DISTINCT COUNT(1) AS `cnt`,SUM(`t3`.`k2`) AS `sum1` FROM ((`shiro_fuzz_r2`.`t0` LEFT JOIN `shiro_fuzz_r2`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) LEFT JOIN `shiro_fuzz_r2`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) LEFT JOIN `shiro_fuzz_r2`.`t4` ON (`t0`.`k0`=`t4`.`k0`) WHERE ((NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r2`.`t1` WHERE (`t1`.`k0`=`t0`.`k0`) ORDER BY COUNT(1) LIMIT 10) OR NOT (`t0`.`k0` IN (SELECT `t0`.`k0` AS `c0` FROM `shiro_fuzz_r2`.`t0` WHERE ((`t0`.`k1`=`t0`.`k1`) AND (`t0`.`k2`!=`t0`.`k2`))))) OR EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r2`.`t4` WHERE ((`t4`.`k0`=`t0`.`k0`) AND (`t4`.`k0`!=`t0`.`k0`)))) ORDER BY SUM(`shiro_fuzz_r2`.`t3`.`k2`) DESC;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT (`t1`.`d1`+`t1`.`d0`) AS `c0`,DENSE_RANK() OVER (ORDER BY `t2`.`k0` DESC) AS `c1`,(`t2`.`d1`-`t1`.`d1`) AS `c2` FROM ((((`shiro_fuzz_r2`.`t0` JOIN `shiro_fuzz_r2`.`v0` ON (1=0)) JOIN `shiro_fuzz_r2`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) JOIN `shiro_fuzz_r2`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) JOIN `shiro_fuzz_r2`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) JOIN `shiro_fuzz_r2`.`t4` ON (`t0`.`k0`=`t4`.`k0`) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r2`.`v0` WHERE ((`v0`.`cnt`<`v0`.`sum1`) AND (`v0`.`sum1`<=>`v0`.`cnt`))) ORDER BY LOWER(_UTF8MB4's88'),LOWER(`t4`.`k0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`) AS SELECT DISTINCT (`t2`.`d1`-`t3`.`id`) AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r2`.`t1` WHERE (`t1`.`k0`=`t0`.`k0`) LIMIT 5) AS `c1` FROM ((`shiro_fuzz_r2`.`t0` LEFT JOIN `shiro_fuzz_r2`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) LEFT JOIN `shiro_fuzz_r2`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) JOIN `shiro_fuzz_r2`.`t1` ON (`t0`.`k0`=`t1`.`k0`) WHERE NOT EXISTS (SELECT `t2`.`d1` AS `c0` FROM `shiro_fuzz_r2`.`t2` WHERE (`t2`.`k0`=`t0`.`k0`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t2`.`d0`) AS `sum1` FROM (((`shiro_fuzz_r2`.`t0` JOIN `shiro_fuzz_r2`.`t4` ON (`t0`.`k0`=`t4`.`k0`)) RIGHT JOIN `shiro_fuzz_r2`.`t1` ON (`t0`.`k0`=`t1`.`k0`)) RIGHT JOIN `shiro_fuzz_r2`.`t2` ON (`t0`.`k0`=`t2`.`k0`)) LEFT JOIN `shiro_fuzz_r2`.`t3` ON (`t0`.`k0`=`t3`.`k0`) WHERE NOT EXISTS (SELECT `v1`.`c2` AS `c0` FROM `shiro_fuzz_r2`.`v1` WHERE NOT (`v1`.`c1` IN (_UTF8MB4's82',_UTF8MB4's8',_UTF8MB4's7')));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`g0`, `cnt`, `sum1`) AS SELECT `t0`.`p0` AS `g0`,COUNT(1) AS `cnt`,SUM(`t2`.`d0`) AS `sum1` FROM ((((`shiro_fuzz_r2`.`t2` JOIN `shiro_fuzz_r2`.`t0` ON (`t2`.`k0`=`t0`.`k0`)) RIGHT JOIN `shiro_fuzz_r2`.`v0` ON (1=0)) JOIN `shiro_fuzz_r2`.`t3` ON (`t0`.`k0`=`t3`.`k0`)) RIGHT JOIN `shiro_fuzz_r2`.`v2` ON (1=0)) LEFT JOIN `shiro_fuzz_r2`.`v1` ON (1=0) WHERE (`t0`.`k0` IN (_UTF8MB4's46',_UTF8MB4's28',_UTF8MB4's18')) GROUP BY `t0`.`p0` HAVING (`t0`.`p0`<47.36) ORDER BY SUM(`shiro_fuzz_r2`.`t2`.`d0`) LIMIT 7;

SET FOREIGN_KEY_CHECKS=1;
