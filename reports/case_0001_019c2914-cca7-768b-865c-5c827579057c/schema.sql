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
  `k2` date NOT NULL,
  `k3` bigint NOT NULL,
  `p0` double NOT NULL,
  `p1` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k2_4` (`k2`),
  KEY `idx_k1_14` (`k1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` datetime NOT NULL,
  `d1` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `k1` varchar(64) NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` float NOT NULL,
  `d1` float NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `k2` date NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` date NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k2_7` (`k2`),
  KEY `idx_id_15` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `k3` bigint NOT NULL,
  `k0` varchar(64) NOT NULL,
  `d0` date NOT NULL,
  `d1` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_k0_1` (`k0`),
  KEY `idx_id_2` (`id`),
  KEY `idx_d0_k0_k3_11` (`d0`,`k0`,`k3`),
  KEY `idx_d1_11` (`d1`),
  KEY `idx_k0_d1_14` (`k0`,`d1`),
  KEY `idx_k3_16` (`k3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t2`.`d1`) AS `sum1` FROM (((`shiro_fuzz`.`t0` RIGHT JOIN `shiro_fuzz`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND (`t0`.`k0`<=`t1`.`k0`))) JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND (`t0`.`k0` IN (_UTF8MB4's84')))) LEFT JOIN `shiro_fuzz`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND ((`t1`.`k0`=`t3`.`k0`) AND (`t0`.`k0`<=`t3`.`k0`)))) JOIN `shiro_fuzz`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND (`t1`.`k0`<`t2`.`k0`)) WHERE NOT EXISTS (SELECT `t4`.`k0` AS `c0` FROM `shiro_fuzz`.`t4` WHERE (`t4`.`k0`=`t0`.`k0`) ORDER BY `t4`.`d1` DESC,`t4`.`k3` LIMIT 5) ORDER BY 1,2;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT ROW_NUMBER() OVER (ORDER BY `t2`.`d0` DESC) AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz`.`v0` WHERE (`v0`.`cnt`=`t0`.`id`)) AS `c1`,`t0`.`p0` AS `c2` FROM ((`shiro_fuzz`.`t0` RIGHT JOIN `shiro_fuzz`.`t1` USING (`k0`)) LEFT JOIN `shiro_fuzz`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND (`t1`.`k0`<=>`t2`.`k0`))) LEFT JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND (`t2`.`k0` IN (_UTF8MB4's85',_UTF8MB4's79',_UTF8MB4's21'))) WHERE NOT EXISTS (SELECT `t2`.`k1` AS `c0` FROM `shiro_fuzz`.`t2` WHERE (`t2`.`k0`=`t0`.`k0`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`g0`, `g1`, `cnt`, `sum1`) AS SELECT `t3`.`id` AS `g0`,`t3`.`k0` AS `g1`,COUNT(1) AS `cnt`,SUM(`t0`.`k3`) AS `sum1` FROM ((`shiro_fuzz`.`t0` JOIN `shiro_fuzz`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND ((`t0`.`k0`=`t1`.`k0`) AND (`t0`.`k0`<=`t1`.`k0`)))) RIGHT JOIN `shiro_fuzz`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t1`.`k0`<=`t4`.`k0`) OR (`t0`.`k0`<`t4`.`k0`)))) LEFT JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND ((`t0`.`k0`=`t4`.`k0`) AND (`t0`.`k0`<=>`t3`.`k0`))) WHERE (EXISTS (SELECT `v1`.`c1` AS `c0` FROM `shiro_fuzz`.`v1` WHERE (`v1`.`c1`<=`v1`.`c0`)) AND NOT EXISTS (SELECT `t4`.`id` AS `c0` FROM `shiro_fuzz`.`t4` WHERE ((`t4`.`d1`<=>`t4`.`id`) AND (`t4`.`k3`=`t4`.`id`)) ORDER BY `t4`.`k3` DESC,`t4`.`id` LIMIT 1)) GROUP BY `t3`.`id`,`t3`.`k0`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`k3`) AS `sum1` FROM ((`shiro_fuzz`.`t0` LEFT JOIN `shiro_fuzz`.`t3` USING (`k0`)) JOIN `shiro_fuzz`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t0`.`k0`>`t3`.`k0`) AND (`t0`.`k0`<`t4`.`k0`)))) RIGHT JOIN `shiro_fuzz`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0` IN (_UTF8MB4's92'))) WHERE NOT EXISTS (SELECT `t4`.`k0` AS `c0` FROM `shiro_fuzz`.`t4` WHERE ((`t4`.`k0`=`t0`.`k0`) AND (`t4`.`k0`!=`t0`.`k0`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`, `c2`) AS SELECT `t0`.`p1` AS `c0`,ROW_NUMBER() OVER (ORDER BY `t3`.`d0` DESC) AS `c1`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz`.`t1` WHERE (`t1`.`k0`=`t0`.`k0`)) AS `c2` FROM (((`shiro_fuzz`.`t0` JOIN `shiro_fuzz`.`t2` ON ((`t0`.`k0`=`t2`.`k0`) AND (`t0`.`k0`>`t2`.`k0`))) RIGHT JOIN `shiro_fuzz`.`t4` ON ((`t0`.`k0`=`t4`.`k0`) AND ((`t0`.`k0`<=>`t2`.`k0`) AND (`t0`.`k0`>`t2`.`k0`)))) JOIN `shiro_fuzz`.`t3` ON ((`t0`.`k0`=`t3`.`k0`) AND (`t0`.`k0`<=`t3`.`k0`))) JOIN `shiro_fuzz`.`t1` ON ((`t0`.`k0`=`t1`.`k0`) AND ((`t0`.`k0`<=`t2`.`k0`) OR (`t0`.`k0`>=`t3`.`k0`))) WHERE (`t0`.`k0`>`t1`.`k0`);

SET FOREIGN_KEY_CHECKS=1;
