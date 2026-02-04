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
  `c0` bigint NOT NULL,
  `c1` decimal(12,2) NOT NULL,
  `c2` bigint NOT NULL,
  `c3` decimal(12,2) NOT NULL,
  `c4` varchar(64) NOT NULL,
  `c5` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c4` (`c4`),
  KEY `idx_c5` (`c5`),
  KEY `idx_c1_7` (`c1`),
  KEY `idx_c0_11` (`c0`),
  KEY `idx_c2_14` (`c2`),
  KEY `idx_c3_16` (`c3`),
  KEY `idx_id_17` (`id`),
  KEY `idx_c0_c3_20` (`c0`,`c3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` double NOT NULL,
  `c1` float DEFAULT NULL,
  `c2` double NOT NULL,
  `c3` timestamp NOT NULL,
  `c4` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c3` (`c3`),
  KEY `idx_c2_5` (`c2`),
  KEY `idx_c0_c2_c4_6` (`c0`,`c2`,`c4`),
  KEY `idx_c4_6` (`c4`),
  KEY `idx_c1_13` (`c1`),
  KEY `idx_id_15` (`id`),
  CONSTRAINT `fk_66` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` int NOT NULL,
  `c1` decimal(12,2) DEFAULT NULL,
  `c2` varchar(64) DEFAULT NULL,
  `c3` datetime NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c1` (`c1`),
  KEY `idx_c2` (`c2`),
  KEY `idx_c3_1` (`c3`),
  KEY `fk_8` (`id`),
  KEY `idx_id_3` (`id`),
  KEY `idx_c0_c1_20` (`c0`,`c1`),
  CONSTRAINT `fk_8` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` bigint NOT NULL,
  `c1` int DEFAULT NULL,
  `c2` decimal(12,2) DEFAULT NULL,
  `c3` int DEFAULT NULL,
  `c4` datetime DEFAULT NULL,
  `c5` date NOT NULL,
  `c6` float NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c3` (`c3`),
  KEY `idx_c6` (`c6`),
  KEY `idx_c0_0` (`c0`),
  KEY `idx_c4_c0_3` (`c4`,`c0`),
  KEY `idx_c4_18` (`c4`),
  KEY `idx_c4_c2_19` (`c4`,`c2`),
  KEY `idx_c3_c2_19` (`c3`,`c2`),
  KEY `idx_c3_c4_c6_20` (`c3`,`c4`,`c6`),
  KEY `idx_id_21` (`id`),
  KEY `idx_c1_22` (`c1`),
  KEY `idx_c2_c4_23` (`c2`,`c4`),
  KEY `idx_c2_23` (`c2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 4;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` float NOT NULL,
  `c1` date NOT NULL,
  `c2` tinyint(1) NOT NULL,
  `c3` decimal(12,2) NOT NULL,
  `c4` float NOT NULL,
  `c5` decimal(12,2) NOT NULL,
  `c6` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c1` (`c1`),
  KEY `idx_c2` (`c2`),
  KEY `idx_c3` (`c3`),
  KEY `idx_c6_2` (`c6`),
  KEY `fk_10` (`id`),
  KEY `idx_c5_8` (`c5`),
  KEY `idx_c4_9` (`c4`),
  KEY `idx_id_10` (`id`),
  CONSTRAINT `fk_10` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` decimal(12,2) NOT NULL,
  `c1` decimal(12,2) NOT NULL,
  `c2` int DEFAULT NULL,
  `c3` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c2` (`c2`),
  KEY `idx_id_4` (`id`),
  KEY `idx_c3_19` (`c3`),
  KEY `idx_c1_20` (`c1`),
  CONSTRAINT `fk_57` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_97` FOREIGN KEY (`id`) REFERENCES `t1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c1`) AS `sum1` FROM `shiro_fuzz_r13`.`t0` RIGHT JOIN `shiro_fuzz_r13`.`t1` ON (1=0) WHERE EXISTS (SELECT `t0`.`c4` AS `c0` FROM `shiro_fuzz_r13`.`t0` WHERE EXISTS (SELECT `t0`.`c3` AS `c0` FROM `shiro_fuzz_r13`.`t0` WHERE (`t0`.`c2`<=`t0`.`id`) ORDER BY `t0`.`c3` LIMIT 9)) ORDER BY SUM(`shiro_fuzz_r13`.`t0`.`c1`) DESC,COUNT(1) LIMIT 6;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c1`) AS `sum1` FROM (`shiro_fuzz_r13`.`t0` LEFT JOIN `shiro_fuzz_r13`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r13`.`t1` ON (1=0) WHERE (((`t1`.`c3` IN (_UTF8MB4'2026-08-16 17:24:42')) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r13`.`t0` WHERE ((`t0`.`c1`>`t0`.`id`) OR (`t0`.`c2`<=`t0`.`c3`)) ORDER BY COUNT(1) LIMIT 2)) AND (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r13`.`t0` WHERE (`t0`.`c2`>=`t0`.`c1`)) AND (`t0`.`c0`<`t1`.`c2`)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t0`.`c1`) AS `sum1` FROM (`shiro_fuzz_r13`.`v0` RIGHT JOIN `shiro_fuzz_r13`.`t0` ON (1=0)) JOIN `shiro_fuzz_r13`.`t1` ON (1=0) WHERE NOT (`t1`.`c2` IN (75.97,96.07,0.2)) ORDER BY SUM(`shiro_fuzz_r13`.`t0`.`c1`),COUNT(1) LIMIT 11;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`g0`, `cnt`, `sum1`) AS SELECT `t1`.`c2` AS `g0`,COUNT(1) AS `cnt`,SUM(`t0`.`c1`) AS `sum1` FROM (`shiro_fuzz_r13`.`v0` RIGHT JOIN `shiro_fuzz_r13`.`t1` ON (1=0)) JOIN `shiro_fuzz_r13`.`t0` ON (1=0) WHERE ((NOT EXISTS (SELECT `t0`.`c3` AS `c0` FROM `shiro_fuzz_r13`.`t0` WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r13`.`v2` WHERE NOT (`v2`.`sum1` IN (SELECT `t1`.`c4` AS `c0` FROM `shiro_fuzz_r13`.`t1` WHERE (`t1`.`c0`=`v2`.`sum1`))))) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r13`.`v0` WHERE (`v0`.`cnt`>`v0`.`sum1`))) AND EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r13`.`v1` WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r13`.`v1` WHERE (`v1`.`cnt` IN (76)) ORDER BY COUNT(1) DESC LIMIT 1))) GROUP BY `t1`.`c2`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`v0`.`sum1`) AS `sum1` FROM (((`shiro_fuzz_r13`.`v0` LEFT JOIN `shiro_fuzz_r13`.`t1` ON (1=0)) JOIN `shiro_fuzz_r13`.`t2` ON (1=0)) JOIN `shiro_fuzz_r13`.`v3` ON (1=0)) JOIN `shiro_fuzz_r13`.`v2` ON (1=0) WHERE (`t1`.`c2`!=`v2`.`sum1`) ORDER BY SUM(`shiro_fuzz_r13`.`v0`.`sum1`),COUNT(1) DESC;

SET FOREIGN_KEY_CHECKS=1;
