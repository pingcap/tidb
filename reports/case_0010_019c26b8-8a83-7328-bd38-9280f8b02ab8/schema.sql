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
  `c0` int NOT NULL,
  `c1` varchar(64) NOT NULL,
  `c2` float DEFAULT NULL,
  `c3` datetime NOT NULL,
  `c4` timestamp NOT NULL,
  `c5` varchar(64) DEFAULT NULL,
  `c6` float NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c3` (`c3`),
  KEY `idx_c4` (`c4`),
  KEY `idx_c5` (`c5`),
  KEY `idx_c4_c6_c5_1` (`c4`,`c6`,`c5`),
  KEY `idx_c6_8` (`c6`),
  KEY `idx_c2_15` (`c2`),
  KEY `idx_c1_17` (`c1`),
  KEY `idx_c0_19` (`c0`),
  KEY `idx_id_22` (`id`),
  KEY `idx_c3_c1_31` (`c3`,`c1`),
  CONSTRAINT `fk_45` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_65` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_69` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_89` FOREIGN KEY (`id`) REFERENCES `t1` (`id`),
  CONSTRAINT `fk_157` FOREIGN KEY (`id`) REFERENCES `t2` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` varchar(64) NOT NULL,
  `c1` int NOT NULL,
  `c2` float NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_id_4` (`id`),
  KEY `idx_c0_5` (`c0`),
  KEY `idx_c1_c0_c2_9` (`c1`,`c0`,`c2`),
  KEY `idx_c2_9` (`c2`),
  KEY `idx_c1_25` (`c1`),
  KEY `idx_c1_c0_28` (`c1`,`c0`),
  CONSTRAINT `fk_142` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_149` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_172` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_194` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` float NOT NULL,
  `c1` decimal(12,2) NOT NULL,
  `c2` datetime DEFAULT NULL,
  `c3` float DEFAULT NULL,
  `c4` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1_6` (`c1`),
  KEY `idx_c2_7` (`c2`),
  KEY `idx_id_10` (`id`),
  KEY `idx_c3_11` (`c3`),
  KEY `idx_c4_21` (`c4`),
  KEY `idx_c0_24` (`c0`),
  KEY `idx_c3_c2_c0_29` (`c3`,`c2`,`c0`),
  KEY `idx_c1_c2_c3_32` (`c1`,`c2`,`c3`),
  KEY `idx_c1_c3_c4_32` (`c1`,`c3`,`c4`),
  CONSTRAINT `fk_37` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_81` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_122` FOREIGN KEY (`id`) REFERENCES `t5` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` double NOT NULL,
  `c2` double NOT NULL,
  `c3` date DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c3` (`c3`),
  KEY `idx_id_3` (`id`),
  KEY `idx_c0_18` (`c0`),
  KEY `idx_c2_20` (`c2`),
  KEY `idx_c3_c0_c2_23` (`c3`,`c0`,`c2`),
  KEY `idx_c1_c2_30` (`c1`,`c2`),
  KEY `idx_c2_c1_32` (`c2`,`c1`),
  KEY `idx_c2_c3_c1_32` (`c2`,`c3`,`c1`),
  KEY `idx_c3_c2_c1_32` (`c3`,`c2`,`c1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` timestamp NOT NULL,
  `c2` decimal(12,2) NOT NULL,
  `c3` int NOT NULL,
  `c4` bigint NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c2_0` (`c2`),
  KEY `idx_id_1` (`id`),
  KEY `idx_c3_12` (`c3`),
  KEY `idx_c4_14` (`c4`),
  KEY `idx_c0_16` (`c0`),
  KEY `idx_c1_c2_c4_30` (`c1`,`c2`,`c4`),
  CONSTRAINT `fk_19` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_124` FOREIGN KEY (`id`) REFERENCES `t1` (`id`),
  CONSTRAINT `fk_125` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_126` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` varchar(64) NOT NULL,
  `c1` timestamp NOT NULL,
  `c2` float NOT NULL,
  `c3` varchar(64) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2_2` (`c2`),
  KEY `fk_20` (`id`),
  KEY `idx_c0_13` (`c0`),
  KEY `idx_c3_26` (`c3`),
  KEY `idx_c1_27` (`c1`),
  KEY `idx_id_28` (`id`),
  KEY `idx_c0_c1_31` (`c0`,`c1`),
  CONSTRAINT `fk_20` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_84` FOREIGN KEY (`id`) REFERENCES `t1` (`id`),
  CONSTRAINT `fk_91` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_140` FOREIGN KEY (`id`) REFERENCES `t3` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`g0`, `cnt`, `sum1`) AS SELECT DISTINCT `t1`.`c0` AS `g0`,COUNT(1) AS `cnt`,SUM(`t1`.`c2`) AS `sum1` FROM `shiro_fuzz_r9`.`t0` JOIN `shiro_fuzz_r9`.`t1` ON (1=0) WHERE (`t0`.`c0`<=>`t0`.`c6`) GROUP BY `t1`.`c0`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`) AS SELECT ABS(`t0`.`id`) AS `c0` FROM (`shiro_fuzz_r9`.`t1` LEFT JOIN `shiro_fuzz_r9`.`v0` ON (1=0)) JOIN `shiro_fuzz_r9`.`t0` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r9`.`t0` WHERE (`t0`.`c4`>=`t0`.`c3`) LIMIT 9);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`c0`, `c1`, `c2`) AS SELECT _UTF8MB4's23' AS `c0`,_UTF8MB4'2026-04-09 10:00:12' AS `c1`,`t0`.`c3` AS `c2` FROM (`shiro_fuzz_r9`.`v1` LEFT JOIN `shiro_fuzz_r9`.`t0` ON (1=0)) JOIN `shiro_fuzz_r9`.`v0` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r9`.`v1` WHERE ((`v1`.`c0`<32) AND (32=87)));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`v0`.`cnt`) AS `sum1` FROM (((`shiro_fuzz_r9`.`v2` LEFT JOIN `shiro_fuzz_r9`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r9`.`t0` ON (1=0)) JOIN `shiro_fuzz_r9`.`v1` ON (1=0)) LEFT JOIN `shiro_fuzz_r9`.`t1` ON (1=0) WHERE EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r9`.`v0` WHERE ((`v0`.`cnt`>=`v0`.`sum1`) AND (`v0`.`cnt`=`v0`.`sum1`))) ORDER BY COUNT(1) DESC LIMIT 14;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`) AS SELECT DISTINCT 99 AS `c0`,`v2`.`c1` AS `c1` FROM ((((`shiro_fuzz_r9`.`v0` RIGHT JOIN `shiro_fuzz_r9`.`v1` ON (1=0)) JOIN `shiro_fuzz_r9`.`v3` ON (1=0)) JOIN `shiro_fuzz_r9`.`t1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r9`.`t0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r9`.`v2` ON (1=0) WHERE (EXISTS (SELECT `v0`.`g0` AS `c0` FROM `shiro_fuzz_r9`.`v0` WHERE (`v0`.`cnt`<=>`v0`.`sum1`)) OR NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r9`.`v3` WHERE (`v3`.`cnt`>=`v3`.`sum1`))) ORDER BY `v2`.`c1` DESC LIMIT 17;

SET FOREIGN_KEY_CHECKS=1;
