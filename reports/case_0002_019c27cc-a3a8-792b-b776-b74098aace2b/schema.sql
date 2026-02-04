SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS v5;
DROP VIEW IF EXISTS v4;
DROP VIEW IF EXISTS v3;
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
  `c1` bigint DEFAULT NULL,
  `c2` int NOT NULL,
  `c3` decimal(12,2) DEFAULT NULL,
  `c4` tinyint(1) NOT NULL,
  `c5` bigint NOT NULL,
  `c6` date NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c2` (`c2`),
  KEY `idx_c6_4` (`c6`),
  KEY `idx_c3_26` (`c3`),
  KEY `idx_c4_27` (`c4`),
  KEY `idx_c2_c5_c0_29` (`c2`,`c5`,`c0`),
  KEY `idx_c0_29` (`c0`),
  KEY `idx_c5_30` (`c5`),
  KEY `idx_id_31` (`id`)
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
  KEY `idx_c3` (`c3`),
  KEY `idx_c0_1` (`c0`),
  KEY `idx_c5_2` (`c5`),
  KEY `idx_c1_8` (`c1`),
  KEY `idx_c4_18` (`c4`),
  KEY `idx_id_20` (`id`),
  KEY `idx_c2_c0_c3_32` (`c2`,`c0`,`c3`),
  CONSTRAINT `fk_102` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_149` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_162` FOREIGN KEY (`id`) REFERENCES `t4` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` int NOT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` double NOT NULL,
  `c3` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `fk_2` (`id`),
  KEY `idx_c2_9` (`c2`),
  KEY `idx_id_11` (`id`),
  KEY `idx_c0_14` (`c0`),
  KEY `idx_c3_21` (`c3`),
  KEY `idx_c2_c1_26` (`c2`,`c1`),
  KEY `idx_c0_c3_c1_29` (`c0`,`c3`,`c1`),
  KEY `idx_c2_c3_c1_31` (`c2`,`c3`,`c1`),
  CONSTRAINT `fk_2` FOREIGN KEY (`id`) REFERENCES `t1` (`id`),
  CONSTRAINT `fk_7` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_79` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_163` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` float NOT NULL,
  `c1` decimal(12,2) NOT NULL,
  `c2` int DEFAULT NULL,
  `c3` decimal(12,2) NOT NULL,
  `c4` double NOT NULL,
  `c5` float DEFAULT NULL,
  `c6` float DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c1` (`c1`),
  KEY `idx_c3_3` (`c3`),
  KEY `idx_c6_5` (`c6`),
  KEY `idx_c4_12` (`c4`),
  KEY `idx_c5_13` (`c5`),
  KEY `idx_c2_17` (`c2`),
  KEY `idx_c0_22` (`c0`),
  KEY `idx_id_23` (`id`),
  KEY `idx_c2_c4_c6_26` (`c2`,`c4`,`c6`),
  KEY `idx_c1_c5_c0_29` (`c1`,`c5`,`c0`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` varchar(64) DEFAULT NULL,
  `c1` float NOT NULL,
  `c2` bigint DEFAULT NULL,
  `c3` tinyint(1) NOT NULL,
  `c4` bigint NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c3_0` (`c3`),
  KEY `idx_c2_6` (`c2`),
  KEY `idx_c1_7` (`c1`),
  KEY `idx_c1_c2_14` (`c1`,`c2`),
  KEY `idx_c4_15` (`c4`),
  KEY `idx_id_16` (`id`),
  KEY `idx_c0_24` (`c0`),
  KEY `idx_c0_c1_c3_26` (`c0`,`c1`,`c3`),
  CONSTRAINT `fk_90` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_123` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_145` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_171` FOREIGN KEY (`id`) REFERENCES `t1` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` datetime NOT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` int DEFAULT NULL,
  `c3` timestamp NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_c3` (`c3`),
  KEY `idx_c0_10` (`c0`),
  KEY `idx_c1_19` (`c1`),
  KEY `idx_id_25` (`id`),
  KEY `idx_c1_c0_28` (`c1`,`c0`),
  KEY `idx_c0_c2_30` (`c0`,`c2`),
  CONSTRAINT `fk_103` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_141` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_147` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_151` FOREIGN KEY (`id`) REFERENCES `t1` (`id`),
  CONSTRAINT `fk_198` FOREIGN KEY (`id`) REFERENCES `t2` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`c0`, `c1`, `c2`) AS SELECT (`t0`.`c2`*`t0`.`c3`) AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t1` WHERE (`t1`.`c1`=`t0`.`c0`)) AS `c1`,`t1`.`c4` AS `c2` FROM `shiro_fuzz_r1`.`t0` LEFT JOIN `shiro_fuzz_r1`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t1` WHERE ((`t1`.`c5`<`t1`.`c4`) OR (`t1`.`c3`>=`t1`.`c2`)) LIMIT 5);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`g0`, `g1`, `cnt`, `sum1`) AS SELECT `t0`.`c5` AS `g0`,`v0`.`c1` AS `g1`,COUNT(1) AS `cnt`,SUM(`t0`.`c3`) AS `sum1` FROM (`shiro_fuzz_r1`.`t1` JOIN `shiro_fuzz_r1`.`t0` ON (1=0)) LEFT JOIN `shiro_fuzz_r1`.`v0` ON (1=0) WHERE (NOT (`t0`.`c1` IN (SELECT `t1`.`id` AS `c0` FROM `shiro_fuzz_r1`.`t1` WHERE (`t1`.`c5`=`v0`.`c1`))) AND (NOT (`t0`.`c2` IN (SELECT `t1`.`c0` AS `c0` FROM `shiro_fuzz_r1`.`t1` WHERE (`t1`.`c5`=`t1`.`c5`) LIMIT 5)) AND ((`t0`.`c6`<`t0`.`c0`) AND EXISTS (SELECT `v0`.`c2` AS `c0` FROM `shiro_fuzz_r1`.`v0` WHERE NOT EXISTS (SELECT `v0`.`c0` AS `c0` FROM `shiro_fuzz_r1`.`v0` WHERE (`v0`.`c1`!=`v0`.`c0`)))))) GROUP BY `t0`.`c5`,`v0`.`c1` HAVING (`t0`.`c5`<=95) ORDER BY 1,2 LIMIT 18;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`c0`) AS SELECT (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t2` WHERE (`t2`.`id`=`t0`.`c1`)) AS `c0` FROM (`shiro_fuzz_r1`.`t0` JOIN `shiro_fuzz_r1`.`t1` ON (1=0)) LEFT JOIN `shiro_fuzz_r1`.`t2` ON (1=0) WHERE (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t0` WHERE (`t0`.`c1`=`t0`.`c5`)) OR (`t0`.`c6`>=`t0`.`c0`));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`c0`, `c1`) AS SELECT DISTINCT (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`v1` WHERE NOT (`v1`.`g0` IN (80,88,76))) AS `c0`,LOWER(`v0`.`c0`) AS `c1` FROM ((`shiro_fuzz_r1`.`t2` LEFT JOIN `shiro_fuzz_r1`.`v3` ON (1=0)) JOIN `shiro_fuzz_r1`.`v0` ON (1=0)) LEFT JOIN `shiro_fuzz_r1`.`v1` ON (1=0) WHERE (NOT (`t2`.`c1` IN (SELECT `t2`.`c1` AS `c0` FROM `shiro_fuzz_r1`.`t2` WHERE (`t2`.`id`=`v1`.`g1`) ORDER BY `t2`.`c1`,`t2`.`c0` DESC LIMIT 10)) AND (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t2` WHERE (`t2`.`c1` IN (1,0,1))) AND EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t2` WHERE ((`t2`.`c2`>=`t2`.`c0`) OR NOT EXISTS (SELECT `t2`.`c2` AS `c0` FROM `shiro_fuzz_r1`.`t2` WHERE ((`t2`.`c0`<`t2`.`id`) AND (`t2`.`c0`<=>`t2`.`c3`)))))));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v5` (`c0`, `c1`) AS SELECT (`t3`.`c0`*`t3`.`c5`) AS `c0`,RANK() OVER (ORDER BY `t0`.`c6` DESC) AS `c1` FROM (((`shiro_fuzz_r1`.`t3` JOIN `shiro_fuzz_r1`.`v1` ON (1=0)) JOIN `shiro_fuzz_r1`.`v4` ON (1=0)) JOIN `shiro_fuzz_r1`.`t0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r1`.`v0` ON (1=0) WHERE ((`v0`.`c0`>=`v4`.`c0`) AND NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r1`.`t2` WHERE ((`t2`.`c2`>=`t2`.`c0`) OR (`t2`.`c2`<=>`t2`.`c3`)))) ORDER BY `v1`.`g0` DESC,`t3`.`c2` LIMIT 17;

SET FOREIGN_KEY_CHECKS=1;
