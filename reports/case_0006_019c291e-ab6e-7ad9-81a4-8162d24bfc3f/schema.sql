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
  `c0` date NOT NULL,
  `c1` datetime NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0_1` (`c0`),
  KEY `idx_id_10` (`id`),
  KEY `idx_c1_11` (`c1`),
  KEY `idx_c1_c0_12` (`c1`,`c0`),
  KEY `idx_c0_c1_32` (`c0`,`c1`),
  CONSTRAINT `fk_329` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_334` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_342` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_375` FOREIGN KEY (`id`) REFERENCES `t4` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t1` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` float NOT NULL,
  `c3` timestamp NOT NULL,
  `c4` timestamp NOT NULL,
  `c5` decimal(12,2) NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c5_13` (`c5`),
  KEY `idx_c0_17` (`c0`),
  KEY `idx_c4_18` (`c4`),
  KEY `idx_c2_19` (`c2`),
  KEY `idx_c3_20` (`c3`),
  KEY `idx_c1_21` (`c1`),
  KEY `idx_id_22` (`id`),
  KEY `idx_c5_c2_c3_39` (`c5`,`c2`,`c3`),
  KEY `idx_c0_c5_c3_39` (`c0`,`c5`,`c3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`id`) PARTITIONS 2;

CREATE TABLE `t2` (
  `id` bigint NOT NULL,
  `c0` tinyint(1) NOT NULL,
  `c1` timestamp NOT NULL,
  `c2` varchar(64) NOT NULL,
  `c3` decimal(12,2) NOT NULL,
  `c4` bigint DEFAULT NULL,
  `c5` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c0` (`c0`),
  KEY `idx_c4` (`c4`),
  KEY `idx_c5` (`c5`),
  KEY `idx_c3_0` (`c3`),
  KEY `idx_c5_c4_1` (`c5`,`c4`),
  KEY `idx_id_6` (`id`),
  KEY `idx_c2_7` (`c2`),
  KEY `idx_c1_8` (`c1`),
  KEY `idx_c0_c3_c2_34` (`c0`,`c3`,`c2`),
  KEY `idx_c5_c0_c1_38` (`c5`,`c0`,`c1`),
  KEY `idx_c4_c0_c2_39` (`c4`,`c0`,`c2`),
  KEY `idx_c0_c1_c2_39` (`c0`,`c1`,`c2`),
  KEY `idx_c5_c4_c0_39` (`c5`,`c4`,`c0`),
  KEY `idx_c3_c0_c1_39` (`c3`,`c0`,`c1`),
  KEY `idx_c4_c0_39` (`c4`,`c0`),
  CONSTRAINT `fk_8` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_41` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_52` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_84` FOREIGN KEY (`id`) REFERENCES `t5` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t3` (
  `id` bigint NOT NULL,
  `c0` int NOT NULL,
  `c1` tinyint(1) NOT NULL,
  `c2` date DEFAULT NULL,
  `c3` timestamp NOT NULL,
  `c4` int NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c4` (`c4`),
  KEY `idx_id_2` (`id`),
  KEY `idx_c0_4` (`c0`),
  KEY `idx_c2_5` (`c2`),
  KEY `idx_c3_9` (`c3`),
  KEY `idx_c1_12` (`c1`),
  KEY `idx_c0_c4_29` (`c0`,`c4`),
  KEY `idx_c1_c2_c4_34` (`c1`,`c2`,`c4`),
  KEY `idx_c4_c3_c1_35` (`c4`,`c3`,`c1`),
  KEY `idx_c4_c1_36` (`c4`,`c1`),
  KEY `idx_c2_c3_36` (`c2`,`c3`),
  KEY `idx_c1_c0_38` (`c1`,`c0`),
  KEY `idx_c2_c0_c1_38` (`c2`,`c0`,`c1`),
  CONSTRAINT `fk_3` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_173` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_192` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_254` FOREIGN KEY (`id`) REFERENCES `t5` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t4` (
  `id` bigint NOT NULL,
  `c0` timestamp NOT NULL,
  `c1` double DEFAULT NULL,
  `c2` double NOT NULL,
  `c3` tinyint(1) NOT NULL,
  `c4` varchar(64) NOT NULL,
  `c5` float NOT NULL,
  `c6` datetime NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c3` (`c3`),
  KEY `idx_c5` (`c5`),
  KEY `idx_c6` (`c6`),
  KEY `idx_c4_c3_4` (`c4`,`c3`),
  KEY `idx_id_16` (`id`),
  KEY `idx_c1_25` (`c1`),
  KEY `idx_c2_26` (`c2`),
  KEY `idx_c4_27` (`c4`),
  KEY `idx_c0_28` (`c0`),
  KEY `idx_c2_c0_31` (`c2`,`c0`),
  KEY `idx_c1_c6_35` (`c1`,`c6`),
  KEY `idx_c2_c5_38` (`c2`,`c5`),
  KEY `idx_c3_c1_c0_39` (`c3`,`c1`,`c0`),
  KEY `idx_c6_c2_39` (`c6`,`c2`),
  CONSTRAINT `fk_118` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_133` FOREIGN KEY (`id`) REFERENCES `t0` (`id`),
  CONSTRAINT `fk_137` FOREIGN KEY (`id`) REFERENCES `t5` (`id`),
  CONSTRAINT `fk_162` FOREIGN KEY (`id`) REFERENCES `t3` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `t5` (
  `id` bigint NOT NULL,
  `c0` bigint NOT NULL,
  `c1` decimal(12,2) NOT NULL,
  `c2` varchar(64) NOT NULL,
  `c3` double NOT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_c2` (`c2`),
  KEY `idx_id_3` (`id`),
  KEY `idx_c3_14` (`c3`),
  KEY `idx_c3_c2_23` (`c3`,`c2`),
  KEY `idx_c1_23` (`c1`),
  KEY `idx_c0_24` (`c0`),
  KEY `idx_c3_c1_31` (`c3`,`c1`),
  KEY `idx_c0_c3_c1_32` (`c0`,`c3`,`c1`),
  KEY `idx_c2_c0_c3_35` (`c2`,`c0`,`c3`),
  KEY `idx_c3_c0_38` (`c3`,`c0`),
  KEY `idx_c0_c1_38` (`c0`,`c1`),
  KEY `idx_c1_c3_38` (`c1`,`c3`),
  KEY `idx_c0_c2_c3_39` (`c0`,`c2`,`c3`),
  KEY `idx_c2_c1_c3_39` (`c2`,`c1`,`c3`),
  KEY `idx_c3_c0_c1_39` (`c3`,`c0`,`c1`),
  CONSTRAINT `fk_7` FOREIGN KEY (`id`) REFERENCES `t4` (`id`),
  CONSTRAINT `fk_23` FOREIGN KEY (`id`) REFERENCES `t2` (`id`),
  CONSTRAINT `fk_144` FOREIGN KEY (`id`) REFERENCES `t3` (`id`),
  CONSTRAINT `fk_163` FOREIGN KEY (`id`) REFERENCES `t0` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v0` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t1`.`c5`) AS `sum1` FROM `shiro_fuzz_r5`.`t0` LEFT JOIN `shiro_fuzz_r5`.`t1` ON (1=0) WHERE NOT EXISTS (SELECT `t1`.`id` AS `c0` FROM `shiro_fuzz_r5`.`t1` WHERE ((`t1`.`c2`>`t1`.`c5`) AND (`t1`.`id`!=`t1`.`c2`)) ORDER BY (`t1`.`c2`+`t1`.`c2`) DESC,(`t1`.`c5`*`t1`.`c2`) LIMIT 5);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v1` (`c0`, `c1`, `c2`) AS SELECT ROW_NUMBER() OVER (ORDER BY `t0`.`id` DESC) AS `c0`,(SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`v0` WHERE (`v0`.`cnt`=`t1`.`c0`)) AS `c1`,(`t0`.`id`*`v0`.`sum1`) AS `c2` FROM (`shiro_fuzz_r5`.`t0` JOIN `shiro_fuzz_r5`.`t1` ON (1=0)) JOIN `shiro_fuzz_r5`.`v0` ON (1=0) WHERE (`t1`.`c1`<=`t1`.`c0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v2` (`cnt`, `sum1`) AS SELECT DISTINCT COUNT(1) AS `cnt`,SUM(`v0`.`sum1`) AS `sum1` FROM (`shiro_fuzz_r5`.`v0` JOIN `shiro_fuzz_r5`.`t0` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`v1` ON (1=0) WHERE (`t0`.`c1`<=`t0`.`c0`);

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v3` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t1`.`c5`) AS `sum1` FROM (((`shiro_fuzz_r5`.`t0` JOIN `shiro_fuzz_r5`.`v2` ON (1=0)) RIGHT JOIN `shiro_fuzz_r5`.`v0` ON (1=0)) RIGHT JOIN `shiro_fuzz_r5`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r5`.`t1` ON (1=0) WHERE (EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`t1` WHERE (`t1`.`c3`<=>`t1`.`c4`) LIMIT 2) AND (NOT (`t1`.`id` IN (58,15,0)) OR NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`t1` WHERE ((`t1`.`c5`<`t1`.`c2`) OR (`t1`.`c4`>`t1`.`c3`)))));

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `v4` (`cnt`, `sum1`) AS SELECT COUNT(1) AS `cnt`,SUM(`t1`.`c5`) AS `sum1` FROM ((((`shiro_fuzz_r5`.`v2` LEFT JOIN `shiro_fuzz_r5`.`t0` ON (1=0)) LEFT JOIN `shiro_fuzz_r5`.`v1` ON (1=0)) RIGHT JOIN `shiro_fuzz_r5`.`v0` ON (1=0)) JOIN `shiro_fuzz_r5`.`v3` ON (1=0)) RIGHT JOIN `shiro_fuzz_r5`.`t1` ON (1=0) WHERE (NOT EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`t0` WHERE ((`t0`.`c1`!=`t0`.`c0`) AND (`t0`.`c1`!=`t0`.`c0`)) ORDER BY COUNT(1) LIMIT 4) AND EXISTS (SELECT COUNT(1) AS `cnt` FROM `shiro_fuzz_r5`.`t1` WHERE NOT (`t1`.`c0` IN (0)))) ORDER BY 1 DESC,2 DESC;

SET FOREIGN_KEY_CHECKS=1;
