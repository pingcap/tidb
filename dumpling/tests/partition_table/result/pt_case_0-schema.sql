/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET NAMES binary*/;
CREATE TABLE `pt_case_0` (
  `a` int DEFAULT NULL,
  `b` int DEFAULT NULL,
  UNIQUE KEY `idx` (`a`) /*T![global_index] GLOBAL */,
  KEY `idx1` (`a`) /*T![global_index] GLOBAL */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY HASH (`b`) PARTITIONS 5;
