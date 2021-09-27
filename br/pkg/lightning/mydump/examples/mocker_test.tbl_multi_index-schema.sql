/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;

CREATE TABLE `tbl_multi_index` (
  `Name` varchar(64) DEFAULT NULL,
  `Age` int(10) UNSIGNED DEFAULT NULL,
  KEY `idx_name` (`Name`),
  KEY `idx_age_name` (`Age`,`Name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
