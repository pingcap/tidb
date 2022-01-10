CREATE TABLE `gbk_test` (
      `id` int(11) DEFAULT NULL,
      `v` varchar(32) DEFAULT NULL,
      `v2` varchar(32) CHARACTER SET gbk COLLATE gbk_chinese_ci DEFAULT NULL,
      KEY `idx_v` (`v`, `id`),
      KEY `idx_gbk` (`v2`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
