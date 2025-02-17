CREATE TABLE `gbk_source` (
	`id` BIGINT PRIMARY KEY COMMENT "自增 ID",
	`v` VARCHAR(32) COMMENT "中文评论",
	KEY `idx_gbk` (`v`, `id`)
) DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;
