CREATE TABLE `gbk_source` (
	`id` BIGINT PRIMARY KEY COMMENT "���� ID",
	`v` VARCHAR(32) COMMENT "��������",
	KEY `idx_gbk` (`v`, `id`)
) DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;
