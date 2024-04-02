CREATE TABLE `dup_detect`
(
    `col1` bigint(20) unsigned NOT NULL,
    `col2` varchar(10)     NOT NULL,
    `col3` bigint(20) unsigned NOT NULL,
    `col4` varchar(10)     NOT NULL,
    `col5` decimal(36, 18) NOT NULL,
    `col6` tinyint(4) NOT NULL,
    `col7` int(11) NOT NULL,
    PRIMARY KEY (`col1`),
    INDEX  `idx_col3` (`col3`),
    INDEX  `idx_col4` (`col4`),
    UNIQUE KEY `uniq_col6_col7` (`col6`, `col7`)
);
