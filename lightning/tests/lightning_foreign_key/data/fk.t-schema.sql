CREATE TABLE `t`
(
    `a` bigint(20) NOT NULL,
    `b` bigint(20) DEFAULT NULL,
    PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */,
    KEY `fk_1` (`b`),
    CONSTRAINT `fk_1` FOREIGN KEY (`b`) REFERENCES `test`.`t2` (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
