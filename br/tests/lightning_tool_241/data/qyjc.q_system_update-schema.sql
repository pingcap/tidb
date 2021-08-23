CREATE TABLE `q_system_update` (
`id` int(10) NOT NULL AUTO_INCREMENT,
`version` varchar(255) DEFAULT '',
`timepoint` datetime NOT NULL,
`status` tinyint(1) NOT NULL DEFAULT '0',
PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;