CREATE TABLE `q_user_log` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`uid` int(11) unsigned NOT NULL DEFAULT '0',
`type` varchar(20) DEFAULT NULL,
`level` tinyint(1) DEFAULT NULL,
`code` varchar(20) DEFAULT NULL,
`string` varchar(255) DEFAULT NULL,
`message` varchar(255) DEFAULT NULL,
`create_time` timestamp NULL DEFAULT NULL,
`status` tinyint(1) DEFAULT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;