CREATE TABLE `q_alarm_message_log` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`host_id` int(11) unsigned DEFAULT NULL,
`phone` char(11) NOT NULL,
`email` varchar(256) DEFAULT NULL,
`title` varchar(180) DEFAULT NULL,
`content` varchar(2000) NOT NULL,
`content_md5` char(32) DEFAULT NULL,
`type` tinyint(1) unsigned NOT NULL DEFAULT '0',
`alarm_type` varchar(128) DEFAULT NULL,
`status` tinyint(1) unsigned NOT NULL DEFAULT '1',
`response_code` varchar(256) NOT NULL DEFAULT '0',
`create_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
`read_status` tinyint(1) unsigned DEFAULT '0',
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;