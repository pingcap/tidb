CREATE TABLE `q_report_desc` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`group_id` int(11) unsigned NOT NULL DEFAULT '0',
`host_count` int(11) unsigned NOT NULL DEFAULT '0',
`vul_count` int(11) unsigned NOT NULL DEFAULT '0',
`hight_num` int(11) unsigned NOT NULL DEFAULT '0',
`mid_num` int(11) unsigned NOT NULL DEFAULT '0',
`low_num` int(11) unsigned NOT NULL DEFAULT '0',
`notice_num` int(11) unsigned NOT NULL DEFAULT '0',
`day` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
`time_bucket` tinyint(1) NOT NULL DEFAULT '1',
`created_at` timestamp NULL DEFAULT NULL,
PRIMARY KEY (`id`),
KEY `group_id` (`group_id`) USING BTREE,
KEY `day` (`day`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;