CREATE TABLE `q_report_circular_data` (
`id` int(11) unsigned NOT NULL AUTO_INCREMENT,
`group_id` int(11) unsigned NOT NULL DEFAULT '0',
`handle_longest` varchar(255) NOT NULL DEFAULT '',
`respond_longest` varchar(255) NOT NULL DEFAULT '',
`circular_more` varchar(255) NOT NULL DEFAULT '',
`circular_count` int(11) unsigned NOT NULL DEFAULT '0',
`iscount_count` int(11) unsigned NOT NULL DEFAULT '0',
`noman_count` int(11) unsigned NOT NULL DEFAULT '0',
`end_time_avg` int(11) unsigned NOT NULL DEFAULT '0',
`respond_time_ave` int(11) unsigned NOT NULL DEFAULT '0',
`day` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
PRIMARY KEY (`id`),
KEY `group_id` (`group_id`) USING BTREE,
KEY `day` (`day`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;