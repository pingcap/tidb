CREATE TABLE `report_case_high_risk` (
`id` int(11) NOT NULL AUTO_INCREMENT,
`report_data` varchar(10) COLLATE utf8_bin NOT NULL DEFAULT '' COMMENT 'ͳ��ʱ��',
`caseType` varchar(60) COLLATE utf8_bin NOT NULL DEFAULT '' COMMENT '��������',
`total_case` int(11) NOT NULL DEFAULT '0' COMMENT '�ܰ���',
`today_new_case` int(11) NOT NULL DEFAULT '0' COMMENT '��������',
PRIMARY KEY(`id`),
KEY `idn_id_report_case_mid_risk` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;