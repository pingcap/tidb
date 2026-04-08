CREATE TABLE `test_perm` (
	`id` int(11) NOT NULL,
	`contract_no` varchar(64) DEFAULT NULL,
	`fund_seq_no` varchar(64) DEFAULT NULL,
	`term_no` int(11) DEFAULT NULL,
	`contract_type` varchar(8) DEFAULT NULL,
	`internal_transfer_tag` varchar(8) DEFAULT NULL,
	`prin_amt` int(11) DEFAULT NULL,
	`start_date` varchar(8) DEFAULT NULL,
	`end_date` varchar(8) DEFAULT NULL,
	`batch_date` varchar(32) DEFAULT NULL,
	`crt_time` timestamp DEFAULT CURRENT_TIMESTAMP,
	`region_code` varchar(8) DEFAULT NULL,
	`credit_code` varchar(64) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY RANGE COLUMNS(batch_date) (
	PARTITION `P20200224` VALUES LESS THAN ("2020-02-05 00:00:00"),
	PARTITION `P20200324` VALUES LESS THAN ("2020-03-05 00:00:00"),
	PARTITION `P20200424` VALUES LESS THAN ("2020-04-05 00:00:00"),
	PARTITION `P20200524` VALUES LESS THAN ("2020-05-05 00:00:00"),
	PARTITION `P_MAXVALUE` VALUES LESS THAN MAXVALUE
);
