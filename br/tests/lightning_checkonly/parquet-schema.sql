-- MySQL dump 10.13  Distrib 5.6.39, for macos10.13 (x86_64)
--
-- Host: 172.16.4.27    Database: test
-- ------------------------------------------------------
-- Server version	5.7.25-TiDB-v4.0.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `customer` (
    `c_id` int(11) NOT NULL,
    `c_d_id` int(11) NOT NULL,
    `c_w_id` int(11) NOT NULL,
    `c_first` varchar(16) DEFAULT NULL,
    `c_middle` char(2) DEFAULT NULL,
    `c_last` varchar(16) DEFAULT NULL,
    `c_street_1` varchar(20) DEFAULT NULL,
    `c_street_2` varchar(20) DEFAULT NULL,
    `c_city` varchar(20) DEFAULT NULL,
    `c_state` char(2) DEFAULT NULL,
    `c_zip` char(9) DEFAULT NULL,
    `c_phone` char(16) DEFAULT NULL,
    `c_since` datetime DEFAULT NULL,
    `c_credit` char(2) DEFAULT NULL,
    `c_credit_lim` decimal(12,2) DEFAULT NULL,
    `c_discount` decimal(4,4) DEFAULT NULL,
    `c_balance` decimal(12,2) DEFAULT NULL,
    `c_ytd_payment` decimal(12,2) DEFAULT NULL,
    `c_payment_cnt` int(11) DEFAULT NULL,
    `c_delivery_cnt` int(11) DEFAULT NULL,
    `c_data` varchar(500) DEFAULT NULL,
    PRIMARY KEY (`c_w_id`,`c_d_id`,`c_id`),
    KEY `idx_customer` (`c_w_id`,`c_d_id`,`c_last`,`c_first`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
