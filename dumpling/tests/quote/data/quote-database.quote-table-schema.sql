/*!40101 SET NAMES binary*/;
/*T![placement] SET PLACEMENT_CHECKS = 0*/;
CREATE TABLE `quo``te/table` (
  `quo``te/col` int NOT NULL,
  `a` int DEFAULT NULL,
  `gen``id` int GENERATED ALWAYS AS (`quo``te/col`) VIRTUAL,
  PRIMARY KEY (`quo``te/col`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
