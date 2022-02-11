/*!40101 SET NAMES binary*/;
CREATE TABLE `quo``te/table` (
  `quo``te/col` int NOT NULL,
  `a` int DEFAULT NULL,
  `gen``id` int GENERATED ALWAYS AS (`quo``te/col`) VIRTUAL,
  PRIMARY KEY (`quo``te/col`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
