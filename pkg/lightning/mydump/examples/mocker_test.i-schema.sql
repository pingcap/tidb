/* The characters 'ı' and 'ſ' have lengths 2 */;
/* but their upper case 'I' and 'S" have length 1 */;
/* Code that relies on indices extracted from ToUpper will fail on this table*/;
CREATE TABLE `ı` (
`ſ` varchar(5) NOT NULL
) ENGINE=InnoDB;
