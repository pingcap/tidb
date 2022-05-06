/*!40101 SET NAMES binary*/;
CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB;
SELECT SETVAL(`s`,1001);
