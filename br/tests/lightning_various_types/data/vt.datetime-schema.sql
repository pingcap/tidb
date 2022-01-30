CREATE TABLE `datetime` (
    `ref` INT NOT NULL,
    `pk` DATETIME(6) NOT NULL,
    `uk` TIMESTAMP(3) NOT NULL,
    PRIMARY KEY(`pk`),
    UNIQUE KEY(`uk`)
);
