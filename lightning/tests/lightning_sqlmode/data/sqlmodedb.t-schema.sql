create table t (
    id INT PRIMARY KEY,
    a TIMESTAMP NOT NULL,
    b TINYINT NOT NULL,
    c VARCHAR(1) CHARSET latin1 NOT NULL,
    d SET('x','y') NOT NULL
);
