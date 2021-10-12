create table nested (
    a int not null primary key,
    b int as (a + 1) virtual unique,
    c int as (b + 1) stored unique,
    d int as (c + 1) virtual unique,
    e int as (d + 1) stored unique,
    f int as (e + 1) virtual unique
);
