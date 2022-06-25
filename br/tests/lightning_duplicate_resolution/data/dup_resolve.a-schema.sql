create table a (
    a int primary key clustered,
    b int not null unique,
    c text,
    d int generated always as (a + b),
    key (d)
);
