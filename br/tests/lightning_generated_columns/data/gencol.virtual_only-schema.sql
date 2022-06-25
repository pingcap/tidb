create table virtual_only (
    id int primary key,
    id_plus_1 int as (id + 1) virtual,
    id_plus_2 int as (id + 2) virtual
);
