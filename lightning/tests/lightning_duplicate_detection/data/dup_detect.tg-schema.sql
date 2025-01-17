create table tg (
    id int not null primary key clustered, /*{{ rand.range(0, 40) }}*/
    name varchar(20) not null, /*{{ rand.regex('[0-9a-f]{8}') }}*/
    size bigint not null /*{{ rand.range_inclusive(-0x8000000000000000, 0x7fffffffffffffff) }}*/
) partition by hash (id) partitions 5;
