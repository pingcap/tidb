create table tf (
    id  int not null primary key clustered,
    name varchar(20) not null,
    size bigint not null,
    unique key uni_name(name)
);
