create table ta (
    id varchar(11) not null primary key nonclustered, -- use varchar here to make sure _tidb_rowid will be generated
    name varchar(20) not null,
    size bigint not null,
    unique key uni_name(name)
);
