create table t (
    id int not null primary key clustered,

    -- each stored generated column occupy about 150 KB of data, so we are 750 KB per row.
    -- without disk quota the engine size will be 750 KB * 2000 rows = 1.5 GB ≈ 1.4 GiB.
    -- (FIXME: making the KV size too large may crash PD?)
    sa longblob as (aes_encrypt(rpad(id, 150000, 'a'), 'xxx', 'iviviviviviviviv')) stored,
    sb longblob as (aes_encrypt(rpad(id, 150000, 'b'), 'xxx', 'iviviviviviviviv')) stored,
    sc longblob as (aes_encrypt(rpad(id, 150000, 'c'), 'xxx', 'iviviviviviviviv')) stored,
    sd longblob as (aes_encrypt(rpad(id, 150000, 'd'), 'xxx', 'iviviviviviviviv')) stored,
    se longblob as (aes_encrypt(rpad(id, 150000, 'e'), 'xxx', 'iviviviviviviviv')) stored
);
