create table various_types (
    int64 bigint as (1 + 2) stored,
    uint64 bigint unsigned as (pow(7, 8)) stored, -- 5764801
    float32 float as (9 / 16) stored,
    float64 double as (5e222) stored,
    string text as (sha1(repeat('x', uint64))) stored, -- '6ad8402ba6610f04d3ec5c9875489a7bc8e259c5'
    bytes blob as (unhex(string)) stored,
    `decimal` decimal(8, 4) as (1234.5678) stored,
    duration time as ('1:2:3') stored,
    enum enum('a','b','c') as ('c') stored,
    bit bit(4) as (int64) stored,
    `set` set('a','b','c') as (enum) stored,
    time timestamp(3) as ('1987-06-05 04:03:02.100') stored,
    json json as (json_object(string, float32)) stored,
    aes blob as (aes_encrypt(`decimal`, 'key', bytes)) stored, -- 0xA876B03CFC8AF93D22D19E2220BD2375, @@block_encryption_mode='aes-256-cbc'
    -- FIXME: column below disabled due to pingcap/tidb#21510
    -- week int as (week('2020-02-02')) stored, -- 6, @@default_week_format=4
    tz varchar(20) as (from_unixtime(1)) stored -- 1969-12-31 16:00:01, @@time_zone='-08:00'
);
