pdctl
========

pdctl is a command line tool for pd

## Build
1. Make sure [*Go*](https://golang.org/) (version 1.5+) is installed.
2. Use `make` in pd root path. `pdctl` will build in `bin` directory.

## Usage

### Example
run:
    
    ./pd-ctl store -d  -u 127.0.0.1:2379
show all stores status. '-u' specify the pd address, it can be overwritten by setting the environment variable PD_ADDR. Such as `export PD_ADDR=127.0.0.1:2379`

### Flags
#### --pd,-u
+ The pd address
+ default: http://127.0.0.1:2379
+ env variable: PD_ADDR

#### --detach,-d
+ Run pdctl without readline 
+ default: false

### Command
#### store [delete] <store_id>
show the store status or delete a store

##### example
``` 
>> store
{
  "count": 3,
  "stores": [...]
}
>> store 1
  ......
>> store delete 1
  ......
```

#### config [show | set  \<option\> \<value\>]
show or set the balance config
##### example
``` 
>> config show
{
  "min-region-count": 10,
  "min-leader-count": 10,
  "max-snapshot-count": 3,
  "min-balance-diff-ratio": 0.01,
  "max-store-down-duration": "30m0s",
  "leader-schedule-limit": 8,
  "leader-schedule-interval": "10s",
  "storage-schedule-limit": 4,
  "storage-schedule-interval": "30s"
}
>> config set leader-schedule-interval 20s
Success!
```

#### Member [leader | delete]
show the pd members status 
##### example
```
>> member
{
  "members": [......] 
}
>> member leader
{
  "name": "pd",
  "addr": "http://192.168.199.229:2379",
  "id": 9724873857558226554
}
>> member delete name pd2
Success!
```

#### Region <region_id>
show one or all regions status
##### Example
```
>> region
{
  "count": 1,
  "regions": [......]
}

>> region 2
{
  "region": {
      "id": 2,
      ......
  }
  "leader": {
      ......
  }
}
```
