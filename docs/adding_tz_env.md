Proposal: adding TZ as an environment variable 

# Proposal: adding TZ as an environment variable as a necessary condition before starting TiDB

- Author(s):  [Zhexuan Yang](www.github.com/zhexuany)  
- Last updated:  2018/09/09
- Discussion at: Not applicable 

## Abstract

When it comes to time-related calculation, it is hard for the distributed system. This proposal tries to solve the system's timezone issue. In this protocol, we'll evaluate the advantage and disadvantage of different approaches and choose the optimal one. The impact of this proposal is changing the default timezone name when we push down to TiKV. Before this proposal, the default timezone name pushed down to TiKV is `System`. After this, TiDB evaluates system's timezone name and then pushes down the real name rather than `System`. 

## Background

After we solved the daylight saving time issue, we found the performance degradation of TiKV side. Thanks for the investigation done by engineers from TiKV. The root cause of such performance delegation is that TiKV infers `System` timezone name via a third party lib, which calls a syscall and costs a lot. In our internal benchmark system, after [this PR](https://github.com/pingcap/tidb/search?q=daylight+saving+time&type=Issues), our codebase is 1000 times slower than before. We have to address this. 

## Proposal

Firstly, we need to introduce the `TZ` environment. In POSIX system, the value of `TZ` variable can be one of the following three formats. A detailed description can be found in [this link](http://www.gnu.org/software/libc/manual/html_node/TZ-Variable.html)

    * std offset
    * std offset dst [offset], start[/time], end[/time]
    * :characters

The std means the IANA timezone name; the offset means timezone offset; the dst indicates the leading timezone having daylight saving time. 

In our case, which I mean both `TiDB` and `TiKV`, we need care the first and third formats. For answering why we do not need the second format, we need to review how Golang evaluates timezone. In `time` package, the method `LoadLocation` reads tzData from pre-specified sources(directories may contain tzData) and then builds `time. Location` from such tzData which already contains daylight saving time information. 

In this proposal, we suggest setting `TZ` to a valid IANA timezone name which can be read from `TiDB`. If `TiDB` can't get `TZ` or the supply of `TZ` is invalid, `TiDB` just fallback to evaluates the path of the soft link of `/etc/localtime`. An warning message telling the user you should set `TZ` properly will be printed. Setting `TZ` can be done in our `tidb-ansible` project. The value of `localStr` should initialized at `TiDB`'s bootstrap stage in order to ensure the uniqueness of gloabl timezone name. If system timezone can not be read from `mysql.tidb`, `UTC` will be used as default timezone name.

The positive side of this change is resolving performance delegation issue to avoid any potential time-related calculation. 

The negative side is just adding a config item which is a very small matter and the user probably does not care it if we can take care of it and more importantly guarantee the correctness. 


## Rationale

We tried to read system timezone name by checking the path of the soft link of `/etc/localtime` but, sadly, failed. The failed case is docker. In docker image, it copies the real timezone file and links to `/usr/share/zoneinfo/utc`. The timezone data is correct but the path is not. Regarding of `UTC`, Golang just returns `UTC` instance and will not further read tzdata from sources.

The impact of this proposed change affects the deploying logic of TiDB, which is just a small matter. 

## Compatibility

It does not have compatibility issue as long as the user deploys by `tidb-ansible`. We may mention this in our release-node and the message printed before tidb quits, which must be easy to understand.


## Implementation

The implementation is relatively easy. We just get `TZ` environment from system and check whether it is valid or not. If it is invalid, TiDB evaluates the path of soft link of `/etc/localtime`. 
In addition, a warning message needs to be printed indicating user has to set `TZ` variable. For example, if `/etc/localtime` links to /usr/share/zoneinfo/Asia/Shanghai, then timezone name should be `Asia/Shangahi`.  

In order to ensure the uniqueness of global timezone, we need to write timezone name into `mysql.tidb` under column `system_tz`. 
 
## Open issues (if applicable)

PR of this proposal: https://github.com/pingcap/tidb/pull/7638/files
PR of change TZ loading logic of golang: https://github.com/golang/go/pull/27570
