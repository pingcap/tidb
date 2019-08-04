# Proposal: Infer the System Timezone of a TiDB cluster via TZ environment variable

- Author(s):  [Zhexuan Yang](www.github.com/zhexuany)
- Last updated:  2018/09/09
- Discussion at: Not applicable 

## Abstract

When it comes to time-related calculation, it is hard for the distributed system. This proposal tries to resolve two problems: 1. timezone may be inconsistent across multiple `TiDB` instances, 2. performance degradation caused by pushing `System` down to `TiKV`. The impact of this proposal is changing the way of `TiDB` inferring system's timezone name. Before this proposal, the default timezone name pushed down to TiKV is `System` when session's timezone is not set. After this, TiDB evaluates system's timezone name via `TZ` environment variable and the path of the soft link of `/etc/localtime`. If both of them are failed, `TiDB` then push `UTC` to `TiKV`.

## Background

After we solved the daylight saving time issue, we found the performance degradation of TiKV side. Thanks for the investigation done by engineers from TiKV. The root cause of such performance degradation is that TiKV infers `System` timezone name via a third party lib, which calls a syscall and costs a lot. In our internal benchmark system, after [this PR](https://github.com/pingcap/tidb/pull/6823), our codebase is 1000 times slower than before. We have to address this. 

Another problem needs also to be addressed is the potentially incosistent timezone name across multiple `TiDB` instances. `TiDB` instances may reside at different timezone which could cause incorrect calculation when it comes to time-related calculation. Just getting `TiDB`'s system timezone could be broken. We need find a way to ensure the uniqueness of global timezone name across multiple `TiDB`'s timezone name and also to leverage to resolve the performance degradation. 

## Proposal

Firstly, we need to introduce the `TZ` environment. In POSIX system, the value of `TZ` variable can be one of the following three formats. A detailed description can be found in [this link](http://www.gnu.org/software/libc/manual/html_node/TZ-Variable.html)

    * std offset
    * std offset dst [offset], start[/time], end[/time]
    * :characters

The std means the IANA timezone name; the offset means timezone offset; the dst indicates the leading timezone having daylight saving time. 

In our case, which means both `TiDB` and `TiKV`, we need care the first and third formats. For answering why we do not need the second format, we need to review how Golang evaluates timezone. In `time` package, the method [LoadLocation](https://golang.org/pkg/time/#LoadLocation) reads tzData from pre-specified sources(directories may contain tzData) and then builds `time.Location` from such tzData which already contains daylight saving time information. 

In this proposal, we suggest setting `TZ` to a valid IANA timezone name which can be read from `TiDB` later. If `TiDB` can't get `TZ` or the supply of `TZ` is invalid, `TiDB` just falls back to evaluate the path of the soft link of `/etc/localtime`. In addition, a warning message telling the user you should set `TZ` properly will be printed. Setting `TZ` can be done in our `tidb-ansible` project, it is also can be done at user side by `export TZ="Asia/Shanghai"`. If both of them are failed, `TiDB` will use `UTC` as timezone name.

The positive side of this change is resolving performance degradation issue and ensuring the uniqueness of global timezone name in multiple `TiDB` instances. 

The negative side is just adding a config item which is a very small matter and the user probably does not care it if we can take care of it and more importantly guarantee the correctness. 


## Rationale

We tried to read system timezone name by checking the path of the soft link of `/etc/localtime` but, sadly, failed at a corner case. The failed case is docker. In docker image, it copies the real timezone file and links to `/usr/share/zoneinfo/utc`. The timezone data is correct but the path is not. Regarding of `UTC`, Golang just returns `UTC` instance and will not further read tzdata from sources. This leads to a fallback solution. When we cannot evaluate from the path, we fall back to `UTC`.

## Compatibility

It does not have compatibility issue as long as the user deploys by `tidb-ansible`. We may mention this in our release-node and the message printed before tidb quits, which must be easy to understand.

The upgrading process need to be handled in particular. `TZ` environment variable has to be set before we start new `TiDB` binary. In this way, the following bootstrap process can benefit from this and avoid any hazard happening.


## Implementation

The implementation is relatively easy. We just get `TZ` environment from system and check whether it is valid or not. If it is invalid, TiDB evaluates the path of soft link of `/etc/localtime`. In addition, a warning message needs to be printed indicating user has to set `TZ` variable properly. For example, if `/etc/localtime` links to `/usr/share/zoneinfo/Asia/Shanghai`, then timezone name `TiDB` gets should be `Asia/Shanghai`.

In order to ensure the uniqueness of global timezone across multiple `TiDB` instances, we need to write timezone name into `variable_value` with variable name `system_tz` in `mysql.tidb`.  This cached value can be read once `TiDB` finishes its bootstrap stage. A method `loadLocalStr` can do this job.
 
## Open issues (if applicable)

PR of this proposal: https://github.com/pingcap/tidb/pull/7638/files
PR of change TZ loading logic of golang: https://github.com/golang/go/pull/27570
