# Labels Proposal

## Goals

* synchronise labels between repos, especially priority labels
* make colours more useful by making them more uniform

## Current issues

* Priotiry labels settings needs to be synchronised among repos.

* TiDB uses a `category/label` scheme, but with some exceptions

- closed, component, priority, status, type

## Proposal

### Naming Aternatives

* Use `category/label` naming schemes
* More categories (e.g., a bot category)
* Allow labels without categories
* Add emoji to highlight some labels or sub-categories
* Separate labels for issues vs PRs

### The labels
`*` indicates I think we should remove an issue, there might be a suggested replacement in `<...>`. `?` means I don't know what the label means.

### Priotity lables

* Priority labels synchronising proposal:

```
priority/critical-urgent        #cc2400        Highest priority. Must be actively worked on as someone's top priority right now. Release blocker.
priority/important-longterm     #fe5733        Important over the long term, but may not be staffed and/or may need multiple releases to complete.
priority/important-soon         #fe5733        Must be staffed and worked on either currently, or very soon, ideally in time for the next release.
priority/backlog                #feca33        Higher priority than priority/awaiting-more-evidence.
priority/awaiting-more-evidence #ffe599        Lowest priority. Possibly useful, but not yet enough support to actually get it done.
needs-priority                  #cdcdcd        Indicates a PR lacks a `priority/foo` label and requires one.
```

* Repos current settings which need to optimize

- TiDB

```
priority/individual-blocker
Priority/P0
Priority/P1
priority/P2
priority/release-blocker
Priority/Unknown
```

- TiKV

```
priority/critical
priority/high
priority/low
priority/release-blocker
```

- PD

```
priority/P0
priority/P1
priority/P2
priority/release-blocker        
```

- tics (TiFlash)

```
priority/p0
priority/p1
priority/p2
priority/release-blocker
```

- br

```
Priority/P0
Priority/P1
priority/P2
priority/P3
question
release-blocker
```

- TiCDC

```
Priority/P0
Priority/P1
Priority/P2
question
release-blocker
```

- DM

```
priority/important
priority/normal
priority/release-blocker
priority/unimportant
question
release-track
```

- TiDB-Lightning

```
priority/P0
priority/P1
priority/P2
priority/P3
question
release-blocker
```

- Dumpling

```
priority/P0
priority/P1
priority/P2
priority/P3
```

- TiDB-binlog

```
p0
p1
p2
priority/important
priority/normal
priority/unimportant
question
```

### Other labels

* TiDB repo paticularly needs to optimize. Current labels and colours are on the left, proposed are on the right.

```
CHANGELOG                       #a206c9     type/CHANGELOG                      #1d76db
closed/no-pr-quota              #b60205     status/closed pr-quote              #e6e6e6
closed/outdated                 #b60205     status/closed outdated              #e6e6e6
component/DDL-need-LGT3         #00bff4     component/DDL (3lgtm)               #d1fad7
component/GC                    #cce2ff     component/GC                        #d1fad7
component/binlog                #8befc9     component/binlog                    #d1fad7
component/build                 #f2b87b     component/build                     #d1fad7
component/charset               #cca1ea     component/charset                   #d1fad7
component/coprocessor           #bfd4f2     component/coprocessor               #d1fad7
component/deployment            #b60205     component/deployment                #d1fad7
component/docs                  #e99695     component/docs                      #d1fad7
component/document store        #e99695     component/document store            #d1fad7
component/executor              #000000     component/executor                  #d1fad7
component/expression            #fbca04     component/expression                #d1fad7
component/metrics               #e26f88     component/metrics                   #d1fad7
component/mysql-protocol        #66dde8     component/MySQL protocol            #d1fad7
component/parser                #fbca04     component/parser                    #d1fad7
component/planner               #1d76db     component/planner                   #d1fad7
component/plugin                #c98fe8     component/plugin                    #d1fad7
component/privilege             #81d5e2     component/privilege                 #d1fad7
component/server                #e0854c     component/server                    #d1fad7
component/session               #49309b     component/session                   #d1fad7
component/statistics            #80e58d     component/statistics                #d1fad7
component/syncer                #ccbf0e     component/syncer                    #d1fad7
component/test                  #6494f4     component/test                      #d1fad7
component/tikv                  #8bef73     component/tikv                      #d1fad7
component/tools                 #cbf232     component/tools                     #d1fad7
component/transaction-need-LGT  #83e2d1     component/transaction (3lgtm)       #d1fad7
component/usability             #0ad8bd     component/usability                 #d1fad7
component/util                  #47e0b0     component/util                      #d1fad7
contribution                    #b0ff5b     type/contributor ⭐️                 #1d76db
errmsg                          #a7c938     type/error message                  #1d76db
for new contributors            #c2e0c6     difficulty/Mentor                   #0e8a16
help wanted                     #159818     status/help wanted                  #e6e6e6
needs-cherry-pick-2.1           #000000     type/cherry pick 2.1                #1d76db
needs-cherry-pick-3.0           #000000     type/cherry pick 3.0                #1d76db
proposal                        #eb6420     type/proposal                       #1d76db
question                        #fbca04     type/question                       #1d76db
release-note                    #1d76db     * <type/CHANGELOG>                    
require-LGT3                    #d93f0b     type/require 3 reviews              #1d76db
status/DNM                      #b60205     status/DNM                          #e6e6e6
status/LGT1                     #d4c5f9     * <status/Waiting on review>            
status/LGT2                     #5319e7     * <status/Waiting on review>            
status/LGT3                     #330099     * <status/Waiting on review>            
status/PTAL                     #2cbe4e     status/waiting on review            #e6e6e6
status/ReqChange                #b60205     status/waiting on author            #e6e6e6
status/TODO                     #207de5     *                                   
status/WIP                      #fbca04     status/WIP                          #e6e6e6
status/all tests passed         #2cbe4e     * <status/bot merge>                    
status/can merge                #2cbe4e     status/bot merge                    #e6e6e6
status/future                   #fbca04     ?                                   
type/1.0 cherry-pick            #c2e0c6     *                                   
type/2.0 cherry-pick            #99d0ef     *                                   
type/2.1 cherry-pick            #9bfff5     *
type/3.0                        #ededed     *                                   
type/3.0 cherry-pick            #297b99     *
type/TEP                        #bfd4f2     type/TEP                            #1d76db
type/bug                        #fc2929     type/bug                            #1d76db
type/bug-fix                    #fc2929     type/bug fix                        #1d76db
type/compatibility              #e99695     type/compatibility                  #1d76db
type/duplicate                  #cccccc     status/closed dup                   #e6e6e6
type/enhancement                #84b6eb     type/enhancement                    #1d76db
type/invalid                    #e6e6e6     status/closed invalid               #e6e6e6
type/investigation              #f46100     type/investigation                  #1d76db
type/new-feature                #e2e876     * <type/enhancement>
type/performance                #006b75     type/performance                    #1d76db
type/regression                 #c41b2f     type/regression                     #1d76db
type/suggestion                 #11b2c4     * <type/proposal>                     
type/wontfix                    #95d3db     * <just close the issue>            
v3.0                            #cb11f9     *                                   
```