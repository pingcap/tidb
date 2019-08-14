# Goals

* synchronise labels between repos
* make labels easier to set automatically
* make labels clearer
* make it easier to tell at a glance what is blocking PRs from landing
* make colours more useful by making them more uniform

# Naming scheme

## Current issues

* TiDB uses a `category/label` scheme, but with some exceptions
  - closed, component, priority, status, type
* TiKV uses a `C: label` scheme
  - C (component), D (difficulty), P (priority), S (status), T (type)
* PD uses a `category/label` scheme, but with some exceptions
  - area, backport, kind, priority, 

Bots work on all repos, but bot-readable labels are not distinguished.

There is some duplication within a repo and some inconsistency about the category a label belongs to.

## Proposal

We use a `C: label` scheme for all repos (I don't feel strongly about the scheme, as long as there is just one. `C: label` is shorter, `category/label` is easier to understand). The categories are: component, priority, status, type, and difficulty.

Where labels are similar across repos we pick one name and use that. For the status labels we use `waiting on CI`, `waiting on review`, `waiting on discussion`, and `waiting on author`, these are taken from the Rust repo where they have been found to be useful. We'll also keep some of the existing statuses where they are useful. The component labels are unique to each repo.

# Colours

Are generally inconsistent.

## Proposal

All labels in the same category have the same colour, taking the colour which is currently most popular.


# Aternatives

* Use `category/label`, `cat/label`, or `C-label` naming schemes
* More categories (e.g., a bot category)
* Allow labels without categories
* Add emoji to highlight some labels or sub-categories
* Separate labels for issues vs PRs


# The labels

`*` indicates I think we should remove an issue, there might be a suggested replacement in `<...>`. `?` means I don't know what the label means.

Current labels and colours are on the left, proposed are on the right. Any non-C labels which are present in any repo will be added to all repos.

## TiDB

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
priority/P1                     #b60205     priority/low                        #eb6420
priority/P2                     #FF7F50     priority/medium                     #0e8a16
priority/non-release-blocker    #0052cc     priority/high                       #eb6420
priority/release-blocker        #b60205     priority/blocker                    #eb6420
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

## TiKV

```
C: Build                        #d1fad7     component/build                     #d1fad7
C: Build-Time                   #d1fad7     component/build time                #d1fad7
C: Copr                         #d1fad7     component/coprocessor               #d1fad7
C: Doc                          #d1fad7     component/docs                      #d1fad7
C: PD-Client                    #d1fad7     component/PD client                 #d1fad7
C: Perf                         #d1fad7     component/perf                      #d1fad7
C: Raft                         #d1fad7     component/raft                      #d1fad7
C: RocksDB                      #d1fad7     component/RocksDB                   #d1fad7
C: Server                       #d1fad7     component/server                    #d1fad7
C: Storage                      #d1fad7     component/storage                   #d1fad7
C: Test/Bench                   #d1fad7     component/test                      #d1fad7
                                            component/bench                     #d1fad7
C: TiKV-Client                  #d1fad7     component/TiKV client               #d1fad7
C: TiKV-Ctl                     #d1fad7     component/TiKV ctl                  #d1fad7
C: Titan                        #d1fad7     component/Titan                     #d1fad7
C: Txn                          #d1fad7     component/transactions              #d1fad7
C: Util                         #d1fad7     component/util                      #d1fad7
C: gRPC                         #d1fad7     component/gRPC                      #d1fad7
D: Easy                         #0e8a16     difficulty/easy                     #0e8a16
D: Medium                       #f4b169     difficulty/medium                   #0e8a16
D: Mentor                       #31c639     difficulty/mentor                   #0e8a16
P: Critical                     #ed0000     priority/high                       #eb6420
P: High                         #ed8888     priority/medium                     #eb6420
P: Low                          #eeee00     priority/low                        #eb6420
P: Release-blocker              #f25c8e     priority/blocker                    #eb6420
S: BotClose                     #c6054c     status/bot close                    #e6e6e6
S: CanMerge                     #4be524     status/bot merge                    #e6e6e6
S: DNM                          #DDDDDD     status/DNM                          #e6e6e6
S: Discussion                   #fbca04     type/discussion                     #1d76db
S: Duplicate                    #dddddd     status/closed dup                   #e6e6e6
S: HelpWanted                   #fbca04     status/help wanted                  #e6e6e6
S: Invalid                      #dddddd     status/closed invalid               #e6e6e6
S: LGT1                         #66d7ee     status/waiting on review            #e6e6e6
S: LGT2                         #66d7ee     * <status/waiting on review>            
S: Proposal                     #fbca04     type/proposal                       #1d76db
S: WIP                          #DDDDDD     status/WIP                          #e6e6e6
S: Waiting                      #DDDDDD     status/waiting on author            #e6e6e6
T: Bug                          #d93f0b     type/bug                            #1d76db
T: BugFix                       #1d76db     type/bug fix                        #1d76db
T: CHANGELOG                    #006b75     type/CHANGELOG                      #1d76db
T: CherryPick                   #1d76db     * <type/cherry pick 2.1 or type/cherry pick 3.0>
T: Contributor ⭐️               #1d76db     type/contributor ⭐️                 #1d76db
T: Enhancement                  #1d76db     type/enhancement                    #1d76db
T: NeedCherryPick-2.1           #333333     type/cherry pick 2.1                #1d76db
T: NeedCherryPick-3.0           #333333     type/cherry pick 3.0                #1d76db
T: Question                     #1d76db     type/question                       #1d76db
```

## PD

```
CanMerge                        #77dd77     status/bot merge                    #e6e6e6
DNM                             #fbca04     status/DNM                          #e6e6e6
WIP                             #fbca04     status/WIP                          #e6e6e6
area/api                        #c2e0c6     component/API                       #d1fad7
area/log                        #c2e0c6     component/log                       #d1fad7
area/metrics                    #c2e0c6     component/metrics                   #d1fad7
area/namespace                  #c2e0c6     component/namespace                 #d1fad7
area/pdctl                      #c2e0c6     component/PD ctl                    #d1fad7
area/schedule                   #c2e0c6     component/schedule                  #d1fad7
area/simulator                  #c2e0c6     component/simulator                 #d1fad7
area/testing                    #c2e0c6     component/testing                   #d1fad7
area/util                       #c2e0c6     component/util                      #d1fad7
backport/release-1.0            #006b75     *                                   
backport/release-2.0            #006b75     *                                   
backport/release-2.1            #006b75     type/cherry pick 2.1                #1d76db
backport/release-3.0            #006b75     type/cherry pick 3.0                #1d76db
cherry-pick                     #0e8a16     * <type/cherry pick 2.1 or type/cherry pick 3.0>  
contributor                     #58f9fc     type/contributor ⭐️                 #1d76db
good first issue                #0e8a16     difficulty/easy                     #0e8a16
help wanted                     #0e8a16     status/help wanted                  #e6e6e6
invalid                         #e6e6e6     status/invalid                      #e6e6e6
kind/bug                        #fc2929     type/bug                            #1d76db
kind/design                     #0052cc     type/design                         #1d76db
kind/discuss                    #0052cc     type/discussion                     #1d76db
kind/enhancement                #0052cc     type/enhancement                    #1d76db
kind/question                   #0052cc     type/question                       #1d76db
kind/todo                       #0052cc     *
priority/P0                     #fc2929     priority/high                       #eb6420
priority/P1                     #d93f0b     priority/medium                     #eb6420
priority/P2                     #fbca04     priority/low                        #eb6420
release-note                    #f188f7     type/CHANGELOG                      #1d76db
tests-passed                    #0e8a16     status/waiting to merge             #e6e6e6
wontfix                         #e6e6e6     * <just close the issue>            
```
