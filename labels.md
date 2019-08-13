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
CHANGELOG                       #a206c9     T: CHANGELOG                        #1d76db
closed/no-pr-quota              #b60205     S: Closed pr-quote                  #e6e6e6
closed/outdated                 #b60205     S: Closed outdated                  #e6e6e6
component/DDL-need-LGT3         #00bff4     C: DDL need LGT3                    #d1fad7
component/GC                    #cce2ff     C: GC                               #d1fad7
component/binlog                #8befc9     C: Binlog                           #d1fad7
component/build                 #f2b87b     C: Build                            #d1fad7
component/charset               #cca1ea     C: Charset                          #d1fad7
component/coprocessor           #bfd4f2     C: Coprocessor                      #d1fad7
component/deployment            #b60205     C: Deployment                       #d1fad7
component/docs                  #e99695     C: Docs                             #d1fad7
component/document store        #e99695     C: Document store                   #d1fad7
component/executor              #000000     C: Executor                         #d1fad7
component/expression            #fbca04     C: Expression                       #d1fad7
component/metrics               #e26f88     C: Metrics                          #d1fad7
component/mysql-protocol        #66dde8     C: MySQL protocol                   #d1fad7
component/parser                #fbca04     C: Parser                           #d1fad7
component/planner               #1d76db     C: Planner                          #d1fad7
component/plugin                #c98fe8     C: Plugin                           #d1fad7
component/privilege             #81d5e2     C: Privilege                        #d1fad7
component/server                #e0854c     C: Server                           #d1fad7
component/session               #49309b     C: Session                          #d1fad7
component/statistics            #80e58d     C: Statistics                       #d1fad7
component/syncer                #ccbf0e     C: Syncer                           #d1fad7
component/test                  #6494f4     C: Test                             #d1fad7
component/tikv                  #8bef73     C: Tikv                             #d1fad7
component/tools                 #cbf232     C: Tools                            #d1fad7
component/transaction-need-LGT  #83e2d1     C: Transaction need LGT             #d1fad7
component/usability             #0ad8bd     C: Usability                        #d1fad7
component/util                  #47e0b0     C: Util                             #d1fad7
contribution                    #b0ff5b     T: Contributor ⭐️                    #1d76db
errmsg                          #a7c938     T: Error message                    #1d76db
for new contributors            #c2e0c6     D: Mentor                           #0e8a16
help wanted                     #159818     S: HelpWanted                       #e6e6e6
needs-cherry-pick-2.1           #000000     T: CherryPick-2.1                   #1d76db
needs-cherry-pick-3.0           #000000     T: CherryPick-3.0                   #1d76db
priority/P1                     #b60205     P: Low                              #eb6420
priority/P2                     #FF7F50     D: Medium                           #0e8a16
priority/non-release-blocker    #0052cc     P: High                             #eb6420
priority/release-blocker        #b60205     P: Blocker                          #eb6420
proposal                        #eb6420     T: Proposal                         #1d76db
question                        #fbca04     T: Question                         #1d76db
release-note                    #1d76db     * <T: CHANGELOG>                    
require-LGT3                    #d93f0b     * <S: Waiting on review>            
status/DNM                      #b60205     S: DNM                              #e6e6e6
status/LGT1                     #d4c5f9     * <S: Waiting on review>            
status/LGT2                     #5319e7     * <S: Waiting on review>            
status/LGT3                     #330099     * <S: Waiting on review>            
status/PTAL                     #2cbe4e     S: Waiting on review                #e6e6e6
status/ReqChange                #b60205     S: Waiting on author                #e6e6e6
status/TODO                     #207de5     *                                   
status/WIP                      #fbca04     S: WIP                              #e6e6e6
status/all tests passed         #2cbe4e     * <S: Bot Merge>                    
status/can merge                #2cbe4e     S: Bot Merge                        #e6e6e6
status/future                   #fbca04     ?                                   
type/1.0 cherry-pick            #c2e0c6     *                                   
type/2.0 cherry-pick            #99d0ef     *                                   
type/2.1 cherry-pick            #9bfff5     * <T: CherryPick-2.1>               
type/3.0                        #ededed     *                                   
type/3.0 cherry-pick            #297b99     * <T: CherryPick-3.0>               
type/TEP                        #bfd4f2     T: TEP                              #1d76db
type/bug                        #fc2929     T: Bug                              #1d76db
type/bug-fix                    #fc2929     T: BugFix                           #1d76db
type/compatibility              #e99695     T: Compatibility                    #1d76db
type/duplicate                  #cccccc     S: Closed dup                       #e6e6e6
type/enhancement                #84b6eb     T: Enhancement                      #1d76db
type/invalid                    #e6e6e6     S: Closed invalid                   #e6e6e6
type/investigation              #f46100     T: Investigation                    #1d76db
type/new-feature                #e2e876     * <T: Enhancement>
type/performance                #006b75     T: Performance                      #1d76db
type/regression                 #c41b2f     T: Regression                       #1d76db
type/suggestion                 #11b2c4     * <T: Proposal>                     
type/wontfix                    #95d3db     * <just close the issue>            
v3.0                            #cb11f9     *                                   
```

## TiKV

```
C: Build                        #d1fad7     C: Build                            #d1fad7
C: Build-Time                   #d1fad7     C: Build Time                       #d1fad7
C: Copr                         #d1fad7     C: Coprocessor                      #d1fad7
C: Doc                          #d1fad7     C: Docs                             #d1fad7
C: PD-Client                    #d1fad7     C: PD Client                        #d1fad7
C: Perf                         #d1fad7     C: Perf                             #d1fad7
C: Raft                         #d1fad7     C: Raft                             #d1fad7
C: RocksDB                      #d1fad7     C: RocksDB                          #d1fad7
C: Server                       #d1fad7     C: Server                           #d1fad7
C: Storage                      #d1fad7     C: Storage                          #d1fad7
C: Test/Bench                   #d1fad7     C: Test/Bench                       #d1fad7
C: TiKV-Client                  #d1fad7     C: TiKV Client                      #d1fad7
C: TiKV-Ctl                     #d1fad7     C: TiKV Ctl                         #d1fad7
C: Titan                        #d1fad7     C: Titan                            #d1fad7
C: Txn                          #d1fad7     C: Txn                              #d1fad7
C: Util                         #d1fad7     C: Util                             #d1fad7
C: gRPC                         #d1fad7     C: gRPC                             #d1fad7
D: Easy                         #0e8a16     D: Easy                             #0e8a16
D: Medium                       #f4b169     D: Medium                           #0e8a16
D: Mentor                       #31c639     D: Mentor                           #0e8a16
P: Critical                     #ed0000     P: High                             #eb6420
P: High                         #ed8888     P: Medium                           #eb6420
P: Low                          #eeee00     P: Low                              #eb6420
P: Release-blocker              #f25c8e     P: Blocker                          #eb6420
S: BotClose                     #c6054c     S: Bot Close                        #e6e6e6
S: CanMerge                     #4be524     S: Bot Merge                        #e6e6e6
S: DNM                          #DDDDDD     S: DNM                              #e6e6e6
S: Discussion                   #fbca04     T: Discussion                       #1d76db
S: Duplicate                    #dddddd     S: Closed dup                       #e6e6e6
S: HelpWanted                   #fbca04     S: HelpWanted                       #e6e6e6
S: Invalid                      #dddddd     S: Closed invalid                   #e6e6e6
S: LGT1                         #66d7ee     S: Waiting on review                #e6e6e6
S: LGT2                         #66d7ee     * <S: Waiting on review>            
S: Proposal                     #fbca04     T: Proposal                         #1d76db
S: WIP                          #DDDDDD     S: WIP                              #e6e6e6
S: Waiting                      #DDDDDD     S: Waiting on author                #e6e6e6
T: Bug                          #d93f0b     T: Bug                              #1d76db
T: BugFix                       #1d76db     T: BugFix                           #1d76db
T: CHANGELOG                    #006b75     T: CHANGELOG                        #1d76db
T: CherryPick                   #1d76db     * <T: CherryPick-2.1 or T: CherryPick-3.0>  #1d76db
T: Contributor ⭐️               #1d76db     T: Contributor ⭐️                     #1d76db
T: Enhancement                  #1d76db     T: Enhancement                      #1d76db
T: NeedCherryPick-2.1           #333333     T: CherryPick-2.1                   #1d76db
T: NeedCherryPick-3.0           #333333     T: CherryPick-3.0                   #1d76db
T: Question                     #1d76db     T: Question                         #1d76db
```

## PD

```
CanMerge                        #77dd77     S: Bot Merge                        #e6e6e6
DNM                             #fbca04     S: DNM                              #e6e6e6
WIP                             #fbca04     S: WIP                              #e6e6e6
area/api                        #c2e0c6     C: API                              #d1fad7
area/log                        #c2e0c6     C: Log                              #d1fad7
area/metrics                    #c2e0c6     C: Metrics                          #d1fad7
area/namespace                  #c2e0c6     C: Namespace                        #d1fad7
area/pdctl                      #c2e0c6     C: PD ctl                           #d1fad7
area/schedule                   #c2e0c6     C: Schedule                         #d1fad7
area/simulator                  #c2e0c6     C: Simulator                        #d1fad7
area/testing                    #c2e0c6     C: Testing                          #d1fad7
area/util                       #c2e0c6     C: Util                             #d1fad7
backport/release-1.0            #006b75     *                                   
backport/release-2.0            #006b75     *                                   
backport/release-2.1            #006b75     T: CherryPick-2.1                   #1d76db
backport/release-3.0            #006b75     T: CherryPick-3.0                   #1d76db
cherry-pick                     #0e8a16     * <T: CherryPick-2.1 or T: CherryPick-3.0>  
contributor                     #58f9fc     T: Contributor ⭐️                    #1d76db
good first issue                #0e8a16     D: Easy                             #0e8a16
help wanted                     #0e8a16     S: HelpWanted                       #e6e6e6
invalid                         #e6e6e6     S: Invalid                          #e6e6e6
kind/bug                        #fc2929     T: Bug                              #1d76db
kind/design                     #0052cc     T: Design                           #1d76db
kind/discuss                    #0052cc     T: Discussion                       #1d76db
kind/enhancement                #0052cc     T: Enhancement                      #1d76db
kind/question                   #0052cc     T: Question                         #1d76db
kind/todo                       #0052cc     *
priority/P0                     #fc2929     P: High                             #eb6420
priority/P1                     #d93f0b     P: Medium                           #eb6420
priority/P2                     #fbca04     P: Low                              #eb6420
release-note                    #f188f7     T: CHANGELOG                        #1d76db
tests-passed                    #0e8a16     S: Waiting to merge                 #e6e6e6
wontfix                         #e6e6e6     * <just close the issue>            
```
