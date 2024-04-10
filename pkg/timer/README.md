# Timer framework usage

<!-- TOC -->
* [Timer framework usage](#timer-framework-usage)
  * [Introduction](#introduction)
  * [Usage](#usage)
    * [Timer client operations](#timer-client-operations)
      * [Create a timer client](#create-a-timer-client)
      * [Create a timer with the client](#create-a-timer-with-the-client)
      * [Query timers with the client](#query-timers-with-the-client)
      * [Update a timer](#update-a-timer)
      * [Delete a timer](#delete-a-timer)
    * [Set up a runtime to schedule timers](#set-up-a-runtime-to-schedule-timers)
    * [Custom your hook class](#custom-your-hook-class)
  * [FAQs](#faqs)
<!-- TOC -->

## Introduction

Timer framework is an internal framework for TiDB to run tasks periodically. You can use it if you have below requirements:

- Run a task periodically and the scheduling policy should fit some specified semantics. For example, run a task every 5 minutes, or run a task every day at 00:00:00.
- Need to run the task in a distributed environment. For example, you have 3 TiDB servers, and you want to run the task on only one of them. And you also want to do some recovery work when the TiDB process restarts.
- Need to read the persisted schedule runtime information of the timer to do some work somewhere like monitoring.
- Your trigger action needs to meet some message delivery semantics such as at least once, at most once, or exactly once.

What timer framework is not suitable for you:

- Some work that the frequency is too high. For example, you want to run a task every 1 second. Though you can set the scheduling policy with an interval of "1s", it is not efficient because every time it triggers, the framework will make some calls to the store. In this case, you should use a timer of Golang in your own code.
- The timer framework only helps you to schedule a "trigger" action but not to schedule some heavy jobs across the nodes. That means the trigger action should be light-weighted and can be finished in a short time. If you want to schedule some heavy jobs, you should use some other distributed job scheduler like `disttask` in another package.

## Usage

### Timer client operations

#### Create a timer client

A client must be created from a store. The store is used to persist the runtime information of the timer. Currently, we use TiDB system table to store the information in the production environment. But if you want to do some simple tests in your own test case, you can use a memory store, for example:

```go
package main

import (
	"fmt"
	"github.com/pingcap/tidb/timer/api"
)

func main() {
	store := api.NewMemoryTimerStore()
	client := api.NewDefaultTimerClient(store)
	fmt.Println(client.GetDefaultNamespace())
}
``` 

The method `client.GetDefaultNamespace()` returns the current namespace of the client. It is used to distinguish different tenants in the future, but now it is always `default`.

You can check the method `tablestore.NewTableTimerStore` to see how to create a store from a TiDB system table.

#### Create a timer with the client

If you want to do something every 1 hour, you can create a timer like this:

```go
timer, err := client.CreateTimer(ctx, api.TimerSpec{
    Key:             "/your/timer/key",
    SchedPolicyType: api.SchedEventInterval,
    SchedPolicyExpr: "1h",
    HookClass:       "your.hook.class.name",
    Watermark:       time.Now(),
    Data:            []byte("yourdata"),
    Enable:          true,
})

if err != nil {
    // handle err
}

fmt.Printf("created timer id: %s\n", timer.ID)
```

The specified `Key` field is unique under a namespace. If you want to create a timer with the same key, you must delete the old one with the same key first if it exists.

To custom the timer schedule policy, you can specify the values of fields `SchedPolicyType` and `SchedPolicyExpr`. Currently, we support 2 types of schedule policy:

1. `INTERVAL`, triggers with a fixed interval, for example, `1h` means every 1 hour.
2. `CRON`, triggers with a cron expression, for example, `0 0 0 * * *` means every day at 00:00:00.

The field `HookClass` is optional. It tells the framework which hook class to use to trigger the action. If you do not specify it, the framework will set the timer's event status to `TRIGGER` without calling any custom hook class code.

The field `Watermark` is optional. If it is set, the timer will not trigger until there is an event not triggered between the watermark and now. If it is not set, the timer will try triggering the timer immediately after it is created.

The field `Enable` is optional. But if you want to keep the timer to schedule the new events, you must set it to `true`. If you set this value to `false`, the timer will stop scheduling any new event until you set it to `true` again.

The `Data` field is also optional, you can set any binary data you want. When the timer triggers, the framework will pass the data to the hook class.

When a timer is created successfully, the field `ID` will be assigned automatically. You can use this `ID` to do some operations on the timer.

#### Query timers with the client

You can use the client to query timer(s), for example:

```go
// query timers with the specified key prefix
timers, err := client.GetTimers(context.TODO(), api.WithKeyPrefix("/your/timer/"))
if err != nil {
	// handle err ...
}

// query timer with the specified key
timer, err := client.GetTimerByKey(ctx, "/your/timer/key")
if err != nil {
    // handle err ...
}

// query timer with the specified id
timer, err := client.GetTimerByID(ctx, "123")
if err != nil {
    // handle err ...
}
```

#### Update a timer

You can update a timer's metadata with the method `UpdateTimer`, for example:

```go
// update the schedule policy
if err := client.UpdateTimer(ctx, timer.ID, api.WithSetSchedExpr(api.SchedEventCron, "0 0 * * * *")); err != nil {
    // handle error
}

// update the watermark
if err := client.UpdateTimer(ctx, timer.ID, api.WithSetWatermark(time.Now())); err != nil {
    // handle error
}
```

#### Delete a timer

You can delete a timer with the method `DeleteTimer`, for example:

```go
exist, err := client.DeleteTimer(ctx, timer.ID)
if err != nil {
    // handle error
}
```

An extra value `exist` is returned. If the timer exists, it will be deleted and `exist` will be `true`. Otherwise, `exist` will be `false`.

### Set up a runtime to schedule timers

If you only create a timer through a client, the timer will not run by default. You must set up a timer runtime to schedule the timers. You can create a new `runtime.TimerGroupRuntime` and then start it, for example:

```go
package main

import "github.com/pingcap/tidb/timer/api"

func main() {
    ctx := ... // some go context
    store := ... // Create a timer store.
    rt := runtime.NewTimerRuntimeBuilder("myGroup", store).
        RegisterHookFactory("your.hook.class.name", MyTimerHookFactory).
        SetCond(&api.TimerCond{
            Key: api.NewOptionalVal("/"), KeyPrefix: true,
        }).
        Build()
	
    rt.Start()
    defer rt.Stop()

    <-ctx.Done()
}
```

We use a builder to build a new timer. You can specify a group name when creating it. The group name is only used by monitoring now. `RegisterHookFactory` registers your custom hook class which will be called when the runtime trys to trigger a event. `SetCond` sets a condition and the runtime will only schedule the timers which match the condition. If you do not set the condition, the runtime will schedule all the timers.

If you start more than one timer runtimes with overlapped conditions, the hook functions have a probability to be called more than once. So it's better to use some distributed lock to ensure that only one runtime is running.

In the future, we are going to provide some preserved runtimes in TiDB system, so that you can use them directly without creating a new one.

### Custom your hook class

You can implement your own hook class by implementing the interface `api.HookClass`, for example:

```go
func MyTimerHookFactory(hookClass string, cli api.TimerClient) api.Hook {
    return &MyTimerHook{
        cli: cli,
    }
}

type MyTimerHook struct {
	cli api.TimerClient
}

func (h *MyTimerHook) Start() {
	// You should do some init works here.
}

func (h *MyTimerHook) Stop() {
	// You should do some clear works here.
}

func (h *MyTimerHook) OnPreSchedEvent(ctx context.Context, event api.TimerShedEvent) (api.PreSchedEventResult, error) {
	fmt.Printf("OnPreSchedEvent: %s\n", event.EventID())
	return api.PreSchedEventResult{}, nil
}

func (h *MyTimerHook) OnSchedEvent(ctx context.Context, event api.TimerShedEvent) error {
    fmt.Printf("OnSchedEvent: %s, event start: %s\n", event.EventID(), event.Timer().EventStart)
	return h.cli.CloseTimerEvent(ctx, event.Timer().ID, event.EventID())
}
```

The function `MyTimerHookFactory` is used as a parameter of `RegisterHookFactory` in the previous section. It is used to create a new hook instance when the runtime needs it. A parameter `cli` with type `api.TimerClient` will be provided to help to construct the hook, and the hook can use the client to do some further timer operations.

All the timers with the same hook class will share the same hook instance. When the runtime finds a timer that is ready to trigger, it will first find whether the corresponding hook is created. If not, it will call the hook factory to create a new hook instance. Then it will call the hook's `Start` method to do some preparation work. The `Start` method will only be called once. After that, for each trigger action, the runtime will call the hook's `OnPreSchedEvent` method to do some pre-trigger works.

`OnPreSchedEvent` returns a `PreSchedEventResult` object to tell the runtime what to do next. In most cases, we just need to return an empty `PreSchedEventResult` to continue the trigger action. But sometimes you may want to delay the schedule for some reason such as some schedule window limits required by business. You can delay the schedule like this:

```go
func shouldDelaySchedule() bool {
	// your function to check whether to delay a timer's schedule
}

func (h *MyTimerHook) OnPreSchedEvent(ctx context.Context, event api.TimerShedEvent) (api.PreSchedEventResult, error) {
    if shouldDelaySchedule() {
        return api.PreSchedEventResult{
            Delay: time.Minute,
        }, nil
    }   
    return api.PreSchedEventResult{}, nil
}
```

In the above example, the delay time is 1 minute. After 1 minute, `OnPreSchedEvent` will be called again, and the trigger action will not continue until the method `OnPreSchedEvent` returns no delay.

If you want to carry some extra data to the trigger action, you can set the `PreSchedEventResult`'s `EventData` field. The data will be passed to the trigger action. For example:

```go
func (h *MyTimerHook) OnPreSchedEvent(ctx context.Context, event api.TimerShedEvent) (api.PreSchedEventResult, error) {
    if shouldDelaySchedule() {
        return api.PreSchedEventResult{
            EventData: []byte("mycustomeventdata"),
        }, nil
    }   
    return api.PreSchedEventResult{}, nil
}
```

The `EventData` will be persisted before the action is actually triggered. So you can use it to do some recovery work if the TiDB process restarts.

After `PreSchedEventResult` indicates to continue the trigger action, the event meta including the `EventData` will then be persisted to the store. After that, if you query this timer with the client, you can see the `EventStatus` changes to `TRIGGER` from `IDLE`, and the `EventID` is also not empty.

Then the hook method `OnSchedEvent` will be called. You can do some real trigger action here. For example, you can send a message to a message queue or submit some job to an outer job system. If your job is not very heavy, you can even do it directly in this method. Please notice that the method `OnSchedEvent` will only be called once if it returns no error. Only some cases may cause the method `OnSchedEvent` to be called again:

- The method `OnSchedEvent` returns an error.
- The runtime restarts and finds a timer's `EventStatus` is `TRIGGER`.

You should close the timer event by calling the client's method `CloseTimerEvent` to reset the timer's state to `IDLE`, or the timer will not be able to schedule new events. By default, closing a timer will also reset the `Watermark` field to the value of `EventStart`, but you can specify the next watermark manually. For example:

```go
func (h *MyTimerHook) OnSchedEvent(ctx context.Context, event api.TimerShedEvent) error {
    // do some things ...
	
	// close the timer event so that the next event can be scheduled
    return h.cli.CloseTimerEvent(
        ctx, 
        event.Timer().ID, 
        event.EventID(),  
        api.WithSetWatermark(time.Now()), // use the current time as the next watermark instead of the event start time
    )
}
```

## FAQs

- Q: What is the difference between the attributes `ID` and `Key` of the timer, they are all unique.
  A: The key difference is that the `Key` is specified by the user but the `ID` is generated by the framework. The `ID` is the real identifier of a timer because it is "absolutely" unique because any two timers cannot have the same `ID` even if one timer is deleted. But it is allowed to create a timer with a `Key` that is the same as another timer which deleted before, and it is also allowed to create timers with the same `Key` in different namespaces. So it is better to use `ID` to log events from a certain timer. But `Key` is also useful because it can be meaningful. `Key` can be used to do some semantic works like querying or ensuring the idempotent creation of timers for certain uses.

- Q: What is the behavior of the timer framework if the TiDB restarts, especially if it shuts down for a long time?
  A: When the TiDB restarts (which means the timer runtime restarts), the runtime will reload all timers from the store and check their state. If a timer's `EventStatus` is `TRIGGER`, the runtime will try to trigger the event again by calling the hook's method `OnSchedEvent` (`OnPreSchedEvent` will not be called). If the `EventStatus` is `IDLE`, the runtime will try to trigger the event if the timer is time to trigger. Notice that when a runtime shuts down for a long time, there may be many time points to trigger between `Watermark` and the current time, but the hook will still be triggered only once. For example, one timer has a cron expression `0 * * * * *` indicating that it should trigger every one hour. If the TiDB shuts down, leave this timer with a watermark `2022-01-01 09:58:00` and then restart at `2022-01-01 12:01:00`, and it tries to trigger the timer. However, there are 3 time points which are not triggered. The hook will still be invoked once with the `EventStart` value `2022-01-01 12:01:00` and `Watmerkark` value `2022-01-01 09:58:00`. It is up to the hook class to determine how to handle this case. For example, it can do the trigger action 3 times in one `OnSchedEvent` call or just do it once and then close the timer with the new watermark `2022-01-01 10:00:00`. For the second case, the timer will be triggered again after a very short time because the next expected trigger time `2022-01-01 11:00:00` is still before the current time.

- Q: Is it thread-safe for the hook call such as `Start`, `Stop`, `OnPreSchedEvent`, `OnSchedEvent`?
  A: Yes, it is thread-safe. They are called in one goroutine now, there is no data race without a lock for variables that are only read or written in these methods. But if you create a goroutine manually and access these variables in the background, you should also use a lock to protect them. In the future, the trigger method such as `OnPreSchedEvent` and `OnSchedEvent` may be called in concurrency of different timers, but it will also be guaranteed that these methods for the same timer will be called in sequence and guarantees the happens-before semantics.

- Q: Will the hook calls from different timers be blocked by each other if some hook calls take a long time?
  A: Yes, it may happen if two timers share the same hook and one timer's call is slow. The reason is timers that share one hook will be triggered in one goroutine (or maybe a pool with some fixed goroutines in the future). To resolve this issue, you can just do the trigger action background and return a non-error value immediately in the foreground call. Notice that if you do this, you should retry yourself when the trigger action fails. 
