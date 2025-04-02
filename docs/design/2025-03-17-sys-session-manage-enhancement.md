# Enhancement of Internal System Session Management

- Author: [Wang Chao](https://github.com/lcwangchao)
- Discussion PR: <https://github.com/pingcap/tidb/pull/59875>
- Tracking Issue: <https://github.com/pingcap/tidb/issues/60115>

## Motivation and Background

The internal system sessions are widely used in TiDB across multiple modules. To minimize the overhead of creating and destroying sessions, we use a session pool for management. However, we have recently encountered several issues with the current session pool implementation—most of which stem from its improper usage.

For example, in issue [#56934](https://github.com/pingcap/tidb/issues/56934), the TTL module retrieves a session from the pool but fails to return it after use, leading to a memory leak. Another issue arose after PR [#32726](https://github.com/pingcap/tidb/pull/32726), where each session retrieved from the pool is stored as a reference in a map. This reference is only removed when the session is returned to the pool. As a result, if a session is closed using the `Close` method, it may still cause a memory leak because it remains referenced in the map.

Some PR such as [#59546](https://github.com/pingcap/tidb/pull/59546) attempt to address these issues by introducing a `Destroy` method for the session pool. However, this approach does not fully resolve the problems and still makes it difficult to ensure that developers call the correct method.

This document proposes a more effective solution for managing internal system sessions to achieve the following goals:

- Prevent concurrent usage across modules: Once a session is returned to the pool, it should no longer be used to avoid potential conflicts from concurrent access.
- Simplify session lifecycle management: Ensure that sessions are always either returned to the pool or properly closed after use, eliminating the risk of memory leaks.
- Enforce session state reset: When a session is returned to the pool, its internal state should be forcibly reset to prevent unexpected behaviors in subsequent usage.

## Design

### Pool

We need to introduce a new enhanced session pool to manage internal system sessions. The new session pool will provide the following methods:

```go
type Pool struct {
    ...
}

func (p *Pool) get() (*Session, error) {
    ...
}

func (p *Pool) put(s *Session) {
    ...
}

func (p *Pool) WithSession(func(*Session) error) error {
    ...
}
```

The most important method of `Pool` is `WithSession` which is introduced to replace the `Get` and `Put` methods of the current session pool. The `WithSession` method accepts a function as an argument, which will be executed with a session retrieved from the pool. After the function completes, the session will be automatically returned to the pool. This design ensures that the session is always returned to the pool after use, preventing memory leaks.

Let's see how `WithSession` works:

```go
func (p *Pool) WithSession(fn func(*Session) error) error {
    se, err := p.get()
    if err != nil {
        return err
    }

    success := false
    defer func() {
        if success {
            p.put(se)
        } else {
            se.Close()
        }
    }()

    if err = fn(se); err != nil {
        return err
    }
    success = true
    return nil
}
```

We can see that the `WithSession` method always calls `p.put(se)` within a defer function to ensure the session is returned to the pool. Some exceptions are when a panic or error happens. In such cases, the session is destroyed instead of being returned to the pool, as it may be in an undefined state, making it safer to discard rather than reuse.

The `Pool` should be responsible for managing the internal sessions' registration and unregistering, and the callers should not care about it. These operations are handled by `Pool.get` and `Pool.put` methods:

```go
func (p *Pool) get() (*Session, error) {
    // get a session from the pool
    se, err := getFromPool()
    if err != nil {
        return nil, err
    }
    // register the internal session to the map
    infosync.StoreInternalSession(se.GetSessionContext())
    return se, nil
}

func (p *Pool) put(s *Session) {
    // rollback the session first to reset the internal state.
    s.Rollback()
    // unregister the internal session from the map
    infosync.DeleteInternalSession(s.GetSessionContext())
    // put the session back to the pool
    p.putToPool(s)
}
```

The above code has been simplified for demonstration purposes. The session pool stores the internal session in a map when `Pool.get` is called and removes it from the map in `Pool.put`. Additionally, before being returned to the pool, the session is automatically rolled back to ensure its internal state is reset.

With the newly implemented session pool that manages the session lifecycle, developers no longer need to worry about the session's state or lifecycle. 

### Session

The type `Session` is not a "real" one but a wrapper of the internal session, see:

```go
// Session is public to callers.
type Session struct {
    internal *session
}
```

It provides a set of methods that proxy access to the internal session. When the caller invokes `WithSession`, a new `Session` instance is created and passed to the function. Once the function completes, the `Session` instance is destroyed, and only the internal session is returned to the pool.

We have several reasons for proposing the above design:

- The wrapped methods allow for additional checks to prevent misuse of the internal session. For example, if a caller attempts to invoke methods after the session has been returned to the pool, we can detect this and return an error.
- The `Session` type can be extended with new methods, providing more convenient functionality for internal use.
- The original session contains numerous methods, many of which may not be necessary for the caller. By filtering out unnecessary methods, we can expose only the required ones, simplifying the interface and improving usability.

The internal session is not exposed to the caller and can only be accessed by the `Session` or `Pool` types. It has several fields, see:

```go
// session is private and can only be accessed by the `Pool` and `Session`.
type session struct {
    mu sync.Mutex
    owner any
    sctx sessionctx.Context
    ...
}
```

The `mu` protects the internal session from concurrent access, and the field `sctx` is the real session context that holds the session's state. 

Let's provide further clarification on the `owner` field. The "owner" refers to the entity that holds the internal session, and only the owner is permitted to use it. When an internal session is in the pool, the pool itself is considered the owner. When a caller retrieves the session from the pool, the wrapped `Session` becomes the new owner, which is then passed to the caller.

When the caller returns the session to the pool, ownership is first transferred back to the pool, which then places the session back into the pool. Since the public `Session` will not be reused, once it loses ownership, it can never regain it. This ensures that the `Session` becomes invalid once it has been used, preventing any further use after its lifecycle is completed.

```go
func (s *session) checkOwner(owner any) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.owner == nil || s.owner != owner {
        return errors.New("invalid owner")
    }
    return nil
}

func (s *Session) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
    if err := s.internal.checkOwner(s); err != nil {
        return nil, err
    }
    return s.internal.sctx.ExecuteInternal(ctx, sql, args...)
}
```

## Tests

We should do the below tests to make sure the new introduced session pool works as expected:

- Unit tests.
- Integration tests for specified scenarios:
  - Test 1000+ TTL tables with small `tidb_ttl_job_internal` settings to test the frequent session creation and destruction in the TTL scene.
  - Test async load with corrupted statistics to ensure there are no memory leaks.
  - More tests for other scenarios.

## More Features to Support

There are also some features we can support in the future:

- Support to detect concurrent usage of the un-thread-safe methods.
- Support to detect more session state changes such as system variables and reset them when returning to the pool.
