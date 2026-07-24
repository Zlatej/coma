# coma

> a more robust implementation is almost done in the [`mutex-cond-impl`](https://github.com/Zlatej/coma/tree/mutex-cond-impl) branch.

*a tiny concurrency manager for go*

coma makes sure your program doesn't get overwhelmed with goroutines and fall into a coma.

1. limits how many goroutines run at once
2. waits until all of them are done
3. is simple
4. works

## install

```sh
go get github.com/zlatej/coma
```

## usage

```go
cm := coma.New(5) // at most 5 goroutines at a time

for _, task := range tasks {
    // blocks until a slot is free and acquires it
    if err := cm.AcquireContext(ctx); err != nil {
        // error means either the context is done or Wait has been called
        return
    } 
    go func(t Task) {
        defer cm.Release() // releases slot
        process(t)
    }(task)
}

cm.Wait() // blocks until all slots are released
```

no context? use `cm.Acquire()` instead

## notes

- `Wait` is terminal: once called, the manager cannot be reused, `Acquire`/`AcquireContext` will return `ErrClosed`.
- calling `Wait` again from the same goroutine is a safe no-op, however calling it concurrently from multiple goroutines may panic.

## license

MIT
