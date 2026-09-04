# coma

[![Go Reference](https://pkg.go.dev/badge/github.com/zlatej/coma.svg)](https://pkg.go.dev/github.com/zlatej/coma)
[![Go](https://github.com/zlatej/coma/actions/workflows/go.yml/badge.svg)](https://github.com/zlatej/coma/actions/workflows/go.yml)

_a tiny concurrency manager for go_

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
    go func() {
        defer cm.Release() // releases slot
        process(task)
    }()
}

fmt.Printf("currently running %d goroutines\n", cm.RunningCount())

cm.Wait() // blocks until all slots are released
```

no context? use `cm.Acquire()` instead

## remote graceful shutdown

if you want to close the ConcurrencyManager from one place, but still wait until all tasks are done in a different place.

```go
cm := coma.New(5) // at most 5 goroutines at a time

go func() {
    <-sigint
    cm.Close() // close from remote call
}()

for {
    task := getTask()
    if err := cm.Acquire(); err != nil { // returns ErrClosed after cm.Close()
        break
    }
    go func() {
        defer cm.Release()
        process(task)
    }()
}

cm.Wait() // already closed, just wait until all slots are released
```

## notes

- `New` treats a `max` of less than 1 as 1.
- `Close` is terminal: once called, the manager cannot be reused, `Acquire`/`AcquireContext` will return `ErrClosed`.
- `Wait` behaves like `Close` but then blocks until all slots are released.
- `Wait` and `Close` can be combined and can safely be called any number of times, including concurrently from multiple goroutines.
- every successful `Acquire`/`AcquireContext` must be matched by exactly one `Release`. An unmatched `Release` panics when no slot is held.

## license

[MIT](LICENSE)
