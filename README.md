# coma

a tiny concurrency manager for go

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
    cm.Wait() // blocks until a slot is free
    go func(t Task) {
        defer cm.Done() // marks done
        process(t)
    }(task)
}

cm.WaitAllDone() // blocks until all are marked as done
```

## license

MIT
