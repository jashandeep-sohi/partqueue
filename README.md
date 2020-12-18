# partqueue

[![Go Reference](https://pkg.go.dev/badge/github.com/jashandeep-sohi/partqueue.svg)](https://pkg.go.dev/github.com/jashandeep-sohi/partqueue)

Package partqueue provides a type of queue to process partitioned items.

Producers can put items into the queue along with a partition key and a set of
consumers can process them in-order per partition.

## Usage

```go
package main

import (
  "context"
  "fmt"
  "time"

  "github.com/jashandeep-sohi/partqueue"
)

func main() {
  // run this for 10 secs
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()

  // create a queue
  q := partqueue.New(ctx, partqueue.WithPartitionCapacity(0), partqueue.WithBufferCapacity(0))

  // spin up consumers
  for i := 0; i < 3; i++ {
    go consumer(i, q)
  }

  // produce some work
  for i := 0; i < 10; i++ {
    partKey := partqueue.PartitionKey(fmt.Sprintf("%v", i%10))
    q.Put(ctx, partKey, i)
  }

  // wait for ctx to timeout
  <-ctx.Done()
}

func consumer(c int, q *partqueue.Queue) {
  for {
    // wait for an item
    i, _ := q.Get(context.Background())

    fmt.Printf("consumer %v got %v\n", c, i)

    // simulate work
    time.Sleep(200 * time.Millisecond)

    // NOTE:
    // must call Done so that the next item in the partition may be sent
    // otherwise no more items from the partition will be received
    i.Done()
  }
}
```
