package partqueue_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jashandeep-sohi/partqueue"
)

func TestPut(t *testing.T) {
	qctx, qcancel := context.WithCancel(context.Background())
	defer qcancel()

	q := partqueue.New(qctx, partqueue.WithBufferCapacity(0), partqueue.WithPartitionCapacity(0))

	t.Run("empty", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		err := q.Put(ctx, "a", "a")
		if err != nil {
			t.Fatal("expected Put to not block")
		}
	})

	t.Run("full", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		err := q.Put(ctx, "a", "a")

		if err == nil {
			t.Fatal("expected Put to block")
		}
	})

}

func TestGet(t *testing.T) {
	qctx, qcancel := context.WithCancel(context.Background())
	defer qcancel()

	q := partqueue.New(qctx, partqueue.WithBufferCapacity(0), partqueue.WithPartitionCapacity(0))

	t.Run("empty", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, err := q.Get(ctx)

		if err == nil {
			t.Fatal("expected Get to block")
		}
	})

	t.Run("not empty", func(t *testing.T) {
		q.Put(qctx, "a", "a")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, err := q.Get(ctx)

		if err != nil {
			t.Fatal("expected Get to not block")
		}
	})

}

func TestPartitionOrder(t *testing.T) {
	qctx, qcancel := context.WithCancel(context.Background())
	defer qcancel()

	q := partqueue.New(qctx, partqueue.WithPartitionCapacity(5), partqueue.WithBufferCapacity(0))

	partAIn := []int{0, 1, 2, 3, 4}
	partAOut := []int{}

	partBIn := []int{5, 6, 7, 8, 9}
	partBOut := []int{}

	for _, v := range partAIn {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		err := q.Put(ctx, "A", v)

		if err != nil {
			t.Fatal("expected Put to partition A to not block")
		}
	}

	for _, v := range partBIn {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		err := q.Put(ctx, "B", v)

		if err != nil {
			t.Fatal("expected Put to partition B to not block")
		}
	}

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		i, err := q.Get(ctx)

		if err != nil {
			t.Fatal("expected Get to not block")
		}

		switch i.Key {
		case "A":
			partAOut = append(partAOut, i.Value.(int))
		case "B":
			partBOut = append(partBOut, i.Value.(int))
		}

		i.Done()
	}

	if !reflect.DeepEqual(partAIn, partAOut) {
		t.Fatalf("partition A expected: %v, got: %v", partAIn, partAOut)
	}

	if !reflect.DeepEqual(partBIn, partBOut) {
		t.Fatalf("partition B expected: %v, got: %v", partBIn, partBOut)
	}
}

func TestPartitionMultipleConsumers(t *testing.T) {
	qctx, qcancel := context.WithCancel(context.Background())
	defer qcancel()

	q := partqueue.New(qctx, partqueue.WithPartitionCapacity(20), partqueue.WithBufferCapacity(0))

	for _, pk := range []partqueue.PartitionKey{"A", "B"} {
		for i := 0; i < 20; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			err := q.Put(ctx, pk, i)
			if err != nil {
				t.Fatal("expected Put to not block")
			}
		}
	}

	aLock := make(chan struct{}, 1)
	bLock := make(chan struct{}, 1)

	start := sync.WaitGroup{}
	start.Add(1)

	consuming := sync.WaitGroup{}

	consumer := func() {
		start.Wait()
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			i, err := q.Get(ctx)
			if err != nil {
				consuming.Done()
				break
			}

			var lock chan struct{}
			switch i.Key {
			case "A":
				lock = aLock
			case "B":
				lock = bLock
			}

			select {
			case lock <- struct{}{}:
			default:
				consuming.Done()
				t.Fatalf("processing partition items in parallel")
			}

			time.Sleep(time.Millisecond)
			<-lock
			i.Done()
		}
	}

	for i := 0; i < 5; i++ {
		consuming.Add(1)
		go consumer()
	}
	start.Done()

	consuming.Wait()
}
