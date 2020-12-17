/*
partqueue
Copyright (C) 2020  Jashandeep Sohi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

// Package partqueue provides a type of queue to process partitioned items.
//
// Producers can put items into the queue along with a partition key and
// a set of consumers can process them in-order per partition.
//
// Consumers signal when they are done with an item, thereby dequeuing another
// item form the same partition. This allows only one consumer to process an
// item per partition at a time.
//
// Items that have a different partition key are however consumed in-parallel.
package partqueue

import (
	"context"
	"sync"
)

// DefaultPartitionCapcity is the default capacity for each partition buffer.
const DefaultPartitionCapcity = 0

// DefaultBufferCapacity is the default capacity for the output buffer.
const DefaultBufferCapacity = 0

// Queue is a partitioned queue. Create it using New.
type Queue struct {
	ctx       context.Context
	bufferCap int
	partCap   int
	parts     map[PartitionKey]*partition
	partsLock sync.Mutex
	buffer    chan *Item
}

// Option is a function that initializes a Queue.
type Option func(*Queue)

// WithBufferCapacity sets the output buffer capacity.
//
// Setting this to the number of consumers is a good starting point.
// A value of zero makes it unbuffered.
func WithBufferCapacity(n int) Option {
	return func(q *Queue) {
		q.bufferCap = n
	}
}

// WithPartitionCapacity sets the buffer capacity for each partition.
//
// Once this capacity is reached, Put will block for that partition (until
// there's room again by consuming items).
// A value of zero makes it unbuffered.
func WithPartitionCapacity(n int) Option {
	return func(q *Queue) {
		q.partCap = n
	}
}

// New returns an initialized Queue that's functional until ctx is
// cancelled.
func New(ctx context.Context, opts ...Option) *Queue {
	q := &Queue{
		ctx:       ctx,
		bufferCap: DefaultBufferCapacity,
		partCap:   DefaultPartitionCapcity,
		parts:     make(map[PartitionKey]*partition),
		partsLock: sync.Mutex{},
	}

	for _, opt := range opts {
		opt(q)
	}

	q.buffer = make(chan *Item, q.bufferCap)

	return q
}

// Put adds an Item into the Queue with Key k and Value v.
//
// If the partition is at capacity it will block until there's room or ctx is
// cancelled.
func (q *Queue) Put(ctx context.Context, k PartitionKey, v interface{}) error {
	p := q.getOrCreatePartition(k)

	i := &Item{Key: k, Value: v, p: p}

	return p.put(ctx, i)
}

// Get pops and returns an Item from the Queue.
//
// It will block until there's an item available or ctx is cancelled.
func (q *Queue) Get(ctx context.Context) (*Item, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case i := <-q.buffer:
		return i, nil
	}
}

// getOrCreatePartition returns the partition for PartitionKey k in a thread
// safe manner, creating it if it doesn't already exist.
func (q *Queue) getOrCreatePartition(k PartitionKey) *partition {
	p, ok := q.parts[k]

	if ok {
		return p
	}

	q.partsLock.Lock()
	defer q.partsLock.Unlock()

	// make sure the partition wasn't created while waiting for the lock
	p, ok = q.parts[k]
	if ok {
		return p
	}

	p = newPartition(q.ctx, q.partCap, q.buffer)
	q.parts[k] = p

	return p
}
