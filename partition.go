/*
partqueue
Copyright (C) 2019  Jashandeep Sohi

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

package partqueue

import (
	"context"
)

type partition struct {
	out      chan *Item
	buffer   chan *Item
	inflight chan struct{}
}

func newPartition(ctx context.Context, capacity int, out chan *Item) *partition {
	p := &partition{
		out:      out,
		buffer:   make(chan *Item, capacity),
		inflight: make(chan struct{}, 1),
	}

	go p.run(ctx)

	return p
}

func (p *partition) run(ctx context.Context) {
	var i *Item
	for {
		// make sure only one item is in flight
		select {
		case <-ctx.Done():
			return
		case p.inflight <- struct{}{}:
		}

		// pop an item from the buffer
		select {
		case <-ctx.Done():
			return
		case i = <-p.buffer:
		}

		// send it
		select {
		case <-ctx.Done():
			return
		case p.out <- i:
		}
	}
}

func (p *partition) put(ctx context.Context, i *Item) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.buffer <- i:
		return nil
	}
}

func (p *partition) decrementInflight() {
	<-p.inflight
}
