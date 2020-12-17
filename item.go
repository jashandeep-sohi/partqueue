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

package partqueue

import (
	"fmt"
)

// PartitionKey is used to sort items into partitions.
type PartitionKey string

// Item is used to wrap a value along with its partition metadata.
// Should not be created explicitly.
type Item struct {
	Key   PartitionKey
	Value interface{}

	p *partition
}

// Done signals to the Queue that it's okay to send another item in the same
// partition (possibly to another consumer).
func (i *Item) Done() {
	if i.p == nil {
		return
	}

	i.p.decrementInflight()
}

// String returns its string representation.
func (i *Item) String() string {
	return fmt.Sprintf("{key: %v, value: %v}", i.Key, i.Value)
}
