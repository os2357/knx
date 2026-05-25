// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

// GenericPool is a pool of types that can be re-used.  Items in
// this pool will not be garbage collected when not in use.
type GenericPool struct {
	pool chan any
	fn   func() any
}

// NewGeneric returns a Generic pool with capacity for max items
// to be pool.
func NewGenericPool(max int, fn func() any) *GenericPool {
	return &GenericPool{
		pool: make(chan any, max),
		fn:   fn,
	}
}

// Get returns a item from the pool or a new instance if the pool
// is empty.  Items returned may not be in the zero state and should
// be reset by the caller.
func (p *GenericPool) Get() any {
	var c any
	select {
	case c = <-p.pool:
	default:
		c = p.fn()
	}

	return c
}

// Put returns an item back to the pool.  If the pool is full, the item
// is discarded.
func (p *GenericPool) Put(c any) {
	select {
	case p.pool <- c:
	default:
	}
}
