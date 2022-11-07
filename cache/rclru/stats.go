// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, stefan@blockwatch.cc

package rclru

import (
    "sync/atomic"
)

type CacheStats struct {
    Hits      int64
    Misses    int64
    Inserts   int64
    Evictions int64
    Count     int64
    Size      int64
}

func (s *CacheStats) Hit() {
    atomic.AddInt64(&s.Hits, 1)
}

func (s *CacheStats) Miss() {
    atomic.AddInt64(&s.Misses, 1)
}

func (s *CacheStats) Add(sz int) {
    atomic.AddInt64(&s.Inserts, 1)
    atomic.AddInt64(&s.Count, 1)
    atomic.AddInt64(&s.Size, int64(sz))
}

func (s *CacheStats) Rem(sz int) {
    atomic.AddInt64(&s.Evictions, 1)
    atomic.AddInt64(&s.Count, -1)
    atomic.AddInt64(&s.Size, int64(-sz))
}
