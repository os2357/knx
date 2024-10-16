// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	blockTypeTests = []BlockType{
		BlockInt64,
		BlockUint64,
		BlockFloat64,
		BlockInt32,
		BlockInt16,
		BlockInt8,
		BlockUint32,
		BlockUint16,
		BlockUint8,
		BlockFloat32,
	}

	// little endian
	maxIntBytes = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
)

func TestAppend64(t *testing.T) {
	n := 2  // 2 elements
	sz := 8 // int64
	block := New(BlockInt64, n)
	require.NotNil(t, block)
	require.NotNil(t, block.ptr)
	require.Equal(t, block.len, 0)
	require.Equal(t, block.cap, n)

	// first append writes buf[0:8]
	block.Int64().Append(math.MaxInt64)
	require.NotNil(t, *(*[]byte)(block.ptr), 0)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 1)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// second append writes buf[8:16]
	block.Int64().Append(math.MaxInt64)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int64().Append(math.MaxInt64)
	})
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr, 0)
}

func TestAppend32(t *testing.T) {
	n := 2  // 2 elements
	sz := 4 // int32
	block := New(BlockInt32, n)
	require.NotNil(t, block)
	require.NotNil(t, block.ptr)
	require.Equal(t, block.len, 0)
	require.Equal(t, block.cap, n)

	// first append writes buf[0:4]
	block.Int32().Append(math.MaxInt32)
	require.NotNil(t, *(*[]byte)(block.ptr), 0)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 1)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// second append writes buf[4:8]
	block.Int32().Append(math.MaxInt32)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int32().Append(math.MaxInt32)
	})
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr, 0)
}

func TestAppend16(t *testing.T) {
	n := 2  // 2 elements
	sz := 2 // int16
	block := New(BlockInt16, n)
	require.NotNil(t, block)
	require.NotNil(t, block.ptr)
	require.Equal(t, block.len, 0)
	require.Equal(t, block.cap, n)

	// first append writes buf[0:2]
	block.Int16().Append(math.MaxInt16)
	require.NotNil(t, *(*[]byte)(block.ptr), 0)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 1)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// second append writes buf[2:4]
	block.Int16().Append(math.MaxInt16)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int16().Append(math.MaxInt16)
	})
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr, 0)
}

func TestAppend8(t *testing.T) {
	n := 2  // 2 elements
	sz := 1 // int8
	block := New(BlockInt8, n)
	require.NotNil(t, block)
	require.NotNil(t, block.ptr)
	require.Equal(t, block.len, 0)
	require.Equal(t, block.cap, n)

	// first append writes buf[0:1]
	block.Int8().Append(math.MaxInt8)
	require.NotNil(t, *(*[]byte)(block.ptr), 0)
	require.Equal(t, block.data()[0:sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 1)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// second append writes buf[1:2]
	block.Int8().Append(math.MaxInt8)
	require.Equal(t, block.data()[sz:sz+sz], maxIntBytes[8-sz:])
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr)

	// third append panics (capacity reached)
	require.Panics(t, func() {
		block.Int8().Append(math.MaxInt8)
	})
	require.Equal(t, block.len, 2)
	require.Equal(t, block.cap, n)
	require.NotNil(t, block.ptr, 0)
}

func TestGet64(t *testing.T) {
	block := New(BlockInt64, 1024)
	for i := 0; i < 1024; i++ {
		v := rand.Int64()
		block.Int64().Append(v)
		w := block.Int64().Get(i)
		require.Equal(t, v, w)
	}
}

func TestSet64(t *testing.T) {
	block := New(BlockInt64, 1024)
	for i := 0; i < 1024; i++ {
		block.Int64().Append(rand.Int64())
		block.Int64().Set(i, int64(i))
		require.Equal(t, int64(i), block.Int64().Get(i))
	}
}

func TestLess64(t *testing.T) {
	block := New(BlockInt64, 1024)
	for i := 0; i < 1024; i++ {
		block.Int64().Append(rand.Int64())
	}
	for i := 1; i < 1024; i++ {
		a, b := block.Int64().Get(i-1), block.Int64().Get(i)
		require.Equal(t, a < b, block.Int64().Less(i-1, i))
	}
}

func TestSwap64(t *testing.T) {
	block := New(BlockInt64, 1024)
	for i := 0; i < 1024; i++ {
		block.Int64().Append(rand.Int64())
	}
	sort.Sort(block)
	for i := 1; i < 1024; i++ {
		require.True(t, block.Int64().Less(i-1, i))
	}
}

func BenchmarkAppend(b *testing.B) {
	block := New(BlockInt64, 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if i%1024 == 0 {
			block.Clear()
		}
		block.Int64().Append(int64(i))
	}
}

func BenchmarkRead(b *testing.B) {
	block := New(BlockInt64, 1024)
	block.Int64().Append(math.MaxInt64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		block.Int64().Get(0)
	}
}

func BenchmarkSet(b *testing.B) {
	block := New(BlockInt64, 1024)
	block.Int64().Append(math.MaxInt64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		block.Int64().Set(0, math.MaxInt64)
	}
}

func BenchmarkLess(b *testing.B) {
	block := New(BlockInt64, 1024)
	block.Int64().Append(math.MaxInt64)
	block.Int64().Append(math.MaxInt64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = block.Int64().Less(0, 1)
	}
}

func BenchmarkSwap(b *testing.B) {
	block := New(BlockInt64, 1024)
	block.Int64().Append(math.MaxInt64)
	block.Int64().Append(math.MaxInt64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		block.Int64().Swap(0, 1)
	}
}
