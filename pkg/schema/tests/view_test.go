// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema_tests

import (
	"bytes"
	"testing"

	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/stretchr/testify/require"
)

func TestViewFixed(t *testing.T) {
	base := NewArrayTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[ArrayTypes]()
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	require.NotNil(t, buf)
	view := NewView(baseSchema).Reset(buf)
	require.True(t, view.IsValid())
	require.True(t, view.IsFixed())
	require.Equal(t, baseSchema.WireSize(), view.Len())
	require.Equal(t, view.Buffer(), buf)
	val, ok := view.Get(0)
	require.True(t, ok)
	require.Equal(t, base.Id, val)
	require.Equal(t, base.Id, view.GetPk())
}

func TestViewDynamic(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)
	require.True(t, view.IsValid())
	require.False(t, view.IsFixed())
	require.Equal(t, baseSchema.WireSize()+8+8+16, view.Len()) // big(8), bytes(8), string(16)
	require.Equal(t, view.Buffer(), buf)
}

func testViewGetVal(t *testing.T, view *View, pos int, cmp any) {
	t.Helper()
	val, ok := view.Get(pos)
	require.True(t, ok)
	require.Equal(t, cmp, val)
}

func testViewGetFail(t *testing.T, view *View, pos int) {
	t.Helper()
	_, ok := view.Get(pos)
	require.False(t, ok)
}

func TestViewGet(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)

	require.Equal(t, base.Id, view.GetPk())
	testViewGetVal(t, view, 0, base.Id)
	testViewGetVal(t, view, 1, base.Int64)
	testViewGetVal(t, view, 2, base.Int32)
	testViewGetVal(t, view, 3, base.Int16)
	testViewGetVal(t, view, 4, base.Int8)
	testViewGetVal(t, view, 5, base.Uint64)
	testViewGetVal(t, view, 6, base.Uint32)
	testViewGetVal(t, view, 7, base.Uint16)
	testViewGetVal(t, view, 8, base.Uint8)
	testViewGetVal(t, view, 9, base.Float64)
	testViewGetVal(t, view, 10, base.Float32)
	testViewGetVal(t, view, 11, base.D32)
	testViewGetVal(t, view, 12, base.D64)
	testViewGetVal(t, view, 13, base.D128)
	testViewGetVal(t, view, 14, base.D256)
	testViewGetVal(t, view, 15, base.I128)
	testViewGetVal(t, view, 16, base.I256)
	testViewGetVal(t, view, 17, base.Bool)
	testViewGetVal(t, view, 18, base.Time)
	testViewGetVal(t, view, 19, base.Hash)
	testViewGetVal(t, view, 20, base.Array[:]) // return type is []byte
	testViewGetVal(t, view, 21, base.String)
}

func TestViewGetWithVisibility(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	visSchema, err := baseSchema.DeleteId(2)
	require.NoError(t, err)
	visSchema, err = visSchema.DeleteId(4)
	require.NoError(t, err)
	visSchema, err = visSchema.DeleteId(5)
	require.NoError(t, err)
	visEnc := NewEncoder(visSchema)
	buf, err := visEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(visSchema).Reset(buf)

	require.Equal(t, base.Id, view.GetPk())
	testViewGetVal(t, view, 0, base.Id)
	testViewGetFail(t, view, 1)
	testViewGetVal(t, view, 2, base.Int32)
	testViewGetFail(t, view, 3)
	testViewGetFail(t, view, 4)
	testViewGetVal(t, view, 5, base.Uint64)
	testViewGetVal(t, view, 6, base.Uint32)
	testViewGetVal(t, view, 7, base.Uint16)
	testViewGetVal(t, view, 8, base.Uint8)
	testViewGetVal(t, view, 9, base.Float64)
	testViewGetVal(t, view, 10, base.Float32)
	testViewGetVal(t, view, 11, base.D32)
	testViewGetVal(t, view, 12, base.D64)
	testViewGetVal(t, view, 13, base.D128)
	testViewGetVal(t, view, 14, base.D256)
	testViewGetVal(t, view, 15, base.I128)
	testViewGetVal(t, view, 16, base.I256)
	testViewGetVal(t, view, 17, base.Bool)
	testViewGetVal(t, view, 18, base.Time)
	testViewGetVal(t, view, 19, base.Hash)
	testViewGetVal(t, view, 20, base.Array[:]) // return type is []byte
	testViewGetVal(t, view, 21, base.String)
}

// TestViewSet tests the Set method of the View struct
func TestViewSet(t *testing.T) {
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseEnc := NewEncoder(baseSchema)
	buf, err := baseEnc.Encode(&base, nil)
	require.NoError(t, err)
	view := NewView(baseSchema).Reset(buf)

	// Test setting uint64 field
	newId := uint64(12345)
	safeSet(t, view, 0, newId)
	val, ok := view.Get(0)
	require.True(t, ok)
	require.Equal(t, newId, val, "Uint64 field should have been updated")

	// Read the original string value
	originalString, ok := view.Get(21)
	require.True(t, ok)

	// Test setting a shorter string
	shortString := "Hello"
	safeSet(t, view, 21, shortString)
	val, ok = view.Get(21)
	require.True(t, ok)
	require.Equal(t, originalString, val, "String value should not have changed when setting a shorter string")

	// Test setting a string of the same length as the original
	sameLength := "0123456789abcdef"
	safeSet(t, view, 21, sameLength)
	val, ok = view.Get(21)
	require.True(t, ok)
	require.Equal(t, originalString, val, "String value should not have changed when setting a same-length string")

	// Test setting a longer string
	longString := sameLength + "extra"
	safeSet(t, view, 21, longString)
	val, ok = view.Get(21)
	require.True(t, ok)
	require.Equal(t, originalString, val, "String value should not have changed when setting a longer string")

	// Test setting invalid index
	panicSet(t, view, -1, 42)
	panicSet(t, view, len(baseSchema.Fields), 42)

	// Test setting incompatible type
	originalId, ok := view.Get(0)
	require.True(t, ok)
	safeSet(t, view, 0, "not a uint64")
	val, ok = view.Get(0)
	require.True(t, ok)
	require.Equal(t, originalId, val, "Value should not have changed when setting incompatible type")
}

// safeSet is a helper function to safely call Set and log any panics
func safeSet(t *testing.T, view *View, index int, value any) {
	t.Helper()
	require.NotPanics(t, func() {
		view.Set(index, value)
	}, "setting index %d with value %v", index, value)
}

// panicSet is a helper function to check if calls to Set panic
func panicSet(t *testing.T, view *View, index int, value any) {
	t.Helper()
	require.Panics(t, func() {
		view.Set(index, value)
	}, "setting index %d with value %v", index, value)
}

func BenchmarkViewCut(b *testing.B) {
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err := baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := NewView(baseSchema)

	b.ReportAllocs()
	for b.Loop() {
		view.Cut(buf.Bytes())
	}
}

func BenchmarkViewCutSkip(b *testing.B) {
	var err error
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	baseSchema, err = baseSchema.DeleteId(2)
	require.NoError(b, err)
	baseSchema, err = baseSchema.DeleteId(10)
	require.NoError(b, err)
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	_, err = baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := NewView(baseSchema)

	b.ReportAllocs()
	for b.Loop() {
		view.Cut(buf.Bytes())
	}
}

func BenchmarkView(b *testing.B) {
	baseSchema := reflect.MustSchemaFor[AllTypes]()
	base := NewAllTypes(int64(0x0faf0faf0faf0faf))
	baseEnc := NewEncoder(baseSchema)
	buf := bytes.NewBuffer(nil)
	_, err := baseEnc.Encode(&base, buf)
	require.NoError(b, err)
	view := NewView(baseSchema)
	view.Reset(buf.Bytes())
	b.Log(view.Schema().String())

	b.Run("reset", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view.Reset(buf.Bytes())
		}
	})

	b.Run("set_pk", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view.SetPk(1)
		}
	})

	b.Run("get_fixed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p, ok := view.Get(0)
			// require.True(b, ok)
			_ = p
			_ = ok
		}
	})

	b.Run("get_fixed_ptr", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p, _, ok := view.GetPtr(0)
			// require.True(b, ok)
			_ = p
			_ = ok
		}
	})

	b.Run("get_fixed_u64", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p, ok := view.Uint64(0)
			// require.True(b, ok)
			_ = p
			_ = ok
		}
	})

	b.Run("get_var", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p, ok := view.Get(21)
			// require.True(b, ok)
			_ = p
			_ = ok
		}
	})

	b.Run("get_var_ptr", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p, _, ok := view.GetPtr(21)
			_ = p
			_ = ok
			// require.True(b, ok)
		}
	})

	b.Run("get_var_string", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p, ok := view.String(21)
			_ = p
			_ = ok
			// require.True(b, ok)
		}
	})
}
