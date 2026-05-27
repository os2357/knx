// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"errors"
	"reflect"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
)

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Integer interface {
	Signed | Unsigned
}

type Float interface {
	float32 | float64
}

type Number interface {
	Integer | Float
}

func SizeOf[T Integer]() int {
	x := uint16(1 << 8)
	y := uint32(2 << 16)
	z := uint64(4 << 32)
	return 1 + int(T(x))>>8 + int(T(y))>>16 + int(T(z))>>32
}

func SizeFor[T any]() int {
	var t T
	return int(unsafe.Sizeof(t))
}

func UnsafeGetBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func UnsafeGetString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func ToByteSlice[T Number](s []T) []byte {
	return unsafe.Slice(
		(*byte)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)*SizeFor[T](),
	)
}

func FromByteSlice[T Number](s []byte) []T {
	return unsafe.Slice(
		(*T)(unsafe.Pointer(unsafe.SliceData(s))),
		len(s)/SizeFor[T](),
	)
}

func ReinterpretSlice[T, S Number](t []T) []S {
	if SizeFor[T]() == SizeFor[S]() {
		return *(*[]S)(unsafe.Pointer(&t))
	}
	panic(errors.New(
		"cannot reinterprete []" +
			reflect.TypeOf(T(0)).String() +
			" to " +
			reflect.TypeOf(S(0)).String(),
	))
}

func ReinterpretValue[T Number, S Number](t T) S {
	if SizeFor[T]() == SizeFor[S]() {
		return *(*S)(unsafe.Pointer(&t))
	}
	return S(0)
}

func ConvertSlice[T, S arena.Number](t []T) (s []S) {
	s = arena.Alloc[S](len(t))[:len(t)]
	for i, v := range t {
		s[i] = S(v)
	}
	return
}

type eface struct {
	typ unsafe.Pointer
	val unsafe.Pointer
}

func UnboxAny(v any) unsafe.Pointer {
	if v == nil {
		return nil
	}
	ef := (*eface)(unsafe.Pointer(&v))
	return ef.val
}
