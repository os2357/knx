// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"iter"
	"reflect"
)

type anySlice struct {
	slice any
}

func Any(v any) anySlice {
	return anySlice{v}
}

func (a anySlice) Len() int {
	rv := reflect.ValueOf(a.slice)
	if rv.Kind() != reflect.Slice {
		return 0
	}
	return rv.Len()
}

func (a anySlice) Index(i int) any {
	rv := reflect.ValueOf(a.slice)
	if rv.Kind() != reflect.Slice {
		return nil
	}
	return rv.Index(i).Interface()
}

func (a anySlice) Iterator() iter.Seq2[int, any] {
	return func(fn func(int, any) bool) {
		rv := reflect.ValueOf(a.slice)
		if rv.Kind() != reflect.Slice {
			return
		}
		for i := range rv.Len() {
			if !fn(i, rv.Index(i).Interface()) {
				return
			}
		}
	}
}

func (a anySlice) Slice() []any {
	rv := reflect.ValueOf(a.slice)
	if rv.Kind() != reflect.Slice {
		return nil
	}
	dst := make([]any, rv.Len())
	for i := range rv.Len() {
		dst[i] = rv.Index(i).Interface()
	}
	return dst
}

// MakeAny unboxes vals from interface to their actual type T
// and returns a typed slice []T.
func MakeAny(vals ...any) any {
	if len(vals) == 0 {
		return nil
	}
	slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(vals[0])), 0, len(vals))
	for _, v := range vals {
		slice = reflect.Append(slice, reflect.ValueOf(v))
	}
	return slice.Interface()
}
