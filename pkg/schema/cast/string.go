// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"encoding"
	"reflect"
)

type Stringer interface {
	String() string
}

// string caster
type StringCaster struct{}

func (c StringCaster) CastValue(val any) (res any, err error) {
	var ok bool
	switch v := val.(type) {
	case string:
		res, ok = []byte(v), true
	case encoding.TextMarshaler:
		buf, err := v.MarshalText()
		if err != nil {
			return nil, err
		}
		res, ok = buf, true
	case Stringer:
		res, ok = []byte(v.String()), true
	case encoding.BinaryMarshaler:
		buf, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		res, ok = buf, true
	default:
		res, ok = []byte(ToString(val)), true
	}
	if !ok {
		err = CastError(val, "string")
	}
	return
}

func (c StringCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case []string:
		cp := make([][]byte, len(v))
		for i := range v {
			cp[i] = []byte(v[i])
		}
		res, ok = cp, true
	default:
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Slice {
			cp := make([][]byte, rv.Len())
			for i := range cp {
				e := rv.Index(i)

				switch vv := e.Interface().(type) {
				case encoding.TextMarshaler:
					buf, err := vv.MarshalText()
					if err != nil {
						return nil, err
					}
					cp[i] = buf
				case Stringer:
					cp[i] = []byte(vv.String())
				case encoding.BinaryMarshaler:
					buf, err := vv.MarshalBinary()
					if err != nil {
						return nil, err
					}
					cp[i] = buf
				default:
					cp[i] = []byte(ToString(vv))
				}
			}
			res, ok = cp, true
		}
	}
	if !ok {
		err = CastError(val, "string")
	}
	return
}
