// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"reflect"

	"blockwatch.cc/knoxdb/pkg/num"
)

// int128 caster
type I128Caster struct{}

func (c I128Caster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = num.Int128FromInt64(int64(v)), true
	case int64:
		res, ok = num.Int128FromInt64(v), true
	case int32:
		res, ok = num.Int128FromInt64(int64(v)), true
	case int16:
		res, ok = num.Int128FromInt64(int64(v)), true
	case int8:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint64:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint32:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint16:
		res, ok = num.Int128FromInt64(int64(v)), true
	case uint8:
		res, ok = num.Int128FromInt64(int64(v)), true
	case float32:
		var i128 num.Int128
		acc := i128.SetFloat64(float64(v))
		res, ok = i128, acc == num.Exact
	case float64:
		var i128 num.Int128
		acc := i128.SetFloat64(v)
		res, ok = i128, acc == num.Exact
	case num.Decimal32:
		res, ok = num.Int128FromInt64(v.Int64()), true
	case num.Decimal64:
		res, ok = num.Int128FromInt64(v.Int64()), true
	case num.Decimal128:
		res, ok = v.Int128(), true
	case num.Decimal256:
		res, ok = v.Int128(), v.Int256().IsInt128()
	case num.Int128:
		res, ok = v, true
	case num.Int256:
		res, ok = v.Int128(), v.IsInt128()
	case num.Big:
		res, ok = v.AsInt128(), v.Big().BitLen() <= 128
	}
	if !ok {
		err = CastError(val, "int128")
	}
	return
}

func (c I128Caster) CastSlice(val any) (res any, err error) {
	var ok bool
	var v any
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([]num.Int128, rv.Len())
		for i := range cp {
			v, err = c.CastValue(rv.Index(i).Interface())
			if err != nil {
				break
			}
			cp[i] = v.(num.Int128)
		}
		res, ok = cp, err == nil
	}
	if !ok {
		err = CastError(val, "int128")
	}
	return
}
