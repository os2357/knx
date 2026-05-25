// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"reflect"

	"blockwatch.cc/knoxdb/pkg/num"
)

// int256 caster
type I256Caster struct{}

func (c I256Caster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = num.Int256FromInt64(int64(v)), true
	case int64:
		res, ok = num.Int256FromInt64(v), true
	case int32:
		res, ok = num.Int256FromInt64(int64(v)), true
	case int16:
		res, ok = num.Int256FromInt64(int64(v)), true
	case int8:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint64:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint32:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint16:
		res, ok = num.Int256FromInt64(int64(v)), true
	case uint8:
		res, ok = num.Int256FromInt64(int64(v)), true
	case float32:
		var i256 num.Int256
		i256.SetFloat64(float64(v))
		res, ok = i256, true
	case float64:
		var i256 num.Int256
		i256.SetFloat64(v)
		res, ok = i256, true
	case num.Decimal32:
		res, ok = num.Int256FromInt64(v.Int64()), true
	case num.Decimal64:
		res, ok = num.Int256FromInt64(v.Int64()), true
	case num.Decimal128:
		res, ok = v.Int256(), true
	case num.Decimal256:
		res, ok = v.Int256(), true
	case num.Int128:
		res, ok = v.Int256(), true
	case num.Int256:
		res, ok = v, true
	case num.Big:
		res, ok = v.AsInt256(), v.Big().BitLen() <= 256
	default:
		var vv reflect.Value
		if vv, ok = val.(reflect.Value); !ok {
			vv = reflect.Indirect(reflect.ValueOf(val))
		}
		switch vv.Kind() {
		case reflect.Float32:
			var i256 num.Int256
			i256.SetFloat64(float64(vv.Float()))
			res, ok = i256, true
		case reflect.Float64:
			var i256 num.Int256
			i256.SetFloat64(vv.Float())
			res, ok = i256, true
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			res, ok = num.Int256FromInt64(vv.Int()), true
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			res, ok = num.Int256FromInt64(int64(vv.Uint())), true
		default:
			ok = false
		}
	}
	if !ok {
		err = CastError(val, "int256")
	}
	return
}

func (c I256Caster) CastSlice(val any) (res any, err error) {
	var ok bool
	var v any
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([]num.Int256, rv.Len())
		for i := range cp {
			v, err = c.CastValue(rv.Index(i).Interface())
			if err != nil {
				break
			}
			cp[i] = v.(num.Int256)
		}
		res, ok = cp, err == nil
	}
	if !ok {
		err = CastError(val, "int256")
	}
	return
}
