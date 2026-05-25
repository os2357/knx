// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"math/big"
	"reflect"

	"blockwatch.cc/knoxdb/pkg/num"
)

// num.Big caster
type BigIntCaster struct{}

func (c BigIntCaster) CastValue(val any) (res any, err error) {
	var ok bool
	res = val
	switch v := val.(type) {
	case int:
		res, ok = num.NewBig(int64(v)), true
	case int64:
		res, ok = num.NewBig(v), true
	case int32:
		res, ok = num.NewBig(int64(v)), true
	case int16:
		res, ok = num.NewBig(int64(v)), true
	case int8:
		res, ok = num.NewBig(int64(v)), true
	case uint:
		res, ok = num.NewBig(int64(v)), true
	case uint64:
		res, ok = num.NewFromBigInt(new(big.Int).SetUint64(v)), true
	case uint32:
		res, ok = num.NewBig(int64(v)), true
	case uint16:
		res, ok = num.NewBig(int64(v)), true
	case uint8:
		res, ok = num.NewBig(int64(v)), true
	case float32:
		res, ok = num.NewBig(int64(v)), true
	case float64:
		res, ok = num.NewBig(int64(v)), true
	case num.Decimal32:
		res, ok = num.NewBig(v.Int64()), true
	case num.Decimal64:
		res, ok = num.NewBig(v.Int64()), true
	case num.Decimal128:
		res, ok = v.Int128().AsBigInt(), true
	case num.Decimal256:
		res, ok = v.Int256().AsBigInt(), true
	case num.Int128:
		res, ok = v.AsBigInt(), true
	case num.Int256:
		res, ok = v.AsBigInt(), true
	case num.Big:
		res, ok = v, true
	default:
		var vv reflect.Value
		if vv, ok = val.(reflect.Value); !ok {
			vv = reflect.Indirect(reflect.ValueOf(val))
		}
		switch vv.Kind() {
		case reflect.Float32:
			res, ok = num.NewBig(int64(vv.Float())), true
		case reflect.Float64:
			res, ok = num.NewBig(int64(vv.Float())), true
		case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
			res, ok = num.NewBig(vv.Int()), true
		case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
			res, ok = num.NewFromBigInt(new(big.Int).SetUint64(vv.Uint())), true
		default:
			ok = false
		}
	}
	if !ok {
		err = CastError(val, "bigint")
	}
	return
}

func (c BigIntCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	var v any
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([]num.Big, rv.Len())
		for i := range cp {
			v, err = c.CastValue(rv.Index(i).Interface())
			if err != nil {
				break
			}
			cp[i] = v.(num.Big)
		}
		res, ok = cp, err == nil
	}
	if !ok {
		err = CastError(val, "bigint")
	}
	return
}
