// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"encoding"
	"encoding/binary"
	"math"
	"reflect"

	"blockwatch.cc/knoxdb/pkg/num"
)

// bytes caster
type BytesCaster struct{}

func (c BytesCaster) CastValue(val any) (res any, err error) {
	var (
		ok bool
		b  [8]byte
	)
	switch v := val.(type) {
	case int:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case int64:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case int32:
		binary.BigEndian.PutUint32(b[:], uint32(v))
		res, ok = b[:4], true
	case int16:
		binary.BigEndian.PutUint16(b[:], uint16(v))
		res, ok = b[:2], true
	case int8:
		res, ok = []byte{byte(v)}, true
	case uint:
		binary.BigEndian.PutUint64(b[:], uint64(v))
		res, ok = b[:], true
	case uint64:
		binary.BigEndian.PutUint64(b[:], v)
		res, ok = b[:], true
	case uint32:
		binary.BigEndian.PutUint32(b[:], v)
		res, ok = b[:4], true
	case uint16:
		binary.BigEndian.PutUint16(b[:], v)
		res, ok = b[:2], true
	case uint8:
		res, ok = []byte{v}, true
	case float64:
		binary.BigEndian.PutUint64(b[:], math.Float64bits(v))
		res, ok = b[:], true
	case float32:
		binary.BigEndian.PutUint32(b[:], math.Float32bits(v))
		res, ok = b[:4], true
	case num.Decimal32:
		binary.BigEndian.PutUint32(b[:], uint32(v.Int32()))
		res, ok = b[:4], true
	case num.Decimal64:
		binary.BigEndian.PutUint64(b[:], uint64(v.Int64()))
		res, ok = b[:], true
	case num.Decimal128:
		b := v.Int128().Bytes16()
		res, ok = b[:], true
	case num.Decimal256:
		b := v.Int256().Bytes32()
		res, ok = b[:], true
	case num.Int128:
		b := v.Bytes16()
		res, ok = b[:], true
	case num.Int256:
		b := v.Bytes32()
		res, ok = b[:], true
	case num.Big:
		res, ok = v.Bytes(), true
	case string:
		res, ok = []byte(v), true
	case []byte:
		res, ok = v, true
	default:
		// binary marshaler
		if v, ok2 := val.(encoding.BinaryMarshaler); ok2 {
			res, err = v.MarshalBinary()
			ok = err == nil
		} else {
			// type aliases
			var vv reflect.Value
			if vv, ok = val.(reflect.Value); !ok {
				vv = reflect.Indirect(reflect.ValueOf(val))
			}
			switch vv.Kind() {
			case reflect.Float32:
				binary.BigEndian.PutUint32(b[:], math.Float32bits(float32(vv.Float())))
				res, ok = b[:4], true
			case reflect.Float64:
				binary.BigEndian.PutUint64(b[:], math.Float64bits(vv.Float()))
				res, ok = b[:], true
			case reflect.Int, reflect.Int64:
				binary.BigEndian.PutUint64(b[:], uint64(vv.Int()))
				res, ok = b[:vv.Type().Size()], true
			case reflect.Int32:
				binary.BigEndian.PutUint32(b[:], uint32(vv.Int()))
				res, ok = b[:4], true
			case reflect.Int16:
				binary.BigEndian.PutUint16(b[:], uint16(vv.Int()))
				res, ok = b[:2], true
			case reflect.Int8:
				res, ok = []byte{byte(vv.Int())}, true
			case reflect.Uint, reflect.Uint64:
				binary.BigEndian.PutUint64(b[:], vv.Uint())
				res, ok = b[:], true
			case reflect.Uint32:
				binary.BigEndian.PutUint32(b[:], uint32(vv.Uint()))
				res, ok = b[:4], true
			case reflect.Uint16:
				binary.BigEndian.PutUint16(b[:], uint16(vv.Uint()))
				res, ok = b[:2], true
			case reflect.Uint8:
				res, ok = []byte{byte(vv.Uint())}, true
			case reflect.String:
				res, ok = []byte(vv.String()), true
			case reflect.Array:
				if vv.Type().Elem().Kind() == reflect.Uint8 {
					if vv.CanAddr() {
						res, ok = vv.Bytes(), true
					} else {
						slice := make([]byte, vv.Len())
						for i := range vv.Len() {
							slice[i] = uint8(vv.Index(i).Uint())
						}
						res, ok = slice, true
					}
				}
			default:
				ok = false
			}
		}
	}
	if !ok {
		err = CastError(val, "byte")
	}
	return
}

func (c BytesCaster) CastSlice(val any) (res any, err error) {
	var ok bool
	var v any
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Slice {
		cp := make([][]byte, rv.Len())
		for i := range cp {
			v, err = c.CastValue(rv.Index(i).Interface())
			if err != nil {
				break
			}
			cp[i] = v.([]byte)
		}
		res, ok = cp, err == nil
	}
	if !ok {
		err = CastError(val, "byte")
	}
	return
}
