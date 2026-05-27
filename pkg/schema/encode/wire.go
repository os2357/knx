// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"encoding/binary"
	"io"
	"reflect"

	"blockwatch.cc/knoxdb/pkg/schema/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

// writeInt writes an integer value to wire format. Accepts all
// integer types as interface an converts them to the wire format
// selected by code.
func writeInt(w io.Writer, code OpCode, val any, layout binary.ByteOrder) (err error) {
	var (
		u64 uint64
		neg bool
	)
	switch v := val.(type) {
	case int:
		u64, neg = uint64(v), v < 0
	case int8:
		u64, neg = uint64(v), v < 0
	case int16:
		u64, neg = uint64(v), v < 0
	case int32:
		u64, neg = uint64(v), v < 0
	case int64:
		u64 = uint64(v)
	case uint:
		u64 = uint64(v)
	case uint8:
		u64 = uint64(v)
	case uint16:
		u64 = uint64(v)
	case uint32:
		u64 = uint64(v)
	case uint64:
		u64 = v
	default:
		return ErrInvalidValueType
	}
	var (
		buf   [8]byte
		over  bool
		width uint
	)
	switch code {
	case OC_I8:
		over = (!neg && u64>>7 > 0) || neg && int64(u64)>>7 != -1
		buf[0] = uint8(u64)
		width = 1
	case OC_U8:
		over = (!neg && u64>>8 > 0) || neg && int64(u64)>>8 != -1
		buf[0] = uint8(u64)
		width = 1
	case OC_I16:
		over = (!neg && u64>>15 > 0) || neg && int64(u64)>>15 != -1
		layout.PutUint16(buf[:], uint16(u64))
		width = 2
	case OC_U16:
		over = (!neg && u64>>16 > 0) || neg && int64(u64)>>16 != -1
		layout.PutUint16(buf[:], uint16(u64))
		width = 2
	case OC_I32:
		over = (!neg && u64>>31 > 0) || neg && int64(u64)>>31 != -1
		layout.PutUint32(buf[:], uint32(u64))
		width = 4
	case OC_U32:
		over = (!neg && u64>>32 > 0) || neg && int64(u64)>>32 != -1
		layout.PutUint32(buf[:], uint32(u64))
		width = 4
	case OC_I64:
		over = (!neg && u64>>63 > 0) || neg && int64(u64)>>63 != -1
		layout.PutUint64(buf[:], u64)
		width = 8
	case OC_U64:
		layout.PutUint64(buf[:], u64)
		width = 8
	}
	if over {
		err = ErrOverflow
	} else {
		_, err = w.Write(buf[:width])
	}
	return
}

// writeBytes writes a fixed or variable length byte slice in wire format.
func writeBytes(w io.Writer, val any, fixed uint8, short bool, layout binary.ByteOrder) (err error) {
	var b []byte
	// type cast values
	switch v := val.(type) {
	case string:
		b = util.UnsafeGetBytes(v)
	case []byte:
		b = v
	default:
		// use reflect for array types
		rv := reflect.Indirect(reflect.ValueOf(val))
		if rv.Type().Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
			b = rv.Bytes()
		} else {
			err = ErrInvalidValueType
		}
	}
	if err != nil {
		return
	}

	// handle fixed values
	switch {
	case fixed > 0:
		if len(b) < int(fixed) {
			return ErrShortValue
		}
		_, err = w.Write(b[:fixed])
	case short:
		if len(b) > types.MAX_BYTES {
			err = ErrLongValue
		} else {
			_, err = w.Write([]byte{byte(len(b))})
		}
		if err == nil {
			_, err = w.Write(b)
		}
	default:
		err = writeU32(w, len(b), layout)
		if err == nil {
			_, err = w.Write(b)
		}
	}

	return
}

// writeBool writes a bool in wire format.
func writeBool(w io.Writer, b bool) (err error) {
	if b {
		_, err = w.Write([]byte{1})
	} else {
		_, err = w.Write([]byte{0})
	}
	return
}

// writeU16 writes a uint16 in given layout to wire.
// used for writing enum codes.
func writeU16[T int | uint16](w io.Writer, v T, layout binary.ByteOrder) error {
	var buf [2]byte
	layout.PutUint16(buf[:], uint16(v))
	_, err := w.Write(buf[:])
	return err
}

// writeU32 writes a uint32 in given layout to wire.
// used for writing string/byte lengths and float32 bits.
func writeU32[T int | uint32](w io.Writer, v T, layout binary.ByteOrder) error {
	var buf [4]byte
	layout.PutUint32(buf[:], uint32(v))
	_, err := w.Write(buf[:])
	return err
}

// writeU64 writes a uint64 in given layout to wire.
// used for writing float64 bits, timestamps.
func writeU64[T int64 | uint64](w io.Writer, v T, layout binary.ByteOrder) error {
	var buf [8]byte
	layout.PutUint64(buf[:], uint64(v))
	_, err := w.Write(buf[:])
	return err
}
