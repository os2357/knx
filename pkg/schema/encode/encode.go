// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

type Option = sreflect.Option

var WithEnums = sreflect.WithEnums

type EncoderT[T any] struct {
	enc *Encoder
}

func NewEncoderFor[T any](opts ...Option) *EncoderT[T] {
	s, err := sreflect.SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	return &EncoderT[T]{
		enc: NewEncoder(s),
	}
}

func (e *EncoderT[T]) Schema() *Schema {
	return e.enc.schema
}

func (e *EncoderT[T]) NewBuffer(sz int) *bytes.Buffer {
	return e.enc.schema.NewBuffer(sz)
}

func (e *EncoderT[T]) Encode(val T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.Encode(&val, buf)
}

func (e *EncoderT[T]) EncodePtr(val *T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.Encode(val, buf)
}

func (e *EncoderT[T]) EncodeSlice(slice []T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.EncodeSlice(&slice, buf)
}

func (e *EncoderT[T]) EncodePtrSlice(slice []*T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.EncodeSlice(&slice, buf)
}

type Encoder struct {
	schema  *Schema
	layout  binary.ByteOrder
	opcodes []OpCode
}

func NewEncoder(s *Schema) *Encoder {
	// ensure we know the memory layout
	sreflect.AppendFieldLayout(s)
	return &Encoder{
		schema:  s,
		layout:  binary.NativeEndian,
		opcodes: CompileCodecs(s),
	}
}

func (e *Encoder) Schema() *Schema {
	return e.schema
}

func (e *Encoder) NewBuffer(sz int) *bytes.Buffer {
	return e.schema.NewBuffer(sz)
}

func (e *Encoder) Encode(val any, buf *bytes.Buffer) ([]byte, error) {
	rval := reflect.Indirect(reflect.ValueOf(val))
	if rval.Kind() == reflect.Slice {
		return e.EncodeSlice(val, buf)
	}
	base := rval.Addr().UnsafePointer()
	if buf == nil {
		buf = e.NewBuffer(1)
	}
	var err error
	for op, code := range e.opcodes {
		if code == OC_SKIP {
			continue
		}
		field := e.schema.Fields[op]
		ptr := unsafe.Add(base, field.Offset)
		err = e.writeField(buf, code, field, ptr)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodeSlice(slice any, buf *bytes.Buffer) ([]byte, error) {
	if slice == nil {
		return nil, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if !rslice.IsValid() || rslice.Kind() != reflect.Slice {
		return nil, ErrInvalidValue
	}
	etyp := rslice.Type().Elem()
	if etyp.Kind() == reflect.Pointer {
		return e.EncodePtrSlice(slice, buf)
	}
	sz := etyp.Size()
	base := rslice.UnsafePointer()
	if buf == nil {
		buf = e.NewBuffer(rslice.Len())
	}

	var err error
	for i, l := 0, rslice.Len(); i < l; i++ {
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			field := e.schema.Fields[op]
			ptr := unsafe.Add(base, field.Offset)
			err = e.writeField(buf, code, field, ptr)
			if err != nil {
				return nil, err
			}
		}
		base = unsafe.Add(base, sz)
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodePtrSlice(slice any, buf *bytes.Buffer) ([]byte, error) {
	if slice == nil {
		return nil, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if !rslice.IsValid() ||
		rslice.Kind() != reflect.Slice ||
		rslice.Type().Elem().Kind() != reflect.Pointer {
		return nil, ErrInvalidValue
	}
	if buf == nil {
		buf = e.NewBuffer(rslice.Len())
	}
	var err error
	for i, l := 0, rslice.Len(); i < l; i++ {
		base := rslice.Index(i).UnsafePointer()
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			field := e.schema.Fields[op]
			ptr := unsafe.Add(base, field.Offset)
			err = e.writeField(buf, code, field, ptr)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

// writes data for a field in native machine byte order layout
func (e *Encoder) writeField(buf *bytes.Buffer, code OpCode, field *Field, ptr unsafe.Pointer) (err error) {
	switch code {
	default:
		// int, uint, float, bool
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), field.Size))

	case OC_FIXBYTES:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), field.Fixed))

	case OC_FIXSTRING:
		s := *(*string)(ptr)
		_, err = buf.Write(unsafe.Slice(unsafe.StringData(s), field.Fixed))

	case OC_STRING:
		s := *(*string)(ptr)
		err = writeU32(buf, len(s), e.layout)
		if err == nil {
			_, err = buf.WriteString(s)
		}

	case OC_BYTES:
		b := *(*[]byte)(ptr)
		err = writeU32(buf, len(b), e.layout)
		if err == nil {
			_, err = buf.Write(b)
		}

	case OC_TIMESTAMP, OC_TIME, OC_DATE:
		tm := *(*time.Time)(ptr)
		err = writeU64(buf,
			uint64(types.TimeScale(field.Scale).ToUnix(tm)),
			e.layout,
		)

	case OC_I256:
		v := *(*num.Int256)(ptr)
		_, err = buf.Write(v.Bytes())

	case OC_I128:
		v := *(*num.Int128)(ptr)
		_, err = buf.Write(v.Bytes())

	case OC_D32:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), 4))

	case OC_D64:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), 8))

	case OC_D128:
		v := *(*num.Decimal128)(ptr)
		_, err = buf.Write(v.Int128().Bytes())

	case OC_D256:
		v := *(*num.Decimal256)(ptr)
		_, err = buf.Write(v.Int256().Bytes())

	case OC_ENUM:
		if field.Enum == nil {
			return ErrEnumUndefined
		}
		v := *(*string)(ptr)
		code, ok := field.Enum.Code(v)
		if !ok {
			err = fmt.Errorf("%s: invalid enum value %q", field.Name, v)
		} else {
			err = writeU16(buf, code, e.layout)
		}

	case OC_BIGINT:
		v := *(*num.Big)(ptr)
		b := v.Bytes()
		err = writeU32(buf, len(b), e.layout)
		if err == nil {
			_, err = buf.Write(b)
		}
	}
	return
}
