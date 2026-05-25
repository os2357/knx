// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

type DecoderT[T any] struct {
	dec *Decoder
}

func NewDecoderFor[T any](opts ...Option) *DecoderT[T] {
	s, err := sreflect.SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	return &DecoderT[T]{
		dec: NewDecoder(s),
	}
}

func (d *DecoderT[T]) Schema() *Schema {
	return d.dec.schema
}

func (d *DecoderT[T]) Read(r io.Reader) (val *T, err error) {
	val = new(T)
	err = d.dec.Read(r, val)
	return
}

func (d *DecoderT[T]) Decode(buf []byte, val *T) (*T, error) {
	if val == nil {
		val = new(T)
	}
	d.dec.DecodePtr(buf, unsafe.Pointer(val))
	return val, nil
}

func (d *DecoderT[T]) DecodeSlice(buf []byte, res []T) ([]T, error) {
	if res == nil {
		// We slightly over-allocate the result slice when data contains
		// long strings/bytes, however this single allocation is still
		// much more performant than growing the slice multiple times.
		// For fixed-size schemas, a single allocation is all we need.
		res = make([]T, len(buf)/max(d.dec.schema.MinWireSize, 1))
	}
	var n int
	for n = range res {
		if len(buf) == 0 {
			break
		}
		buf = d.dec.DecodePtr(buf, unsafe.Pointer(&res[n]))
	}
	return res[:n], nil
}

type Decoder struct {
	schema  *Schema
	buf     *bytes.Buffer
	layout  binary.ByteOrder
	opcodes []OpCode
}

func NewDecoder(s *Schema) *Decoder {
	// ensure we know the memory layout
	sreflect.AppendFieldLayout(s)
	return &Decoder{
		schema:  s,
		buf:     bytes.NewBuffer(make([]byte, 0, s.MaxWireSize)),
		layout:  binary.NativeEndian,
		opcodes: CompileCodecs(s),
	}
}

func (d *Decoder) Schema() *Schema {
	return d.schema
}

// Read reads wire encoded data from r and decodes into a
// new heap allocated elemen of type T.
//
// When wire size is fixed we can read and decode in one step.
// Otherwise we take a slow path that reads variable length data
// as length fields are encountered. This requires multiple calls
// to the underlying reader.
//
// Reading is staged through an internal decoder buffer
// with an inital size of minWireSize bytes. This buffer gets
// extended whenever a dynamic data type length is found so
// that it contains at least the bytes for the dynamic data
// plus all fixed bytes for following fields a that time.
// Because the buffer may grow and reallocate it is NOT SAFE
// to reference memory for strings and byte slices and hence
// we make explicit copies. Moreover, a copy is necessary to
// safely retain returned objects since the internal buffer is
// re-used between calls.
func (d *Decoder) Read(r io.Reader, val any) error {
	// reset decoder buffer
	d.buf.Reset()

	// read first chunk of data (this is sufficient when schema is fixed size)
	n, err := io.CopyN(d.buf, r, int64(d.schema.MinWireSize))
	if err != nil {
		return err
	}
	if n != int64(d.schema.MinWireSize) {
		return ErrShortBuffer
	}

	// fast path decode fixed size data
	if d.schema.IsFixedSize {
		return d.Decode(d.buf.Bytes(), val)
	}

	// slow path decode with additional read calls (may reallocate buffer!)
	if val == nil {
		return ErrNilValue
	}
	rval := reflect.Indirect(reflect.ValueOf(val))
	base := rval.Addr().UnsafePointer()

	for op, code := range d.opcodes {
		field := d.schema.Fields[op]
		ptr := unsafe.Add(base, field.Offset)
		switch code {
		default:
			// int, uint, float, bool
			_, err = d.buf.Read(unsafe.Slice((*byte)(ptr), field.Size))

		case OC_SKIP:
			// noop

		case OC_FIXBYTES:
			_, err = d.buf.Read(unsafe.Slice((*byte)(ptr), field.Fixed))

		case OC_FIXSTRING:
			// explicit copy
			*(*string)(ptr) = string(d.buf.Next(int(field.Fixed)))

		case OC_STRING:
			l := d.layout.Uint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			// explicit copy
			*(*string)(ptr) = string(d.buf.Next(int(l)))

		case OC_BYTES:
			l := d.layout.Uint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			// explicit copy
			*(*[]byte)(ptr) = bytes.Clone(d.buf.Next(int(l)))

		case OC_TIMESTAMP, OC_TIME, OC_DATE:
			ts := int64(d.layout.Uint64(d.buf.Next(8)))
			*(*time.Time)(ptr) = types.TimeScale(field.Scale).FromUnix(ts)

		case OC_I128:
			*(*num.Int128)(ptr) = num.Int128FromBytes(d.buf.Next(16))

		case OC_I256:
			*(*num.Int256)(ptr) = num.Int256FromBytes(d.buf.Next(32))

		case OC_D32:
			(*(*num.Decimal32)(ptr)).Set(int32(d.layout.Uint32(d.buf.Next(4))))
			(*(*num.Decimal32)(ptr)).SetScale(field.Scale)

		case OC_D64:
			(*(*num.Decimal64)(ptr)).Set(int64(d.layout.Uint64(d.buf.Next(8))))
			(*(*num.Decimal64)(ptr)).SetScale(field.Scale)

		case OC_D128:
			(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(d.buf.Next(16)))
			(*(*num.Decimal128)(ptr)).SetScale(field.Scale)

		case OC_D256:
			(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(d.buf.Next(32)))
			(*(*num.Decimal256)(ptr)).SetScale(field.Scale)

		case OC_ENUM:
			u16 := d.layout.Uint16(d.buf.Next(2))
			if enum := field.Enum; enum != nil {
				val, ok := enum.Value(u16)
				if !ok {
					err = fmt.Errorf("%s: invalid enum value %d", field.Name, u16)
				}
				*(*string)(ptr) = val
			} else {
				err = fmt.Errorf("translation for enum %q not registered", field.Name)
			}
		case OC_BIGINT:
			// read as raw bytes and create num.Big
			l := d.layout.Uint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			err = (*num.Big)(ptr).UnmarshalBinary(d.buf.Next(int(l)))
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) Decode(buf []byte, val any) error {
	if val == nil {
		return ErrNilValue
	}
	rval := reflect.Indirect(reflect.ValueOf(val))
	base := rval.Addr().UnsafePointer()
	d.DecodePtr(buf, base)
	return nil
}

func (d *Decoder) DecodePtr(buf []byte, base unsafe.Pointer) []byte {
	for op, code := range d.opcodes {
		if code == OC_SKIP {
			continue
		}
		field := d.schema.Fields[op]
		ptr := unsafe.Add(base, field.Offset)
		buf = d.readField(code, field, ptr, buf)
	}
	return buf
}

func (d *Decoder) DecodeSlice(buf []byte, slice any) (int, error) {
	if slice == nil {
		return 0, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	base := rslice.UnsafePointer()
	sz := rslice.Type().Elem().Size()
	num := rslice.Len()

	var i int
	for i = 0; i < num && len(buf) > 0; i++ {
		for op, code := range d.opcodes {
			if code == OC_SKIP {
				continue
			}
			field := d.schema.Fields[op]
			ptr := unsafe.Add(base, field.Offset)
			buf = d.readField(code, field, ptr, buf)
		}
		base = unsafe.Add(base, sz)
	}
	return i, nil
}

// reads data for a field in native machine byte order layout
func (d *Decoder) readField(code OpCode, field *Field, ptr unsafe.Pointer, buf []byte) []byte {
	switch code {

	case OC_I64, OC_U64, OC_F64:
		_ = buf[7]
		*(*uint64)(ptr) = *(*uint64)(unsafe.Pointer(&buf[0]))
		buf = buf[8:]

	case OC_I32, OC_U32, OC_F32:
		_ = buf[3]
		*(*uint32)(ptr) = *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]

	case OC_I16, OC_U16:
		_ = buf[1]
		*(*uint16)(ptr) = *(*uint16)(unsafe.Pointer(&buf[0]))
		buf = buf[2:]

	case OC_I8, OC_U8, OC_BOOL:
		_ = buf[0]
		*(*uint8)(ptr) = *(*uint8)(unsafe.Pointer(&buf[0]))
		buf = buf[1:]

	case OC_FIXBYTES:
		_ = buf[field.Fixed-1]
		copy(unsafe.Slice((*byte)(ptr), field.Fixed), buf[:field.Fixed])
		buf = buf[field.Fixed:]

	case OC_FIXSTRING:
		_ = buf[field.Fixed-1]
		*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), field.Fixed)
		buf = buf[field.Fixed:]

	case OC_STRING:
		l := d.layout.Uint32(buf)
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), l)
			buf = buf[l:]
		}

	case OC_BYTES:
		l := d.layout.Uint32(buf)
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			*(*[]byte)(ptr) = buf[:l]
			buf = buf[l:]
		}

	case OC_TIMESTAMP, OC_TIME, OC_DATE:
		ts := int64(d.layout.Uint64(buf))
		*(*time.Time)(ptr) = types.TimeScale(field.Scale).FromUnix(ts)
		buf = buf[8:]

	case OC_I128:
		_ = buf[15]
		*(*num.Int128)(ptr) = num.Int128FromBytes(buf[:16])
		buf = buf[16:]

	case OC_I256:
		_ = buf[31]
		*(*num.Int256)(ptr) = num.Int256FromBytes(buf[:32])
		buf = buf[32:]

	case OC_D32:
		(*(*num.Decimal32)(ptr)).Set(int32(d.layout.Uint32(buf)))
		(*(*num.Decimal32)(ptr)).SetScale(field.Scale)
		buf = buf[4:]

	case OC_D64:
		(*(*num.Decimal64)(ptr)).Set(int64(d.layout.Uint64(buf)))
		(*(*num.Decimal64)(ptr)).SetScale(field.Scale)
		buf = buf[8:]

	case OC_D128:
		_ = buf[15]
		(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(buf[:16]))
		(*(*num.Decimal128)(ptr)).SetScale(field.Scale)
		buf = buf[16:]

	case OC_D256:
		_ = buf[31]
		(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(buf[:32]))
		(*(*num.Decimal256)(ptr)).SetScale(field.Scale)
		buf = buf[32:]

	case OC_ENUM:
		u16 := d.layout.Uint16(buf)
		buf = buf[2:]
		val, ok := field.Enum.Value(u16)
		if !ok {
			panic(fmt.Errorf("field[%s]: invalid enum value %d, have %#v", field.Name, u16, field.Enum))
		}
		*(*string)(ptr) = val // FIXME: may break when enum dict grows

	case OC_BIGINT:
		l := d.layout.Uint32(buf)
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			_ = (*num.Big)(ptr).UnmarshalBinary(buf[:l])
			buf = buf[l:]
		}
	}
	return buf
}
