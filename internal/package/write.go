// Copyright (c) 2014 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"encoding"
	"fmt"
	"reflect"
	"runtime/debug"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// AppendWire appends a new row of values from a wire protocol message. The caller must
// ensure the message matches the currrent package schema.
func (p *Package) AppendWire(buf []byte) error {
	for i, field := range p.schema.Exported() {
		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}
		// skipped and new blocks in old packages are missing
		b := p.blocks[i]
		if b == nil {
			continue
		}
		switch field.Type {
		case FieldTypeUint64, FieldTypeInt64, FieldTypeDatetime, FieldTypeFloat64, FieldTypeDecimal64:
			b.Uint64().Append(*(*uint64)(unsafe.Pointer(&buf[0])))
			buf = buf[8:]

		case FieldTypeUint32, FieldTypeInt32, FieldTypeFloat32, FieldTypeDecimal32:
			b.Uint32().Append(*(*uint32)(unsafe.Pointer(&buf[0])))
			buf = buf[4:]

		case FieldTypeUint16, FieldTypeInt16:
			b.Uint16().Append(*(*uint16)(unsafe.Pointer(&buf[0])))
			buf = buf[2:]

		case FieldTypeUint8, FieldTypeInt8:
			b.Uint8().Append(*(*uint8)(unsafe.Pointer(&buf[0])))
			buf = buf[1:]

		case FieldTypeBoolean:
			if *(*bool)(unsafe.Pointer(&buf[0])) {
				b.Bool().Set(p.nRows)
			}
			buf = buf[1:]

		case FieldTypeString, FieldTypeBytes:
			if fixed := field.Fixed; fixed > 0 {
				b.Bytes().Append(buf[:fixed])
				buf = buf[fixed:]
			} else {
				l, n := schema.ReadUint32(buf)
				buf = buf[n:]
				b.Bytes().Append(buf[:l])
				buf = buf[l:]
			}

		case FieldTypeInt256, FieldTypeDecimal256:
			b.Int256().Append(num.Int256FromBytes(buf[:32]))
			buf = buf[32:]

		case FieldTypeInt128, FieldTypeDecimal128:
			b.Int128().Append(num.Int128FromBytes(buf[:16]))
			buf = buf[16:]

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled field type",
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name(),
				"version", p.schema.Version(),
			)
		}
		b.SetDirty()
	}
	p.nRows++
	p.dirty = true
	return nil
}

// AppendStruct appends a new row of values from a Go struct. The caller must
// ensure strict type match as no additional check, cast or conversion is done.
//
// Column mapping uses the default struct tag `knox`.
// Only flat structs are supported (no anonymous nesting)
func (p *Package) AppendStruct(val any) error {
	assert.Always(p.CanGrow(1), "pack: overflow on push",
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
	)

	// TODO: move all sanity checks to higher layer that deals with user input, e.g. journal
	// if p.schema.IsInterface() {
	rval := reflect.Indirect(reflect.ValueOf(val))
	if !rval.IsValid() || rval.Kind() != reflect.Struct {
		return fmt.Errorf("push: pushed invalid %s value of type %T", rval.Kind(), val)
	}
	// expensive check and val's Go type may not yet be known
	// if s, ok := schema.LookupSchema(rval.Type()); ok {
	// 	if p.schema.Hash() != s.Hash() {
	// 		return fmt.Errorf("push: incompatible value %T for schema %s", val, p.schema.Name())
	// 	}
	// }
	base := rval.Addr().UnsafePointer()
	for i, field := range p.schema.Exported() {
		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}

		// skipped and new blocks in old packages are missing
		b := p.blocks[i]
		if b == nil {
			continue
		}

		// use unsafe.Add instead of reflect (except marshal types)
		fptr := unsafe.Add(base, field.Offset)

		switch field.Type {
		case FieldTypeInt64, FieldTypeUint64, FieldTypeFloat64:
			b.Uint64().Append(*(*uint64)(fptr))

		case FieldTypeInt32, FieldTypeUint32, FieldTypeFloat32:
			b.Uint32().Append(*(*uint32)(fptr))

		case FieldTypeInt16, FieldTypeUint16:
			b.Uint16().Append(*(*uint16)(fptr))

		case FieldTypeInt8, FieldTypeUint8:
			b.Uint8().Append(*(*uint8)(fptr))

		case FieldTypeDatetime:
			b.Int64().Append((*(*time.Time)(fptr)).UnixNano())

		case FieldTypeBoolean:
			if *(*bool)(fptr) {
				b.Bool().Set(p.nRows)
			}

		case FieldTypeBytes:
			switch {
			case field.Iface&schema.IfaceBinaryMarshaler > 0:
				rfield := field.StructValue(rval)
				buf, err := rfield.Interface().(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					return err
				}
				b.Bytes().AppendZeroCopy(buf)
			case field.IsArray:
				b.Bytes().AppendZeroCopy(unsafe.Slice((*byte)(fptr), field.Fixed))
			default:
				b.Bytes().Append(*(*[]byte)(fptr))
			}

		case FieldTypeString:
			switch {
			case field.Iface&schema.IfaceTextMarshaler > 0:
				rfield := field.StructValue(rval)
				buf, err := rfield.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return err
				}
				b.Bytes().AppendZeroCopy(buf)
			case field.Iface&schema.IfaceStringer > 0:
				rfield := field.StructValue(rval)
				s := rfield.Interface().(fmt.Stringer).String()
				b.Bytes().AppendZeroCopy(UnsafeGetBytes(s))
			default:
				s := *(*string)(fptr)
				b.Bytes().Append(unsafe.Slice(unsafe.StringData(s), len(s)))
			}

		case FieldTypeInt256:
			b.Int256().Append((*(*num.Int256)(fptr)))

		case FieldTypeInt128:
			b.Int128().Append((*(*num.Int128)(fptr)))

		case FieldTypeDecimal256:
			b.Int256().Append((*(*num.Decimal256)(fptr)).Quantize(field.Scale).Int256())

		case FieldTypeDecimal128:
			b.Int128().Append((*(*num.Decimal128)(fptr)).Quantize(field.Scale).Int128())

		case FieldTypeDecimal64:
			b.Int64().Append((*(*num.Decimal64)(fptr)).Quantize(field.Scale).Int64())

		case FieldTypeDecimal32:
			b.Int32().Append((*(*num.Decimal32)(fptr)).Quantize(field.Scale).Int32())

		default:
			// oh, its a type we don't support yet
			assert.Unreachable("unhandled value type",
				// "rtype":   rfield.Type().String(),
				// "rkind":   rfield.Kind().String(),
				"field", field.Name,
				"type", field.Type.String(),
				"pack", p.key,
				"schema", p.schema.Name(),
				"version", p.schema.Version(),
			)
		}
		b.SetDirty()
	}
	p.nRows++
	p.dirty = true
	return nil
}

// SetValue overwrites a single value at a given col/row offset. The caller must
// ensure strict type match as no additional check, cast or conversion is done.
//
// Replaces for SetFieldAt() used in journal and join
func (p *Package) SetValue(col, row int, val any) error {
	f, ok := p.schema.FieldById(uint16(col))
	assert.Always(ok, "invalid field id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	assert.Always(f.IsVisible(), "field is invisble",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
	)
	assert.Always(col >= 0 && col < len(p.blocks), "invalid block id",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	assert.Always(row >= 0 && row < p.nRows, "invalid row",
		"row", row,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
	)

	b := p.blocks[col]
	assert.Always(b != nil, "nil block",
		"id", col,
		"pack", p.key,
		"schema", p.schema.Name(),
		"version", p.schema.Version(),
		"nFields", p.schema.NumFields(),
		"nBlocks", len(p.blocks),
	)
	if b == nil {
		return nil
	}

	// try direct types first
	switch v := val.(type) {
	case int64:
		b.Int64().Set(row, v)
	case int32:
		b.Int32().Set(row, v)
	case int16:
		b.Int16().Set(row, v)
	case int8:
		b.Int8().Set(row, v)
	case int:
		b.Int64().Set(row, int64(v))
	case uint64:
		b.Uint64().Set(row, v)
	case uint32:
		b.Uint32().Set(row, v)
	case uint16:
		b.Uint16().Set(row, v)
	case uint8:
		b.Uint8().Set(row, v)
	case uint:
		b.Uint64().Set(row, uint64(v))
	case float64:
		b.Float64().Set(row, v)
	case float32:
		b.Float32().Set(row, v)
	case time.Time:
		if v.IsZero() {
			b.Int64().Set(row, 0)
		} else {
			b.Int64().Set(row, v.UnixNano())
		}
	case bool:
		if v {
			b.Bool().Set(row)
		} else {
			b.Bool().Clear(row)
		}
	case string:
		b.Bytes().Set(row, UnsafeGetBytes(v))
	case []byte:
		b.Bytes().Set(row, v)
	case num.Int256:
		b.Int256().Set(row, v)
	case num.Int128:
		b.Int128().Set(row, v)
	case num.Decimal256:
		// re-quantize nums to allow table joins, etc
		b.Int256().Set(row, v.Quantize(f.Scale()).Int256())
	case num.Decimal128:
		b.Int128().Set(row, v.Quantize(f.Scale()).Int128())
	case num.Decimal64:
		b.Int64().Set(row, v.Quantize(f.Scale()).Int64())
	case num.Decimal32:
		b.Int32().Set(row, v.Quantize(f.Scale()).Int32())
	default:
		// fallback to reflect for enum types
		rval := reflect.Indirect(reflect.ValueOf(val))
		switch rval.Type().Kind() {
		case reflect.Uint8:
			b.Uint8().Set(row, uint8(rval.Uint()))
		case reflect.Uint16:
			b.Uint16().Set(row, uint16(rval.Uint()))
		case reflect.Int8:
			b.Int8().Set(row, int8(rval.Int()))
		case reflect.Int16:
			b.Int16().Set(row, int16(rval.Int()))
		case reflect.Uint32:
			b.Uint32().Set(row, uint32(rval.Uint()))
		case reflect.Int32:
			b.Int32().Set(row, int32(rval.Int()))
		case reflect.Uint, reflect.Uint64:
			b.Uint64().Set(row, rval.Uint())
		case reflect.Int, reflect.Int64:
			b.Int64().Set(row, rval.Int())
		default:
			// for all other types, check if they implement marshalers
			// this is unlikely due to the internal use of this feature
			// but allows for future extension of DB internals like
			// aggregators, reducers, etc
			switch {
			case f.Can(schema.IfaceBinaryMarshaler):
				buf, err := val.(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
						f.Type(), f.Name(), err)
				}
				b.Bytes().SetZeroCopy(row, buf)
			case f.Can(schema.IfaceTextMarshaler):
				buf, err := val.(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
						f.Type(), f.Name(), err)
				}
				b.Bytes().SetZeroCopy(row, buf)
			case f.Can(schema.IfaceStringer):
				b.Bytes().SetZeroCopy(row, UnsafeGetBytes(val.((fmt.Stringer)).String()))
			default:
				// oh, its a type we don't support yet
				assert.Unreachable("unhandled value type",
					"rtype", rval.Type().String(),
					"rkind", rval.Kind().String(),
					"field", f.Name(),
					"type", f.Type().String(),
					"pack", p.key,
					"schema", p.schema.Name(),
					"version", p.schema.Version(),
				)
			}
		}
	}
	p.dirty = true
	return nil
}

// SetRow overwrites an entire row at a given offset. The caller must
// ensure strict type match as no additional check, cast or conversion is done.
// Column mapping uses the default struct tag `knox`.
//
// Replaces for ReplaceAt() used in journal
func (p *Package) SetRow(row int, val any) error {
	rval := reflect.Indirect(reflect.ValueOf(val))

	if !rval.IsValid() || rval.Kind() != reflect.Struct {
		return fmt.Errorf("set: pushed invalid %s value of type %T", rval.Kind(), val)
	}

	// expensive check and val's Go type may not yet be known
	if s, ok := schema.LookupSchema(rval.Type()); ok {
		if p.schema.Hash() != s.Hash() {
			return fmt.Errorf("push: incompatible value %T for schema %s", val, p.schema.Name())
		}
	}

	for i, field := range p.schema.Exported() {
		// deleted and internal fields are invisible
		if !field.IsVisible {
			continue
		}
		// skipped and new blocks in old packages are missing
		b := p.blocks[i]
		if b == nil {
			continue
		}
		// lookup struct field
		rfield := field.StructValue(rval)

		// try direct types first
		switch rfield.Kind() {
		case reflect.Int, reflect.Int64:
			b.Int64().Set(row, rfield.Int())
		case reflect.Int32:
			b.Int32().Set(row, int32(rfield.Int()))
		case reflect.Int16:
			b.Int16().Set(row, int16(rfield.Int()))
		case reflect.Int8:
			b.Int8().Set(row, int8(rfield.Int()))
		case reflect.Uint64, reflect.Uint:
			b.Uint64().Set(row, rfield.Uint())
		case reflect.Uint32:
			b.Uint32().Set(row, uint32(rfield.Uint()))
		case reflect.Uint16:
			b.Uint16().Set(row, uint16(rfield.Uint()))
		case reflect.Uint8:
			b.Uint8().Set(row, uint8(rfield.Uint()))
		case reflect.Float64:
			b.Float64().Set(row, rfield.Float())
		case reflect.Float32:
			b.Float32().Set(row, float32(rfield.Float()))
		case reflect.Bool:
			if rfield.Bool() {
				b.Bool().Set(row)
			} else {
				b.Bool().Clear(row)
			}
		case reflect.String:
			b.Bytes().Set(row, UnsafeGetBytes(rfield.String()))
		case reflect.Slice:
			switch {
			case field.Iface&schema.IfaceBinaryMarshaler > 0:
				buf, err := rfield.Interface().(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
						rfield.Type(), field.Name, err)
				}
				b.Bytes().SetZeroCopy(row, buf)
			case field.Iface&schema.IfaceTextMarshaler > 0:
				buf, err := rfield.Interface().(encoding.TextMarshaler).MarshalText()
				if err != nil {
					return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
						field.Type, field.Name, err)
				}
				b.Bytes().SetZeroCopy(row, buf)
			default:
				b.Bytes().Set(row, rfield.Bytes())
			}

		case reflect.Array:
			switch rfield.Type().String() {
			case "num.Int256":
				b.Int256().Set(row, *(*num.Int256)(rfield.Addr().UnsafePointer()))
			case "num.Int128":
				b.Int128().Set(row, *(*num.Int128)(rfield.Addr().UnsafePointer()))
			default:
				switch {
				case field.Iface&schema.IfaceBinaryMarshaler > 0:
					buf, err := rfield.Interface().(encoding.BinaryMarshaler).MarshalBinary()
					if err != nil {
						return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
							field.Type, field.Name, err)
					}
					b.Bytes().SetZeroCopy(row, buf)
				case field.Iface&schema.IfaceTextMarshaler > 0:
					buf, err := rfield.Interface().(encoding.TextMarshaler).MarshalText()
					if err != nil {
						return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
							field.Type, field.Name, err)
					}
					b.Bytes().SetZeroCopy(row, buf)
				case rfield.Type().Elem().Kind() == reflect.Uint8:
					b.Bytes().SetZeroCopy(row, rfield.Bytes())
				default:
					// oh, its a type we don't support yet
					assert.Unreachable("unhandled array type",
						"rtype", rfield.Type().String(),
						"rkind", rfield.Kind().String(),
						"field", field.Name,
						"type", field.Type.String(),
						"pack", p.key,
						"schema", p.schema.Name(),
						"version", p.schema.Version(),
					)
				}
			}

		case reflect.Struct:
			switch rfield.Type().String() {
			case "time.Time":
				if tm := *(*time.Time)(rfield.Addr().UnsafePointer()); tm.IsZero() {
					b.Int64().Set(row, 0)
				} else {
					b.Int64().Set(row, tm.UnixNano())
				}
			case "num.Decimal256":
				b.Int256().Set(row, (*(*num.Decimal256)(rfield.Addr().UnsafePointer())).Quantize(field.Scale).Int256())
			case "num.Decimal128":
				b.Int128().Set(row, (*(*num.Decimal128)(rfield.Addr().UnsafePointer())).Quantize(field.Scale).Int128())
			case "num.Decimal64":
				b.Int64().Set(row, (*(*num.Decimal64)(rfield.Addr().UnsafePointer())).Quantize(field.Scale).Int64())
			case "num.Decimal32":
				b.Int32().Set(row, (*(*num.Decimal32)(rfield.Addr().UnsafePointer())).Quantize(field.Scale).Int32())
			default:
				if field.Iface&schema.IfaceBinaryMarshaler > 0 {
					buf, err := rfield.Interface().(encoding.BinaryMarshaler).MarshalBinary()
					if err != nil {
						return fmt.Errorf("set_value: marshal failed on %s field %s: %v",
							field.Type, field.Name, err)
					}
					b.Bytes().SetZeroCopy(row, buf)
				} else {
					// oh, its a type we don't support yet
					assert.Unreachable("unhandled struct type",
						"rtype", rfield.Type().String(),
						"rkind", rfield.Kind().String(),
						"field", field.Name,
						"type", field.Type.String(),
						"pack", p.key,
						"schema", p.schema.Name(),
						"version", p.schema.Version(),
					)
				}
			}
		}
	}
	p.dirty = true
	return nil
}

// Replace replaces at most n rows in the current package starting at
// offset `to` with rows from `spack` starting at offset `from`.
// Both packages must have same schema and block order.
func (p *Package) Replace(spack *Package, to, from, n int) error {
	if spack.schema.Hash() != p.schema.Hash() {
		return fmt.Errorf("replace: schema mismatch src=%s dst=%s", spack.schema.Name(), p.schema.Name())
	}
	if spack.nRows <= from {
		return fmt.Errorf("replace: invalid src offset=%d rows=%d", from, spack.nRows)
	}
	if spack.nRows <= from+n-1 {
		return fmt.Errorf("replace: src overflow from+n=%d rows=%d", from+n, spack.nRows)
	}
	if p.nRows <= to {
		return fmt.Errorf("replace: invalid dst offset=%d rows=%d", to, p.nRows)
	}
	if p.nRows <= to+n {
		return fmt.Errorf("replace: dst overflow to+n=%d rows=%d", to+n, p.nRows)
	}
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Replace: %v\n", e)
			fmt.Printf("SRC: id=%d vals=%d pklen=%d\n", spack.key, spack.nRows, len(spack.PkColumn()))
			fmt.Printf("DST: id=%d vals=%d pklen=%d\n", p.key, p.nRows, len(p.PkColumn()))
			fmt.Printf("REQ: dst-to=%d src-from=%d slen=%d\n", to, from, n)
			fmt.Printf("%s\n", string(debug.Stack()))
			panic(e)
		}
	}()
	// copy at most N rows without overflowing dst
	n = Min(p.nRows-to, n)
	for i, b := range p.blocks {
		b.ReplaceBlock(spack.blocks[i], from, to, n)
	}
	p.dirty = true
	return nil
}

// Append copies `n` rows from `spack` starting at offset `from` to the end of
// the package. Both packages must have same schema and block order.
func (p *Package) AppendPack(spack *Package, from, n int) error {
	if spack.schema.Hash() != p.schema.Hash() {
		return fmt.Errorf("append: schema mismatch src=%s dst=%s", spack.schema.Name(), p.schema.Name())
	}
	if spack.nRows <= from {
		return fmt.Errorf("append: invalid src offset=%d rows=%d", from, spack.nRows)
	}
	if spack.nRows <= from+n-1 {
		return fmt.Errorf("append: src overflow from+n=%d rows=%d", from+n, spack.nRows)
	}
	assert.Always(p.CanGrow(n), "pack: overflow on append",
		"rows", n,
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
		"blockLen", p.blocks[0].Len(),
		"blockCap", p.blocks[0].Cap(),
	)
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Append: %v\n", e)
			fmt.Printf("SRC: id=%d vals=%d pklen=%d\n", spack.key, spack.nRows, len(spack.PkColumn()))
			fmt.Printf("DST: id=%d vals=%d pklen=%d\n", p.key, p.nRows, len(p.PkColumn()))
			fmt.Printf("REQ: src-from=%d slen=%d\n", from, n)
			fmt.Printf("%s\n", string(debug.Stack()))
			panic(e)
		}
	}()
	for i, b := range p.blocks {
		b.AppendBlock(spack.blocks[i], from, n)
	}
	p.nRows += n
	p.dirty = true
	return nil
}

// Insert copies `n` rows from `spack` starting at offset `from` into the
// current package from position `to`. Both packages must have same schema
// and block order.
func (p *Package) InsertPack(spack *Package, to, from, n int) error {
	if spack.schema.Hash() != p.schema.Hash() {
		return fmt.Errorf("insert: schema mismatch src=%s dst=%s", spack.schema.Name(), p.schema.Name())
	}
	if spack.nRows <= from {
		return fmt.Errorf("insert: invalid src offset=%d rows=%d", from, spack.nRows)
	}
	if spack.nRows <= from+n-1 {
		return fmt.Errorf("insert: src overflow from+n=%d rows=%d", from+n, spack.nRows)
	}
	assert.Always(p.CanGrow(n), "pack: overflow on insert",
		"rows", n,
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
		"blockLen", p.blocks[0].Len(),
		"blockCap", p.blocks[0].Cap(),
	)
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("Insert: %v\n", e)
			fmt.Printf("SRC: id=%d vals=%d pklen=%d\n", spack.key, spack.nRows, len(spack.PkColumn()))
			fmt.Printf("DST: id=%d vals=%d pklen=%d\n", p.key, p.nRows, len(p.PkColumn()))
			fmt.Printf("REQ: dst-to=%d src-from=%d slen=%d\n", to, from, n)
			fmt.Printf("%s\n", string(debug.Stack()))
			panic(e)
		}
	}()
	for i, b := range p.blocks {
		b.InsertBlock(spack.blocks[i], from, to, n)
	}
	p.nRows += n
	p.dirty = true
	return nil
}

// Grow appends a new rows with zero values to all underlying blocks.
func (p *Package) Grow(n int) error {
	if n <= 0 {
		return nil
	}
	assert.Always(p.CanGrow(n), "pack: overflow on grow",
		"rows", n,
		"pack", p.key,
		"len", p.nRows,
		"cap", p.maxRows,
		"blockLen", p.blocks[0].Len(),
		"blockCap", p.blocks[0].Cap(),
	)
	for _, b := range p.blocks {
		b.Grow(n)
	}
	p.nRows += n
	p.dirty = true
	return nil
}

func (p *Package) Delete(start, n int) error {
	if start <= 0 || n <= 0 {
		return nil
	}
	// n = min(p.rows-start, n)
	if p.nRows <= start+n {
		return fmt.Errorf("delete: invalid range [%d:%d] (max %d)", start, start+n, p.nRows)
	}
	for _, b := range p.blocks {
		b.Delete(start, n)
	}
	p.nRows -= n
	p.dirty = true
	return nil
}
