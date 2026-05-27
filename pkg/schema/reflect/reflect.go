// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

var (
	ErrNilValue        = errors.New("schema: nil value")
	ErrUnsupportedType = errors.New("schema: unsupported type")
)

const TAG_NAME = "knox"

var schemaRegistry sync.Map

type Option func(*schema.Schema)

func WithEnums(r *enum.EnumRegistry) Option {
	return func(s *schema.Schema) {
		s.WithEnums(r)
	}
}

func LookupSchema(typ reflect.Type) (*schema.Schema, bool) {
	sval, ok := schemaRegistry.Load(typ)
	if ok {
		return sval.(*schema.Schema), ok
	}
	return nil, ok
}

func SchemaFor[T any](opts ...Option) (*schema.Schema, error) {
	var m T
	return SchemaOf(m, opts...)
}

func MustSchemaFor[T any](opts ...Option) *schema.Schema {
	s, err := SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	return s
}

func SchemaOf(m any, opts ...Option) (*schema.Schema, error) {
	return SchemaOfTag(m, TAG_NAME, opts...)
}

func MustSchemaOf(m any, opts ...Option) *schema.Schema {
	s, err := SchemaOf(m, opts...)
	if err != nil {
		panic(err)
	}
	return s
}

func SchemaOfTag(m any, tag string, opts ...Option) (*schema.Schema, error) {
	// interface must not be nil
	if m == nil {
		return nil, ErrNilValue
	}

	// validate type
	val := reflect.Indirect(reflect.ValueOf(m))
	if !val.IsValid() {
		return nil, fmt.Errorf("invalid value of type %T", m)
	}

	// must be a struct, pointer to struct or slice of struct
	typ := val.Type()
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("type %s (%s) is not a struct", typ, typ.Kind())
	}

	// lookup registry
	sval, ok := schemaRegistry.Load(typ)
	if ok {
		return sval.(*schema.Schema), nil
	}

	// create new schema
	s := &schema.Schema{
		Name:        fromCamelCase(typ.Name(), "_"),
		Fields:      make([]*schema.Field, 0),
		IsFixedSize: true,
		Version:     1,
	}

	// use table name when type implements the Model interface
	if typ.Implements(modelType) {
		if n := val.Interface().(Model).Key(); len(n) > 0 {
			s.Name = n
		}
	}

	for _, f := range reflect.VisibleFields(typ) {
		// skip private fields and embedded structs, promoted embedded fields
		// fields are still processed, only the anon struct itself is skipped
		if !f.IsExported() || f.Anonymous || f.Tag.Get(tag) == "-" {
			continue
		}

		// skip empty structs (used to define composite indexes)
		if f.Type == emptyType {
			continue
		}

		// analyze field
		field, err := reflectStructField(f, tag)
		if err != nil {
			return nil, err
		}

		// assign id starting at 1, allow pre-assigned ids
		if field.Id == 0 {
			field.Id = uint16(len(s.Fields)) + 1
		}
		s.Fields = append(s.Fields, field)
	}

	// detect indexes
	idxs, err := IndexesOfTag(reflect.New(typ).Interface(), tag, s)
	if err != nil {
		return nil, err
	}
	s.Indexes = idxs

	// apply options
	for _, o := range opts {
		o(s)
	}

	// calculate wire size
	s.Finalize()

	// validate schema conformance
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// register schema
	schemaRegistry.Store(typ, s)

	return s, nil
}

// Produces a dynamic struct type only using native types like
// int64 for Decimal64, [16]byte for Int128, etc. to make internal
// types compatible with external libraries.
func NativeStructType(s *Schema) reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		if !f.IsVisible() {
			continue
		}
		var rtyp reflect.Type
		switch f.Type {
		case FT_TIMESTAMP, FT_TIME, FT_DATE, FT_I64, FT_D64:
			rtyp = typeOfInt64
		case FT_I32, FT_D32:
			rtyp = typeOfInt32
		case FT_I16:
			rtyp = typeOfInt16
		case FT_I8:
			rtyp = typeOfInt8
		case FT_U64:
			rtyp = typeOfUint64
		case FT_U32:
			rtyp = typeOfUint32
		case FT_U16:
			rtyp = typeOfUint16
		case FT_U8:
			rtyp = typeOfUint8
		case FT_F64:
			rtyp = typeOfFloat64
		case FT_F32:
			rtyp = typeOfFloat32
		case FT_BOOL:
			rtyp = typeOfBool
		case FT_STRING, FT_TEXT:
			rtyp = typeOfString
		case FT_BYTES, FT_BIGINT, FT_BLOB:
			if f.IsArray() {
				rtyp = reflect.ArrayOf(int(f.Scale), reflect.TypeFor[byte]())
			} else {
				rtyp = typeOfByteSlice
			}
		case FT_I256, FT_D256:
			rtyp = reflect.TypeFor[[32]byte]()
		case FT_I128, FT_D128:
			rtyp = reflect.TypeFor[[16]byte]()
		default:
			continue
		}
		sfields = append(sfields, reflect.StructField{
			Name: toTitle(sanitize(f.Name)),
			Type: rtyp,
		})
	}
	return reflect.StructOf(sfields)
}

// Produces a dynamic Go struct type compatible with SchemaOf which
// uses native and custom types (e.g. num.Int128, num.Decimal64).
// Adds struct tags, but excludes index tags.
func StructType(s *Schema) reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		if !f.IsVisible() {
			continue
		}
		tag := fmt.Sprintf(`%s:"%s,id=%d`, TAG_NAME, f.Name, f.Id)
		if f.IsPrimary() {
			tag += ",pk"
		}
		if f.IsEnum() {
			tag += ",enum"
		}
		if f.IsArray() && f.Type == FT_STRING {
			tag += fmt.Sprintf(",array=%d", f.Scale)
		}
		if !f.IsArray() && f.Scale > 0 {
			tag += fmt.Sprintf(",scale=%d", f.Scale)
		}
		if f.IsCompressed() {
			tag += ",zip=" + f.Compress.String()
		}
		tag += `"`
		sfields = append(sfields, reflect.StructField{
			Name: toTitle(sanitize(f.Name)),
			Type: GoType(f),
			Tag:  reflect.StructTag(tag),
		})
	}
	return reflect.StructOf(sfields)
}

func GoType(f *Field) reflect.Type {
	if f.Type == FT_BYTES && f.IsArray() {
		return reflect.ArrayOf(int(f.Scale), reflect.TypeFor[byte]())
	}
	if f.Type == FT_U16 && f.IsEnum() {
		return reflect.TypeFor[string]()
	}
	return reflect.TypeOf(f.Type.Zero())
}

// AppendFieldLayout generates a Go struct from schema to
// identify its memory layout and appends path/index and
// memory offset to each field. Decoders require this info
// to address the memory location of every field in this struct.
func AppendFieldLayout(s *Schema) {
	// check if this already exists
	if len(s.Fields) == 0 || s.Fields[0].Path != nil {
		return
	}

	// use logical types here
	styp := StructType(s)
	for i, f := range s.Fields {
		// skip invisible fields
		if !f.IsVisible() {
			continue
		}
		// fill struct type info
		sf := styp.Field(i)
		f.Path = sf.Index
		f.Offset = sf.Offset
	}
}

// // FieldStructValue resolves a struct field from a struct. When the field
// // is a pointer it allocates the target type and dereferences it
// // so that the return value can consistently be used for interface calls.
// func FieldStructValue(f *Field, rval reflect.Value) reflect.Value {
// 	dst := rval.FieldByIndex(f.Path)
// 	if dst.Kind() == reflect.Pointer {
// 		if dst.IsNil() && dst.CanSet() {
// 			dst.Set(reflect.New(dst.Type().Elem()))
// 		}
// 		dst = dst.Elem()
// 	}
// 	return dst
// }

// // StructPointer unwraps an interface and returns the embedded
// // struct pointer if addressable. Panics if interface is nil
// // or warps an unaddressable value.
// func StructPointer(v any) unsafe.Pointer {
// 	rval := reflect.Indirect(reflect.ValueOf(v))
// 	assert.Always(rval.IsValid() && rval.Kind() == reflect.Struct, "invalid value",
// 		"kind", rval.Kind().String(),
// 		"type", rval.Type().String(),
// 	)
// 	return rval.Addr().UnsafePointer()
// }

var rx = regexp.MustCompile("[^a-zA-Z0-9]+")

func sanitize(s string) string {
	if len(s) == 0 {
		return s
	}

	// Prefix internal field names
	if s[0] == '$' {
		s = "X" + s[1:]
	}

	// Replace invalid characters
	s = rx.ReplaceAllString(s, "_")

	// Replace multiple __ with single _
	s = strings.ReplaceAll(s, "__", "_")

	return s
}

func toTitle(src string) string {
	if len(src) == 0 {
		return src
	}
	return strings.ToUpper(src[:1]) + src[1:]
}

func fromCamelCase(src, sep string) string {
	var b strings.Builder
	for idx := 0; idx < len(src); {
		offs := strings.IndexFunc(src[idx+1:], func(r rune) bool {
			return r >= 'A' && r <= 'Z'
		}) + 1
		if offs <= 0 {
			offs = len(src) - idx
		}
		if b.Len() > 0 {
			b.WriteString(sep)
		}
		b.WriteString(strings.ToLower(src[idx : idx+offs]))
		idx += offs
	}
	return b.String()
}

func reflectStructField(structField reflect.StructField, tagName string) (*Field, error) {
	tag := structField.Tag.Get(tagName)
	field := &Field{
		Name: structField.Name,
	}
	// extract alias name
	if n, _, _ := strings.Cut(tag, ","); n != "" {
		field.Name = n
	}

	// clean name
	field.Name = strings.ToLower(strings.TrimSpace(field.Name))

	// identify field type from Go type
	err := inferFieldType(field, structField.Type)
	if err != nil {
		return nil, fmt.Errorf("field[%s]: %w", field.Name, err)
	}

	// parse tags, allow feature override
	err = parseFieldTag(field, tag)
	if err != nil {
		return nil, fmt.Errorf("field[%s]: %w", field.Name, err)
	}

	// fill en/decoder info
	field.Path = structField.Index
	field.Offset = structField.Offset

	return field, nil
}

var (
	emptyType       = reflect.TypeFor[struct{}]()
	modelType       = reflect.TypeFor[Model]()
	typeOfTime      = reflect.TypeFor[time.Time]()
	typeOfInt256    = reflect.TypeFor[num.Int256]()
	typeOfInt128    = reflect.TypeFor[num.Int128]()
	typeOfDec32     = reflect.TypeFor[num.Decimal32]()
	typeOfDec64     = reflect.TypeFor[num.Decimal64]()
	typeOfDec128    = reflect.TypeFor[num.Decimal128]()
	typeOfDec256    = reflect.TypeFor[num.Decimal256]()
	typeOfBigInt    = reflect.TypeFor[num.Big]()
	typeOfByteSlice = reflect.TypeFor[[]byte]()
	typeOfInt8      = reflect.TypeFor[int8]()
	typeOfInt16     = reflect.TypeFor[int16]()
	typeOfInt32     = reflect.TypeFor[int32]()
	typeOfInt64     = reflect.TypeFor[int64]()
	typeOfUint8     = reflect.TypeFor[uint8]()
	typeOfUint16    = reflect.TypeFor[uint16]()
	typeOfUint32    = reflect.TypeFor[uint32]()
	typeOfUint64    = reflect.TypeFor[uint64]()
	typeOfFloat32   = reflect.TypeFor[float32]()
	typeOfFloat64   = reflect.TypeFor[float64]()
	typeOfBool      = reflect.TypeFor[bool]()
	typeOfString    = reflect.TypeFor[string]()
)

func inferFieldType(f *Field, t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t == typeOfByteSlice {
		f.Type = FT_BYTES
		return nil
	}

	switch t.Kind() {
	case reflect.Array:
		return inferArrayFieldType(f, t)
	case reflect.Slice:
		// TODO: list type
		return fmt.Errorf("unsupported Go slice type %v: %w", t, ErrUnsupportedType)
	case reflect.Map:
		// TODO: map type
		return fmt.Errorf("unsupported Go map type %v: %w", t, ErrUnsupportedType)
	case reflect.Struct:
		return inferStructFieldType(f, t)
	default:
		return inferPrimitiveFieldType(f, t)
	}
}

func inferArrayFieldType(f *Field, t reflect.Type) error {
	switch t {
	case typeOfInt256:
		f.Type = FT_I256
	case typeOfInt128:
		f.Type = FT_I128
	default:
		if t.Elem() != typeOfUint8 {
			return fmt.Errorf("unsupported Go array type %v: %w", t, ErrUnsupportedType)
		}
		if t.Len() > types.MAX_ARRAY {
			f.Type = FT_BLOB
		} else {
			f.Type = FT_BYTES
			f.Scale = uint8(t.Len())
			f.Flags |= F_ARRAY
		}
	}
	return nil
}

func inferStructFieldType(f *Field, t reflect.Type) error {
	switch t {
	case typeOfTime:
		f.Type = FT_TIMESTAMP
		f.Scale = types.TIME_SCALE_NANO.AsUint()
	case typeOfDec32:
		f.Type = FT_D32
		f.Scale = num.MaxDecimal32Precision
	case typeOfDec64:
		f.Type = FT_D64
		f.Scale = num.MaxDecimal64Precision
	case typeOfDec128:
		f.Type = FT_D128
		f.Scale = num.MaxDecimal128Precision
	case typeOfDec256:
		f.Type = FT_D256
		f.Scale = num.MaxDecimal256Precision
	case typeOfBigInt:
		f.Type = FT_BIGINT
	default:
		return fmt.Errorf("unsupported nested Go struct type %v: %w", t, ErrUnsupportedType)
	}
	return nil
}

func inferPrimitiveFieldType(f *Field, t reflect.Type) error {
	switch t {
	case typeOfInt64:
		f.Type = FT_I64
	case typeOfInt32:
		f.Type = FT_I32
	case typeOfInt16:
		f.Type = FT_I16
	case typeOfInt8:
		f.Type = FT_I8
	case typeOfUint64:
		f.Type = FT_U64
	case typeOfUint32:
		f.Type = FT_U32
	case typeOfUint16:
		f.Type = FT_U16
	case typeOfUint8:
		f.Type = FT_U8
	case typeOfFloat64:
		f.Type = FT_F64
	case typeOfFloat32:
		f.Type = FT_F32
	case typeOfString:
		f.Type = FT_STRING
	case typeOfBool:
		f.Type = FT_BOOL
	default:
		return inferPrimitiveFieldTypeAlias(f, t)
	}
	return nil
}

func inferPrimitiveFieldTypeAlias(f *Field, t reflect.Type) error {
	switch t.Kind() {
	case reflect.Int64:
		f.Type = FT_I64
	case reflect.Int32:
		f.Type = FT_I32
	case reflect.Int16:
		f.Type = FT_I16
	case reflect.Int8:
		f.Type = FT_I8
	case reflect.Uint64:
		f.Type = FT_U64
	case reflect.Uint32:
		f.Type = FT_U32
	case reflect.Uint16:
		f.Type = FT_U16
	case reflect.Uint8:
		f.Type = FT_U8
	case reflect.Float64:
		f.Type = FT_F64
	case reflect.Float32:
		f.Type = FT_F32
	case reflect.String:
		f.Type = FT_STRING
	case reflect.Bool:
		f.Type = FT_BOOL
	default:
		return fmt.Errorf("unsupported Go type %v: %w", t, ErrUnsupportedType)
	}
	return nil
}

func parseFieldTag(f *Field, tag string) error {
	// first part is field name
	tokens := strings.Split(tag, ",")
	if len(tokens) < 2 {
		return nil
	}

	var (
		scale    = f.Scale // time scale, decimal scale, fixed array length
		maxScale = f.Scale // max decimal/time scale
		flags    = f.Flags
		compress types.BlockCompression
		filter   types.FilterType
	)

	for _, flag := range tokens[1:] {
		key, val, ok := strings.Cut(strings.TrimSpace(flag), "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "index", "fields", "extra":
			// skip here
		case "pk":
			if f.Type == FT_U64 {
				flags |= F_PRIMARY
			} else {
				return fmt.Errorf("pk tag unsupported on field type %s", f.Type)
			}
		case "filter":
			switch val {
			case "bits":
				filter = FL_BITS
			case "bloom2b":
				filter = FL_BLOOM2B
			case "bloom3b":
				filter = FL_BLOOM3B
			case "bloom4b":
				filter = FL_BLOOM4B
			case "bloom5b":
				filter = FL_BLOOM5B
			case "bfuse8":
				filter = FL_BFUSE8
			case "bfuse16":
				filter = FL_BFUSE16
			default:
				return fmt.Errorf("unsupported filter type %q", val)
			}
		case "zip":
			switch val {
			case "", "no", "none":
				compress = types.BlockCompressNone
			case "snappy":
				compress = types.BlockCompressSnappy
			case "lz4":
				compress = types.BlockCompressLZ4
			case "zstd":
				compress = types.BlockCompressZstd
			default:
				return fmt.Errorf("unsupported compression type %q", val)
			}
		case "array":
			// only compatible with strings, bytes must use [n]byte arrays):
			if f.Type != FT_STRING {
				return fmt.Errorf("array tag unsupported on type %s", f.Type)
			}
			if ok {
				fx, err := parseInt(val, "array", 1, types.MAX_ARRAY)
				if err != nil {
					return err
				}
				scale = uint8(fx)
				flags |= F_ARRAY
			} else {
				return fmt.Errorf("missing value for array tag")
			}
		case "scale":
			// only compatible with:
			// - decimal types
			// - datetime
			switch f.Type {
			case FT_D32, FT_D64, FT_D128, FT_D256:
				if ok {
					sc, err := parseInt(val, "scale", 0, int(maxScale))
					if err != nil {
						return err
					}
					scale = uint8(sc)
				} else {
					return fmt.Errorf("missing value for scale tag")
				}
			case FT_TIMESTAMP, FT_TIME:
				s, ok := types.ParseTimeScale(val)
				if !ok {
					return fmt.Errorf("invalid time scale value %q", val)
				}
				scale = s.AsUint()
			default:
				return fmt.Errorf("scale tag unsupported on type %s", f.Type)
			}
		case "enum":
			if f.Type == FT_STRING {
				// ok
				flags |= F_ENUM
				f.Type = FT_U16
			} else {
				return fmt.Errorf("unsupported enum type %s", f.Type)
			}
		case "metadata":
			flags |= F_METADATA
		case "id":
			num, err := strconv.ParseUint(val, 0, 16)
			if err != nil {
				return fmt.Errorf("invalid field id %q: %v", val, err)
			}
			f.Id = uint16(num)
		case "null":
			flags |= F_NULLABLE
		case "notnull":
			flags &^= F_NULLABLE
		case "timestamp":
			f.Type = FT_TIMESTAMP
			scale = types.TIME_SCALE_NANO.AsUint()
		case "date":
			f.Type = FT_DATE
			scale = types.TIME_SCALE_DAY.AsUint()
		case "time":
			f.Type = FT_TIME
			scale = types.TIME_SCALE_SECOND.AsUint()
		case "timebase":
			flags |= F_TIMEBASE
		case "text":
			if f.Type != FT_STRING {
				return fmt.Errorf("text tag unsupported on type %s", f.Type)
			}
			f.Type = FT_TEXT
			flags &^= F_ARRAY
			scale = 0
		case "blob":
			if f.Type != FT_BYTES {
				return fmt.Errorf("blob tag unsupported on type %s", f.Type)
			}
			f.Type = FT_BLOB
			flags &^= F_ARRAY
			scale = 0
		default:
			return fmt.Errorf("unsupported struct tag '%s'", key)
		}
	}

	f.Scale = scale
	f.Flags = flags
	f.Compress = compress
	f.Filter = filter

	return nil
}

func parseInt(val, name string, minVal, maxVal int) (int, error) {
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %s: %v", name, val, err)
	}
	return types.ValidateInt(name, n, minVal, maxVal)
}
