// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package reflect

import (
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/types"
)

var (
	ErrNilValue = errors.New("schema: nil value")
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

	// must be a struct or slice of struct
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Struct:
		// ok
	case reflect.Slice:
		telem := typ.Elem()
		if telem.Kind() == reflect.Pointer {
			telem = telem.Elem()
		}
		if telem.Kind() != reflect.Struct {
			return nil, fmt.Errorf("slice element type %s (%s) is not a struct", telem, telem.Kind())
		}
		typ = telem
	default:
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

		// catch duplicates
		if exist, ok := s.Find(field.Name); ok {
			return nil, fmt.Errorf("%s field %q conflicts with field %q",
				field.Type, field.Name, exist.Name)
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

// Produces a dynamic struct type only using native types like int64 for Decimal64
// [16]byte for Int128, etc.
func NativeStructType(s *Schema) reflect.Type {
	sfields := make([]reflect.StructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		if !f.IsVisible() {
			continue
		}
		var rtyp reflect.Type
		switch f.Type {
		case FT_TIMESTAMP, FT_TIME, FT_DATE, FT_I64, FT_D64:
			rtyp = reflect.TypeFor[int64]()
		case FT_U64:
			rtyp = reflect.TypeFor[uint64]()
		case FT_F64:
			rtyp = reflect.TypeFor[float64]()
		case FT_BOOL:
			rtyp = reflect.TypeFor[bool]()
		case FT_STRING:
			rtyp = reflect.TypeFor[string]()
		case FT_BYTES, FT_BIGINT:
			if f.Fixed > 0 {
				rtyp = reflect.ArrayOf(int(f.Fixed), reflect.TypeFor[byte]())
			} else {
				rtyp = reflect.TypeFor[[]byte]()
			}
		case FT_I32, FT_D32:
			rtyp = reflect.TypeFor[int32]()
		case FT_I16:
			rtyp = reflect.TypeFor[int16]()
		case FT_I8:
			rtyp = reflect.TypeFor[int8]()
		case FT_U32:
			rtyp = reflect.TypeFor[uint32]()
		case FT_U16:
			rtyp = reflect.TypeFor[uint16]()
		case FT_U8:
			rtyp = reflect.TypeFor[uint8]()
		case FT_F32:
			rtyp = reflect.TypeFor[float32]()
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

// Produces a dynamic struct type compatible with SchemaOf which uses
// custom types for large numeric values (num.Int128) and decimals
// (num.Decimal64).
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
		if f.IsFixedSize() && f.Fixed > 0 {
			tag += fmt.Sprintf(",fixed=%d", f.Fixed)
		}
		// if f.IsIndexed() {
		// 	tag += fmt.Sprintf(",index=%s", f.Index.Type)
		// }
		if f.Scale > 0 {
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
	if f.Type == FT_BYTES && f.Fixed > 0 {
		return reflect.ArrayOf(int(f.Fixed), reflect.TypeFor[byte]())
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

// FieldStructValue resolves a struct field from a struct. When the field
// is a pointer it allocates the target type and dereferences it
// so that the return value can consistently be used for interface calls.
func FieldStructValue(f *Field, rval reflect.Value) reflect.Value {
	dst := rval.FieldByIndex(f.Path)
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() && dst.CanSet() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	return dst
}

// StructPointer unwraps an interface and returns the embedded
// struct pointer if addressable. Panics if interface is nil
// or warps an unaddressable value.
func StructPointer(v any) unsafe.Pointer {
	rval := reflect.Indirect(reflect.ValueOf(v))
	assert.Always(rval.IsValid() && rval.Kind() == reflect.Struct, "invalid value",
		"kind", rval.Kind().String(),
		"type", rval.Type().String(),
	)
	return rval.Addr().UnsafePointer()
}

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
	var chunks []string
	for idx := 0; idx < len(src); {
		offs := strings.IndexFunc(src[idx+1:], unicode.IsUpper) + 1
		if offs <= 0 {
			offs = len(src) - idx
		}
		chunks = append(chunks, strings.ToLower(src[idx:idx+offs]))
		idx += offs
	}
	return strings.Join(chunks, sep)
}

var (
	emptyType     = reflect.TypeFor[struct{}]()
	uint8Type     = reflect.TypeFor[uint8]()
	byteSliceType = reflect.TypeFor[[]byte]()
	modelType     = reflect.TypeFor[Model]()
)

func reflectStructField(f reflect.StructField, tagName string) (field *Field, err error) {
	tag := f.Tag.Get(tagName)
	field = &Field{
		Name: f.Name,
	}
	// extract alias name
	if n, _, _ := strings.Cut(tag, ","); n != "" {
		field.Name = n
	}

	// clean name
	field.Name = strings.ToLower(strings.TrimSpace(field.Name))

	// identify field type from Go type
	err = parseFieldType(field, f)
	if err != nil {
		err = fmt.Errorf("field %s: %v", field.Name, err)
		return
	}

	// parse tags, allow type & fixed override
	err = parseFieldTag(field, tag)
	if err != nil {
		err = fmt.Errorf("field %s: %v", field.Name, err)
		return
	}

	// Validate field

	// pk field must be of type uint64
	if field.Flags&F_PRIMARY > 0 {
		switch f.Type.Kind() {
		case reflect.Uint64:
		default:
			err = fmt.Errorf("field %s: invalid primary key type %s", field.Name, f.Type)
			return
		}
	}

	// fill en/decoder info
	field.Path = f.Index
	field.Offset = f.Offset
	field.Size = uint16(field.Type.Size())

	return
}

func parseFieldType(f *Field, r reflect.StructField) error {
	var (
		typ   types.FieldType
		flags types.FieldFlags
		fixed uint16
		scale uint8
	)

	// field must have supported kind
	switch r.Type.Kind() {
	case reflect.Complex64,
		reflect.Complex128,
		reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Pointer,
		reflect.UnsafePointer:
		return fmt.Errorf("unsupported kind %s", r.Type.Kind())

	case reflect.Int:
		if bits.UintSize == 64 {
			typ = FT_I64
		} else {
			typ = FT_I32
		}
	case reflect.Int64:
		typ = FT_I64
	case reflect.Int32:
		typ = FT_I32
	case reflect.Int16:
		typ = FT_I16
	case reflect.Int8:
		typ = FT_I8
	case reflect.Uint:
		if bits.UintSize == 64 {
			typ = FT_U64
		} else {
			typ = FT_U32
		}
	case reflect.Uint64:
		typ = FT_U64
	case reflect.Uint32:
		typ = FT_U32
	case reflect.Uint16:
		typ = FT_U16
	case reflect.Uint8:
		typ = FT_U8
	case reflect.Float64:
		typ = FT_F64
	case reflect.Float32:
		typ = FT_F32
	case reflect.String:
		typ = FT_STRING
	case reflect.Bool:
		typ = FT_BOOL
	case reflect.Map:
		return fmt.Errorf("unsupported map type %s", r.Type)
	case reflect.Slice:
		if r.Type == byteSliceType {
			typ = FT_BYTES
		} else {
			return fmt.Errorf("unsupported slice type %s", r.Type)
		}
	case reflect.Struct:
		// string-check is much quicker
		switch r.Type.String() {
		case "time.Time":
			typ = FT_TIMESTAMP
			scale = types.TIME_SCALE_NANO.AsUint()
		case "num.Decimal32":
			typ = FT_D32
			scale = num.MaxDecimal32Precision
		case "num.Decimal64":
			typ = FT_D64
			scale = num.MaxDecimal64Precision
		case "num.Decimal128":
			typ = FT_D128
			scale = num.MaxDecimal128Precision
		case "num.Decimal256":
			typ = FT_D256
			scale = num.MaxDecimal256Precision
		case "num.Big":
			typ = FT_BIGINT
		default:
			return fmt.Errorf("unsupported nested struct type %s", r.Type)
		}
	case reflect.Array:
		// string-check is much quicker
		switch r.Type.String() {
		case "num.Int128":
			typ = FT_I128
		case "num.Int256":
			typ = FT_I256
		default:
			if r.Type.Elem() == uint8Type {
				typ = FT_BYTES
				fixed = uint16(r.Type.Len())
			} else {
				return fmt.Errorf("unsupported array type %s", r.Type)
			}
		}
	default:
		return fmt.Errorf("unsupported type %s (%v)", r.Type, r.Type.Kind())
	}

	f.Type = typ
	f.Flags = flags
	f.Fixed = fixed
	f.Scale = scale

	return nil
}

func parseFieldTag(f *Field, tag string) error {
	// first part is field name
	tokens := strings.Split(tag, ",")
	if len(tokens) < 2 {
		return nil
	}

	var (
		scale    uint8
		fixed    = f.Fixed
		maxFixed = types.MAX_FIXED
		maxScale = f.Scale
		flags    types.FieldFlags
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
			flags |= F_PRIMARY
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
		case "fixed":
			// only compatible with strings, bytes must use [n]byte arrays):
			if f.Type != FT_STRING {
				return fmt.Errorf("fixed tag unsupported on type %s", f.Type)
			}
			if ok {
				fx, err := parseInt(val, "fixed", 1, int(maxFixed))
				if err != nil {
					return err
				}
				fixed = uint16(fx)
			} else {
				return fmt.Errorf("missing value for fixed tag")
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
		default:
			return fmt.Errorf("unsupported struct tag '%s'", key)
		}
	}

	f.Scale = scale
	f.Fixed = fixed
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
