// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"strconv"
)

type (
	BuilderOption func(*Builder)
	IndexOption   func(*IndexSchema)
)

func Fixed[T int | uint16](n T) BuilderOption {
	return func(b *Builder) {
		b.currentField().Fixed = uint16(n)
	}
}

func Scale[T int | uint8](n T) BuilderOption {
	return func(b *Builder) {
		b.currentField().Scale = uint8(n)
	}
}

func Filter(f FilterType) BuilderOption {
	return func(b *Builder) {
		b.currentField().Filter = f
	}
}

func Compression(c BlockCompression) BuilderOption {
	return func(b *Builder) {
		b.currentField().Compress = c
	}
}

func Primary() BuilderOption {
	return func(b *Builder) {
		b.currentField().Flags |= F_PRIMARY
	}
}

func Timebase() BuilderOption {
	return func(b *Builder) {
		b.currentField().Flags |= F_TIMEBASE
	}
}

func Nullable() BuilderOption {
	return func(b *Builder) {
		b.currentField().Flags |= F_NULLABLE
	}
}

func Id(id uint16) BuilderOption {
	return func(b *Builder) {
		b.currentField().Id = id
	}
}

func IndexField(name string) IndexOption {
	return func(idx *IndexSchema) {
		f, ok := idx.Base.Find(name)
		if ok {
			idx.Fields = append(idx.Fields, f)
		}
	}
}

func ExtraField(name string) IndexOption {
	return func(idx *IndexSchema) {
		f, ok := idx.Base.Find(name)
		if ok {
			idx.Extra = append(idx.Extra, f)
		}
	}
}

type Builder struct {
	s     *Schema
	meta  bool
	final bool
}

func NewBuilder() *Builder {
	return &Builder{
		s: NewSchema(),
	}
}

func (b *Builder) Validate() error {
	return b.s.Validate()
}

func (b *Builder) Finalize() *Builder {
	if b.meta && !b.s.HasMeta() {
		b.s = b.s.WithMeta() // clones and calls finalize
	} else {
		b.s.Finalize()
	}
	b.final = true
	return b
}

func (b *Builder) Schema() *Schema {
	if !b.final {
		b.Finalize()
	}
	return b.s
}

func (b *Builder) WithName(s string) *Builder {
	b.s.WithName(s)
	b.final = false
	return b
}

func (b *Builder) WithMeta(v bool) *Builder {
	b.meta = v
	b.final = false
	return b
}

func (b *Builder) WithVersion(v uint32) *Builder {
	b.s.WithVersion(v)
	b.final = false
	return b
}

func (b *Builder) currentField() *Field {
	return b.s.Fields[len(b.s.Fields)-1]
}

func (b *Builder) addField(typ FieldType, name string, opts ...BuilderOption) *Builder {
	if name == "" {
		name = "F" + strconv.Itoa(len(b.s.Fields))
	}
	b.s.WithField(NewField(typ).WithName(name))
	for _, o := range opts {
		o(b)
	}
	b.final = false
	return b
}

func (b *Builder) Field(fields ...*Field) *Builder {
	for _, f := range fields {
		b.s.WithField(f.Clone())
	}
	b.final = false
	return b
}

func (b *Builder) Add(name string, typ FieldType, opts ...BuilderOption) *Builder {
	return b.addField(typ, name, opts...)
}

func (b *Builder) SetFieldOpts(opts ...BuilderOption) *Builder {
	for _, o := range opts {
		o(b)
	}
	return b
}

func (b *Builder) Int64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I64, name, opts...)
}

func (b *Builder) Int32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I32, name, opts...)
}

func (b *Builder) Int16(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I16, name, opts...)
}

func (b *Builder) Int8(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I8, name, opts...)
}

func (b *Builder) Uint64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U64, name, opts...)
}

func (b *Builder) Uint32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U32, name, opts...)
}

func (b *Builder) Uint16(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U16, name, opts...)
}

func (b *Builder) Uint8(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_U8, name, opts...)
}

func (b *Builder) Timestamp(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_TIMESTAMP, name, opts...)
}

func (b *Builder) Date(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_DATE, name, opts...)
}

func (b *Builder) Time(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_TIME, name, opts...)
}

func (b *Builder) Float64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_F64, name, opts...)
}

func (b *Builder) Float32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_F32, name, opts...)
}

func (b *Builder) Bool(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_BOOL, name, opts...)
}

func (b *Builder) String(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_STRING, name, opts...)
}

func (b *Builder) Bytes(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_BYTES, name, opts...)
}

func (b *Builder) Int128(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I128, name, opts...)
}

func (b *Builder) Int256(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_I256, name, opts...)
}

func (b *Builder) Decimal32(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D32, name, opts...)
}

func (b *Builder) Decimal64(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D64, name, opts...)
}

func (b *Builder) Decimal128(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D128, name, opts...)
}

func (b *Builder) Decimal256(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_D256, name, opts...)
}

func (b *Builder) Bigint(name string, opts ...BuilderOption) *Builder {
	return b.addField(FT_BIGINT, name, opts...)
}

func (b *Builder) AddIndex(name string, typ IndexType, opts ...IndexOption) *Builder {
	if name == "" {
		name = "I" + strconv.Itoa(len(b.s.Indexes))
	}
	idx := &IndexSchema{
		Name: name,
		Type: typ,
		Base: b.s,
	}
	for _, o := range opts {
		o(idx)
	}
	b.s.Indexes = append(b.s.Indexes, idx)
	b.final = false
	return b
}

func (b *Builder) PkIndex() *Builder {
	return b.AddIndex("pk_index", IT_PK, IndexField(b.s.Pk().Name))
}

func (b *Builder) HashIndex(fname string, opts ...IndexOption) *Builder {
	opts = append([]IndexOption{IndexField(fname)}, opts...)
	return b.AddIndex(fname+"_index", IT_HASH, opts...)
}

func (b *Builder) IntIndex(fname string, opts ...IndexOption) *Builder {
	opts = append([]IndexOption{IndexField(fname)}, opts...)
	return b.AddIndex(fname+"_index", IT_INT, opts...)
}

func (b *Builder) CompositeIndex(name string, opts ...IndexOption) *Builder {
	return b.AddIndex(name, IT_COMPOSITE, opts...)
}
