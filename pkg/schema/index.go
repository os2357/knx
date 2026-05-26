// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash"
)

// Knox index spec parsing
//
// Examples
//
// Id      uint64    `"knox:X,pk"`            // implies PK index type
// F1      int       `"knox:Y,index=hash"`
// F2      int       `"knox:Z,index=int,extra=X+Y"`
// _       struct{}  `"knox:idx,index=composite,fields=X+Y,extra=Z+X"`

type IndexSchema struct {
	Name   string    // index name
	Type   IndexType // index type: hash, int, composite
	Base   *Schema   // base schema
	Fields []*Field  // indexed fields in order
	Extra  []*Field  // extra (inline) fields
}

func NewIndexSchema(typ IndexType, s *Schema, f ...*Field) *IndexSchema {
	return &IndexSchema{
		Name:   strings.Join([]string{s.Name, f[0].Name, typ.String(), "index"}, "_"),
		Type:   typ,
		Base:   s,
		Fields: f,
	}
}

func (s *IndexSchema) IsValid() bool {
	return s.Type.IsValid() && len(s.Fields) > 0
}

// Hash returns a unique index schema hash.
func (s *IndexSchema) Hash() uint64 {
	h := hash.New()

	// index type
	h.Write([]byte{byte(s.Type)})

	// base schema hash
	var b [8]byte
	LE.PutUint64(b[:], s.Base.Hash)

	// hash: id, type, flags, fixed, scale (not: filter, compress, name)
	hashField := func(f *Field) {
		LE.PutUint16(b[:], f.Id)
		h.Write(b[:2])
		h.Write([]byte{byte(f.Type)})
		h.Write([]byte{byte(f.Flags)})
		h.Write([]byte{f.Scale})
	}

	// index fields
	for _, f := range s.Fields {
		hashField(f)
	}

	// extra fields
	for _, f := range s.Fields {
		hashField(f)
	}

	return h.Sum64()
}

// Ids returns an ordered list of all field ids required by this index.
// This includes rowid, all index fields and extra include fields. Note the
// schema requires metadata.
func (s *IndexSchema) Ids() []uint16 {
	ids := make([]uint16, 0, 1+len(s.Fields)+len(s.Extra))
	ids = append(ids, MetaRid)
	for _, f := range s.Fields {
		ids = append(ids, f.Id)
	}
	for _, f := range s.Extra {
		ids = append(ids, f.Id)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func (s *IndexSchema) ExtraIds() []uint16 {
	if len(s.Extra) == 0 {
		return nil
	}
	ids := make([]uint16, len(s.Extra))
	for k, f := range s.Extra {
		ids[k] = f.Id
	}
	return ids
}

func (s *IndexSchema) Indices() []int {
	ixs := make([]int, len(s.Fields))
	for k, f := range s.Fields {
		x, _ := s.Base.IndexId(f.Id)
		ixs[k] = x
	}
	return ixs
}

func (s *IndexSchema) ExtraIndices() []int {
	if len(s.Extra) == 0 {
		return nil
	}
	ixs := make([]int, len(s.Extra))
	for k, f := range s.Extra {
		x, _ := s.Base.IndexId(f.Id)
		ixs[k] = x
	}
	return ixs
}

// IndexSchema returns a sub-schema from base which contains all fields
// required by the index including row_id, index and extra fields.
func (s *IndexSchema) IndexSchema() (*Schema, error) {
	base, err := s.Base.SelectIds(s.Ids()...)
	if err != nil {
		return nil, err
	}
	return base.As(s.Name), nil
}

// Contains returns true if all named fields exist in order.
func (s *IndexSchema) Contains(names ...string) bool {
	if len(names) == 0 || len(names) > len(s.Fields) {
		return false
	}
	for k, n := range names {
		if s.Fields[k].Name == n {
			continue
		}
		return false
	}
	return true
}

// StorageSchema returns a sub-schema usable for storing index records.
// Hash and composite hash indexes will contain a synthetic hash field
// as the first element.
func (s *IndexSchema) StorageSchema() (*Schema, error) {
	// validate again just to be sure
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// we need row_id to be present
	if rid := s.Base.RowId(); !rid.IsValid() {
		return nil, ErrNoMeta
	}

	// build storage schema (without flags to make all fields visible)
	var b *Builder
	switch s.Type {
	case IT_PK:
		// pk -> rid
		b = NewBuilder().
			WithName(s.Name).
			WithVersion(s.Base.Version).
			Uint64(s.Fields[0].Name, Id(s.Fields[0].Id)).
			Uint64("rid", Id(MetaRid))

	case IT_HASH:
		// hash(any) -> rid
		b = NewBuilder().
			WithName(s.Name).
			WithVersion(s.Base.Version).
			Uint64("hash").
			Uint64("rid", Id(MetaRid))

	case IT_INT:
		// int -> rid
		b = NewBuilder().
			WithName(s.Name).
			WithVersion(s.Base.Version).
			Uint64(s.Fields[0].Name, Id(s.Fields[0].Id)).
			Uint64("rid", Id(MetaRid))

	case IT_COMPOSITE:
		// hash(...) -> rid
		b = NewBuilder().
			WithName(s.Name).
			WithVersion(s.Base.Version).
			Uint64("hash").
			Uint64("rid", Id(MetaRid))
	}

	// add extra fields (assign new ids)
	b.Field(s.Extra...)

	// finalize and validate our new schema
	final := b.Finalize().Schema()
	if err := final.Validate(); err != nil {
		return nil, err
	}

	return final, nil
}

func (s *IndexSchema) Validate() error {
	// require index type in range
	if s.Name == "" {
		return fmt.Errorf("index: empty name")
	}

	// require index type in range
	if !s.Type.IsValid() {
		return fmt.Errorf("index[%s]: invalid index type %d", s.Name, s.Type)
	}

	// requires at least 1 index field
	if len(s.Fields) == 0 {
		return fmt.Errorf("index[%s]: empty field list", s.Name)
	}

	// fields must be defined in base schema
	for _, f := range s.Fields {
		if _, ok := s.Base.FindId(f.Id); !ok {
			return fmt.Errorf("index[%s]: field %s (%d) not in base schema %s",
				s.Name, f.Name, f.Id, s.Base.Name)
		}
	}
	for _, f := range s.Extra {
		if _, ok := s.Base.FindId(f.Id); !ok {
			return fmt.Errorf("index[%s]: extra field %s (%d) not in base schema %s",
				s.Name, f.Name, f.Id, s.Base.Name)
		}
	}

	// fields and extra lists must not contain duplicate entries
	unique := make(map[uint16]struct{})
	for _, f := range s.Fields {
		if _, ok := unique[f.Id]; ok {
			return fmt.Errorf("index[%s]: duplicate index field %s (%d)", s.Name, f.Name, f.Id)
		}
		unique[f.Id] = struct{}{}
	}
	clear(unique)
	for _, f := range s.Extra {
		if _, ok := unique[f.Id]; ok {
			return fmt.Errorf("index[%s]: duplicate extra field %s (%d)", s.Name, f.Name, f.Id)
		}
		unique[f.Id] = struct{}{}
	}

	// check type-specific restrictions
	switch s.Type {
	case IT_INT:
		// requires single integer field
		if len(s.Fields) > 1 {
			return fmt.Errorf("index[%s]: integer index requires single field", s.Name)
		}
		f := s.Fields[0]
		switch f.Type {
		case FT_TIME, FT_DATE, FT_TIMESTAMP,
			FT_I64, FT_I32, FT_I16, FT_I8, FT_U64, FT_U32, FT_U16, FT_U8:
			// ok
		default:
			return fmt.Errorf("index[%s]: unsupported integer index on field %s type %s",
				s.Name, f.Name, f.Type)
		}

	case IT_PK:
		// requires single integer field
		if len(s.Fields) > 1 {
			return fmt.Errorf("index[%s]: primary index requires single field", s.Name)
		}
		// require pk index on pk field only
		f := s.Fields[0]
		if f.Type != FT_U64 || f.Flags&F_PRIMARY == 0 {
			return fmt.Errorf("field[%s]: pk index on unsupported field %s type %s",
				s.Name, f.Name, f.Type)
		}

	case IT_HASH:
		// requires single field
		if len(s.Fields) > 1 {
			return fmt.Errorf("index[%s]: hash index requires single field", s.Name)
		}

	case IT_COMPOSITE:
		// requires multiple fields
		if len(s.Fields) < 2 {
			return fmt.Errorf("index[%s]: composite index requires at least 2 fields", s.Name)
		}
	}

	return nil
}

func (s IndexSchema) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 22+len(s.Name)+32*(len(s.Fields)+len(s.Extra))))

	// version: byte
	buf.WriteByte(1)

	// type: byte
	buf.WriteByte(byte(s.Type))

	// base schema hash: u64
	binary.Write(buf, LE, s.Base.Hash)

	// name: string
	binary.Write(buf, LE, uint32(len(s.Name)))
	buf.WriteString(s.Name)

	// fields
	binary.Write(buf, LE, uint32(len(s.Fields)))
	for _, f := range s.Fields {
		f.WriteTo(buf)
	}

	// extra
	binary.Write(buf, LE, uint32(len(s.Extra)))
	for _, f := range s.Extra {
		f.WriteTo(buf)
	}

	return buf.Bytes(), nil
}

func (s *IndexSchema) UnmarshalBinary(b []byte) (err error) {
	if len(b) < 22 {
		return io.ErrShortBuffer
	}

	// version
	if b[0] != 1 {
		return fmt.Errorf("invalid index schema version %d", b[0])
	}

	// type
	s.Type = IndexType(b[1])
	if !s.Type.IsValid() {
		return fmt.Errorf("invalid index type %d", b[1])
	}

	buf := bytes.NewBuffer(b[2:])

	// base schema hash: u64
	s.Base = &Schema{}
	err = binary.Read(buf, LE, &s.Base.Hash)
	if err != nil {
		return
	}

	// name: string
	var l uint32
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.Name = string(buf.Next(int(l)))
	if len(s.Name) != int(l) {
		return io.ErrShortBuffer
	}

	// fields
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.Fields = make([]*Field, l)
	for i := range s.Fields {
		f := &Field{}
		if err = f.ReadFrom(buf); err != nil {
			return
		}
		s.Fields[i] = f
	}

	// extra fields
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	s.Extra = make([]*Field, l)
	for i := range s.Extra {
		f := &Field{}
		if err = f.ReadFrom(buf); err != nil {
			return
		}
		s.Extra[i] = f
	}

	// Note: although not strictly required, users may want to resolve
	// base schema from its hash

	return nil
}
