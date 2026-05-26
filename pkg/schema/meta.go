// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

const (
	// reserved metadata field ids
	MetaRid    uint16 = 0xFFFF
	MetaRef    uint16 = 0xFFFE
	MetaXmin   uint16 = 0xFFFD
	MetaXmax   uint16 = 0xFFFC
	MetaDel    uint16 = 0xFFFB
	MetaAction uint16 = 0xFFFA
)

var (
	MetaFieldIds = []uint16{MetaRid, MetaRef, MetaXmin, MetaXmax, MetaDel}
	MetaSchema   = &Schema{
		Name: "meta",
		Fields: []*Field{
			{Name: "$rid", Id: MetaRid, Type: FT_U64, Flags: F_METADATA},
			{Name: "$ref", Id: MetaRef, Type: FT_U64, Flags: F_METADATA},
			{Name: "$xmin", Id: MetaXmin, Type: FT_U64, Flags: F_METADATA},
			{Name: "$xmax", Id: MetaXmax, Type: FT_U64, Flags: F_METADATA},
			{Name: "$del", Id: MetaDel, Type: FT_BOOL, Flags: F_METADATA},
		},
	}
)

// WithMeta extends a schema with metadata fields. The extended schema
// will have the same identity as the original. Metadata is treated
// as internal info and skipped by struct encoders.
func (s *Schema) WithMeta() *Schema {
	// check if metadata fields already exist
	if _, ok := s.FindId(MetaRid); ok {
		return s
	}

	// ensure no collision with user defined fields
	for _, v := range s.Fields {
		for _, vv := range MetaSchema.Fields {
			if v.Name == vv.Name {
				return s
			}
			if v.Id == vv.Id {
				return s
			}
		}
	}

	// add metadata fields (internal fields don't change hash)
	clone := s.Clone()
	clone.Fields = append(clone.Fields, MetaSchema.Fields...)
	return clone.Finalize()
}

func (s *Schema) HasMeta() bool {
	return s.RowIdIndex() >= 0
}

func (s *Schema) NumMeta() int {
	var n int
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			n++
		}
	}
	return n
}

func (s *Schema) MetaNames() []string {
	list := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			list = append(list, f.Name)
		}
	}
	return list
}

func (s *Schema) MetaIds() []uint16 {
	list := make([]uint16, 0, len(s.Fields))
	for _, f := range s.Fields {
		if f.IsMeta() && f.IsActive() {
			list = append(list, f.Id)
		}
	}
	return list
}

func (s *Schema) RowId() *Field {
	for _, f := range s.Fields {
		if f.Id == MetaRid && f.IsMeta() && f.IsActive() {
			return f
		}
	}
	return &Field{}
}

func (s *Schema) RowIdIndex() int {
	for i, f := range s.Fields {
		if f.Id == MetaRid && f.IsMeta() && f.IsActive() {
			return i
		}
	}
	return -1
}
