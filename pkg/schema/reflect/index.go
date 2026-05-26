package reflect

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"blockwatch.cc/knoxdb/internal/hash"
)

func IndexesOf(m any) ([]*IndexSchema, error) {
	// need base schema which implictly parses indexes
	base, err := SchemaOf(m)
	if err != nil {
		return nil, err
	}
	return base.Indexes, nil
}

func MustIndexesOf(m any) []*IndexSchema {
	v, err := IndexesOf(m)
	if err != nil {
		panic(err)
	}
	return v
}

func IndexesOfTag(m any, tag string, base *Schema) ([]*IndexSchema, error) {
	// reflect type
	typ := reflect.Indirect(reflect.ValueOf(m)).Type()

	// prepare result
	res := make([]*IndexSchema, 0)

	// detect duplicate index names
	unique := make(map[string]struct{})

	// walk all fields and identify index tag, use a custom reflect
	// walker here because reflect.VisibleFields() won't return
	// fields with _ as name (which we use to define composite indexes)
	for _, f := range nestedStructFields(typ) {
		// skip private fields and embedded structs, promoted embedded fields
		// fields are still processed, only the anon struct itself is skipped
		if f.Tag.Get(tag) == "-" {
			continue
		}
		// explicitly keep fields with name _ as canonical way to add composite indexes
		if f.Name != "_" && (!f.IsExported() || f.Anonymous) {
			continue
		}

		// analyze field for index definitions
		index, err := reflectStructFieldForIndex(f, tag, base)
		if err != nil {
			return nil, err
		}
		if index == nil {
			continue
		}

		// catch duplicate index names
		if _, ok := unique[index.Name]; ok {
			return nil, fmt.Errorf("duplicate index name %q", index.Name)
		}

		// validate index schema conformance
		if err := index.Validate(); err != nil {
			return nil, err
		}

		res = append(res, index)
	}

	return res, nil
}

func nestedStructFields(typ reflect.Type) []reflect.StructField {
	fields := make([]reflect.StructField, 0)
	for i := range typ.NumField() {
		f := typ.Field(i)
		if f.Anonymous {
			t := f.Type
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			if t.Kind() == reflect.Struct {
				inner := nestedStructFields(t)
				for k := range inner {
					inner[k].Index = append([]int{i}, inner[k].Index...)
				}
				fields = append(fields, inner...)
			}
		} else {
			fields = append(fields, f)
		}
	}
	return fields
}

func reflectStructFieldForIndex(f reflect.StructField, tagName string, base *Schema) (*IndexSchema, error) {
	tag := f.Tag.Get(tagName)

	// skip fields with empty tags
	if len(tag) == 0 {
		return nil, nil
	}

	index := &IndexSchema{
		Name: f.Name,
		Base: base,
	}

	// extract alias name
	if n, _, _ := strings.Cut(tag, ","); n != "" {
		index.Name = n
	}

	// clean name
	index.Name = strings.ToLower(strings.TrimSpace(index.Name))

	// create index name when empty or _
	if index.Name == "" || index.Name == "_" {
		index.Name = "index_" + strconv.FormatUint(hash.Hash([]byte(tag)), 16)
	}

	// lookup current field in base schema when its type is not empty
	if f.Type != emptyType {
		field, ok := base.Find(index.Name)
		if !ok {
			return nil, fmt.Errorf("missing field %q", index.Name)
		}
		index.Fields = append(index.Fields, field)
		index.Name += "_index"
	}

	// prefix index name with base name
	index.Name = base.Name + "_" + index.Name

	// parse tags, we need at least a type
	tokens := strings.Split(tag, ",")

	for _, flag := range tokens[1:] {
		// parse index spec
		key, val, _ := strings.Cut(strings.TrimSpace(flag), "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "pk":
			index.Type = IT_PK
		case "index":
			switch val {
			case "hash":
				index.Type = IT_HASH
			case "int":
				index.Type = IT_INT
			case "pk":
				index.Type = IT_PK
			case "composite":
				index.Type = IT_COMPOSITE
			default:
				return nil, fmt.Errorf("unsupported index type %q", val)
			}
		case "fields":
			if index.Type != IT_COMPOSITE {
				return nil, fmt.Errorf("unsupported fields list for index type %q", index.Type)
			}
			// parse field names
			for fname := range strings.SplitSeq(val, "+") {
				field, ok := base.Find(fname)
				if !ok {
					return nil, fmt.Errorf("undefined indexed field %q in base schema %s", fname, base.Name)
				}
				index.Fields = append(index.Fields, field)
			}
		case "extra":
			// parse field names
			for fname := range strings.SplitSeq(val, "+") {
				field, ok := base.Find(fname)
				if !ok {
					return nil, fmt.Errorf("undefined extra field %q in base schema %s", fname, base.Name)
				}
				index.Extra = append(index.Extra, field)
			}
		}
	}

	// not every field may have an index
	if !index.Type.IsValid() {
		return nil, nil
	}

	return index, nil
}
