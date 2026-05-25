// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cast

import (
	"encoding"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

func ToString(s any) string {
	if s == nil {
		return ""
	}
	switch v := s.(type) {
	case string:
		return v
	case encoding.TextMarshaler:
		buf, err := v.MarshalText()
		if err == nil {
			return string(buf)
		}
		return ""
	case Stringer:
		return v.String()
	default:
		if v, err := ToRawString(s); err == nil {
			return v
		}
		return fmt.Sprintf("%v", s)
	}
}

func ToRawString(t any) (string, error) {
	val := reflect.Indirect(reflect.ValueOf(t))
	if !val.IsValid() {
		return "", nil
	}
	typ := val.Type()
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(val.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'g', -1, val.Type().Bits()), nil
	case reflect.String:
		return val.String(), nil
	case reflect.Bool:
		return strconv.FormatBool(val.Bool()), nil
	case reflect.Array:
		if typ.Elem().Kind() != reflect.Uint8 {
			break
		}
		// [...]byte
		var b []byte
		if val.CanAddr() {
			b = val.Slice(0, val.Len()).Bytes()
		} else {
			b = make([]byte, val.Len())
			reflect.Copy(reflect.ValueOf(b), val)
		}
		return hex.EncodeToString(b), nil
	case reflect.Slice:
		switch typ.Elem().Kind() {
		case reflect.Slice:
			var b strings.Builder
			for i := 0; i < val.Len(); i++ {
				if i > 0 {
					b.WriteByte(',')
				}
				b.WriteString(ToString(val.Index(i).Interface()))
			}
			return b.String(), nil
		case reflect.Uint8:
			// []byte
			b := val.Bytes()
			if s := string(b); IsASCII(s) {
				return s, nil
			}
			return hex.EncodeToString(b), nil
		}
	case reflect.Struct:
		var b strings.Builder
		for i, l := 0, val.NumField(); i < l; i++ {
			f := val.Field(i)
			b.WriteString(typ.Field(i).Name)
			b.WriteByte(':')
			b.WriteString(ToString(f.Interface()))
			b.WriteByte(' ')
		}
		return b.String(), nil
	}
	return "", fmt.Errorf("no method for converting type %s (%v) to string", typ, val.Kind())
}

func IsASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 || s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}
