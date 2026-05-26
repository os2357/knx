// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"fmt"
	"strings"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
)

type FieldType byte

const (
	FieldTypeInvalid FieldType = iota
	FieldTypeTimestamp
	FieldTypeInt64
	FieldTypeUint64
	FieldTypeFloat64
	FieldTypeBoolean
	FieldTypeString
	FieldTypeBytes
	FieldTypeInt32
	FieldTypeInt16
	FieldTypeInt8
	FieldTypeUint32
	FieldTypeUint16
	FieldTypeUint8
	FieldTypeFloat32
	FieldTypeInt256
	FieldTypeInt128
	FieldTypeDecimal256
	FieldTypeDecimal128
	FieldTypeDecimal64
	FieldTypeDecimal32
	FieldTypeBigint
	FieldTypeDate
	FieldTypeTime

	// TODO: new types
	FieldTypeText
	FieldTypeBlob
	FieldTypeList
	FieldTypeMap
	FieldTypeAny
)

const MAX_FIXED = uint16(1<<16 - 1)

var (
	fieldTypeString  = "__timestamp_int64_uint64_float64_boolean_string_bytes_int32_int16_int8_uint32_uint16_uint8_float32_int256_int128_decimal256_decimal128_decimal64_decimal32_bigint_date_time"
	fieldTypeIdx     = [...]int{0, 2, 12, 18, 25, 33, 41, 48, 54, 60, 66, 71, 78, 85, 91, 99, 106, 113, 124, 135, 145, 155, 162, 167, 172}
	fieldTypeReverse = map[string]FieldType{}

	fieldTypeWireSize = [...]int{
		FieldTypeInvalid:    0,
		FieldTypeTimestamp:  8, // i64
		FieldTypeInt64:      8,
		FieldTypeUint64:     8,
		FieldTypeFloat64:    8,
		FieldTypeBoolean:    1,
		FieldTypeString:     4, // minimum uint32 for size
		FieldTypeBytes:      4, // minimum uint32 for size
		FieldTypeInt32:      4,
		FieldTypeInt16:      2,
		FieldTypeInt8:       1,
		FieldTypeUint32:     4,
		FieldTypeUint16:     2,
		FieldTypeUint8:      1,
		FieldTypeFloat32:    4,
		FieldTypeInt256:     32,
		FieldTypeInt128:     16,
		FieldTypeDecimal256: 32,
		FieldTypeDecimal128: 16,
		FieldTypeDecimal64:  8,
		FieldTypeDecimal32:  4,
		FieldTypeBigint:     4, // stored as var bytes
		FieldTypeDate:       8, // i64
		FieldTypeTime:       8, // i64
	}
)

func init() {
	for t := FieldTypeInvalid; t <= FieldTypeTime; t++ {
		fieldTypeReverse[t.String()] = t
	}
	for f := FieldFlagPrimary; f <= FieldFlagAction; f++ {
		fieldFlagReverse[f.String()] = f
	}
}

func (t FieldType) IsValid() bool {
	return t > FieldTypeInvalid && t <= FieldTypeTime
}

func (t FieldType) String() string {
	return fieldTypeString[fieldTypeIdx[t] : fieldTypeIdx[t+1]-1]
}

func (t FieldType) Zero() any {
	switch t {
	case FT_TIMESTAMP, FT_DATE, FT_TIME:
		var t time.Time
		return t.UTC()
	case FT_I64:
		return int64(0)
	case FT_U64:
		return uint64(0)
	case FT_F64:
		return float64(0)
	case FT_BOOL:
		return false
	case FT_STRING:
		return ""
	case FT_BYTES:
		return []byte{}
	case FT_I32:
		return int32(0)
	case FT_I16:
		return int16(0)
	case FT_I8:
		return int8(0)
	case FT_U32:
		return uint32(0)
	case FT_U16:
		return uint16(0)
	case FT_U8:
		return uint8(0)
	case FT_F32:
		return float32(0)
	case FT_I256:
		return num.ZeroInt256
	case FT_I128:
		return num.ZeroInt128
	case FT_D256:
		return num.ZeroDecimal256
	case FT_D128:
		return num.ZeroDecimal128
	case FT_D64:
		return num.ZeroDecimal64
	case FT_D32:
		return num.ZeroDecimal32
	case FT_BIGINT:
		return num.BigZero
	default:
		return nil
	}
}

func ParseFieldType(s string) FieldType {
	return fieldTypeReverse[s]
}

func (t FieldType) Size() int {
	return fieldTypeWireSize[t]
}

func (t FieldType) BlockType() BlockType {
	return BlockTypes[t]
}

type FieldFlags byte

const (
	FieldFlagPrimary  FieldFlags = 1 << iota // primary key
	FieldFlagFixed                           // fixed length string/byte
	FieldFlagEnum                            // enumeration
	FieldFlagDeleted                         // is deleted, hide
	FieldFlagMetadata                        // field is metadata
	FieldFlagNullable                        // can be null
	FieldFlagTimebase                        // event time timestamp
	FieldFlagAction                          // field is CDC action metadata
)

var (
	fieldFlagNames   = "primary_fixed_enum_deleted_metadata_nullable_timebase_action"
	fieldFlagIdx     = [...]int{0, 8, 14, 19, 27, 36, 45, 54, 61}
	fieldFlagReverse = map[string]FieldFlags{}
)

func (i FieldFlags) Is(f FieldFlags) bool {
	return i&f > 0
}

func (i FieldFlags) String() string {
	if i == 0 {
		return ""
	}
	var b strings.Builder
	for p, k := 0, FieldFlags(1); p < 7; p, k = p+1, k<<1 {
		if i.Is(k) {
			start, end := fieldFlagIdx[p], fieldFlagIdx[p+1]-1
			if b.Len() > 0 {
				b.WriteString(",")
			}
			b.WriteString(fieldFlagNames[start:end])
		}
	}
	return b.String()
}

func ParseFieldFlag(s string) FieldFlags {
	return fieldFlagReverse[s]
}

func ValidateInt(name string, n, minVal, maxVal int) (int, error) {
	if n < minVal || (maxVal > 0 && n > maxVal) {
		return 0, fmt.Errorf("%s %d out of bounds [%d..%d]", name, n, minVal, maxVal)
	}
	return n, nil
}
