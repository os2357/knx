// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"strconv"
)

type OpCode byte

const (
	OC_INVALID   OpCode = iota // 0x0  0
	OC_I8                      // 0x1  1
	OC_I16                     // 0x2  2
	OC_I32                     // 0x3  3
	OC_I64                     // 0x4  4
	OC_U8                      // 0x5  5
	OC_U16                     // 0x6  6
	OC_U32                     // 0x7  7
	OC_U64                     // 0x8  8
	OC_F32                     // 0x9  9
	OC_F64                     // 0xA  10
	OC_BOOL                    // 0xB  11
	OC_FIXBYTES                // 0xC  12
	OC_FIXSTRING               // 0xD  13
	OC_STRING                  // 0xE  14
	OC_BYTES                   // 0xF  15
	OC_TIMESTAMP               // 0x10 16
	OC_TIME                    // 0x11 17
	OC_DATE                    // 0x12 18
	OC_I128                    // 0x13 19
	OC_I256                    // 0x14 20
	OC_D32                     // 0x15 21
	OC_D64                     // 0x16 22
	OC_D128                    // 0x17 23
	OC_D256                    // 0x18 24
	OC_BIGINT                  // 0x19 25
	OC_ENUM                    // 0x1A 26
	OC_SKIP                    // 0x1B 27
)

var (
	opCodeStrings = "__i8_i16_i32_i64_u8_u16_u32_u64_f32_f64_bool_fixbyte_fixstr_str_byte_timestamp_time_date_i128_i256_d32_d64_d128_d256_bigint_enum_skip"
	opCodeIdx     = [...]int{
		0,                           // invalid
		2, 5, 9, 13, 17, 20, 24, 28, // int/uint
		32, 36, // float
		40,     // bool
		45, 53, // fixed
		60, 64, // string, bytes
		69, 79, 84, // datetime
		89, 94, // i128/256
		99, 103, 107, 112, // decimals
		117, // bigint
		124, // enum
		129, // skip
		134, // end-of-string
	}
)

func (c OpCode) String() string {
	if int(c) >= len(opCodeIdx)-1 {
		return "opcode_" + strconv.Itoa(int(c))
	}
	return opCodeStrings[opCodeIdx[c] : opCodeIdx[c+1]-1]
}

func CompileCodecs(s *Schema) (enc []OpCode) {
	enc = make([]OpCode, len(s.Fields))
	for i, f := range s.Fields {
		enc[i] = CodecFor(f)
	}
	return
}

func CodecFor(f *Field) OpCode {
	if !f.IsVisible() {
		return OC_SKIP
	}

	switch f.Type {
	case FT_TIMESTAMP:
		return OC_TIMESTAMP

	case FT_DATE:
		return OC_DATE

	case FT_TIME:
		return OC_TIME

	case FT_I64:
		return OC_I64

	case FT_I32:
		return OC_I32

	case FT_I16:
		return OC_I16

	case FT_I8:
		return OC_I8

	case FT_U64:
		return OC_U64

	case FT_U32:
		return OC_U32

	case FT_U16:
		if f.IsEnum() {
			return OC_ENUM
		}
		return OC_U16

	case FT_U8:
		return OC_U8

	case FT_F64:
		return OC_F64

	case FT_F32:
		return OC_F32

	case FT_BOOL:
		return OC_BOOL

	case FT_STRING:
		if f.IsArray() {
			return OC_FIXSTRING
		} else {
			return OC_STRING
		}

	case FT_BYTES:
		if f.IsArray() {
			return OC_FIXBYTES
		} else {
			return OC_BYTES
		}

	case FT_I256:
		return OC_I256

	case FT_I128:
		return OC_I128

	case FT_D256:
		return OC_D256

	case FT_D128:
		return OC_D128

	case FT_D64:
		return OC_D64

	case FT_D32:
		return OC_D32

	case FT_BIGINT:
		return OC_BIGINT

	default:
		return OC_INVALID
	}
}
