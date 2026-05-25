// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

// Note: uses 5 bit encoding (max 32 values)
type BlockType byte

const (
	BlockInvalid BlockType = iota // 0
	BlockInt64                    // 1
	BlockInt32                    // 2
	BlockInt16                    // 3
	BlockInt8                     // 4
	BlockUint64                   // 5
	BlockUint32                   // 6
	BlockUint16                   // 7
	BlockUint8                    // 8
	BlockFloat64                  // 9
	BlockFloat32                  // 10
	BlockBool                     // 11
	BlockBytes                    // 12
	BlockInt128                   // 13
	BlockInt256                   // 14
)

type BlockCompression byte

const (
	BlockCompressNone BlockCompression = iota
	BlockCompressSnappy
	BlockCompressLZ4
	BlockCompressZstd
)

func (i BlockCompression) Is(f BlockCompression) bool {
	return i&f > 0
}

var (
	blockTypeNames        = "__i64_i32_i16_i8_u64_u32_u16_u8_f64_f32_bool_bytes_i128_i256"
	blockTypeNamesOfs     = []int{0, 2, 6, 10, 14, 17, 21, 25, 29, 32, 36, 40, 45, 51, 56, 61}
	blockCompressNames    = "__snappy_lz4_zstd"
	blockCompressNamesOfs = []int{0, 2, 7, 13, 18}

	blockTypeDataSize = [...]int{
		BlockInvalid: 0,
		BlockInt64:   8,
		BlockInt32:   4,
		BlockInt16:   2,
		BlockInt8:    1,
		BlockUint64:  8,
		BlockUint32:  4,
		BlockUint16:  2,
		BlockUint8:   1,
		BlockFloat64: 8,
		BlockFloat32: 4,
		BlockBool:    1,
		BlockBytes:   0, // fixed or variable
		BlockInt128:  16,
		BlockInt256:  32,
	}

	BlockTypes = [...]BlockType{
		FieldTypeInvalid:    BlockInvalid,
		FieldTypeTimestamp:  BlockInt64,
		FieldTypeInt64:      BlockInt64,
		FieldTypeUint64:     BlockUint64,
		FieldTypeFloat64:    BlockFloat64,
		FieldTypeBoolean:    BlockBool,
		FieldTypeString:     BlockBytes,
		FieldTypeBytes:      BlockBytes,
		FieldTypeInt32:      BlockInt32,
		FieldTypeInt16:      BlockInt16,
		FieldTypeInt8:       BlockInt8,
		FieldTypeUint32:     BlockUint32,
		FieldTypeUint16:     BlockUint16,
		FieldTypeUint8:      BlockUint8,
		FieldTypeFloat32:    BlockFloat32,
		FieldTypeInt256:     BlockInt256,
		FieldTypeInt128:     BlockInt128,
		FieldTypeDecimal256: BlockInt256,
		FieldTypeDecimal128: BlockInt128,
		FieldTypeDecimal64:  BlockInt64,
		FieldTypeDecimal32:  BlockInt32,
		FieldTypeBigint:     BlockBytes,
		FieldTypeDate:       BlockInt64,
		FieldTypeTime:       BlockInt64,
	}
)

func (t BlockCompression) String() string {
	return blockCompressNames[blockCompressNamesOfs[t] : blockCompressNamesOfs[t+1]-1]
}

func (t BlockType) IsValid() bool {
	return t > 0 && t <= BlockInt256
}

func (t BlockType) String() string {
	if !t.IsValid() {
		return "invalid block type"
	}
	return blockTypeNames[blockTypeNamesOfs[t] : blockTypeNamesOfs[t+1]-1]
}

func (t BlockType) Size() int {
	if int(t) < len(blockTypeDataSize) {
		return blockTypeDataSize[t]
	}
	return 0
}
