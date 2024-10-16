// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"fmt"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/hash"
	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/vec"
)

type packBenchmarkSize struct {
	name string
	l    int
}

// generates n slices of length u
func randByteSlice(n, u int) [][]byte {
	s := make([][]byte, n)
	for i := 0; i < n; i++ {
		s[i] = randBytes(u)
	}
	return s
}

func randBytes(n int) []byte {
	v := make([]byte, n)
	for i, _ := range v {
		v[i] = byte(rand.Intn(256))
	}
	return v
}

var packBenchmarkSizes = []packBenchmarkSize{
	// {"32", 32},
	// {"128", 128},
	// {"1K", 1 * 1024},
	{"16K", 16 * 1024},
	{"32K", 32 * 1024},
	{"64K", 64 * 1024},
	// {"128K", 128 * 1024},
	// {"1M", 1024 * 1024},
	// {"16M", 16 * 1024 * 1024},
}

var (
	benchMaxConds = 6
	benchFields   = FieldList{
		&Field{
			Index: 0,
			Name:  "uint64",
			Alias: "one",
			Type:  FieldTypeUint64,
			Flags: FlagPrimary,
		},
		&Field{
			Index: 1,
			Name:  "int64",
			Alias: "two",
			Type:  FieldTypeInt64,
			Flags: 0,
		},
		&Field{
			Index: 2,
			Name:  "float64",
			Alias: "three",
			Type:  FieldTypeFloat64,
			Flags: 0,
		},
		&Field{
			Index: 3,
			Name:  "uint8",
			Alias: "four",
			Type:  FieldTypeUint8,
			Flags: 0,
		},
		&Field{
			Index: 4,
			Name:  "uint32",
			Alias: "five",
			Type:  FieldTypeUint32,
			Flags: 0,
		},
		&Field{
			Index: 5,
			Name:  "int16",
			Alias: "six",
			Type:  FieldTypeInt16,
			Flags: 0,
		},
		// used for IN bechmarks
		&Field{
			Index: 6,
			Name:  "bytes",
			Alias: "seven",
			Type:  FieldTypeBytes,
			Flags: 0,
		},
	}
	benchModes = []FilterMode{
		FilterModeGt,
		FilterModeLt,
		FilterModeGt,
		FilterModeLt,
		FilterModeGt,
		FilterModeLt,
	}
)

func makeTestPackage(sz int) *Package {
	pkg := NewPackage(sz, nil)
	pkg.InitFields(benchFields, nil)
	for i := 0; i < sz; i++ {
		pkg.Grow(1)
		pkg.SetFieldAt(0, i, uint64(i+1))
		pkg.SetFieldAt(1, i, rand.Intn(10000000))
		pkg.SetFieldAt(2, i, rand.Float64())
		pkg.SetFieldAt(3, i, uint8(rand.Intn(256)))
		pkg.SetFieldAt(4, i, uint32(rand.Intn(1<<32-1)))
		pkg.SetFieldAt(5, i, int16(rand.Intn(1<<16-1)))
		pkg.SetFieldAt(6, i, randBytes(32))
	}
	return pkg
}

func makeAndConds(c, n int) ConditionTreeNode {
	conds := ConditionTreeNode{}
	for i := 0; i < c; i++ {
		var val interface{}
		switch benchFields[i].Type {
		case FieldTypeFloat64:
			val = float64(n / (i + 1))
		case FieldTypeInt64:
			val = int64(n / (i + 1))
		case FieldTypeUint64:
			val = uint64(n / (i + 1))
		case FieldTypeUint32:
			val = uint32(n / (i + 1))
		case FieldTypeUint16:
			val = uint16(n / (i + 1))
		case FieldTypeUint8:
			val = uint8(n % (i + 1))
		}
		conds.AddAndCondition(&Condition{
			Field: benchFields[i],
			Mode:  benchModes[i],
			Value: val,
		})
	}
	conds.Compile()
	return conds
}

func BenchmarkAndConditionRun(B *testing.B) {
	for i := 1; i <= benchMaxConds; i += 2 {
		for _, n := range packBenchmarkSizes {
			B.Run(fmt.Sprintf("%s_%dC", n.name, i), func(B *testing.B) {
				pkg := makeTestPackage(n.l)
				conds := makeAndConds(i, n.l)
				B.ResetTimer()
				B.ReportAllocs()
				bits := conds.MatchPack(pkg, PackInfo{})
				B.SetBytes(int64(bits.Count()))
				for b := 0; b < B.N; b++ {
					// this is the core of a matching loop design
					bits = conds.MatchPack(pkg, PackInfo{})
					for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
						// handle rows
					}
					bits.Close()
				}
			})
		}
	}
}

func BenchmarkAndConditionIndexes(B *testing.B) {
	for i := 1; i <= benchMaxConds; i += 2 {
		for _, n := range packBenchmarkSizes {
			B.Run(fmt.Sprintf("%s_%dC", n.name, i), func(B *testing.B) {
				pkg := makeTestPackage(n.l)
				conds := makeAndConds(i, n.l)
				slice := make([]uint32, 0, n.l)
				B.ResetTimer()
				B.ReportAllocs()
				bits := conds.MatchPack(pkg, PackInfo{})
				B.SetBytes(int64(bits.Count()))
				for b := 0; b < B.N; b++ {
					// this is the core of a matching loop design
					bits = conds.MatchPack(pkg, PackInfo{})
					_ = bits.IndexesU32(slice)
					bits.Close()
				}
			})
		}
	}
}

func BenchmarkFNVHash(B *testing.B) {
	testslice := randByteSlice(64*1024, 32)
	B.ResetTimer()
	B.ReportAllocs()
	for b := 0; b < B.N; b++ {
		h := hash.NewInlineFNV64a()
		h.Write(testslice[b%len(testslice)])
	}
}

func BenchmarkXXHash(B *testing.B) {
	testslice := randByteSlice(64*1024, 32)
	B.ResetTimer()
	B.ReportAllocs()
	for b := 0; b < B.N; b++ {
		xxhash.Sum64(testslice[b%len(testslice)])
	}
}

func BenchmarkInConditionRun(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			// build IN slice of size 0.1*pack.Size() from
			// - 5% (min 2) pack values
			// - 5% random values
			checkN := max(n.l/20, 2)
			inSlice := make([][]byte, 0, 2*checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				buf, err := pkg.BytesAt(6, rand.Intn(n.l))
				if err != nil {
					B.Fatalf("error with pack bytes: %v", err)
				}
				inSlice = append(inSlice, buf)
			}
			// add random values
			inSlice = append(inSlice, randByteSlice(checkN, 32)...)
			conds := ConditionTreeNode{}
			conds.AddAndCondition(&Condition{
				Field: benchFields[6],
				Mode:  FilterModeIn,
				Value: inSlice,
			})
			conds.Compile()
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 32)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				for idx, length := bits.Run(0); idx >= 0; idx, length = bits.Run(idx + length) {
					// handle rows
				}
				bits.Close()
			}
		})
	}
}

func BenchmarkInConditionIndexes(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pkg := makeTestPackage(n.l)
			// build IN slice of size 0.1*pack.Size() from
			// - 5% (min 2) pack values
			// - 5% random values
			checkN := max(n.l/20, 2)
			inSlice := make([][]byte, 0, 2*checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				buf, err := pkg.BytesAt(6, rand.Intn(n.l))
				if err != nil {
					B.Fatalf("error with pack bytes: %v", err)
				}
				inSlice = append(inSlice, buf)
			}
			// add random values
			inSlice = append(inSlice, randByteSlice(checkN, 32)...)

			conds := ConditionTreeNode{}
			conds.AddAndCondition(&Condition{
				Field: benchFields[6],
				Mode:  FilterModeIn,
				Value: inSlice,
			})
			conds.Compile()
			slice := make([]uint32, 0, n.l)
			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 32)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				bits := conds.MatchPack(pkg, PackInfo{})
				_ = bits.IndexesU32(slice)
				bits.Close()
			}
		})
	}
}

func loopCheck(in, pk []uint64, bits *vec.Bitset) *vec.Bitset {
	for i, p, il, pl := 0, 0, len(in), len(pk); i < il && p < pl; {
		if pk[p] < in[i] {
			p++
		}
		if p == pl {
			break
		}
		if pk[p] > in[i] {
			i++
		}
		if i == il {
			break
		}
		if pk[p] == in[i] {
			bits.Set(p)
			i++
		}
	}
	return bits
}

func nestedLoopCheck(in, pk []uint64, bits *vec.Bitset) *vec.Bitset {
	maxin, maxpk := in[len(in)-1], pk[len(pk)-1]
	for i, p, il, pl := 0, 0, len(in), len(pk); i < il; {
		if pk[p] > maxin || maxpk < in[i] {
			// no more matches in this pack
			break
		}
		for pk[p] < in[i] && p < pl {
			p++
		}
		if p == pl {
			break
		}
		for pk[p] > in[i] && i < il {
			i++
		}
		if i == il {
			break
		}
		if pk[p] == in[i] {
			bits.Set(p)
			i++
		}
	}
	return bits
}

func mapCheck(in map[uint64]struct{}, pk []uint64, bits *vec.Bitset) *vec.Bitset {
	for i, v := range pk {
		if _, ok := in[v]; !ok {
			bits.Set(i)
		}
	}
	return bits
}

func BenchmarkInLoop(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pk := make([]uint64, n.l)
			for i := 0; i < n.l; i++ {
				pk[i] = uint64(i + 1)
			}
			// build IN slice of size 0.1*pack.Size() from
			// - 10% (min 2) pack values
			checkN := max(n.l/10, 2)
			inSlice := make([]uint64, checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				inSlice[i] = pk[rand.Intn(n.l)]
			}
			// unique and sort
			inSlice = vec.UniqueUint64Slice(inSlice)

			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				loopCheck(inSlice, pk, vec.NewBitset(n.l)).Close()
			}
		})
	}
}

func BenchmarkInNestedLoop(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pk := make([]uint64, n.l)
			for i := 0; i < n.l; i++ {
				pk[i] = uint64(i + 1)
			}
			// build IN slice of size 0.1*pack.Size() from
			// - 10% (min 2) pack values
			checkN := max(n.l/10, 2)
			inSlice := make([]uint64, checkN)
			for i := 0; i < checkN; i++ {
				// add existing values
				inSlice[i] = pk[rand.Intn(n.l)]
			}
			// unique and sort
			inSlice = vec.UniqueUint64Slice(inSlice)

			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				nestedLoopCheck(inSlice, pk, vec.NewBitset(n.l)).Close()
			}
		})
	}
}

func BenchmarkInMap(B *testing.B) {
	for _, n := range packBenchmarkSizes {
		B.Run(n.name, func(B *testing.B) {
			pk := make([]uint64, n.l)
			inmap := make(map[uint64]struct{}, n.l)
			for i := 0; i < n.l; i++ {
				pk[i] = uint64(i + 1)
			}
			// build IN slice of size 0.1*pack.Size() from
			// - 10% (min 2) pack values
			checkN := max(n.l/10, 2)
			for i := 0; i < checkN; i++ {
				// add existing values
				inmap[pk[rand.Intn(n.l)]] = struct{}{}
			}

			B.ResetTimer()
			B.ReportAllocs()
			B.SetBytes(int64(n.l) * 8)
			for b := 0; b < B.N; b++ {
				// this is the core of a new matching loop design
				mapCheck(inmap, pk, vec.NewBitset(n.l)).Close()
			}
		})
	}
}
