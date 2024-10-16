// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"fmt"
	"math/rand"
	"testing"

	"blockwatch.cc/knoxdb/vec"
)

func randUint64Slice(n, u int) []uint64 {
	s := make([]uint64, n*u)
	for i := 0; i < n; i++ {
		s[i] = uint64(rand.Int63())
	}
	for i := 0; i < u; i++ {
		s = append(s, s[:n]...)
	}
	return s
}

func makeSortedPackInfoList(n int) PackInfoList {
	// generate random values
	values := randUint64Slice(n, 1)

	// strip duplicates and sort
	values = vec.UniqueUint64Slice(values)

	// generate pack packers
	packs := make(PackInfoList, 0)
	for i, v := range values {
		max := uint64(v + 1000)
		if i < len(values)-1 {
			max = values[i+1] - 1
		}
		pack := PackInfo{
			Key:     uint32(i),
			NValues: 1,
			Blocks: BlockInfoList{
				BlockInfo{
					MinValue: uint64(v),
					MaxValue: max,
				},
			},
		}
		packs = append(packs, pack)
	}
	return packs
}

func makeUnsortedPackInfoList(n int) PackInfoList {
	// generate random values
	values := randUint64Slice(n, 1)

	// strip duplicates and sort
	values = vec.UniqueUint64Slice(values)
	maxvalues := make([]uint64, len(values))
	minvalues := make([]uint64, len(values))

	// shuffle but keep original max values
	for i, v := range rand.Perm(len(values)) {
		max := uint64(values[v] + 1000)
		if v < len(values)-1 {
			max = values[v+1] - 1
		}
		minvalues[i] = values[v]
		maxvalues[i] = max
	}

	// generate pack packers
	packs := make(PackInfoList, 0)
	for i, v := range minvalues {
		pack := PackInfo{
			Key:     uint32(i),
			NValues: 1,
			Blocks: BlockInfoList{
				BlockInfo{
					MinValue: v,
					MaxValue: maxvalues[i],
				},
			},
		}
		packs = append(packs, pack)
	}
	return packs
}

type packIndexTestListItem struct {
	Key uint32
	Min uint64
	Max uint64
}

type packIndexTestValueItem struct {
	Value  uint64
	ExpKey uint32
	ExpMin uint64
	ExpMax uint64
}

type packIndexTestCase struct {
	Name   string
	Info   PackInfo // used for Add
	Key    uint32   // used for Remove
	List   []packIndexTestListItem
	Values []packIndexTestValueItem
}

var packIndexTestCases = []packIndexTestCase{
	packIndexTestCase{
		Name: "single",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// after max match
			packIndexTestValueItem{
				Value:  3000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
		},
	},
	packIndexTestCase{
		Name: "multi-sorted",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: 2,
				Min: 3000,
				Max: 4000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min first match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			packIndexTestValueItem{
				Value:  1500,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max first match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			packIndexTestValueItem{
				Value:  2001,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min second -1
			packIndexTestValueItem{
				Value:  2999,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// within second pack
			packIndexTestValueItem{
				Value:  3500,
				ExpKey: 2,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// after max match
			packIndexTestValueItem{
				Value:  5000,
				ExpKey: 2,
				ExpMin: 3000,
				ExpMax: 4000,
			},
		},
	},
	packIndexTestCase{
		Name: "multi-duplicate",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: 1,
				Min: 1000,
				Max: 1000,
			},
			packIndexTestListItem{
				Key: 2,
				Min: 1000,
				Max: 1000,
			},
			packIndexTestListItem{
				Key: 3,
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: 4,
				Min: 2001,
				Max: 3000,
			},
		},
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 1000,
			},
			// exact min match, should return last pack where min equals
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			packIndexTestValueItem{
				Value:  1500,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			packIndexTestValueItem{
				Value:  2001,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// max -1
			packIndexTestValueItem{
				Value:  2999,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// after max match
			packIndexTestValueItem{
				Value:  5000,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
		},
	},
}

func buildPackHeader(key uint32, min, max uint64) PackInfo {
	return PackInfo{
		Key:     key,
		NValues: 1,
		Blocks: BlockInfoList{
			BlockInfo{
				MinValue: min,
				MaxValue: max,
			},
		},
	}
}

func buildPackHeaderInt(key int, min, max uint64) PackInfo {
	return buildPackHeader(uint32(key), min, max)
}

func buildPackHeaderList(items []packIndexTestListItem) PackInfoList {
	packs := make(PackInfoList, len(items))
	for i, v := range items {
		packs[i] = buildPackHeader(v.Key, v.Min, v.Max)
	}
	return packs
}

func TestPackHeaderBest(t *testing.T) {
	for _, c := range packIndexTestCases {
		idx := NewPackHeader(buildPackHeaderList(c.List), 0, 1)
		for _, v := range c.Values {
			p, min, max, _, _ := idx.Best(v.Value)
			if exp, got := v.ExpKey, idx.packs[p].Key; exp != got {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

var packListAddTestCases = []packIndexTestCase{
	packIndexTestCase{
		Name: "add_middle",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: 2,
				Min: 5000,
				Max: 6000,
			},
		},
		Info: buildPackHeaderInt(3, 3000, 4000),
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// added pack min match
			packIndexTestValueItem{
				Value:  3000,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack middle match
			packIndexTestValueItem{
				Value:  3500,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack max match
			packIndexTestValueItem{
				Value:  4000,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// added pack max + 1 match
			packIndexTestValueItem{
				Value:  4001,
				ExpKey: 3,
				ExpMin: 3000,
				ExpMax: 4000,
			},
			// last pack match
			packIndexTestValueItem{
				Value:  5500,
				ExpKey: 2,
				ExpMin: 5000,
				ExpMax: 6000,
			},
		},
	},
	packIndexTestCase{
		Name: "add-duplicate",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: 1,
				Min: 1000,
				Max: 1000,
			},
			packIndexTestListItem{
				Key: 2,
				Min: 1000,
				Max: 1000,
			},
			packIndexTestListItem{
				Key: 3,
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: 4,
				Min: 2001,
				Max: 3000,
			},
		},
		Info: buildPackHeaderInt(5, 1000, 1000),
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 1000,
			},
			// exact min match, should return last pack where min equals
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// min between match
			packIndexTestValueItem{
				Value:  1500,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: 3,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// max first +1
			packIndexTestValueItem{
				Value:  2001,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// max -1
			packIndexTestValueItem{
				Value:  2999,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
			// after max match
			packIndexTestValueItem{
				Value:  5000,
				ExpKey: 4,
				ExpMin: 2001,
				ExpMax: 3000,
			},
		},
	},
}

func TestPackHeaderAfterAdd(t *testing.T) {
	for _, c := range packListAddTestCases {
		idx := NewPackHeader(buildPackHeaderList(c.List), 0, 1)
		idx.AddOrUpdate(c.Info)
		for _, v := range c.Values {
			p, min, max, _, _ := idx.Best(v.Value)
			if exp, got := v.ExpKey, idx.packs[p].Key; exp != got {
				t.Errorf("invalid pack selected by exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

var packListRemoveTestCases = []packIndexTestCase{
	packIndexTestCase{
		Name: "remove_middle",
		List: []packIndexTestListItem{
			packIndexTestListItem{
				Key: 1,
				Min: 1000,
				Max: 2000,
			},
			packIndexTestListItem{
				Key: 2,
				Min: 3000,
				Max: 4000,
			},
			packIndexTestListItem{
				Key: 3,
				Min: 5000,
				Max: 6000,
			},
		},
		Key: 2,
		Values: []packIndexTestValueItem{
			// before min match (should return first pack in list)
			packIndexTestValueItem{
				Value:  100,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact min match
			packIndexTestValueItem{
				Value:  1000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// exact max match
			packIndexTestValueItem{
				Value:  2000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// removed pack value redirected to min
			packIndexTestValueItem{
				Value:  3000,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// removed pack value closer to previous
			packIndexTestValueItem{
				Value:  3501,
				ExpKey: 1,
				ExpMin: 1000,
				ExpMax: 2000,
			},
			// last pack match
			packIndexTestValueItem{
				Value:  5500,
				ExpKey: 3,
				ExpMin: 5000,
				ExpMax: 6000,
			},
		},
	},
}

func TestPackHeaderAfterRemove(t *testing.T) {
	for _, c := range packListRemoveTestCases {
		idx := NewPackHeader(buildPackHeaderList(c.List), 0, 1)
		idx.Remove(c.Key)
		for _, v := range c.Values {
			p, min, max, _, _ := idx.Best(v.Value)
			if exp, got := v.ExpKey, idx.packs[p].Key; exp != got {
				// min, max := v1.MinMax(p1)
				t.Errorf("invalid pack selected by exp=%08x [%d/%d] got=%08x [%d/%d] for value %d",
					exp, v.ExpMin, v.ExpMax, got, min, max, v.Value)
			}
		}
	}
}

type benchmarkSize struct {
	name string
	l    int
}

var bestPackBenchmarkSizes = []benchmarkSize{
	{"1", 1},
	{"10", 10},
	{"1K", 1024},
	{"16k", 16 * 1024},
	{"32k", 32 * 1024},
	{"64k", 64 * 1024},
}

func BenchmarkPackHeaderBestSorted(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackHeader(makeSortedPackInfoList(n.l), 0, 1)
			max := v2.packs[v2.pos[len(v2.pos)-1]].Blocks[v2.pkidx].MaxValue.(uint64)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v2.Best(uint64(rand.Int63n(int64(max)) + 1))
			}
		})
	}
}

func BenchmarkPackHeaderBestUnsorted(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackHeader(makeUnsortedPackInfoList(n.l), 0, 1)
			max := v2.packs[v2.pos[len(v2.pos)-1]].Blocks[v2.pkidx].MaxValue.(uint64)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				v2.Best(uint64(rand.Int63n(int64(max)) + 1))
			}
		})
	}
}

func BenchmarkPackHeaderAppend(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackHeader(makeSortedPackInfoList(n.l), 0, 1)
			l := v2.Len()
			_, max := v2.MinMax(l - 1)
			// pack := buildPackHeaderInt(l, max+1, max+1000)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// append to end of list
				v2.AddOrUpdate(buildPackHeaderInt(i+l, max+1, max+1000))
				max += 1000
			}
		})
	}
}

func BenchmarkPackHeaderAdd(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackHeader(makeSortedPackInfoList(n.l), 0, 1)
			l := v2.Len() / 2
			min, max := v2.MinMax(l)
			pack := buildPackHeaderInt(l, min, max)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// remove and re-append an existing packer
				v2.Remove(pack.Key)
				v2.AddOrUpdate(pack)
			}
		})
	}
}

func BenchmarkPackHeaderUpdate(B *testing.B) {
	for _, n := range bestPackBenchmarkSizes {
		B.Run(fmt.Sprintf("%s", n.name), func(B *testing.B) {
			v2 := NewPackHeader(makeSortedPackInfoList(n.l), 0, 1)
			pos := v2.Len() / 2
			min, max := v2.MinMax(pos)
			B.ResetTimer()
			for i := 0; i < B.N; i++ {
				// replace the middle pack, toggle min between min and min+1
				// to force updates
				setmin := min
				if i&0x1 == 1 {
					setmin = min + 1
				}
				v2.AddOrUpdate(buildPackHeaderInt(pos, setmin, max))
			}
		})
	}
}
