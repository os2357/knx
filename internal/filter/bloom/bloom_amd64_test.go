package bloom

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/pkg/util"
)

func TestFilterAddContainsUint32AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	filter := NewFilter(fsize)
	slice := make([]uint32, num)
	for i := 0; i < num; i++ {
		slice[i] = uint32(i)
	}
	filterAddManyUint32AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint32(uint32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint32(uint32(i)) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

func TestFilterAddContainsInt32AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	filter := NewFilter(fsize)
	slice := make([]int32, num)
	for i := 0; i < num; i++ {
		slice[i] = int32(i)
	}
	filterAddManyInt32AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsInt32(int32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsInt32(int32(i)) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

func TestFilterAddContainsUint64AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	filter := NewFilter(fsize)
	slice := make([]uint64, num)
	for i := 0; i < num; i++ {
		slice[i] = uint64(i)
	}
	filterAddManyUint64AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint64(uint64(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint64(uint64(i)) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

func TestFilterAddContainsInt64AVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	filter := NewFilter(fsize)
	slice := make([]int64, num)
	for i := 0; i < num; i++ {
		slice[i] = int64(i)
	}
	filterAddManyInt64AVX2(*filter, slice, xxHash32Seed)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsInt64(int64(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsInt64(int64(i)) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

func TestFilterMergeAVX2(t *testing.T) {
	if !util.UseAVX2 {
		t.Skip()
	}

	var num, fsize int
	if testing.Short() {
		num = 100000
		fsize = 1437758
	} else {
		num = 10000000
		fsize = 143775876
	}

	// These parameters will result, for 10M entries, with a bloom filter
	// with 0.001 false positive rate (1 in 1000 values will be incorrectly
	// identified as being present in the set).
	filter := NewFilter(fsize)
	slice := make([]uint32, num/2)
	for i := 0; i < num/2; i++ {
		slice[i] = uint32(i)
	}
	filterAddManyUint32AVX2(*filter, slice, xxHash32Seed)

	filter2 := NewFilter(fsize)
	for i := num / 2; i < num; i++ {
		slice[i-num/2] = uint32(i)
	}
	filterAddManyUint32AVX2(*filter2, slice, xxHash32Seed)

	filterMergeAVX2(filter.b, filter2.b)

	// None of the values inserted should ever be considered "not possibly in
	// the filter".
	for i := 0; i < num; i++ {
		if !filter.ContainsUint32(uint32(i)) {
			t.Fatalf("got false for value %v, expected true", i)
		}
	}

	// If we check for 100,000,000 values that we know are not present in the
	// filter then we might expect around 100,000 of them to be false positives.
	var fp int
	for i := num; i < 11*num; i++ {
		if filter.ContainsUint32(uint32(i)) {
			fp++
		}
	}

	if fp > num/10 {
		// If we're an order of magnitude off, then it's arguable that there
		// is a bug in the bloom filter.
		t.Fatalf("got %d false positives which is an error rate of %f, expected error rate <=0.001", fp, float64(fp)/100000000)
	}
	t.Logf("Bloom false positive error rate was %f", float64(fp)/float64(num)/10)
}

func BenchmarkFilterAddManyUint32AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, c := range benchCases {
		data := make([]uint32, c.n)
		for i := 0; i < c.n; i++ {
			data[i] = uint32(i)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(4 * int64(c.n))
			for i := 0; i < b.N; i++ {
				filterAddManyUint32AVX2(*filter, data, xxHash32Seed)
			}
		})

	}
}

func BenchmarkFilterAddManyUint64AVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, c := range benchCases {
		data := make([]uint64, c.n)
		for i := 0; i < c.n; i++ {
			data[i] = uint64(i)
		}

		filter := NewFilter(c.m)
		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(8 * int64(c.n))
			for i := 0; i < b.N; i++ {
				filterAddManyUint64AVX2(*filter, data, xxHash32Seed)
			}
		})

	}
}

func BenchmarkFilterMergeAVX2(b *testing.B) {
	if !util.UseAVX2 {
		b.Skip()
	}
	for _, c := range benchCases {
		data1 := make([]uint32, c.n)
		data2 := make([]uint32, c.n)
		for i := 0; i < c.n; i++ {
			data1[i] = uint32(i)
			data2[i] = uint32(c.n + i)
		}

		filter1 := NewFilter(c.m)
		filter2 := NewFilter(c.m)
		filter1.AddManyUint32(data1)
		filter2.AddManyUint32(data2)

		b.Run(fmt.Sprintf("m=%d_n=%d", c.m, c.n), func(b *testing.B) {
			b.SetBytes(int64(c.m >> 3))
			for i := 0; i < b.N; i++ {
				filterMergeAVX2(filter1.b, filter2.b)
			}
		})
	}
}
