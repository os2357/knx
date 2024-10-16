package tests

var S8bBenchmarkSize = 6000

type BenchmarkData struct {
	Name string
	Fn   func(n int) func() []uint64
	Size int
}

var S8bBenchmarksUint64 = []BenchmarkData{
	{"0 bit", onesN(), S8bBenchmarkSize},
	{"1 bit", bitsN(1), S8bBenchmarkSize},
	{"2 bits", bitsN(2), S8bBenchmarkSize},
	{"3 bits", bitsN(3), S8bBenchmarkSize},
	{"4 bits", bitsN(4), S8bBenchmarkSize},
	{"5 bits", bitsN(5), S8bBenchmarkSize},
	{"6 bits", bitsN(6), S8bBenchmarkSize},
	{"7 bits", bitsN(7), S8bBenchmarkSize},
	{"8 bits", bitsN(8), S8bBenchmarkSize},
	{"10 bits", bitsN(10), S8bBenchmarkSize},
	{"12 bits", bitsN(12), S8bBenchmarkSize},
	{"15 bits", bitsN(15), S8bBenchmarkSize},
	{"20 bits", bitsN(20), S8bBenchmarkSize},
	{"30 bits", bitsN(30), S8bBenchmarkSize},
	{"60 bits", bitsN(60), S8bBenchmarkSize},
	{"combination", combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(20),
		bitsN(30),
		bitsN(60),
	), 15 * S8bBenchmarkSize},
}

var S8bBenchmarksUint32 = []BenchmarkData{
	{"0 bit", onesN(), S8bBenchmarkSize},
	{"1 bit", bitsN(1), S8bBenchmarkSize},
	{"2 bits", bitsN(2), S8bBenchmarkSize},
	{"3 bits", bitsN(3), S8bBenchmarkSize},
	{"4 bits", bitsN(4), S8bBenchmarkSize},
	{"5 bits", bitsN(5), S8bBenchmarkSize},
	{"6 bits", bitsN(6), S8bBenchmarkSize},
	{"7 bits", bitsN(7), S8bBenchmarkSize},
	{"8 bits", bitsN(8), S8bBenchmarkSize},
	{"10 bits", bitsN(10), S8bBenchmarkSize},
	{"12 bits", bitsN(12), S8bBenchmarkSize},
	{"15 bits", bitsN(15), S8bBenchmarkSize},
	{"20 bits", bitsN(20), S8bBenchmarkSize},
	{"30 bits", bitsN(30), S8bBenchmarkSize},
	{"60 bits", bitsN(32), S8bBenchmarkSize},
	{"combination", combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(20),
		bitsN(30),
		bitsN(32),
	), 15 * S8bBenchmarkSize},
}

var S8bBenchmarksUint16 = []BenchmarkData{
	{"0 bit", onesN(), S8bBenchmarkSize},
	{"1 bit", bitsN(1), S8bBenchmarkSize},
	{"2 bits", bitsN(2), S8bBenchmarkSize},
	{"3 bits", bitsN(3), S8bBenchmarkSize},
	{"4 bits", bitsN(4), S8bBenchmarkSize},
	{"5 bits", bitsN(5), S8bBenchmarkSize},
	{"6 bits", bitsN(6), S8bBenchmarkSize},
	{"7 bits", bitsN(7), S8bBenchmarkSize},
	{"8 bits", bitsN(8), S8bBenchmarkSize},
	{"10 bits", bitsN(10), S8bBenchmarkSize},
	{"12 bits", bitsN(12), S8bBenchmarkSize},
	{"15 bits", bitsN(15), S8bBenchmarkSize},
	{"20 bits", bitsN(16), S8bBenchmarkSize},
	{"combination", combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		bitsN(10),
		bitsN(12),
		bitsN(15),
		bitsN(16),
	), 15 * S8bBenchmarkSize},
}

var S8bBenchmarksUint8 = []BenchmarkData{
	{"0 bit", onesN(), S8bBenchmarkSize},
	{"1 bit", bitsN(1), S8bBenchmarkSize},
	{"2 bits", bitsN(2), S8bBenchmarkSize},
	{"3 bits", bitsN(3), S8bBenchmarkSize},
	{"4 bits", bitsN(4), S8bBenchmarkSize},
	{"5 bits", bitsN(5), S8bBenchmarkSize},
	{"6 bits", bitsN(6), S8bBenchmarkSize},
	{"7 bits", bitsN(7), S8bBenchmarkSize},
	{"8 bits", bitsN(8), S8bBenchmarkSize},
	{"combination", combineN(
		onesN(),
		bitsN(1),
		bitsN(2),
		bitsN(3),
		bitsN(4),
		bitsN(5),
		bitsN(6),
		bitsN(7),
		bitsN(8),
		),  15 * S8bBenchmarkSize},
}