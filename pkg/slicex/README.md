# Package SliceX

This package contains extended features over the standard Go slices package which helps perform operations on unique and sorted slices.

### SliceX (this package)

```go
import "blockwatch.cc/knoxdb/pkg/slicex"

// Unsorted Integer slices (using optimized Radix Sort)
Intersect[T Integer](s, t []T) []T 
IntersectRange[T Integer](s []T, from, to T) []T
Range[T Integer](s []T) (T, T, bool)
Remove[T Integer](s []T, t ...T) []T 
Union[T Integer](s, t []T) []T 
Unique[T Integer](s []T) []T 

// Unsorted Float slices (slices.Sort)
IntersectFloat[T Float](s, t []T) []T
IntersectRangeFloat[T Float](s []T, from, to T) []T 
UnionFloat[T Float](s, t []T) []T 
RangeFloat[T Float](s []T) (T, T, bool) 
RemoveFloat[T Float](s []T, t ...T) []T
UniqueFloat[T Float](s []T) []T 

// Pre-sorted Integer/Float slices
ContainsSorted[T Integer | Float](s []T, v T) bool
ContainsRangeSorted[T Integer | Float](s []T, from, to T) bool 
IntersectRangeSorted[T Integer | Float](s []T, from, to T) []T 
RangeSorted[T Integer | Float](s []T) (T, T, bool) 
RemoveSorted[T Integer | Float](s []T, t ...T) []T 
RemoveZeros[T Integer | Float](s []T) []T

// Unsorted byte slices
IntersectBytes(s, t [][]byte) [][]byte
IntersectRangeBytes(s [][]byte, from, to []byte) [][]byte
RangeBytes(s [][]byte) ([]byte, []byte)
RemoveBytes(s [][]byte, t ...[]byte) [][]byte
UnionBytes(s, t [][]byte) [][]byte
UniqueBytes(s [][]byte) [][]byte

// Pre-sorted byte slices
ContainsBytesSorted(s [][]byte, val []byte) bool
ContainsBytesRangeSorted(s [][]byte, from, to []byte) bool
RangeBytesSorted(s [][]byte) ([]byte, []byte)

// Strings
UniqueStrings(s []string) []string
UniqueStringsStable(s []string) []string
```


## Algorithms

Vector algorithms for sorted slices based on binary search from Go's sort package. All ordered/comparable types `constraints.Ordered` (signed, unsigned, float, string, arrays) are supported by generic functions and special types via `OrderedBytes` and `OrderedStrings`.

Algorithms are available for
- `unique(s []T) []T`
- `contains[T](s []T, e T, optimzed bool) bool`
- `containsRange[T](s []T from, to T) bool`
- `intersect[T](dst, x, y []T) []T`
- `mergeUnique[T](s [T], v ...T) []T`

## Range coverage algorithm

Checks if a sparse sorted slice contains any value(s) in the closed interval `[from, to]`. This is used when deciding whether a pack contains any of the values from an IN condition based on the packs min/max range.
```
val slice ->       |- - - - - - - - - -|
                   .                   .
Range A      [--]  .                   .
Range B.1       [--]                   .
Range B.2      [-------]               .
Range B.3          [--]                .
Range C.1          .       [--]        .            // some values in range
Range C.2          .       [--]        .            // no values in range
Range D.1          .                [--]
Range D.2          .               [-------]
Range D.3          .                   [--]
Range E            .                   .  [--]
Range F     [-----------------------------------]
```