// internal/segment/segment.go
package segment

// Segment is a contiguous, inclusive byte range [Start, End].
type Segment struct {
	Index int
	Start int64
	End   int64
}

// Length returns the number of bytes in the segment.
func (s Segment) Length() int64 { return s.End - s.Start + 1 }

// Split divides [0, size) into n contiguous segments with inclusive offsets.
// n < 1 is treated as 1; n larger than size is clamped to size. Remainder
// bytes are distributed to the earliest segments, so lengths differ by at
// most one. Returns nil when size <= 0.
func Split(size int64, n int) []Segment {
	if size <= 0 {
		return nil
	}
	if n < 1 {
		n = 1
	}
	if int64(n) > size {
		n = int(size)
	}
	base := size / int64(n)
	rem := size % int64(n)
	segs := make([]Segment, 0, n)
	var start int64
	for i := 0; i < n; i++ {
		length := base
		if int64(i) < rem {
			length++
		}
		end := start + length - 1
		segs = append(segs, Segment{Index: i, Start: start, End: end})
		start = end + 1
	}
	return segs
}
