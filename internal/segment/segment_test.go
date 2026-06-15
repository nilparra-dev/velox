// internal/segment/segment_test.go
package segment

import "testing"

func TestSplitCoversWholeRange(t *testing.T) {
	segs := Split(100, 4)
	if len(segs) != 4 {
		t.Fatalf("got %d segments, want 4", len(segs))
	}
	if segs[0].Start != 0 {
		t.Errorf("first segment starts at %d, want 0", segs[0].Start)
	}
	if segs[3].End != 99 {
		t.Errorf("last segment ends at %d, want 99", segs[3].End)
	}
	var total int64
	for i, s := range segs {
		if i > 0 && s.Start != segs[i-1].End+1 {
			t.Errorf("gap/overlap before segment %d: prev end %d, start %d", i, segs[i-1].End, s.Start)
		}
		total += s.Length()
	}
	if total != 100 {
		t.Errorf("segments cover %d bytes, want 100", total)
	}
}

func TestSplitUnevenRemainderGoesEarly(t *testing.T) {
	segs := Split(10, 3) // 3,3,3 + remainder 1 -> 4,3,3
	want := []int64{4, 3, 3}
	for i, s := range segs {
		if s.Length() != want[i] {
			t.Errorf("segment %d length %d, want %d", i, s.Length(), want[i])
		}
	}
}

func TestSplitClampsAndGuards(t *testing.T) {
	if got := Split(5, 8); len(got) != 5 {
		t.Errorf("n>size: got %d segments, want 5", len(got))
	}
	if got := Split(0, 4); got != nil {
		t.Errorf("size 0: got %v, want nil", got)
	}
	if got := Split(100, 0); len(got) != 1 {
		t.Errorf("n<1: got %d segments, want 1", len(got))
	}
}
