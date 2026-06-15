package chunk

import "testing"

func TestPlanFixedSizeWithShortLast(t *testing.T) {
	chunks := Plan(100, 30) // 30,30,30,10
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(chunks))
	}
	if chunks[0].Start != 0 {
		t.Errorf("first chunk Start = %d, want 0", chunks[0].Start)
	}
	want := []int64{30, 30, 30, 10}
	var total int64
	for i, c := range chunks {
		if c.Index != i {
			t.Errorf("chunk %d has Index %d", i, c.Index)
		}
		if c.Length() != want[i] {
			t.Errorf("chunk %d length %d, want %d", i, c.Length(), want[i])
		}
		if i > 0 && c.Start != chunks[i-1].End+1 {
			t.Errorf("gap/overlap before chunk %d", i)
		}
		total += c.Length()
	}
	if total != 100 {
		t.Errorf("chunks cover %d bytes, want 100", total)
	}
	if chunks[3].End != 99 {
		t.Errorf("last chunk ends at %d, want 99", chunks[3].End)
	}
}

func TestPlanGuards(t *testing.T) {
	if got := Plan(100, 100); len(got) != 1 || got[0].End != 99 {
		t.Errorf("chunkSize==size: want 1 chunk [0,99], got %v", got)
	}
	if got := Plan(100, 500); len(got) != 1 || got[0].End != 99 {
		t.Errorf("chunkSize>size: want 1 chunk [0,99], got %v", got)
	}
	if got := Plan(50, 0); len(got) != 1 || got[0].Length() != 50 {
		t.Errorf("chunkSize<1: want 1 chunk of 50, got %v", got)
	}
	if got := Plan(50, -1); len(got) != 1 || got[0].Length() != 50 {
		t.Errorf("negative chunkSize: want 1 chunk of 50, got %v", got)
	}
	if got := Plan(0, 10); got != nil {
		t.Errorf("size 0: want nil, got %v", got)
	}
}
