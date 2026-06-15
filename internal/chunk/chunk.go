package chunk

// Chunk is a contiguous, inclusive byte range [Start, End].
type Chunk struct {
	Index int
	Start int64
	End   int64
}

// Length returns the number of bytes in the chunk.
func (c Chunk) Length() int64 { return c.End - c.Start + 1 }

// Plan divides [0, size) into fixed-size chunks of chunkSize bytes; the last
// chunk may be shorter. chunkSize < 1 is treated as size (one chunk). Returns
// nil when size <= 0.
func Plan(size, chunkSize int64) []Chunk {
	if size <= 0 {
		return nil
	}
	if chunkSize < 1 {
		chunkSize = size
	}
	var chunks []Chunk
	var start int64
	for idx := 0; start < size; idx++ {
		end := start + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		chunks = append(chunks, Chunk{Index: idx, Start: start, End: end})
		start = end + 1
	}
	return chunks
}
