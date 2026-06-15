// internal/download/resume.go
package download

import (
	"os"

	"github.com/nilparra-dev/velox/internal/chunk"
	"github.com/nilparra-dev/velox/internal/manifest"
	"github.com/nilparra-dev/velox/internal/probe"
)

// plan describes how a chunked download will run.
type plan struct {
	manifest *manifest.Manifest
	chunks   []chunk.Chunk
	pending  []int
	resumed  int64 // bytes already on disk from completed chunks
	fresh    bool  // true when starting from scratch (no usable resume state)
}

// prepare decides whether to resume an existing download or start fresh.
// part is the <output>.part path and mpath the <output>.dm manifest path.
func prepare(opts Options, info *probe.RemoteInfo, part, mpath string) *plan {
	chunkSize := opts.ChunkSize
	if chunkSize < 1 {
		chunkSize = defaultChunkSize
	}

	if opts.Restart {
		os.Remove(part)
		os.Remove(mpath)
	} else if m, err := manifest.Load(mpath); err == nil && m.Validate(info) && fileExists(part) {
		chunks := chunk.Plan(info.Size, m.ChunkSize)
		pending := m.Pending(len(chunks))
		var resumed int64
		for _, c := range chunks {
			if m.IsDone(c.Index) {
				resumed += c.Length()
			}
		}
		return &plan{manifest: m, chunks: chunks, pending: pending, resumed: resumed, fresh: false}
	} else {
		// No usable manifest (missing, corrupt, changed remote, or no .part).
		os.Remove(part)
		os.Remove(mpath)
	}

	m := manifest.New(mpath, info, chunkSize)
	chunks := chunk.Plan(info.Size, chunkSize)
	pending := make([]int, len(chunks))
	for i := range chunks {
		pending[i] = i
	}
	return &plan{manifest: m, chunks: chunks, pending: pending, resumed: 0, fresh: true}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
