package manifest

import (
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"

	"github.com/nilparra-dev/velox/internal/probe"
)

// Version is the manifest schema version.
const Version = 1

// Manifest is the resume state persisted next to a .part file. Only the indices
// of fully-completed chunks are stored; an interrupted chunk is re-downloaded
// in full on resume.
type Manifest struct {
	Version      int    `json:"version"`
	URL          string `json:"url"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag"`
	LastModified string `json:"lastModified"`
	ChunkSize    int64  `json:"chunkSize"`
	Completed    []int  `json:"completed"`

	mu   sync.Mutex   // guards done
	done map[int]bool // in-memory set; serialized to Completed on Save
	path string
}

// New creates a fresh manifest for info, to be persisted at path.
func New(path string, info *probe.RemoteInfo, chunkSize int64) *Manifest {
	return &Manifest{
		Version:      Version,
		URL:          info.URL,
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		ChunkSize:    chunkSize,
		done:         make(map[int]bool),
		path:         path,
	}
}

// Load reads and parses a manifest from path.
func Load(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Version != Version {
		return nil, errors.New("manifest: unsupported version")
	}
	m.path = path
	m.done = make(map[int]bool, len(m.Completed))
	for _, i := range m.Completed {
		m.done[i] = true
	}
	return &m, nil
}

// MarkDone records chunk idx as fully downloaded.
func (m *Manifest) MarkDone(idx int) {
	m.mu.Lock()
	m.done[idx] = true
	m.mu.Unlock()
}

// IsDone reports whether chunk idx is complete.
func (m *Manifest) IsDone(idx int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done[idx]
}

// Pending returns the indices in [0,total) that are not yet complete.
func (m *Manifest) Pending(total int) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	var p []int
	for i := 0; i < total; i++ {
		if !m.done[i] {
			p = append(p, i)
		}
	}
	return p
}

// Validate reports whether info matches this manifest (so resuming is safe):
// the size must match, and a validator must match when one exists on both
// sides; with no validators, a size match alone is accepted.
func (m *Manifest) Validate(info *probe.RemoteInfo) bool {
	if m.Size <= 0 || info.Size != m.Size {
		return false
	}
	if m.ETag != "" && info.ETag != "" {
		return m.ETag == info.ETag
	}
	if m.LastModified != "" && info.LastModified != "" {
		return m.LastModified == info.LastModified
	}
	return true
}

// Save atomically writes the manifest (tmp file + rename).
func (m *Manifest) Save() error {
	m.mu.Lock()
	m.Completed = make([]int, 0, len(m.done))
	for i := range m.done {
		m.Completed = append(m.Completed, i)
	}
	sort.Ints(m.Completed)
	data, err := json.MarshalIndent(m, "", "  ")
	m.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}
