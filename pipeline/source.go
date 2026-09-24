package pipeline

import (
	"sync"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// MemorySource simulates an upstream collector: it replays a fixed script of
// samples, including out-of-order, duplicate (same SourceID+DedupKey) and late
// arrivals. Replay is deterministic, so results are reproducible.
type MemorySource struct {
	mu      sync.Mutex
	samples []model.Sample
	pos     int
}

// NewMemorySource builds a source that replays the given samples in order.
func NewMemorySource(samples []model.Sample) *MemorySource {
	cp := make([]model.Sample, len(samples))
	copy(cp, samples)
	return &MemorySource{samples: cp}
}

// Next returns the next scripted sample and false when exhausted.
func (m *MemorySource) Next() (model.Sample, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pos >= len(m.samples) {
		return model.Sample{}, false
	}
	s := m.samples[m.pos]
	m.pos++
	return s, true
}

// Reset rewinds the source to the first sample.
func (m *MemorySource) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pos = 0
}

// Len reports the script length.
func (m *MemorySource) Len() int { return len(m.samples) }
