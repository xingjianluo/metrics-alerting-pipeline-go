// Package storage provides an in-memory, deduplicated, windowed metric store.
//
// Samples are bucketed into epoch-aligned fixed windows, so out-of-order and
// late arrivals land in the window they belong to. Each window keeps a
// timestamp->value index, making repeated reports idempotent. Windows past the
// retention horizon are evicted, and queries touching the evicted region fail
// with ErrPartialData instead of silently returning incomplete results.
package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/model"
)

// ErrPartialData means a query range starts before the retention horizon, so
// some of the requested windows may have been evicted and the answer would be
// incomplete.
var ErrPartialData = errors.New("query range overlaps evicted data; partial results are not returned")

// Clock returns the current time in unix milliseconds (injectable for tests).
type Clock func() int64

// defaultFutureSkewMs bounds how far ahead of the wall clock a sample may be.
const defaultFutureSkewMs = 60_000

// windowData holds one series' samples inside one base window.
type windowData struct {
	start  int64
	values map[int64]float64 // sample timestamp -> value (dedup index)
	agg    model.WindowAggregate
	lastTs int64 // greatest sample timestamp currently in the window
}

// Storage is a thread-safe in-memory metric store with retention enforcement.
type Storage struct {
	mu           sync.RWMutex
	windowSizeMs int64
	retentionMs  int64
	futureSkewMs int64
	now          Clock
	// series -> windowStart -> window data
	data map[model.SeriesID]map[int64]*windowData
}

// New builds an empty Storage. windowSizeMs is the base bucketing granularity;
// retentionMs must be at least one window. Invalid configuration panics, as it
// is a wiring-time error.
func New(windowSizeMs, retentionMs int64, now Clock) *Storage {
	if windowSizeMs <= 0 {
		panic("storage: windowSizeMs must be positive")
	}
	if retentionMs < windowSizeMs {
		panic("storage: retentionMs must be >= windowSizeMs")
	}
	if now == nil {
		panic("storage: clock is required")
	}
	return &Storage{
		windowSizeMs: windowSizeMs,
		retentionMs:  retentionMs,
		futureSkewMs: defaultFutureSkewMs,
		now:          now,
		data:         make(map[model.SeriesID]map[int64]*windowData),
	}
}

// WriteResult reports the outcome of one write attempt.
type WriteResult struct {
	Accepted bool
	Window   int64 // window start the sample belongs to
	Reason   model.RejectReason
}

// horizonStart returns the first window start that is still retained.
// Windows with start < horizonStart are (or must be) gone.
func (s *Storage) horizonStart() int64 {
	return model.AlignWindow(s.now()-s.retentionMs, s.windowSizeMs)
}

// RetentionHorizon returns the current retention horizon (first retained
// window start).
func (s *Storage) RetentionHorizon() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.horizonStart()
}

// WindowSize returns the base window size in milliseconds.
func (s *Storage) WindowSize() int64 { return s.windowSizeMs }

// Write inserts one sample. Out-of-order samples are accepted as long as their
// window is still retained; duplicates are never counted twice.
func (s *Storage) Write(sample model.Sample) (WriteResult, error) {
	if sample.Name == "" {
		return WriteResult{}, errors.New("storage: metric name is required")
	}
	now := s.now()

	// Future samples are rejected (with a small clock-skew tolerance).
	if sample.Timestamp > now+s.futureSkewMs {
		return WriteResult{Reason: model.ReasonFuture}, &model.WriteError{
			Reason: model.ReasonFuture,
			Msg: fmt.Sprintf("timestamp %d is more than %dms in the future (now=%d)",
				sample.Timestamp, s.futureSkewMs, now),
		}
	}

	wStart := model.AlignWindow(sample.Timestamp, s.windowSizeMs)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-evaluate horizon under lock; a sample whose window aged out is rejected
	// with a distinguishable reason rather than resurrecting evicted state.
	if wStart < s.horizonStart() {
		return WriteResult{Reason: model.ReasonTooOld, Window: wStart}, &model.WriteError{
			Reason: model.ReasonTooOld,
			Msg: fmt.Sprintf("timestamp %d belongs to expired window %d (horizon=%d)",
				sample.Timestamp, wStart, s.horizonStart()),
		}
	}

	series := model.SeriesIDOf(sample.Name, sample.Labels)
	windows, ok := s.data[series]
	if !ok {
		windows = make(map[int64]*windowData)
		s.data[series] = windows
	}
	w, ok := windows[wStart]
	if !ok {
		w = &windowData{
			start:  wStart,
			values: make(map[int64]float64),
			agg: model.WindowAggregate{
				Series:      series,
				WindowStart: wStart,
				WindowEnd:   wStart + s.windowSizeMs,
			},
		}
		windows[wStart] = w
	}

	// Dedup on exact (series, timestamp, value); a conflicting value at the
	// same timestamp is a distinct, explicit error.
	if existing, dup := w.values[sample.Timestamp]; dup {
		if existing == sample.Value {
			return WriteResult{Reason: model.ReasonDuplicate, Window: wStart}, &model.WriteError{
				Reason: model.ReasonDuplicate,
				Msg: fmt.Sprintf("series=%s ts=%d value=%v already recorded",
					series, sample.Timestamp, sample.Value),
			}
		}
		return WriteResult{Reason: model.ReasonDuplicateMismatch, Window: wStart}, &model.WriteError{
			Reason: model.ReasonDuplicateMismatch,
			Msg: fmt.Sprintf("series=%s ts=%d reported with conflicting values %v vs %v",
				series, sample.Timestamp, existing, sample.Value),
		}
	}

	w.values[sample.Timestamp] = sample.Value
	if w.agg.Count == 0 {
		w.agg.Min = sample.Value
		w.agg.Max = sample.Value
	} else {
		if sample.Value < w.agg.Min {
			w.agg.Min = sample.Value
		}
		if sample.Value > w.agg.Max {
			w.agg.Max = sample.Value
		}
	}
	w.agg.Count++
	w.agg.Sum += sample.Value
	// "Last" follows sample time, not arrival order, so late arrivals produce
	// the same aggregate as in-order arrivals.
	if w.agg.Count == 1 || sample.Timestamp > w.lastTs {
		w.lastTs = sample.Timestamp
		w.agg.Last = sample.Value
	}

	return WriteResult{Accepted: true, Window: wStart}, nil
}

// QueryRange returns per-window aggregates for one series over [startMs,endMs),
// one result per stepMs slot (which must be a positive aligned multiple of the
// base window). Empty slots contain zero-value aggregates with Count=0.
func (s *Storage) QueryRange(series model.SeriesID, startMs, endMs, stepMs int64) ([]model.WindowAggregate, error) {
	if endMs <= startMs {
		return nil, fmt.Errorf("storage: empty range [%d,%d)", startMs, endMs)
	}
	if stepMs <= 0 || stepMs%s.windowSizeMs != 0 {
		return nil, fmt.Errorf("storage: step %d must be a positive multiple of base window %d",
			stepMs, s.windowSizeMs)
	}
	if startMs%s.windowSizeMs != 0 || endMs%s.windowSizeMs != 0 {
		return nil, fmt.Errorf("storage: range bounds %d..%d must align to base window %d",
			startMs, endMs, s.windowSizeMs)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Refuse ranges that reach into data the store cannot vouch for.
	if startMs < s.horizonStart() {
		return nil, fmt.Errorf("%w: series=%s rangeStart=%d horizon=%d",
			ErrPartialData, series, startMs, s.horizonStart())
	}

	windows := s.data[series]
	out := make([]model.WindowAggregate, 0, (endMs-startMs)/stepMs)
	for slot := startMs; slot < endMs; slot += stepMs {
		merged := model.WindowAggregate{
			Series:      series,
			WindowStart: slot,
			WindowEnd:   slot + stepMs,
		}
		haveMin, haveMax := false, false
		var lastBaseTs int64
		for base := slot; base < slot+stepMs; base += s.windowSizeMs {
			w, ok := windows[base]
			if !ok || w.agg.Count == 0 {
				continue
			}
			merged.Count += w.agg.Count
			merged.Sum += w.agg.Sum
			if !haveMin || w.agg.Min < merged.Min {
				merged.Min = w.agg.Min
			}
			if !haveMax || w.agg.Max > merged.Max {
				merged.Max = w.agg.Max
			}
			haveMin, haveMax = true, true
			if w.lastTs >= lastBaseTs {
				lastBaseTs = w.lastTs
				merged.Last = w.agg.Last
			}
		}
		out = append(out, merged)
	}
	return out, nil
}

// Evict drops windows strictly older than the retention horizon. Returns the
// number of windows removed.
func (s *Storage) Evict() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	horizon := s.horizonStart()
	removed := 0
	for series, windows := range s.data {
		for start := range windows {
			if start < horizon {
				delete(windows, start)
				removed++
			}
		}
		if len(windows) == 0 {
			delete(s.data, series)
		}
	}
	return removed
}

// OldestWindowStart returns the oldest retained window start for a series.
func (s *Storage) OldestWindowStart(series model.SeriesID) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	windows, ok := s.data[series]
	if !ok || len(windows) == 0 {
		return 0, false
	}
	var oldest int64
	first := true
	for start := range windows {
		if first || start < oldest {
			oldest = start
			first = false
		}
	}
	return oldest, true
}
