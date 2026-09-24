// Package storage implements the in-memory time-windowed metric store:
// aligned window aggregation, idempotent (deduplicated) writes, retention
// eviction and gap-aware range queries. It is safe for concurrent use.
package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// Config configures WindowStore.
type Config struct {
	// WindowSeconds is the base bucket granularity; must be > 0.
	WindowSeconds int64
	// RetentionWindows is how many base windows (including the current one)
	// are kept per series. Samples landing in older windows are rejected.
	RetentionWindows int
	// MaxFutureSeconds rejects samples more than this many seconds ahead of now.
	MaxFutureSeconds int64
	// Now supplies the current unix-seconds clock (injectable in tests).
	Now func() int64
}

// WindowStore is the in-memory store.
type WindowStore struct {
	cfg Config

	mu sync.RWMutex
	// series keyed by model.Labels.Fingerprint()
	series map[string]*seriesState
	// dedup indexes accepted reports so retries are never double counted.
	dedup map[string]dedupEntry
	// horizon is the lower bound of retained windows: every retained window
	// has start >= horizon. 0 means nothing has been evicted. Monotonic.
	horizon int64
}

// seriesState holds one series' labels and retained windows.
type seriesState struct {
	labels  model.Labels
	windows map[int64]*model.Aggregate
}

// dedupEntry remembers an accepted report for the lifetime of its window.
type dedupEntry struct {
	windowStart int64
	ts          int64
}

// NewStore constructs an empty store with normalized configuration.
func NewStore(cfg Config) *WindowStore {
	if cfg.WindowSeconds <= 0 {
		cfg.WindowSeconds = 60
	}
	if cfg.RetentionWindows <= 0 {
		cfg.RetentionWindows = 10
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().Unix() }
	}
	return &WindowStore{
		cfg:    cfg,
		series: map[string]*seriesState{},
		dedup:  map[string]dedupEntry{},
	}
}

// alignFloor returns the start of the size-seconds window containing ts.
func alignFloor(ts, size int64) int64 {
	if ts >= 0 {
		return ts - ts%size
	}
	return ts - (ts%size + size)
}

// dedupID composes the global identity of an upstream report.
func dedupID(sourceID, key string) string {
	var b strings.Builder
	b.WriteString(sourceID)
	b.WriteByte(0)
	b.WriteString(key)
	return b.String()
}

// minRetainedStart returns the oldest window start still retained at time now.
func (s *WindowStore) minRetainedStart(now int64) int64 {
	cur := alignFloor(now, s.cfg.WindowSeconds)
	return cur - int64(s.cfg.RetentionWindows-1)*s.cfg.WindowSeconds
}

// evict drops windows (and their dedup entries) older than the retention
// boundary and advances the monotonic horizon. Caller must hold s.mu.
func (s *WindowStore) evict(now int64) {
	minStart := s.minRetainedStart(now)
	for _, st := range s.series {
		for ws := range st.windows {
			if ws < minStart {
				delete(st.windows, ws)
			}
		}
	}
	for id, e := range s.dedup {
		if e.windowStart < minStart {
			delete(s.dedup, id)
		}
	}
	if minStart > s.horizon {
		s.horizon = minStart
	}
}

// Tick advances retention using the store clock, evicting expired windows
// without ingesting anything. Returns the resulting horizon.
func (s *WindowStore) Tick() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evict(s.cfg.Now())
	return s.horizon
}

// reject builds a rejected WriteResult.
func reject(ws int64, reason model.RejectReason, detail string) model.WriteResult {
	return model.WriteResult{Status: model.WriteRejected, Reason: reason, WindowStart: ws, Detail: detail}
}

// Write records one sample, assigning it to its owning (aligned) window.
// Out-of-order/late samples are merged into the correct window; repeat
// reports are de-duplicated; samples outside retention are rejected with a
// distinguishable reason.
func (s *WindowStore) Write(sample model.Sample) model.WriteResult {
	if sample.SourceID == "" || sample.DedupKey == "" {
		return reject(0, model.RejectValidation, "SourceID and DedupKey are required for idempotent writes")
	}
	if sample.Ts < 0 {
		return reject(0, model.RejectValidation, "negative timestamp")
	}
	if sample.Labels == nil {
		sample.Labels = model.Labels{}
	}

	now := s.cfg.Now()
	ws := alignFloor(sample.Ts, s.cfg.WindowSeconds)

	if s.cfg.MaxFutureSeconds > 0 && sample.Ts > now+s.cfg.MaxFutureSeconds {
		return reject(ws, model.RejectInFuture,
			fmt.Sprintf("ts=%d is more than %ds after now=%d", sample.Ts, s.cfg.MaxFutureSeconds, now))
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.evict(now)
	minStart := s.minRetainedStart(now)
	if ws < minStart {
		return reject(ws, model.RejectTooOld,
			fmt.Sprintf("window [%d,%d) is before retention boundary %d at now=%d",
				ws, ws+s.cfg.WindowSeconds, minStart, now))
	}

	id := dedupID(sample.SourceID, sample.DedupKey)
	if existing, ok := s.dedup[id]; ok {
		// Repeat report: acknowledge but never count twice.
		return model.WriteResult{
			Status:      model.WriteDeduped,
			Reason:      model.RejectDuplicate,
			WindowStart: existing.windowStart,
			Detail: fmt.Sprintf("repeat of report accepted into window [%d,%d)",
				existing.windowStart, existing.windowStart+s.cfg.WindowSeconds),
		}
	}

	fp := sample.Labels.Fingerprint()
	st, ok := s.series[fp]
	if !ok {
		st = &seriesState{labels: sample.Labels, windows: map[int64]*model.Aggregate{}}
		s.series[fp] = st
	}
	agg, ok := st.windows[ws]
	if !ok {
		agg = &model.Aggregate{
			Labels:      sample.Labels,
			WindowStart: ws,
			WindowEnd:   ws + s.cfg.WindowSeconds,
			Min:         sample.Value,
			Max:         sample.Value,
			Sum:         sample.Value,
			Count:       1,
			Latest:      sample.Value,
			LatestTs:    sample.Ts,
		}
		st.windows[ws] = agg
	} else {
		agg.Count++
		agg.Sum += sample.Value
		if sample.Value < agg.Min {
			agg.Min = sample.Value
		}
		if sample.Value > agg.Max {
			agg.Max = sample.Value
		}
		// Out-of-order timestamps: latest is defined by Ts, not arrival order.
		if sample.Ts >= agg.LatestTs {
			agg.Latest = sample.Value
			agg.LatestTs = sample.Ts
		}
	}
	s.dedup[id] = dedupEntry{windowStart: ws, ts: sample.Ts}

	return model.WriteResult{
		Status:      model.WriteAccepted,
		WindowStart: ws,
		Detail:      fmt.Sprintf("merged into window [%d,%d), count=%d", ws, ws+s.cfg.WindowSeconds, agg.Count),
	}
}

// Horizon returns the lower bound of retained data (window starts >= it);
// returns 0 when nothing has been evicted yet.
func (s *WindowStore) Horizon() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.horizon
}

// GapError reports that a query range spans windows whose data was evicted.
// Partial results are still returned by Query; callers must not treat them as
// complete when this error is present.
type GapError struct {
	Gaps []model.QueryGap
}

func (e *GapError) Error() string {
	if len(e.Gaps) == 0 {
		return "query spans evicted windows"
	}
	first, last := e.Gaps[0], e.Gaps[len(e.Gaps)-1]
	return fmt.Sprintf("query spans %d evicted window(s): [%d,%d) .. [%d,%d) have been dropped by retention",
		len(e.Gaps), first.WindowStart, first.WindowEnd, last.WindowStart, last.WindowEnd)
}

// Query returns aggregates for selector-matching series over [start,end),
// merging base windows into granularitySeconds buckets. It returns a
// *GapError when the range covers evicted windows instead of silently
// returning partial data.
func (s *WindowStore) Query(start, end, granularitySeconds int64, selector model.Labels) (*model.QueryResult, error) {
	w := s.cfg.WindowSeconds
	if granularitySeconds <= 0 {
		return nil, errors.New("granularitySeconds must be > 0")
	}
	if granularitySeconds%w != 0 {
		return nil, fmt.Errorf("granularitySeconds %d must be a multiple of base window %d", granularitySeconds, w)
	}
	if start < 0 || end <= start {
		return nil, fmt.Errorf("invalid range [%d,%d)", start, end)
	}
	if start%granularitySeconds != 0 || end%granularitySeconds != 0 {
		return nil, fmt.Errorf("range [%d,%d) must align to granularity %d", start, end, granularitySeconds)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	res := &model.QueryResult{}
	var gaps []model.QueryGap

	fps := make([]string, 0, len(s.series))
	for fp, st := range s.series {
		if selector != nil && !st.labels.Match(selector) {
			continue
		}
		fps = append(fps, fp)
	}
	sort.Strings(fps)

	for _, fp := range fps {
		st := s.series[fp]
		sp := model.SeriesPoints{Labels: st.labels}
		for bstart := start; bstart < end; bstart += granularitySeconds {
			bend := bstart + granularitySeconds
			if s.horizon > 0 && bstart < s.horizon {
				gaps = append(gaps, model.QueryGap{
					WindowStart: bstart,
					WindowEnd:   bend,
					Reason:      model.RejectTooOld,
				})
				continue // no partial buckets across the eviction boundary
			}
			var merged *model.Aggregate
			for ws := bstart; ws < bend; ws += w {
				a, ok := st.windows[ws]
				if !ok {
					continue
				}
				if merged == nil {
					cp := *a
					merged = &cp
					continue
				}
				mergeInto(merged, a)
			}
			if merged != nil {
				// Reported at query granularity boundaries.
				merged.WindowStart = bstart
				merged.WindowEnd = bend
				sp.Points = append(sp.Points, *merged)
			}
		}
		res.Series = append(res.Series, sp)
	}

	if len(gaps) > 0 {
		return res, &GapError{Gaps: gaps}
	}
	return res, nil
}

// mergeInto folds src into dst (same series, adjacent base windows).
func mergeInto(dst, src *model.Aggregate) {
	dst.Count += src.Count
	dst.Sum += src.Sum
	if src.Min < dst.Min {
		dst.Min = src.Min
	}
	if src.Max > dst.Max {
		dst.Max = src.Max
	}
	if src.LatestTs >= dst.LatestTs {
		dst.Latest = src.Latest
		dst.LatestTs = src.LatestTs
	}
}
