package storage

import (
	"errors"
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/model"
)

// controlled clock
func fakeClock(t *int64) Clock { return func() int64 { return *t } }

func writeOK(t *testing.T, s *Storage, sample model.Sample, wantWindow int64) {
	t.Helper()
	res, err := s.Write(sample)
	if err != nil {
		t.Fatalf("write %v rejected unexpectedly: %v", sample, err)
	}
	if !res.Accepted || res.Window != wantWindow {
		t.Fatalf("write %v got %+v, want accepted window=%d", sample, res, wantWindow)
	}
}

func writeReject(t *testing.T, s *Storage, sample model.Sample, want model.RejectReason) {
	t.Helper()
	res, err := s.Write(sample)
	if err == nil {
		t.Fatalf("write %v accepted unexpectedly: %+v", sample, res)
	}
	var we *model.WriteError
	if !errors.As(err, &we) {
		t.Fatalf("write %v error %v is not *WriteError", sample, err)
	}
	if we.Reason != want {
		t.Fatalf("write %v reason=%q want %q (err=%v)", sample, we.Reason, want, err)
	}
	t.Logf("[reject] input sample={name=%s labels=%v ts=%d value=%v} -> reason=%s: %v",
		sample.Name, sample.Labels, sample.Timestamp, sample.Value, want, err)
}

// TestOutOfOrderAndDuplicate verifies that late samples land in their own
// window and that exact duplicates are counted exactly once, in either order.
func TestOutOfOrderAndDuplicate(t *testing.T) {
	now := int64(3_000)
	const win int64 = 1_000
	s := New(win, 100*win, fakeClock(&now))
	labels := map[string]string{"path": "/a"}
	series := model.SeriesIDOf("http_latency", labels)

	// Arrival order is deliberately scrambled across two windows.
	samples := []model.Sample{
		{Name: "http_latency", Labels: labels, Timestamp: 1_500, Value: 30},
		{Name: "http_latency", Labels: labels, Timestamp: 1_100, Value: 10}, // late
		{Name: "http_latency", Labels: labels, Timestamp: 1_500, Value: 30}, // dup
		{Name: "http_latency", Labels: labels, Timestamp: 1_900, Value: 20},
		{Name: "http_latency", Labels: labels, Timestamp: 2_500, Value: 50},
	}
	for _, sm := range samples {
		t.Logf("[input] sample={name=%s labels=%v ts=%d value=%v}",
			sm.Name, sm.Labels, sm.Timestamp, sm.Value)
	}

	writeOK(t, s, samples[0], 1_000)
	writeOK(t, s, samples[1], 1_000)
	writeReject(t, s, samples[2], model.ReasonDuplicate)
	writeOK(t, s, samples[3], 1_000)
	writeOK(t, s, samples[4], 2_000)

	got, err := s.QueryRange(series, 1_000, 3_000, win)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 windows, got %d", len(got))
	}
	w1, w2 := got[0], got[1]
	if w1.Count != 3 || w1.Sum != 60 || w1.Min != 10 || w1.Max != 30 || w1.Last != 20 {
		t.Fatalf("window1 aggregate wrong: %+v (want count=3 sum=60 min=10 max=30 last=20)", w1)
	}
	if w2.Count != 1 || w2.Sum != 50 {
		t.Fatalf("window2 aggregate wrong: %+v", w2)
	}
	t.Logf("[verdict] out-of-order window1=%+v window2=%+v; duplicate counted zero times", w1, w2)

	// Same dataset arriving in timestamp order must produce identical result.
	s2 := New(win, 10*win, fakeClock(&now))
	for _, sm := range []model.Sample{samples[1], samples[3], samples[0], samples[4]} {
		writeOK(t, s2, sm, model.AlignWindow(sm.Timestamp, win))
	}
	got2, _ := s2.QueryRange(series, 1_000, 3_000, win)
	for i := range got2 {
		if got2[i] != got[i] {
			t.Fatalf("order-dependent aggregate at %d: %+v vs %+v", i, got2[i], got[i])
		}
	}
	t.Logf("[verdict] in-order vs out-of-order aggregates identical: %+v", got2)
}

// TestRejectReasons distinguishes too-old, future and value-conflict rejects.
func TestRejectReasons(t *testing.T) {
	now := int64(100_000)
	const win int64 = 1_000
	s := New(win, 3*win, fakeClock(&now)) // retention = 3 windows
	labels := map[string]string{"host": "h1"}
	series := model.SeriesIDOf("cpu", labels)

	fresh := model.Sample{Name: "cpu", Labels: labels, Timestamp: 99_500, Value: 1}
	writeOK(t, s, fresh, 99_000)
	t.Logf("[input] fresh sample ts=%d value=%v accepted into window 99000", fresh.Timestamp, fresh.Value)

	// Advance time: window 99000 falls below horizon (100000-3000=97000 aligned).
	now = 103_000
	t.Logf("[clock advanced] now=%d horizon=%d", now, s.RetentionHorizon())
	old := model.Sample{Name: "cpu", Labels: labels, Timestamp: 99_500, Value: 2}
	writeReject(t, s, old, model.ReasonTooOld)

	future := model.Sample{Name: "cpu", Labels: labels, Timestamp: now + 120_000, Value: 9}
	writeReject(t, s, future, model.ReasonFuture)

	// Same timestamp, different value -> conflict, distinct reason.
	back := model.Sample{Name: "cpu", Labels: labels, Timestamp: 102_500, Value: 7}
	writeOK(t, s, back, 102_000)
	conflict := model.Sample{Name: "cpu", Labels: labels, Timestamp: 102_500, Value: 8}
	writeReject(t, s, conflict, model.ReasonDuplicateMismatch)

	t.Logf("[verdict] series=%s rejects partitioned into too_old/future/mismatch; no data corrupted", series)
}

// TestGranularityConsistency checks self-consistency across window boundaries:
// a coarser step must equal the merge of its underlying base windows.
func TestGranularityConsistency(t *testing.T) {
	now := int64(100_000)
	const win int64 = 1_000
	s := New(win, 100*win, fakeClock(&now))
	labels := map[string]string{"path": "/x"}
	series := model.SeriesIDOf("qps", labels)

	var values = []struct {
		ts int64
		v  float64
	}{
		{1_000, 1}, {1_100, 3}, {2_100, 5}, {3_900, 7}, {4_050, 9},
	}
	for _, p := range values {
		t.Logf("[input] sample ts=%d value=%v", p.ts, p.v)
		writeOK(t, s, model.Sample{Name: "qps", Labels: labels, Timestamp: p.ts, Value: p.v},
			model.AlignWindow(p.ts, win))
	}

	base, err := s.QueryRange(series, 1_000, 5_000, win)
	if err != nil {
		t.Fatal(err)
	}
	coarse, err := s.QueryRange(series, 1_000, 5_000, 2*win)
	if err != nil {
		t.Fatal(err)
	}
	// coarse[0] covers base[0]+base[1]; coarse[1] covers base[2]+base[3].
	checks := []struct{ idx, b1, b2 int }{
		{0, 0, 1}, {1, 2, 3},
	}
	for _, c := range checks {
		m := model.WindowAggregate{
			Series: series, WindowStart: base[c.b1].WindowStart,
			WindowEnd: base[c.b2].WindowEnd,
			Count:     base[c.b1].Count + base[c.b2].Count,
			Sum:       base[c.b1].Sum + base[c.b2].Sum,
			Min:       base[c.b1].Min, Max: base[c.b1].Max,
			Last: base[c.b2].Last,
		}
		if base[c.b2].Min < m.Min {
			m.Min = base[c.b2].Min
		}
		if base[c.b2].Max > m.Max {
			m.Max = base[c.b2].Max
		}
		got := coarse[c.idx]
		if got.Count != m.Count || got.Sum != m.Sum || got.Min != m.Min ||
			got.Max != m.Max || got.Last != m.Last {
			t.Fatalf("coarse window %d inconsistent: got %+v want %+v", c.idx, got, m)
		}
		t.Logf("[verdict] coarse[%d]=%+v == merge(base[%d],base[%d])", c.idx, got, c.b1, c.b2)
	}

	// Invalid steps and misaligned bounds must error.
	if _, err := s.QueryRange(series, 1_000, 2_000, 1_500); err == nil {
		t.Fatal("expected error for non-multiple step")
	}
	if _, err := s.QueryRange(series, 1_500, 2_500, win); err == nil {
		t.Fatal("expected error for misaligned bounds")
	}
}

// TestEvictAndPartialData ensures eviction removes old windows and queries
// crossing the evicted region fail rather than returning silent gaps.
func TestEvictAndPartialData(t *testing.T) {
	now := int64(10_000)
	const win int64 = 1_000
	s := New(win, 3*win, fakeClock(&now))
	labels := map[string]string{"id": "1"}
	series := model.SeriesIDOf("m", labels)

	for ts := int64(7_000); ts < 10_000; ts += 500 {
		writeOK(t, s, model.Sample{Name: "m", Labels: labels, Timestamp: ts, Value: 1},
			model.AlignWindow(ts, win))
		t.Logf("[input] sample ts=%d value=1", ts)
	}

	now = 12_000
	removed := s.Evict()
	t.Logf("[evict] now=%d horizon=%d removed=%d windows", now, s.RetentionHorizon(), removed)
	if removed == 0 {
		t.Fatal("expected windows to be evicted")
	}

	// Query entirely within retained horizon works (empty slots count=0).
	ok, err := s.QueryRange(series, 9_000, 10_000, win)
	if err != nil || ok[0].Count != 2 {
		t.Fatalf("retained-range query wrong: %+v err=%v", ok, err)
	}
	t.Logf("[verdict] retained range query ok: %+v", ok)

	// Query reaching into evicted region must fail loudly.
	_, err = s.QueryRange(series, 8_000, 10_000, win)
	if !errors.Is(err, ErrPartialData) {
		t.Fatalf("expected ErrPartialData, got %v", err)
	}
	t.Logf("[verdict] query across evicted data rejected: %v", err)

	if _, exists := s.OldestWindowStart(series); !exists {
		t.Fatal("series should still retain some windows")
	}
}

// TestConcurrentWritesDedup stresses that concurrent duplicate reports are
// never double counted.
func TestConcurrentWritesDedup(t *testing.T) {
	now := int64(50_000)
	const win int64 = 1_000
	s := New(win, 10*win, fakeClock(&now))
	labels := map[string]string{"k": "v"}
	series := model.SeriesIDOf("c", labels)

	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(offset int64) {
			defer func() { done <- struct{}{} }()
			for i := int64(0); i < 50; i++ {
				// Same 5 timestamps across goroutines: only unique ones count.
				ts := 45_000 + (i%5)*100
				s.Write(model.Sample{Name: "c", Labels: labels, Timestamp: ts, Value: float64(ts)})
				_ = offset
			}
		}(int64(g))
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	got, err := s.QueryRange(series, 45_000, 46_000, win)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Count != 5 {
		t.Fatalf("concurrent dedup failed: count=%d want 5", got[0].Count)
	}
	t.Logf("[verdict] concurrent duplicate writes -> count=%d (want 5)", got[0].Count)
}
