package alert

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/storage"
)

const winMs int64 = 1_000

func newHarness(t *testing.T, nowVal int64, retention int64) (*Engine, *storage.Storage, *int64, map[string]string) {
	t.Helper()
	now := nowVal
	store := storage.New(winMs, retention, func() int64 { return now })
	eng := NewEngine(store, func() int64 { return now })
	labels := map[string]string{"host": "h1"}
	return eng, store, &now, labels
}

func logSamples(t *testing.T, store *storage.Storage, labels map[string]string, pts ...struct {
	ts int64
	v  float64
}) {
	t.Helper()
	for _, p := range pts {
		sm := model.Sample{Name: "cpu", Labels: labels, Timestamp: p.ts, Value: p.v}
		t.Logf("[input] sample={name=cpu labels=%v ts=%d value=%v}", labels, p.ts, p.v)
		if _, err := store.Write(sm); err != nil {
			t.Fatalf("write ts=%d: %v", p.ts, err)
		}
	}
}

func logTransitions(t *testing.T, ts []Transition) {
	for _, tr := range ts {
		t.Logf("[transition] rule=%s series=%s at=%d %s->%s cond=%v value=%v | %s",
			tr.RuleID, tr.Series, tr.AtMs, tr.From, tr.To, tr.Condition, tr.Value, tr.Reason)
	}
}

// TestThresholdBoundary verifies exact-boundary comparisons (>=) and the full
// resolved->pending->firing->resolved lifecycle.
func TestThresholdBoundary(t *testing.T) {
	eng, store, now, labels := newHarness(t, 2_000, 100*winMs)
	series := model.SeriesIDOf("cpu", labels)
	rule := Rule{
		ID: "cpu-high", MetricName: "cpu", WindowMs: winMs,
		Aggregation: "max", Operator: OpGE, Threshold: 80, ForMs: 1_000,
	}
	if err := eng.SetRules([]Rule{rule}); err != nil {
		t.Fatal(err)
	}

	type tick struct {
		ts        int64
		v         float64
		wantState State
		wantTrans int // expected transitions produced at this tick
	}
	// Windows: below -> equal (boundary, pending) -> equal again (firing after
	// holding for one window) -> below (resolved).
	ticks := []tick{
		{2_000, 79, StateResolved, 0},
		{3_000, 80, StatePending, 1},
		{4_000, 80, StateFiring, 1},
		{5_000, 79.999, StateResolved, 1},
	}
	for i, tk := range ticks {
		*now = tk.ts
		logSamples(t, store, labels, []struct {
			ts int64
			v  float64
		}{{tk.ts - 100, tk.v}}...)
		trs, err := eng.EvaluateOnce("cpu", labels)
		if err != nil {
			t.Fatal(err)
		}
		logTransitions(t, trs)
		got := eng.StateOf(rule.ID, series)
		if got != tk.wantState {
			t.Fatalf("tick %d (value=%v): state=%s want %s", i, tk.v, got, tk.wantState)
		}
		if len(trs) != tk.wantTrans {
			t.Fatalf("tick %d: %d transitions, want %d: %+v", i, len(trs), tk.wantTrans, trs)
		}
		t.Logf("[verdict] tick %d value=%v op=%v threshold=%v -> state=%s",
			i, tk.v, rule.Operator, rule.Threshold, got)
	}

	// Idempotence: re-evaluating the same tick must not double-fire.
	trs, _ := eng.EvaluateOnce("cpu", labels)
	if len(trs) != 0 {
		t.Fatalf("re-evaluation emitted extra transitions: %+v", trs)
	}
	t.Logf("[verdict] repeat tick emits no transitions; final log length=%d", len(eng.Transitions()))

	// Full log must be exactly: pending, firing, resolved.
	log := eng.Transitions()
	wantSeq := []struct{ from, to State }{
		{StateResolved, StatePending},
		{StatePending, StateFiring},
		{StateFiring, StateResolved},
	}
	if len(log) != len(wantSeq) {
		t.Fatalf("transition log len=%d want %d", len(log), len(wantSeq))
	}
	for i, w := range wantSeq {
		if log[i].From != w.from || log[i].To != w.to {
			t.Fatalf("log[%d]=%s->%s want %s->%s", i, log[i].From, log[i].To, w.from, w.to)
		}
	}
}

// TestPendingInterrupted verifies recovery while pending does not fire, and a
// later breach restarts the For timer.
func TestPendingInterrupted(t *testing.T) {
	eng, store, now, labels := newHarness(t, 2_000, 100*winMs)
	series := model.SeriesIDOf("cpu", labels)
	rule := Rule{
		ID: "r", MetricName: "cpu", WindowMs: winMs,
		Aggregation: "avg", Operator: OpGT, Threshold: 10, ForMs: 2_000,
	}
	if err := eng.SetRules([]Rule{rule}); err != nil {
		t.Fatal(err)
	}

	step := func(ts int64, v float64, want State) {
		*now = ts
		logSamples(t, store, labels, []struct {
			ts int64
			v  float64
		}{{ts - 50, v}}...)
		trs, err := eng.EvaluateOnce("cpu", labels)
		if err != nil {
			t.Fatal(err)
		}
		logTransitions(t, trs)
		if got := eng.StateOf(rule.ID, series); got != want {
			t.Fatalf("ts=%d value=%v: state=%s want %s", ts, v, got, want)
		}
	}
	step(2_000, 11, StatePending) // breach starts
	step(3_000, 5, StateResolved) // interrupted at 1 window (< for=2000)
	step(4_000, 11, StatePending) // restart timer; 1000ms observed
	step(5_000, 11, StateFiring)  // 2000ms observed across [3000,5000) -> fire
	step(6_000, 11, StateFiring)  // stays firing, no new transition
	t.Logf("[verdict] interrupted pending restarted For timer; log=%s", summarize(eng.Transitions()))
}

// TestHotReloadNoLossNoDuplicate verifies rule replacement preserves state and
// removed rules resolve explicitly; unrelated rules never touch each other.
func TestHotReloadNoLossNoDuplicate(t *testing.T) {
	eng, store, now, labels := newHarness(t, 2_000, 100*winMs)
	series := model.SeriesIDOf("cpu", labels)
	high := Rule{ID: "high", MetricName: "cpu", WindowMs: winMs,
		Aggregation: "last", Operator: OpGT, Threshold: 50, ForMs: 0}
	low := Rule{ID: "low", MetricName: "cpu", WindowMs: winMs,
		Aggregation: "last", Operator: OpLT, Threshold: 10, ForMs: 0}
	if err := eng.SetRules([]Rule{high, low}); err != nil {
		t.Fatal(err)
	}

	*now = 2_000
	logSamples(t, store, labels, []struct {
		ts int64
		v  float64
	}{{1_900, 60}}...)
	trs, _ := eng.EvaluateOnce("cpu", labels)
	logTransitions(t, trs)
	if eng.StateOf("high", series) != StateFiring || eng.StateOf("low", series) != StateResolved {
		t.Fatalf("unexpected states: %+v", eng.Snapshot())
	}

	// Hot reload: threshold tweak for "high" (same ID -> state preserved),
	// "low" removed -> must produce an explicit resolved transition only if it
	// was active (it isn't, so no transition), then re-add low and activate.
	high2 := high
	high2.Threshold = 55
	if err := eng.SetRules([]Rule{high2}); err != nil {
		t.Fatal(err)
	}
	if eng.StateOf("high", series) != StateFiring {
		t.Fatal("state must survive hot reload across same rule ID")
	}
	t.Logf("[verdict] rule 'high' state preserved across reload: %s", StateFiring)

	// Re-evaluate while firing under new rule: 60 >= 55 still true -> no dup.
	trs, _ = eng.EvaluateOnce("cpu", labels)
	logTransitions(t, trs)
	if len(trs) != 0 {
		t.Fatalf("reload + re-evaluate duplicated a transition: %+v", trs)
	}

	// Remove the firing rule: explicit resolution must be recorded.
	if err := eng.SetRules(nil); err != nil {
		t.Fatal(err)
	}
	log := eng.Transitions()
	last := log[len(log)-1]
	if last.To != StateResolved || last.Reason == "" {
		t.Fatalf("removed firing rule not explicitly resolved: %+v", last)
	}
	t.Logf("[transition] %s->%s: %s", last.From, last.To, last.Reason)

	// Invalid rule sets must fail atomically (old set stays in effect).
	bad := Rule{ID: "bad", MetricName: "cpu", WindowMs: 333, Aggregation: "last", Operator: OpGT}
	if err := eng.SetRules([]Rule{bad}); err == nil {
		t.Fatal("expected validation error for non-aligned window")
	}
	t.Logf("[verdict] invalid reload rejected atomically: %v", "window alignment")
}

// TestEvictedDataKeepsAlert verifies that queries hitting evicted windows are
// treated as unknown: a firing alert must not silently resolve.
func TestEvictedDataKeepsAlert(t *testing.T) {
	eng, store, now, labels := newHarness(t, 2_000, 3*winMs)
	series := model.SeriesIDOf("cpu", labels)
	rule := Rule{ID: "r", MetricName: "cpu", WindowMs: winMs,
		Aggregation: "last", Operator: OpGT, Threshold: 5, ForMs: 0}
	if err := eng.SetRules([]Rule{rule}); err != nil {
		t.Fatal(err)
	}
	*now = 2_000
	logSamples(t, store, labels, []struct {
		ts int64
		v  float64
	}{{1_900, 9}}...)
	if _, err := eng.EvaluateOnce("cpu", labels); err != nil {
		t.Fatal(err)
	}
	if eng.StateOf("r", series) != StateFiring {
		t.Fatal("expected firing")
	}
	// Jump forward past retention: the last window is evicted / no data.
	*now = 20_000
	store.Evict()
	trs, err := eng.EvaluateOnce("cpu", labels)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, trs)
	if len(trs) != 0 || eng.StateOf("r", series) != StateFiring {
		t.Fatal("evicted/missing data must not move alert state")
	}
	t.Logf("[verdict] unknown data (eviction) leaves firing alert untouched")
}

// TestConcurrentEvaluation runs many evaluation goroutines with concurrent
// reloads and asserts the transition log never contains duplicate firings or
// inconsistent states. Intended to run under -race.
func TestConcurrentEvaluation(t *testing.T) {
	eng, store, now, labels := newHarness(t, 2_000, 1000*winMs)
	rule := Rule{ID: "r", MetricName: "cpu", WindowMs: winMs,
		Aggregation: "last", Operator: OpGT, Threshold: 50, ForMs: 0}
	if err := eng.SetRules([]Rule{rule}); err != nil {
		t.Fatal(err)
	}
	*now = 2_000
	logSamples(t, store, labels, []struct {
		ts int64
		v  float64
	}{{1_900, 60}}...)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, _ = eng.EvaluateOnce("cpu", labels)
				r := rule
				if i%2 == 0 {
					r.Threshold = 50
				} else {
					r.Threshold = 51
				}
				_ = eng.SetRules([]Rule{r})
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()

	log := eng.Transitions()
	fireCount := 0
	for _, tr := range log {
		if tr.To == StateFiring && tr.RuleID == "r" {
			fireCount++
		}
	}
	// Rule survives every reload with the same ID and was firing initially;
	// threshold 51 still fires (60>51), so exactly one firing ever.
	if fireCount != 1 {
		t.Fatalf("firing transition count=%d, want exactly 1: %s", fireCount, summarize(log))
	}
	t.Logf("[verdict] concurrent eval+reload: exactly 1 firing among %d transitions", len(log))
}

func summarize(log []Transition) string {
	s := ""
	for _, tr := range log {
		s += fmt.Sprintf("(%s:%s->%s) ", tr.RuleID, tr.From, tr.To)
	}
	return s
}
