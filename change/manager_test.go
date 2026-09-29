package change

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

type clk struct{ t int64 }

func rule(id string, host string, thr float64) model.Rule {
	return model.Rule{
		ID:            id,
		Selector:      model.Labels{"__name__": "m", "host": host},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: thr,
		PendingWindows: 0, KeepFiringWindows: 0,
	}
}

func harness(t *testing.T) (*clk, *storage.WindowStore, *alert.Evaluator, *Manager) {
	t.Helper()
	c := &clk{t: 100}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 1000, Now: func() int64 { return c.t }})
	ev := alert.NewEvaluator(st, []model.Rule{rule("r1", "a", 10)}, func() int64 { return c.t })
	m := NewManager(ev, func() int64 { return c.t })
	return c, st, ev, m
}

// TestCheckDoesNotMutate: check-only never switches rules or records history.
func TestCheckDoesNotMutate(t *testing.T) {
	_, _, ev, m := harness(t)

	if verrs := m.Check([]model.Rule{rule("r1", "a", 10)}); verrs != nil {
		t.Fatalf("valid batch check: %v", verrs)
	}
	t.Log("JUDGE Check(valid) -> no errors, no history, rules untouched")
	bad := rule("bad", "a", 10)
	bad.WindowSeconds = 0
	verrs := m.Check([]model.Rule{bad, bad})
	// One duplicate-name error plus one bad-window error per occurrence (=3):
	// the whole batch's problems are listed in one shot.
	if verrs == nil || len(verrs.Errs) != 3 {
		t.Fatalf("want 3 problems (1 dup + 2 bad window), got %+v", verrs)
	}
	for _, e := range verrs.Errs {
		t.Logf("JUDGE Check problem: %s", e)
	}
	if len(m.History()) != 0 {
		t.Fatal("Check must not write history")
	}
	if rs := ev.Rules(); len(rs) != 1 || rs[0].ID != "r1" {
		t.Fatalf("Check must not change rules, got %+v", rs)
	}
}

// TestSubmitAuditRecordAndIdempotency covers applied/rejected records and
// retried pushes not double-applying.
func TestSubmitAuditRecordAndIdempotency(t *testing.T) {
	c, st, ev, m := harness(t)
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 1, Value: 11, SourceID: "s", DedupKey: "k0"})
	rep0, _ := ev.Eval(10)
	if len(rep0.States) != 1 || rep0.States[0].Status != model.StatusFiring {
		t.Fatalf("setup want firing, got %+v", rep0.States)
	}

	// Rejected batch first: invalid rule, nothing changes, but recorded.
	bad := rule("r2", "a", 10)
	bad.AggField = "nope"
	rep, verrs := m.Submit([]model.Rule{bad}, "push-1")
	t.Logf("INPUT push-1 invalid agg -> applied=%v errors=%d", rep.Applied(), len(verrs.Errs))
	if rep.Applied() || verrs == nil {
		t.Fatal("invalid submit must not apply")
	}
	if len(rep.ValidationErrors) == 0 {
		t.Fatal("rejected record must carry validation errors")
	}
	if rs := ev.Rules(); len(rs) != 1 {
		t.Fatalf("rejected submit must keep old rules, got %d", len(rs))
	}

	// Valid batch: modify r1 threshold (still fires), add r2; r1 firing kept.
	rep, verrs = m.Submit([]model.Rule{rule("r1", "a", 10), rule("r2", "b", 5)}, "push-2")
	if verrs != nil || !rep.Applied() {
		t.Fatalf("valid submit: %v", verrs)
	}
	if len(rep.Changes) != 2 {
		t.Fatalf("want 2 changes (r1 unchanged, r2 added), got %+v", rep.Changes)
	}
	for _, ch := range rep.Changes {
		t.Logf("JUDGE push-2 change rule=%s kind=%s", ch.RuleID, ch.Kind)
	}
	post, _ := ev.Eval(10)
	var r1 model.AlertState
	for _, s := range post.States {
		if s.RuleID == "r1" {
			r1 = s
		}
	}
	if r1.Status != model.StatusFiring || len(post.Events) != 0 {
		t.Fatalf("surviving firing alert must be preserved without re-fire: events=%+v state=%+v",
			post.Events, r1)
	}
	firstID := rep.BatchID

	// Retry with the same idempotency key must NOT re-apply (same record).
	c.t = 200
	rep2, _ := m.Submit([]model.Rule{rule("r1", "a", 10), rule("r2", "b", 5)}, "push-2")
	if rep2.BatchID != firstID {
		t.Fatalf("idempotent retry must return same batch %s, got %s", firstID, rep2.BatchID)
	}
	t.Logf("JUDGE retry push-2 -> same batch id %s (not double-applied)", firstID)

	hist := m.History()
	if len(hist) != 2 || hist[0].BatchID != firstID {
		t.Fatalf("history newest-first want applied push-2 first, got %+v", batchIDs(hist))
	}
	got, ok := m.Batch(firstID)
	if !ok || got.BatchID != firstID {
		t.Fatalf("batch lookup by id failed: %v", ok)
	}
}

// TestDeleteBatchRecordsAffectedAlerts: a deleting batch's record shows which
// tracked alerts it closed.
func TestDeleteBatchRecordsAffectedAlerts(t *testing.T) {
	_, st, ev, m := harness(t)
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 1, Value: 11, SourceID: "s", DedupKey: "k"})
	rep0, _ := ev.Eval(10)
	if len(rep0.States) != 1 || rep0.States[0].Status != model.StatusFiring {
		t.Fatalf("setup want one firing state, got %+v", rep0.States)
	}
	t.Log("INPUT r1 firing on host a; submit batch that deletes r1")

	rep, verrs := m.Submit(nil, "delete")
	if verrs != nil || !rep.Applied() {
		t.Fatalf("delete batch: %v", verrs)
	}
	if len(rep.ClosedEvents) != 1 || rep.ClosedEvents[0].Type != model.EventClosed {
		t.Fatalf("want one closed event, got %+v", rep.ClosedEvents)
	}
	if len(rep.ClosedAlerts) != 1 || rep.ClosedAlerts[0].RuleID != "r1" ||
		rep.ClosedAlerts[0].PreviousStatus != model.StatusFiring {
		t.Fatalf("closed record wrong: %+v", rep.ClosedAlerts)
	}
	t.Logf("JUDGE batch %s closed %s: %s", rep.BatchID, rep.ClosedAlerts[0].RuleID, rep.ClosedEvents[0].Reason)
}

// TestConcurrentEvalSeesWholeRuleSet: while many evaluation passes run, batch
// switches flip the rule set atomically. Every pass's captured rule snapshot
// must match one known complete batch, never a half-swapped intermediate. The
// test rendezvouses with B and then with the post-switch A2 snapshot (not an
// in-flight initial-A0 pass). The violating window fires exactly once.
func TestConcurrentEvalSeesWholeRuleSet(t *testing.T) {
	_, st, ev, m := harness(t)
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 5, Value: 11, SourceID: "s", DedupKey: "k0"})

	initialA := []model.Rule{rule("r1", "a", 10)}
	setB := []model.Rule{rule("r1", "a", 9), rule("r2", "b", 9)}
	setA := []model.Rule{rule("r1", "a", 8)}
	sigInitialA := ruleSetSignature(initialA)
	sigA := ruleSetSignature(setA)
	sigB := ruleSetSignature(setB)
	signatures := map[string]bool{sigInitialA: true, sigA: true, sigB: true}
	t.Logf("INPUT complete rule set initialA=%s", sigInitialA)
	t.Logf("INPUT complete rule set B=%s", sigB)

	type evalObservation struct {
		signature string
		firing    int
	}
	const workers = 4
	observations := make(chan evalObservation, workers)
	sawB := make(chan struct{}, 1)
	sawA := make(chan struct{}, 1)
	stop := make(chan struct{})
	badObservations := 0
	firingEvents := 0
	observed := map[string]int{}
	var evalWG, collectorWG sync.WaitGroup
	var recordMu sync.Mutex
	recordObservation := func(obs evalObservation) {
		recordMu.Lock()
		defer recordMu.Unlock()
		observed[obs.signature]++
		if !signatures[obs.signature] {
			badObservations++
		}
		firingEvents += obs.firing
	}
	notify := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	waitSignal := func(ch <-chan struct{}, want string) {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for an evaluation that captured complete set %s", want)
		}
	}

	// Persistent evaluators overlap with every submission in this test. A
	// dedicated collector means a worker can never be blocked while the main test
	// waits for a B/A rendezvous signal.
	for g := 0; g < workers; g++ {
		evalWG.Add(1)
		go func() {
			defer evalWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rep, err := ev.Eval(10)
				if err != nil {
					t.Errorf("eval: %v", err)
					return
				}
				firing := 0
				for _, e2 := range rep.Events {
					if e2.Type == model.EventFiring {
						firing++
					}
				}
				observations <- evalObservation{signature: ruleSetSignature(rep.Rules), firing: firing}
			}
		}()
	}
	collectorWG.Add(1)
	go func() {
		defer collectorWG.Done()
		for obs := range observations {
			recordObservation(obs)
			switch obs.signature {
			case sigB:
				notify(sawB)
			case sigA:
				notify(sawA)
			}
		}
	}()

	// Rendezvous with actual evaluations of each complete batch. This makes the
	// test deterministic without sleeps or merely checking the current set.
	if _, verrs := m.Submit(setB, "rendezvous-b"); verrs != nil {
		t.Fatalf("submit set B: %v", verrs)
	}
	waitSignal(sawB, "B")
	// A2 has a different signature from initial A0, so a pass that captured the
	// initial batch cannot satisfy this second rendezvous.
	select {
	case <-sawA:
	default:
	}
	if _, verrs := m.Submit(setA, "rendezvous-a"); verrs != nil {
		t.Fatalf("submit set A: %v", verrs)
	}
	waitSignal(sawA, "A")

	// Continue switching concurrently with the long-lived evaluation workers.
	var switchWG sync.WaitGroup
	switchWG.Add(1)
	go func() {
		defer switchWG.Done()
		for i := 0; i < 100; i++ {
			rules := setA
			if i%2 == 0 {
				rules = setB
			}
			m.Submit(rules, fmt.Sprintf("switch-%03d", i))
		}
	}()
	switchWG.Wait()
	close(stop)
	evalWG.Wait()
	close(observations)
	collectorWG.Wait()

	recordMu.Lock()
	totalObserved := observed[sigA] + observed[sigB] + observed[sigInitialA] + badObservations
	setACount := observed[sigA]
	setBCount := observed[sigB]
	initialACount := observed[sigInitialA]
	badCount := badObservations
	firingCount := firingEvents
	recordMu.Unlock()
	t.Logf("JUDGE evaluation snapshots: total=%d initialA=%d setA2=%d setB=%d torn=%d; firing events=%d",
		totalObserved, initialACount, setACount, setBCount, badCount, firingCount)
	if totalObserved < 2 {
		t.Fatalf("rendezvous must account for at least one pass per complete batch, got %d", totalObserved)
	}
	if badCount != 0 {
		t.Fatalf("eval observed a half-switched (torn) rule set %d times", badCount)
	}
	if setACount == 0 || setBCount == 0 {
		t.Fatalf("concurrent test must observe both complete batches, got setA=%d setB=%d",
			setACount, setBCount)
	}
	if firingCount != 1 {
		t.Fatalf("single violating window must fire exactly once, got %d firing events", firingCount)
	}
	final, _ := ev.Eval(10)
	var firing bool
	for _, s := range final.States {
		if s.RuleID == "r1" && s.Status == model.StatusFiring {
			firing = true
		}
	}
	if !firing {
		t.Fatal("r1 must consistently retain firing across switches")
	}
}

// ruleSetSignature produces an order-independent deterministic signature of a
// complete batch: rule ID plus selector and every critical configuration field.
// It cannot mistake two rules for one merely because their count matches.
func ruleSetSignature(rules []model.Rule) string {
	cp := append([]model.Rule(nil), rules...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	var b strings.Builder
	for i, r := range cp {
		if i > 0 {
			b.WriteByte(';')
		}
		keys := make([]string, 0, len(r.Selector))
		for k := range r.Selector {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "%s{selector={", r.ID)
		for j, k := range keys {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s=%s", k, r.Selector[k])
		}
		fmt.Fprintf(&b, "},window=%d,field=%s,op=%s,threshold=%s,pending=%d,keep=%d}",
			r.WindowSeconds, r.AggField, r.Op, model.FormatValue(r.Threshold),
			r.PendingWindows, r.KeepFiringWindows)
	}
	return b.String()
}

// --- small helpers ---

func batchIDs(bs []alert.BatchReport) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = b.BatchID
	}
	return out
}
