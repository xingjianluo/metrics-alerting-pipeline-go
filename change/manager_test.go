package change

import (
	"fmt"
	"sync"
	"testing"

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
// tracked alerts it closed, and the two outcome groups are kept separate.
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
	if len(rep.Kept) != 0 {
		t.Fatalf("deleting the only rule must keep nothing tracked, got %+v", rep.Kept)
	}
	if len(rep.Closed) != 1 || rep.Closed[0].RuleID != "r1" ||
		rep.Closed[0].PreviousStatus != model.StatusFiring ||
		rep.Closed[0].Outcome != alert.OutcomeClosed ||
		rep.Closed[0].Conclusion != alert.ConclusionRuleDeleted {
		t.Fatalf("closed record wrong: %+v", rep.Closed)
	}
	all := rep.AffectedAlerts()
	if len(all) != 1 || all[0].Outcome != alert.OutcomeClosed {
		t.Fatalf("combined affected view wrong: %+v", all)
	}
	t.Logf("JUDGE batch %s closed %s: %s", rep.BatchID, rep.Closed[0].RuleID, rep.ClosedEvents[0].Reason)
}

// TestConfigOnlySubmitKeepsTrackedRecord: a batch that changes only critical
// config while still covering the same series records the firing alert under
// the "tracked" outcome, keeps it firing and does not re-trigger it.
func TestConfigOnlySubmitKeepsTrackedRecord(t *testing.T) {
	_, st, ev, m := harness(t)
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 1, Value: 11, SourceID: "s", DedupKey: "k"})
	if rep0, _ := ev.Eval(10); rep0.States[0].Status != model.StatusFiring {
		t.Fatalf("setup want firing, got %+v", rep0.States)
	}
	t.Log("INPUT r1 firing on host a; submit threshold-only change (selector unchanged)")

	// New threshold 5 keeps the observation 11 violating, so the alert must be
	// preserved and not re-triggered (a genuinely non-violating threshold would
	// be a legitimate data-driven resolution, not what this case pins down).
	rep, verrs := m.Submit([]model.Rule{rule("r1", "a", 5)}, "cfg")
	if verrs != nil || !rep.Applied() {
		t.Fatalf("config-only submit: %v", verrs)
	}
	if len(rep.Changes) != 1 || rep.Changes[0].Kind != alert.ChangeModified {
		t.Fatalf("want one modified change, got %+v", rep.Changes)
	}
	if len(rep.Closed) != 0 || len(rep.ClosedEvents) != 0 {
		t.Fatalf("config-only change must close nothing, got %+v / %+v", rep.Closed, rep.ClosedEvents)
	}
	if len(rep.Kept) != 1 {
		t.Fatalf("want the firing alert recorded as kept/tracked, got %+v", rep.Kept)
	}
	k := rep.Kept[0]
	if k.RuleID != "r1" || k.Labels["host"] != "a" ||
		k.Outcome != alert.OutcomeTracked || k.Conclusion != alert.ConclusionStillTracked ||
		k.PreviousStatus != model.StatusFiring {
		t.Fatalf("kept record wrong: %+v", k)
	}
	post, _ := ev.Eval(10)
	if len(post.Events) != 0 {
		t.Fatalf("kept firing must not re-fire, events=%+v", post.Events)
	}
	if post.States[0].Status != model.StatusFiring {
		t.Fatalf("firing must be preserved, got %+v", post.States)
	}
	// The stored audit record carries the same separated views.
	got, ok := m.Batch(rep.BatchID)
	if !ok || len(got.Kept) != 1 || len(got.Closed) != 0 {
		t.Fatalf("history record must retain kept/closed views, got ok=%v kept=%d closed=%d",
			ok, len(got.Kept), len(got.Closed))
	}
	t.Logf("JUDGE batch %s: firing alert outcome=%s conclusion=%s reason=%q",
		rep.BatchID, k.Outcome, k.Conclusion, k.Reason)
}

// recordingReader wraps the store and records, in call order, the selector
// fingerprint of every Query an evaluation pass issues. Because Eval holds the
// evaluator lock for the whole pass, passes (and hence their Query calls) never
// interleave: grouping the recorded calls by rulesPerPass reconstructs exactly
// the rule set each individual evaluation saw.
type recordingReader struct {
	inner *storage.WindowStore
	mu    sync.Mutex
	calls []string
}

func (r *recordingReader) Query(start, end, granularitySeconds int64, selector model.Labels) (*model.QueryResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, selector.Fingerprint())
	r.mu.Unlock()
	return r.inner.Query(start, end, granularitySeconds, selector)
}

func (r *recordingReader) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func recRule(id, host string) model.Rule {
	return model.Rule{
		ID:            id,
		Selector:      model.Labels{"__name__": "m", "host": host},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
		PendingWindows: 0, KeepFiringWindows: 0,
	}
}

// TestConcurrentEvalSeesWholeRuleSet: while many evaluation passes run, batch
// switches flip the rule set atomically. The test reconstructs the exact rule
// set each pass queried storage with and requires it to be one complete set or
// the other — never a half-swapped mix — and the continuously-covered anchor
// rule fires exactly once for its single violating window.
func TestConcurrentEvalSeesWholeRuleSet(t *testing.T) {
	c := &clk{t: 100}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 1000, Now: func() int64 { return c.t }})
	rr := &recordingReader{inner: st}

	// Both sets have 3 rules. r0 (host e) is identical in both sets and stays
	// continuously covered; r1 and r2 flip selectors, so the ordered selector
	// fingerprints of a pass differ in TWO slots between the sets. Any torn,
	// half-swapped intermediate would produce a chunk matching neither set.
	setA := []model.Rule{recRule("r0", "e"), recRule("r1", "a"), recRule("r2", "b")}
	setB := []model.Rule{recRule("r0", "e"), recRule("r1", "c"), recRule("r2", "d")}
	fp := func(host string) string {
		return model.Labels{"__name__": "m", "host": host}.Fingerprint()
	}
	sigA := [3]string{fp("e"), fp("a"), fp("b")}
	sigB := [3]string{fp("e"), fp("c"), fp("d")}
	const rulesPerPass = 3

	ev := alert.NewEvaluator(rr, setA, func() int64 { return c.t })
	m := NewManager(ev, func() int64 { return c.t })

	// Violating data for every host the two sets touch.
	for _, h := range []string{"e", "a", "b", "c", "d"} {
		st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": h}, Ts: 5, Value: 11, SourceID: "s", DedupKey: "k-" + h})
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var tornPasses int
	var anchorFiring int

	matchSig := func(chunk []string, sig [3]string) bool {
		return chunk[0] == sig[0] && chunk[1] == sig[1] && chunk[2] == sig[2]
	}

	// Goroutine 0 also flips setA<->setB in lockstep with its loop counter;
	// goroutines 1..3 only evaluate.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				rep, err := ev.Eval(10)
				if err != nil {
					t.Errorf("eval: %v", err)
					return
				}
				mu.Lock()
				if len(ev.Rules()) != rulesPerPass {
					tornPasses++
				}
				for _, e2 := range rep.Events {
					if e2.Type == model.EventFiring && e2.RuleID == "r0" {
						anchorFiring++
					}
				}
				mu.Unlock()
				if g == 0 && i%10 == 0 {
					rules := setA
					if i/10%2 == 1 {
						rules = setB
					}
					m.Submit(rules, fmt.Sprintf("switch-%d", i))
				}
			}
		}(g)
	}
	wg.Wait()

	// Reconstruct each evaluation pass from the recorded Query calls and
	// require a complete set A or complete set B for every single pass.
	calls := rr.snapshot()
	if len(calls)%rulesPerPass != 0 {
		t.Fatalf("query calls %d not divisible by %d (interleaved/torn pass)", len(calls), rulesPerPass)
	}
	for i := 0; i < len(calls); i += rulesPerPass {
		chunk := calls[i : i+rulesPerPass]
		if !matchSig(chunk, sigA) && !matchSig(chunk, sigB) {
			tornPasses++
			t.Errorf("pass %d saw a torn rule set: %v (neither whole A %v nor whole B %v)",
				i/rulesPerPass, chunk, sigA, sigB)
		}
	}

	t.Logf("JUDGE reconstructed %d evaluation passes from %d queries; torn=%d anchor(r0) firing events=%d",
		len(calls)/rulesPerPass, len(calls), tornPasses, anchorFiring)
	if tornPasses != 0 {
		t.Fatalf("at least one evaluation observed a half-switched rule set")
	}
	if anchorFiring != 1 {
		t.Fatalf("continuously-covered anchor rule must fire exactly once, got %d firing events", anchorFiring)
	}
	final, _ := ev.Eval(10)
	var anchorOK bool
	for _, s := range final.States {
		if s.RuleID == "r0" && s.Status == model.StatusFiring {
			anchorOK = true
		}
	}
	if !anchorOK {
		t.Fatal("anchor rule r0 must consistently retain firing across atomic switches")
	}
}

// --- small helpers ---

func batchIDs(bs []alert.BatchReport) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = b.BatchID
	}
	return out
}
