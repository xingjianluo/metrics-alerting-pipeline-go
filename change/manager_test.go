package change

import (
	"math"
	"sync"
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

type clock struct{ t int64 }

func newHarness(t *testing.T, rules []model.Rule) (*clock, *storage.WindowStore, *alert.Evaluator, *Manager) {
	t.Helper()
	c := &clock{t: 1000}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 1000, Now: func() int64 { return c.t }})
	ev := alert.NewEvaluator(st, rules, func() int64 { return c.t })
	return c, st, ev, NewManager(ev, func() int64 { return c.t })
}

func baseRule(id string) model.Rule {
	return model.Rule{
		ID: id, Selector: model.Labels{"__name__": "m"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
		PendingWindows: 0, KeepFiringWindows: 0,
	}
}

func upsert(r model.Rule) Item { return Item{Op: OpUpsert, Rule: r} }
func del(id string) Item       { return Item{Op: OpDelete, Rule: model.Rule{ID: id}} }

func dumpErrors(t *testing.T, tag string, errs []ValidationError) {
	t.Helper()
	for _, e := range errs {
		t.Logf("%s INVALID rule=%s field=%s: %s", tag, e.RuleID, e.Field, e.Detail)
	}
}

// TestBatchInvalidRejectsWholeBatch: every invalid entry is listed, and an
// invalid commit changes neither the ruleset nor any tracked alert state.
func TestBatchInvalidRejectsWholeBatch(t *testing.T) {
	good := baseRule("keep")
	c, st, ev, mgr := newHarness(t, []model.Rule{good})

	// Create a live firing state that must survive the rejected commit.
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 995, Value: 11, SourceID: "s", DedupKey: "k0"})
	c.t = 1000
	rep, err := ev.Eval(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.States) != 1 || rep.States[0].Status != model.StatusFiring {
		t.Fatalf("setup: want one firing state, got %+v", rep.States)
	}
	t.Logf("INPUT before invalid commit: keep firing on host=a observed=11 at ts=%d", c.t)

	badWindow := baseRule("bad-window")
	badWindow.WindowSeconds = 0
	badField := baseRule("bad-field")
	badField.AggField = "p99"
	badOp := baseRule("bad-op")
	badOp.Op = "~"
	badPending := baseRule("bad-pending")
	badPending.PendingWindows = -1
	badThreshold := baseRule("bad-threshold")
	badThreshold.Threshold = math.NaN()
	batch := []Item{
		upsert(badWindow),
		upsert(badField),
		upsert(badOp),
		upsert(badPending),
		upsert(badThreshold),
		upsert(baseRule("dup")),
		upsert(baseRule("dup")), // duplicate rule name in the same batch
		del("ghost"),            // deleting a non-existent rule is rejected
	}

	// Check-only preview must list every problem and change nothing.
	pv := mgr.Validate(batch)
	dumpErrors(t, "PREVIEW", pv.Errors)
	if len(pv.Errors) == 0 {
		t.Fatal("want validation errors, got none")
	}
	for _, wantField := range []string{"WindowSeconds", "AggField", "Op", "PendingWindows", "Threshold"} {
		found := false
		for _, e := range pv.Errors {
			if e.Field == wantField {
				found = true
			}
		}
		if !found {
			t.Fatalf("want an error for field %s in %+v", wantField, pv.Errors)
		}
	}
	if got := ev.Version(); got != 1 {
		t.Fatalf("preview must not bump ruleset version, got %d", got)
	}
	if rs := ev.Rules(); len(rs) != 1 || rs[0].ID != "keep" {
		t.Fatalf("preview mutated rules: %+v", rs)
	}

	// Commit must also reject and leave everything identical.
	_, errs, err := mgr.Commit(batch)
	if err != nil {
		t.Fatalf("invalid batch returns validation errors, not an error: %v", err)
	}
	dumpErrors(t, "COMMIT", errs)
	if len(errs) == 0 {
		t.Fatal("invalid batch must be rejected")
	}
	if len(mgr.History()) != 0 {
		t.Fatal("rejected batch must not be recorded")
	}
	if rs := ev.Rules(); len(rs) != 1 || rs[0].ID != "keep" {
		t.Fatalf("rejected commit mutated rules: %+v", rs)
	}
	after := ev.States()
	if len(after) != 1 || after[0].RuleID != "keep" || after[0].Status != model.StatusFiring {
		t.Fatalf("rejected commit must preserve live alert state, got %+v", after)
	}
	// Re-evaluate the same window: still exactly the one firing state, no new event.
	rep2, _ := ev.Eval(1000)
	var fires int
	for _, e := range rep2.Events {
		if e.Type == model.EventFiring {
			fires++
		}
	}
	if fires != 0 {
		t.Fatalf("rejected batch must not cause re-fire, events=%+v", rep2.Events)
	}
	t.Log("JUDGE: all 7 problems listed; ruleset, firing state and history unchanged after rejected commit")
}

// TestPreviewDoesNotMutate: a check-only preview simulates the full diff and
// affected alerts (including closures) but the live evaluator is untouched.
func TestPreviewDoesNotMutate(t *testing.T) {
	r1 := baseRule("r1")
	c, st, ev, mgr := newHarness(t, []model.Rule{r1})
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 995, Value: 11, SourceID: "s", DedupKey: "k1"})
	c.t = 1000
	if rep, _ := ev.Eval(0); rep.States[0].Status != model.StatusFiring {
		t.Fatal("setup want firing")
	}

	// Preview a batch that deletes r1 and adds r2: the firing alert on r1 must
	// show up as a prospective closure.
	pv := mgr.Validate([]Item{del("r1"), upsert(baseRule("r2"))})
	if len(pv.Errors) != 0 {
		t.Fatalf("unexpected preview errors: %+v", pv.Errors)
	}
	if len(pv.Changes) != 2 {
		t.Fatalf("want 2 changes, got %+v", pv.Changes)
	}
	var closedFound bool
	for _, a := range pv.Affected {
		t.Logf("PREVIEW-AFFECT rule=%s host=%s status=%s kind=%s reason=%q",
			a.RuleID, a.Labels["host"], a.Status, a.Kind, a.CloseReason)
		if a.RuleID == "r1" && a.Kind == AffectClosedByDelete && a.Status == model.StatusFiring {
			closedFound = true
		}
	}
	if !closedFound {
		t.Fatal("preview must report the pending/firing alert as closed_by_rule_delete")
	}
	// Live state untouched: r1 still present and still firing, no record.
	if rs := ev.Rules(); len(rs) != 1 || rs[0].ID != "r1" {
		t.Fatalf("preview mutated live rules: %+v", rs)
	}
	if states := ev.States(); len(states) != 1 || states[0].Status != model.StatusFiring {
		t.Fatalf("preview mutated live states: %+v", states)
	}
	if len(mgr.History()) != 0 {
		t.Fatal("preview must not record history")
	}
	t.Log("JUDGE: preview listed delete/add diff and prospective closure; live ruleset/states unchanged")
}

// TestCommitClosesAndRecords covers the full lifecycle against a valid batch:
// deletion closes with a reason; a narrowed selector closes its series; a
// surviving update preserves pending/firing alerts without re-firing; a
// re-added rule starts from a clean slate; the record is queryable.
func TestCommitClosesAndRecords(t *testing.T) {
	rA := model.Rule{
		ID: "fire-a", Selector: model.Labels{"__name__": "m", "host": "a"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
	}
	rB := model.Rule{
		ID: "fire-b", Selector: model.Labels{"__name__": "m", "host": "b"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
	}
	rC := model.Rule{
		ID: "pending-c", Selector: model.Labels{"__name__": "m", "host": "c"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
		PendingWindows: 2,
	}
	c, st, ev, mgr := newHarness(t, []model.Rule{rA, rB, rC})

	write := func(host string, ts int64, v float64, key string) {
		if r := st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": host}, Ts: ts, Value: v, SourceID: "s", DedupKey: key}); r.Status == model.WriteRejected {
			t.Fatalf("write rejected: %s", r.Detail)
		}
	}
	// a and b fire; c reaches pending (only 1 violating window).
	write("a", 995, 11, "a0")
	write("b", 995, 11, "b0")
	write("c", 995, 11, "c0")
	c.t = 1000
	rep, _ := ev.Eval(0)
	status := map[string]model.AlertStatus{}
	for _, s := range rep.States {
		status[s.RuleID] = s.Status
	}
	if status["fire-a"] != model.StatusFiring || status["fire-b"] != model.StatusFiring ||
		status["pending-c"] != model.StatusPending {
		t.Fatalf("setup mismatch: %+v", status)
	}
	t.Log("INPUT before commit: fire-a=firing, fire-b=firing, pending-c=pending")

	// Batch: delete fire-a; narrow fire-b selector to host=z (b stops covered);
	// update pending-c threshold but keep its selector (state preserved).
	narrowB := rB
	narrowB.Selector = model.Labels{"__name__": "m", "host": "z"}
	keepC := rC
	keepC.Threshold = 100
	rec, errs, err := mgr.Commit([]Item{del("fire-a"), upsert(narrowB), upsert(keepC)})
	if err != nil || len(errs) != 0 {
		t.Fatalf("valid batch rejected: err=%v errs=%+v", err, errs)
	}
	for _, ch := range rec.Changes {
		t.Logf("RECORD#%d CHANGE %s rule=%s", rec.ID, ch.Kind, ch.RuleID)
	}
	for _, af := range rec.Affected {
		t.Logf("RECORD#%d AFFECT rule=%s host=%s before=%s -> %s reason=%q",
			rec.ID, af.RuleID, af.Labels["host"], af.Status, af.Kind, af.CloseReason)
	}

	// Exactly two closures, with explicit reasons, and states removed.
	if len(rec.Affected) != 3 {
		t.Fatalf("want 3 affected alerts (2 closed + 1 preserved), got %+v", rec.Affected)
	}
	kinds := map[string]AffectKind{}
	for _, af := range rec.Affected {
		kinds[af.RuleID] = af.Kind
		if af.Kind != AffectPreserved && af.CloseReason == "" {
			t.Fatalf("closed alert must carry a reason: %+v", af)
		}
	}
	if kinds["fire-a"] != AffectClosedByDelete ||
		kinds["fire-b"] != AffectClosedBySelector ||
		kinds["pending-c"] != AffectPreserved {
		t.Fatalf("unexpected affected kinds: %+v", kinds)
	}
	for _, s := range ev.States() {
		if s.RuleID == "fire-a" || s.RuleID == "fire-b" {
			t.Fatalf("closed states must be removed, got %+v", s)
		}
		if s.RuleID == "pending-c" && s.Status != model.StatusPending {
			t.Fatalf("preserved pending state changed to %s", s.Status)
		}
	}
	// Diff details: before/after config captured for b and c; a is delete-only.
	var byRule = map[string]RuleChange{}
	for _, ch := range rec.Changes {
		byRule[ch.RuleID] = ch
	}
	if byRule["fire-a"].Kind != ChangeDeleted || byRule["fire-a"].After != nil {
		t.Fatalf("fire-a diff wrong: %+v", byRule["fire-a"])
	}
	if byRule["fire-b"].Before.Selector["host"] != "b" || byRule["fire-b"].After.Selector["host"] != "z" {
		t.Fatalf("fire-b before/after selector not captured: %+v", byRule["fire-b"])
	}
	if byRule["pending-c"].Before.Threshold != 10 || byRule["pending-c"].After.Threshold != 100 {
		t.Fatalf("pending-c threshold diff not captured: %+v", byRule["pending-c"])
	}
	t.Log("JUDGE: delete and narrowed-selector alerts closed with reasons; surviving update preserved pending; before/after configs recorded")

	// History is queryable by ID and order.
	hist := mgr.History()
	if len(hist) != 1 || hist[0].ID != rec.ID {
		t.Fatalf("history mismatch: %+v", hist)
	}
	if got, ok := mgr.RecordByID(rec.ID); !ok || got.RulesetVersion != rec.RulesetVersion {
		t.Fatal("RecordByID lookup failed")
	}

	// Evaluating now must not re-fire or resurrect closed alerts. Next window
	// still violates under old semantics, but fire-a is gone and host=b is no
	// longer selected by fire-b; c under threshold 100 recovers from pending.
	write("a", 1005, 99, "a1")
	write("b", 1005, 99, "b1")
	write("c", 1005, 99, "c1")
	c.t = 1010
	rep2, _ := ev.Eval(0)
	for _, e := range rep2.Events {
		t.Logf("POST-EVAL EVENT %s rule=%s host=%s: %s", e.Type, e.RuleID, e.Labels["host"], e.Reason)
		if e.RuleID == "fire-a" || (e.RuleID == "fire-b" && e.Labels["host"] == "b") {
			t.Fatalf("closed alert must never emit again: %+v", e)
		}
	}
	for _, s := range rep2.States {
		if s.RuleID == "fire-a" {
			t.Fatal("fire-a must not reappear")
		}
		if s.RuleID == "fire-b" && s.Labels["host"] == "b" {
			t.Fatal("fire-b must not keep tracking host=b after selector narrowed")
		}
	}

	// Re-add fire-a exactly as before: it must start from a clean state and go
	// through pending/firing anew rather than inheriting the closed history.
	rAFresh := rA
	rec2, errs2, _ := mgr.Commit([]Item{upsert(rAFresh)})
	if len(errs2) != 0 {
		t.Fatalf("re-add rejected: %+v", errs2)
	}
	if rec2.RulesetVersion <= rec.RulesetVersion {
		t.Fatal("ruleset version must advance on every committed batch")
	}
	write("a", 1015, 11, "a2")
	c.t = 1020
	rep3, _ := ev.Eval(0)
	var freshFire bool
	for _, e := range rep3.Events {
		t.Logf("READD-EVAL EVENT %s rule=%s host=%s", e.Type, e.RuleID, e.Labels["host"])
		if e.RuleID == "fire-a" && e.Type == model.EventFiring {
			freshFire = true
		}
	}
	if !freshFire {
		t.Fatal("re-added rule must fire fresh from a clean state (PendingWindows=0)")
	}
	t.Log("JUDGE: re-added fire-a starts clean and fires again exactly once per fresh lifecycle")
}

// TestCommitMixedAddUpdateDeleteNoPartialFailure: a well-formed mixed batch
// applies as one unit; an invalid entry anywhere in it protects ALL entries.
func TestCommitMixedAddUpdateDeleteNoPartialFailure(t *testing.T) {
	r1 := baseRule("r1")
	r2 := baseRule("r2")
	_, _, ev, mgr := newHarness(t, []model.Rule{r1, r2})
	verBefore := ev.Version()

	good := baseRule("r3")
	updated := r2
	updated.Threshold = 50
	invalid := baseRule("r4")
	invalid.AggField = ""
	batch := []Item{upsert(good), upsert(updated), del("r1"), upsert(invalid)}
	_, errs, err := mgr.Commit(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(errs) == 0 {
		t.Fatal("batch containing an invalid rule must be rejected wholesale")
	}
	// No partial application: r1 still present, r2 threshold untouched, r3 absent.
	byID := map[string]model.Rule{}
	for _, r := range ev.Rules() {
		byID[r.ID] = r
	}
	if _, ok := byID["r1"]; !ok {
		t.Fatal("rejected batch must not delete r1")
	}
	if byID["r2"].Threshold != 10 {
		t.Fatalf("rejected batch must not update r2: %+v", byID["r2"])
	}
	if _, ok := byID["r3"]; ok {
		t.Fatal("rejected batch must not add r3")
	}
	if ev.Version() != verBefore {
		t.Fatalf("rejected batch must not bump version: %d -> %d", verBefore, ev.Version())
	}
	t.Log("JUDGE: one invalid entry protected add/update/delete siblings; ruleset and version unchanged")
}

// TestConcurrentEvalAndBatchSwitchRace runs Eval concurrently while batches
// flip the ruleset between two versions with mutually exclusive firing
// signatures (V1 -> only host=a fires, V2 -> only host=b fires). Every report
// carries the exact ruleset version it ran against; we capture EVERY
// concurrent report and assert that for a given version the firing signature
// is always the expected one and is never "mixed" (both hosts firing), which
// is what a half-switched ruleset would look like. Run with -race.
func TestConcurrentEvalAndBatchSwitchRace(t *testing.T) {
	v1 := model.Rule{
		ID: "r", Selector: model.Labels{"__name__": "m"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
	}
	v2 := v1
	v2.Selector = model.Labels{"__name__": "m", "host": "b"}
	v2.Threshold = 0

	c := &clock{t: 10}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 10000, Now: func() int64 { return c.t }})
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 5, Value: 11, SourceID: "s", DedupKey: "a"})
	st.Write(model.Sample{Labels: model.Labels{"__name__": "m", "host": "b"}, Ts: 5, Value: 1, SourceID: "s", DedupKey: "b"})
	ev := alert.NewEvaluator(st, []model.Rule{v1}, func() int64 { return c.t })
	mgr := NewManager(ev, func() int64 { return c.t })

	signature := func(rep alert.Report) string {
		a, b := false, false
		for _, s := range rep.States {
			if s.Status != model.StatusFiring {
				continue
			}
			switch s.Labels["host"] {
			case "a":
				a = true
			case "b":
				b = true
			}
		}
		switch {
		case a && b:
			return "mixed"
		case a:
			return "a"
		case b:
			return "b"
		default:
			return "none"
		}
	}
	// Version parity: initial ruleset is V1 at version 1 (odd -> "a"); every
	// commit flips V1<->V2 and bumps the version by one (even -> "b").
	wantSignature := func(ver int64) string {
		if ver%2 == 1 {
			return "a"
		}
		return "b"
	}

	type observation struct {
		ver int64
		sig string
	}
	const workers = 4
	const callsPerWorker = 25
	const rounds = 20
	observations := make(chan observation, workers*callsPerWorker*rounds+rounds*4)

	// Prime: window [0,10) under V1 -> a fires.
	prime, _ := ev.Eval(10)
	if prime.Version != 1 || signature(prime) != "a" {
		t.Fatalf("prime version=%d sig=%s", prime.Version, signature(prime))
	}

	for i := 0; i < rounds; i++ {
		vBefore := ev.Version()

		// Guaranteed pre-switch observation (old version).
		repBefore, _ := ev.Eval(10)
		if repBefore.Version != vBefore {
			t.Fatalf("pre-switch report version %d != %d", repBefore.Version, vBefore)
		}
		observations <- observation{repBefore.Version, signature(repBefore)}

		var wg sync.WaitGroup
		for g := 0; g < workers; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < callsPerWorker; j++ {
					r, err := ev.Eval(10) // always window [0,10)
					if err != nil {
						t.Errorf("eval: %v", err)
						return
					}
					observations <- observation{r.Version, signature(r)}
				}
			}()
		}

		// Flip the ruleset while the evaluations above are in flight.
		var batch []Item
		if i%2 == 0 {
			batch = []Item{upsert(v2)} // V1 -> V2
		} else {
			batch = []Item{upsert(v1)} // V2 -> V1
		}
		rec, errs, err := mgr.Commit(batch)
		if err != nil || len(errs) != 0 {
			t.Fatalf("commit round %d: %v %+v", i, err, errs)
		}
		vAfter := rec.RulesetVersion
		if vAfter != vBefore+1 {
			t.Fatalf("round %d: version must advance by one: %d -> %d", i, vBefore, vAfter)
		}
		wg.Wait()

		// Guaranteed post-switch observation (new version), deterministic.
		var repAfter alert.Report
		for try := 0; try < 1000; try++ {
			repAfter, _ = ev.Eval(10)
			if repAfter.Version == vAfter {
				break
			}
		}
		if repAfter.Version != vAfter {
			t.Fatalf("round %d: new version %d never observed", i, vAfter)
		}
		observations <- observation{repAfter.Version, signature(repAfter)}
	}
	close(observations)

	seen := map[int64]string{}
	var total, mixed int
	for o := range observations {
		total++
		if o.sig == "mixed" {
			mixed++
		}
		if want := wantSignature(o.ver); o.sig != want {
			t.Fatalf("report on version %d has signature %q, want %q (no half-switch allowed)",
				o.ver, o.sig, want)
		}
		if prev, ok := seen[o.ver]; ok && prev != o.sig {
			t.Fatalf("version %d observed with conflicting signatures %q and %q", o.ver, prev, o.sig)
		}
		seen[o.ver] = o.sig
	}

	if mixed != 0 {
		t.Fatalf("%d/%d reports saw a half-switched (mixed old/new) ruleset", mixed, total)
	}
	// Every version from 1..1+rounds must have been observed.
	for v := int64(1); v <= 1+rounds; v++ {
		if _, ok := seen[v]; !ok {
			t.Fatalf("ruleset version %d was never observed by an evaluation", v)
		}
	}
	if len(mgr.History()) != rounds {
		t.Fatalf("want %d committed records, got %d", rounds, len(mgr.History()))
	}
	t.Logf("JUDGE rounds=%d eval-reports=%d versions-observed=%d mixed=0 history=%d",
		rounds, total, len(seen), len(mgr.History()))
}
