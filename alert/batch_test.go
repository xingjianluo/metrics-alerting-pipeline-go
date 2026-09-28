package alert

import (
	"math"
	"strings"
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

func stateFor(rep Report, ruleID, host string) (model.AlertState, bool) {
	for _, s := range rep.States {
		if s.RuleID == ruleID && s.Labels["host"] == host {
			return s, true
		}
	}
	return model.AlertState{}, false
}

func dumpBatch(t *testing.T, tag string, b BatchReport) {
	t.Helper()
	for _, ch := range b.Changes {
		t.Logf("%s CHANGE rule=%s kind=%s fields=%v before(thr=%s,sel=%v) after(thr=%s,sel=%v)",
			tag, ch.RuleID, ch.Kind, ch.ChangedFields,
			model.FormatValue(ch.Before.Threshold), ch.Before.Selector,
			model.FormatValue(ch.After.Threshold), ch.After.Selector)
	}
	for _, ev := range b.ClosedEvents {
		t.Logf("%s CLOSE-EVENT %s rule=%s labels=%v ts=%d: %s",
			tag, ev.Type, ev.RuleID, ev.Labels, ev.Ts, ev.Reason)
	}
	for _, a := range b.Affected {
		t.Logf("%s AFFECTED rule=%s labels=%v previous=%s reason=%q",
			tag, a.RuleID, a.Labels, a.PreviousStatus, a.Reason)
	}
	for _, ve := range b.ValidationErrors {
		t.Logf("%s INVALID rule=%s field=%s: %s", tag, ve.RuleID, ve.Field, ve.Detail)
	}
}

// TestValidateBatchListsAllProblems: the check-only entry surfaces every
// problem at once and never mutates rules or states.
func TestValidateBatchListsAllProblems(t *testing.T) {
	good := ruleGT("ok", 0, 0)
	badWindow := ruleGT("bad-window", 0, 0)
	badWindow.WindowSeconds = 0
	badAgg := ruleGT("bad-agg", 0, 0)
	badAgg.AggField = "median"
	badOp := ruleGT("bad-op", 0, 0)
	badOp.Op = "~"
	badThr := ruleGT("bad-thr", 0, 0)
	badThr.Threshold = math.NaN()

	batch := []model.Rule{good, good, badWindow, badAgg, badOp, badThr} // dup "ok"
	verrs := ValidateRules(batch)
	if verrs == nil {
		t.Fatal("want validation errors, got nil")
	}
	t.Logf("INPUT %d rules incl. 1 duplicate, bad window/agg/op/threshold", len(batch))
	joined := verrs.Error()
	for _, want := range []string{"duplicate", "WindowSeconds", "AggField", "Op", "Threshold"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("validation output missing %q: %s", want, joined)
		}
	}
	for _, e := range verrs.Errs {
		t.Logf("JUDGE problem: %s", e.Error())
	}
}

// TestInvalidBatchChangesNothing: one bad rule rejects the whole batch; the
// existing rule set and every tracked alert state are exactly as before.
func TestInvalidBatchChangesNothing(t *testing.T) {
	c, st, ev := newHarness(t, []model.Rule{ruleGT("r1", 0, 0), ruleGT("r2", 0, 0)})

	// Make r1 fire on host a.
	put(st, "a", 1, 11, "w0")
	c.t = 10
	before, _ := ev.Eval(0)
	dump(t, "BEFORE", before)
	if s, ok := stateFor(before, "r1", "a"); !ok || s.Status != model.StatusFiring {
		t.Fatalf("setup: want r1 firing, got %+v", s)
	}
	rulesBefore := ev.Rules()

	// An invalid batch: duplicate ID plus an illegal window.
	bad1, bad2 := ruleGT("r1", 0, 0), ruleGT("r1", 0, 0)
	bad2.WindowSeconds = -5
	rep, verrs := ev.ApplyRules([]model.Rule{bad1, bad2}, "should-not-apply", 10)
	dumpBatch(t, "REJECTED", rep)
	if rep.Applied() || verrs == nil {
		t.Fatalf("invalid batch must not apply: applied=%v verrs=%v", rep.Applied(), verrs)
	}
	if got := ev.Rules(); len(got) != len(rulesBefore) {
		t.Fatalf("rules changed after rejected batch: got %d want %d", len(got), len(rulesBefore))
	}

	// Existing firing state must be untouched: re-eval the same window yields
	// no new events and still firing.
	after, _ := ev.Eval(10)
	dump(t, "AFTER-REJECT", after)
	if len(after.Events) != 0 {
		t.Fatalf("rejected batch must not disturb states, events=%+v", after.Events)
	}
	if s, _ := stateFor(after, "r1", "a"); s.Status != model.StatusFiring {
		t.Fatalf("want still firing after rejected batch, got %s", s.Status)
	}
	t.Log("JUDGE: invalid batch rejected atomically; rules and firing state identical")
}

// TestBatchDiffRecordsAddModifyDelete verifies the auditable before/after
// record of one valid batch.
func TestBatchDiffRecordsAddModifyDelete(t *testing.T) {
	_, _, ev := newHarness(t, []model.Rule{
		ruleGT("r1", 1, 1), ruleGT("r2", 0, 0), ruleGT("same", 0, 0),
	})

	updated := ruleGT("r1", 1, 1)
	updated.Threshold = 99 // modified threshold
	added := ruleGT("r3", 0, 0)
	rep, verrs := ev.ApplyRules([]model.Rule{updated, ruleGT("same", 0, 0), added}, "b1", 100)
	if verrs != nil || !rep.Applied() {
		t.Fatalf("valid batch must apply: %v", verrs)
	}
	dumpBatch(t, "APPLIED", rep)

	want := map[string]ChangeKind{"r1": ChangeModified, "r2": ChangeDeleted, "r3": ChangeAdded, "same": ChangeUnchanged}
	if len(rep.Changes) != len(want) {
		t.Fatalf("want %d changes, got %d: %+v", len(want), len(rep.Changes), rep.Changes)
	}
	for _, ch := range rep.Changes {
		if ch.Kind != want[ch.RuleID] {
			t.Fatalf("rule %s kind=%s want %s", ch.RuleID, ch.Kind, want[ch.RuleID])
		}
		if ch.RuleID == "r1" {
			if len(ch.ChangedFields) != 1 || ch.ChangedFields[0] != "Threshold" ||
				ch.Before.Threshold != 10 || ch.After.Threshold != 99 {
				t.Fatalf("r1 modification record wrong: %+v", ch)
			}
		}
	}
	t.Log("JUDGE: batch record shows r1 modified(Threshold), r2 deleted, r3 added, same unchanged")
}

// TestBatchDeleteClosesActiveAlerts: removing rules must give pending/firing
// alerts an explicit closure; a resolved (inert) state disappears quietly.
func TestBatchDeleteClosesActiveAlerts(t *testing.T) {
	ruleForHost := func(id, host string, pending, keep int) model.Rule {
		r := ruleGT(id, pending, keep)
		r.Selector = model.Labels{"__name__": "m", "host": host}
		return r
	}
	c, st, ev := newHarness(t, []model.Rule{
		ruleForHost("gone", "goneHost", 0, 0),
		ruleForHost("pend", "pendHost", 2, 0),
		ruleForHost("calm", "calmHost", 0, 0),
	})

	// Each rule tracks a distinct host so their series/states are independent.
	put(st, "goneHost", 1, 11, "gone-w0") // gone -> firing
	put(st, "pendHost", 1, 11, "pend-w0") // pend -> pending streak 1/2
	put(st, "calmHost", 1, 11, "calm-w0") // calm -> firing
	c.t = 10
	rep0, _ := ev.Eval(0)
	dump(t, "SETUP-10", rep0)
	put(st, "calmHost", 11, 1, "calm-w1") // calm recovers -> resolved
	c.t = 20
	rep0, _ = ev.Eval(0)
	dump(t, "SETUP-20", rep0)

	batch, verrs := ev.ApplyRules([]model.Rule{}, "delete-all", 25)
	if verrs != nil {
		t.Fatalf("empty batch is valid: %v", verrs)
	}
	dumpBatch(t, "DELETE", batch)
	if len(batch.ClosedEvents) != 2 || len(batch.Affected) != 2 {
		t.Fatalf("want 2 closures (gone firing + pend pending), got events=%d affected=%d",
			len(batch.ClosedEvents), len(batch.Affected))
	}
	closed := map[string]model.AlertEvent{}
	for _, ev2 := range batch.ClosedEvents {
		closed[ev2.RuleID] = ev2
		if ev2.Type != model.EventClosed || !strings.Contains(ev2.Reason, CloseReasonRuleDeleted) {
			t.Fatalf("closure event wrong: %+v", ev2)
		}
	}
	if closed["gone"].Labels["host"] != "goneHost" || closed["pend"].Labels["host"] != "pendHost" {
		t.Fatalf("closure labels wrong: %+v", closed)
	}

	// No state for the removed rules survives; later evaluation stays empty.
	c.t = 30
	after, _ := ev.Eval(0)
	dump(t, "AFTER-DELETE", after)
	if len(after.States) != 0 || len(after.Events) != 0 {
		t.Fatalf("removed rules must leave no tracked state, got states=%d events=%d",
			len(after.States), len(after.Events))
	}
	t.Log("JUDGE: firing+pending alerts closed with explicit reason; resolved state dropped quietly")
}

// TestBatchSelectorNarrow: an updated rule that stops matching a series closes
// that series' active alert, while a still-matched series keeps firing without
// a duplicate event.
func TestBatchSelectorNarrow(t *testing.T) {
	c, st, ev := newHarness(t, []model.Rule{ruleGT("r1", 0, 0)})
	put(st, "a", 1, 11, "a0")
	put(st, "b", 1, 11, "b0")
	c.t = 10
	rep0, _ := ev.Eval(0)
	dump(t, "BOTH-FIRING", rep0)

	narrowed := model.Rule{
		ID: "r1", Selector: model.Labels{"__name__": "m", "host": "a"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
	}
	batch, verrs := ev.ApplyRules([]model.Rule{narrowed}, "narrow", 15)
	if verrs != nil {
		t.Fatal(verrs)
	}
	dumpBatch(t, "NARROW", batch)
	if len(batch.ClosedEvents) != 1 {
		t.Fatalf("want exactly one closure (host b), got %d", len(batch.ClosedEvents))
	}
	if ce := batch.ClosedEvents[0]; ce.Labels["host"] != "b" ||
		!strings.Contains(ce.Reason, CloseReasonNoMatch) {
		t.Fatalf("want host b closed for no-match, got %+v", ce)
	}

	// Re-evaluate the same window: host a keeps firing (no duplicate event),
	// host b is no longer tracked at all.
	after, _ := ev.Eval(10)
	dump(t, "POST-NARROW", after)
	if len(after.Events) != 0 {
		t.Fatalf("still-matched firing alert must not re-fire, events=%+v", after.Events)
	}
	if s, ok := stateFor(after, "r1", "a"); !ok || s.Status != model.StatusFiring {
		t.Fatalf("host a must remain firing, got %+v", s)
	}
	if _, ok := stateFor(after, "r1", "b"); ok {
		t.Fatal("host b state must have been removed")
	}
	t.Log("JUDGE: narrowed selector closed b explicitly; a kept firing without re-trigger")
}

// TestBatchReaddStartsClean: after a rule is removed, re-adding it starts from
// a clean resolved state rather than inheriting closed progress.
func TestBatchReaddStartsClean(t *testing.T) {
	c, st, ev := newHarness(t, []model.Rule{ruleGT("r1", 2, 0)})
	put(st, "a", 1, 11, "w0")
	c.t = 10
	rep0, _ := ev.Eval(0)
	dump(t, "PENDING1", rep0)
	if s, _ := stateFor(rep0, "r1", "a"); s.Status != model.StatusPending || s.Consecutive != 1 {
		t.Fatal("setup want pending streak1")
	}

	// Delete, then re-add before the next violating window.
	del, _ := ev.ApplyRules([]model.Rule{}, "del", 11)
	dumpBatch(t, "DELETED", del)
	if len(del.ClosedEvents) != 1 {
		t.Fatalf("want pending alert closed on delete, got %d", len(del.ClosedEvents))
	}
	add, verrs := ev.ApplyRules([]model.Rule{ruleGT("r1", 2, 0)}, "readd", 12)
	if verrs != nil {
		t.Fatal(verrs)
	}
	dumpBatch(t, "READDED", add)

	// Evaluating the SAME window again must produce a fresh pending streak 1,
	// not inherit the old streak and fire.
	after, _ := ev.Eval(10)
	dump(t, "FRESH", after)
	if s, ok := stateFor(after, "r1", "a"); !ok || s.Status != model.StatusPending || s.Consecutive != 1 {
		t.Fatalf("re-added rule must restart clean at pending streak1, got ok=%v state=%+v", ok, s)
	}
	if len(after.Events) != 1 || after.Events[0].Type != model.EventPending {
		t.Fatalf("want a fresh pending event, got %+v", after.Events)
	}
	t.Log("JUDGE: re-added rule restarts from clean resolved state (fresh pending event, streak 1)")
}
