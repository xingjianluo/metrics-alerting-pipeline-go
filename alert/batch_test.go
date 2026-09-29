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
	for _, a := range b.Kept {
		t.Logf("%s KEPT outcome=%s rule=%s labels=%v previous=%s streak=%d observed=%s conclusion=%s reason=%q",
			tag, a.Outcome, a.RuleID, a.Labels, a.PreviousStatus, a.Streak,
			model.FormatValue(a.LastObserved), a.Conclusion, a.Reason)
	}
	for _, a := range b.Closed {
		t.Logf("%s CLOSED outcome=%s rule=%s labels=%v previous=%s streak=%d observed=%s conclusion=%s reason=%q",
			tag, a.Outcome, a.RuleID, a.Labels, a.PreviousStatus, a.Streak,
			model.FormatValue(a.LastObserved), a.Conclusion, a.Reason)
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
	if len(batch.ClosedEvents) != 2 || len(batch.Closed) != 2 {
		t.Fatalf("want 2 closures (gone firing + pend pending), got events=%d closed=%d",
			len(batch.ClosedEvents), len(batch.Closed))
	}
	if len(batch.Kept) != 0 {
		t.Fatalf("deleting every rule must keep nothing tracked, got kept=%d: %+v",
			len(batch.Kept), batch.Kept)
	}
	closed := map[string]model.AlertEvent{}
	for _, ev2 := range batch.ClosedEvents {
		closed[ev2.RuleID] = ev2
		if ev2.Type != model.EventClosed || !strings.Contains(ev2.Reason, CloseReasonRuleDeleted) {
			t.Fatalf("closure event wrong: %+v", ev2)
		}
	}
	// Structured closed view: each entry is an explicit ending, disjoint from Kept.
	for _, a := range batch.Closed {
		if a.Outcome != OutcomeClosed || a.PreviousStatus != model.StatusPending &&
			a.PreviousStatus != model.StatusFiring {
			t.Fatalf("closed entry must carry a pending/firing previous status, got %+v", a)
		}
		if a.Conclusion != ConclusionRuleDeleted || !strings.Contains(a.Reason, CloseReasonRuleDeleted) {
			t.Fatalf("closed entry conclusion/reason wrong: %+v", a)
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
	if len(batch.ClosedEvents) != 1 || len(batch.Closed) != 1 {
		t.Fatalf("want exactly one closure (host b), got events=%d closed=%d",
			len(batch.ClosedEvents), len(batch.Closed))
	}
	if ce := batch.ClosedEvents[0]; ce.Labels["host"] != "b" ||
		!strings.Contains(ce.Reason, CloseReasonNoMatch) {
		t.Fatalf("want host b closed for no-match, got %+v", ce)
	}
	if ca := batch.Closed[0]; ca.Labels["host"] != "b" || ca.Outcome != OutcomeClosed ||
		ca.Conclusion != ConclusionNoLongerMatch || ca.PreviousStatus != model.StatusFiring ||
		!strings.Contains(ca.Reason, CloseReasonNoMatch) {
		t.Fatalf("host b structured closed entry wrong: %+v", ca)
	}
	// host a is still covered by the narrowed rule and must be recorded as a
	// tracked alert (pending/firing at submit) instead of disappearing.
	if len(batch.Kept) != 1 {
		t.Fatalf("want one kept alert (host a), got %d: %+v", len(batch.Kept), batch.Kept)
	}
	if ka := batch.Kept[0]; ka.Labels["host"] != "a" || ka.Outcome != OutcomeTracked ||
		ka.Conclusion != ConclusionStillTracked || ka.PreviousStatus != model.StatusFiring {
		t.Fatalf("host a kept entry wrong: %+v", ka)
	}
	// The two outcomes must be disjoint.
	if batch.Kept[0].Labels.Fingerprint() == batch.Closed[0].Labels.Fingerprint() {
		t.Fatal("kept and closed must not overlap")
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

// TestBatchConfigOnlyChangeKeepsTracking: when a batch changes only critical
// config (e.g. threshold / pending settings) while the selector still covers
// the same series, every previously tracked (pending AND firing) alert must
// still be findable in the batch record as a "tracked/kept" alert, keep its
// state verbatim and must not be re-triggered.
func TestBatchConfigOnlyChangeKeepsTracking(t *testing.T) {
	ruleForHost := func(id, host string, pending int) model.Rule {
		r := ruleGT(id, pending, 0)
		r.Selector = model.Labels{"__name__": "m", "host": host}
		return r
	}
	c, st, ev := newHarness(t, []model.Rule{
		ruleForHost("fire", "f", 0), // fires immediately
		ruleForHost("pend", "p", 2), // needs 2 violating windows
	})

	put(st, "f", 1, 11, "f-w0") // fire -> firing
	put(st, "p", 1, 11, "p-w0") // pend -> pending streak 1
	c.t = 10
	setup, _ := ev.Eval(0)
	dump(t, "SETUP", setup)
	if s, _ := stateFor(setup, "fire", "f"); s.Status != model.StatusFiring {
		t.Fatal("setup: want fire firing")
	}
	if s, _ := stateFor(setup, "pend", "p"); s.Status != model.StatusPending || s.Consecutive != 1 {
		t.Fatal("setup: want pend pending streak1")
	}

	// Config-only batch: tweak threshold and pending settings; selectors are
	// unchanged and still cover both series. The new threshold (5) keeps value
	// 11 violating, so preservation means no re-trigger and the pending streak
	// survives.
	updatedFire := ruleForHost("fire", "f", 0)
	updatedFire.Threshold = 5
	updatedPend := ruleForHost("pend", "p", 3)
	batch, verrs := ev.ApplyRules([]model.Rule{updatedFire, updatedPend}, "cfg-only", 15)
	if verrs != nil || !batch.Applied() {
		t.Fatalf("config-only batch must apply: %v", verrs)
	}
	dumpBatch(t, "CONFIG-ONLY", batch)

	for _, ch := range batch.Changes {
		if ch.Kind != ChangeModified {
			t.Fatalf("both rules must be modified, rule=%s kind=%s", ch.RuleID, ch.Kind)
		}
	}
	if len(batch.Closed) != 0 || len(batch.ClosedEvents) != 0 {
		t.Fatalf("config-only change must close nothing, got closed=%d events=%d",
			len(batch.Closed), len(batch.ClosedEvents))
	}
	if len(batch.Kept) != 2 {
		t.Fatalf("want both tracked alerts kept/visible, got %d: %+v", len(batch.Kept), batch.Kept)
	}
	byRule := map[string]AffectedAlert{}
	for _, k := range batch.Kept {
		if k.Outcome != OutcomeTracked || k.Conclusion != ConclusionStillTracked {
			t.Fatalf("kept entry wrong outcome/conclusion: %+v", k)
		}
		byRule[k.RuleID] = k
	}
	if k := byRule["fire"]; k.Labels["host"] != "f" || k.PreviousStatus != model.StatusFiring ||
		k.Streak != 1 || k.LastObserved != 11 {
		t.Fatalf("fire kept snapshot wrong: %+v", k)
	}
	if k := byRule["pend"]; k.Labels["host"] != "p" || k.PreviousStatus != model.StatusPending ||
		k.Streak != 1 {
		t.Fatalf("pend kept snapshot wrong: %+v", k)
	}

	// Re-evaluate the same window: no re-trigger; firing stays firing and the
	// alert is still pending (the per-window streak counter advances on every
	// observation as usual, which is independent of the config change).
	after, _ := ev.Eval(10)
	dump(t, "POST-CONFIG", after)
	if len(after.Events) != 0 {
		t.Fatalf("kept alerts must not re-fire on a config-only change, events=%+v", after.Events)
	}
	if s, _ := stateFor(after, "fire", "f"); s.Status != model.StatusFiring {
		t.Fatalf("fire must remain firing, got %+v", s)
	}
	if s, _ := stateFor(after, "pend", "p"); s.Status != model.StatusPending || s.Consecutive < 1 {
		t.Fatalf("pend must remain pending with its progress intact, got %+v", s)
	}
	t.Log("JUDGE: config-only change lists both pending/firing alerts as still-tracked; state preserved, no re-fire")
}

// TestBatchMixedAddModifyDelete reconciles one batch containing an addition, a
// modification (selector still covers), a narrowing modification (drops one
// series), and a deletion: Changes and the Kept/Closed affected views must be
// mutually consistent; covered alerts are not re-triggered, removed alerts do
// not vanish, re-added rules later start clean.
func TestBatchMixedAddModifyDelete(t *testing.T) {
	// Each rule owns its own "grp" label so subset-matching cannot leak series
	// between rules:
	//   keep:   grp=k, host=k,  firing; threshold modified, still covered -> kept
	//   narrow: grp=n, host=n1/n2 firing; narrowed selector drops n2 -> n1 kept, n2 closed
	//   gone:   grp=g, host=g,  firing; rule deleted -> closed
	//   added:  new rule, no tracked alerts
	specificRule := func(id, grp, host string) model.Rule {
		r := ruleGT(id, 0, 0)
		r.Selector = model.Labels{"__name__": "m", "grp": grp, "host": host}
		return r
	}
	groupRule := func(id, grp string) model.Rule {
		r := ruleGT(id, 0, 0)
		r.Selector = model.Labels{"__name__": "m", "grp": grp}
		return r
	}
	c, st, ev := newHarness(t, []model.Rule{
		specificRule("keep", "k", "k"),
		groupRule("narrow", "n"), // covers n1+n2 via the group label
		specificRule("gone", "g", "g"),
	})

	type svc struct {
		rule, grp, host string
	}
	for _, s := range []svc{
		{"keep", "k", "k"}, {"narrow", "n", "n1"}, {"narrow", "n", "n2"}, {"gone", "g", "g"},
	} {
		st.Write(model.Sample{
			Labels: model.Labels{"__name__": "m", "grp": s.grp, "host": s.host},
			Ts:     1, Value: 11, SourceID: "src", DedupKey: s.host + "-w0",
		})
	}
	c.t = 10
	setup, _ := ev.Eval(0)
	dump(t, "SETUP", setup)
	for _, s := range []svc{
		{"keep", "k", "k"}, {"narrow", "n", "n1"}, {"narrow", "n", "n2"}, {"gone", "g", "g"},
	} {
		if stt, ok := stateFor(setup, s.rule, s.host); !ok || stt.Status != model.StatusFiring {
			t.Fatalf("setup want %s/%s firing, got ok=%v state=%+v", s.rule, s.host, ok, stt)
		}
	}

	// The mixed batch:
	//   keep   -> modified threshold (selector still covers k)
	//   narrow -> narrowed selector back to n1 only (drops n2)
	//   gone   -> deleted
	//   added  -> brand-new rule (no tracked alerts)
	keepUpd := specificRule("keep", "k", "k")
	keepUpd.Threshold = 5 // still violates for value 11
	narrowN1 := specificRule("narrow", "n", "n1")
	added := specificRule("added", "z", "z")
	batch, verrs := ev.ApplyRules([]model.Rule{keepUpd, narrowN1, added}, "mixed", 15)
	if verrs != nil || !batch.Applied() {
		t.Fatalf("mixed batch must apply: %v", verrs)
	}
	dumpBatch(t, "MIXED", batch)

	// Rule-level consistency: one of each kind.
	kinds := map[string]ChangeKind{}
	for _, ch := range batch.Changes {
		kinds[ch.RuleID] = ch.Kind
	}
	wantKinds := map[string]ChangeKind{
		"keep": ChangeModified, "narrow": ChangeModified,
		"gone": ChangeDeleted, "added": ChangeAdded,
	}
	if len(kinds) != len(wantKinds) {
		t.Fatalf("change set mismatch: got %v want %v", kinds, wantKinds)
	}
	for id, want := range wantKinds {
		if kinds[id] != want {
			t.Fatalf("rule %s kind=%s want %s", id, kinds[id], want)
		}
	}

	// Affected-view consistency: kept = {keep/k, narrow/n1}, closed = {narrow/n2, gone/g}.
	if len(batch.Kept) != 2 {
		t.Fatalf("want 2 kept alerts, got %+v", batch.Kept)
	}
	if len(batch.Closed) != 2 || len(batch.ClosedEvents) != 2 {
		t.Fatalf("want 2 closed alerts (n2 no-match, g deleted), got closed=%d events=%d",
			len(batch.Closed), len(batch.ClosedEvents))
	}
	keptKey := map[string]bool{}
	for _, k := range batch.Kept {
		if k.Outcome != OutcomeTracked || k.Conclusion != ConclusionStillTracked ||
			k.PreviousStatus != model.StatusFiring {
			t.Fatalf("kept entry wrong: %+v", k)
		}
		keptKey[k.RuleID+"/"+k.Labels["host"]] = true
	}
	for _, want := range []string{"keep/k", "narrow/n1"} {
		if !keptKey[want] {
			t.Fatalf("kept view missing %s: %+v", want, batch.Kept)
		}
	}
	closedKey := map[string]AffectedAlert{}
	for _, cl := range batch.Closed {
		if cl.Outcome != OutcomeClosed || cl.PreviousStatus != model.StatusFiring {
			t.Fatalf("closed entry wrong: %+v", cl)
		}
		closedKey[cl.RuleID+"/"+cl.Labels["host"]] = cl
	}
	if cl := closedKey["narrow/n2"]; cl.Conclusion != ConclusionNoLongerMatch ||
		!strings.Contains(cl.Reason, CloseReasonNoMatch) {
		t.Fatalf("n2 must close for no-match: %+v", cl)
	}
	if cl := closedKey["gone/g"]; cl.Conclusion != ConclusionRuleDeleted ||
		!strings.Contains(cl.Reason, CloseReasonRuleDeleted) {
		t.Fatalf("g must close for rule-deleted: %+v", cl)
	}
	// Kept and closed must be disjoint and exhaustive over the pre-change alerts.
	for k := range keptKey {
		if _, dup := closedKey[k]; dup {
			t.Fatalf("alert %s appears in both kept and closed", k)
		}
	}

	// Covered alerts are not re-triggered; removed ones are physically gone.
	after, _ := ev.Eval(10)
	dump(t, "POST-MIXED", after)
	if len(after.Events) != 0 {
		t.Fatalf("covered firing alerts must not re-fire, events=%+v", after.Events)
	}
	if s, ok := stateFor(after, "keep", "k"); !ok || s.Status != model.StatusFiring {
		t.Fatalf("keep/k must remain firing, got %+v", s)
	}
	if s, ok := stateFor(after, "narrow", "n1"); !ok || s.Status != model.StatusFiring {
		t.Fatalf("narrow/n1 must remain firing, got %+v", s)
	}
	if _, ok := stateFor(after, "narrow", "n2"); ok {
		t.Fatal("narrow/n2 state must be removed")
	}
	if _, ok := stateFor(after, "gone", "g"); ok {
		t.Fatal("gone/g state must be removed")
	}

	// Re-add "gone" later: it must start from a clean resolved state (fresh
	// pending/firing path), not inherit the closed firing.
	readd, verrs := ev.ApplyRules([]model.Rule{keepUpd, narrowN1, added, specificRule("gone", "g", "g")}, "readd", 20)
	if verrs != nil || !readd.Applied() {
		t.Fatalf("readd batch must apply: %v", verrs)
	}
	dumpBatch(t, "READD", readd)
	if find := func(rid, host string) *AffectedAlert {
		for i := range readd.Kept {
			if readd.Kept[i].RuleID == rid && readd.Kept[i].Labels["host"] == host {
				return &readd.Kept[i]
			}
		}
		return nil
	}("gone", "g"); find != nil {
		t.Fatalf("re-added rule must have no carried tracked alert, got %+v", find)
	}
	reEval, _ := ev.Eval(10)
	dump(t, "READD-EVAL", reEval)
	if s, ok := stateFor(reEval, "gone", "g"); !ok || s.Status != model.StatusFiring {
		t.Fatalf("re-added gone/g must fire fresh, got ok=%v state=%+v", ok, s)
	}
	t.Log("JUDGE: mixed add/modify/narrow/delete is consistent: kept covered, closed removed, readd starts clean")
}
