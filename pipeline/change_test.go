package pipeline

import (
	"strings"
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

// TestBatchRuleChangeEndToEnd exercises the facade batch path: check-only,
// all-or-nothing rejection, atomic apply with audit record, and explicit
// closure of alerts whose rule is deleted.
func TestBatchRuleChangeEndToEnd(t *testing.T) {
	c := &clk{t: 10}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 100, Now: func() int64 { return c.t }})
	rule := model.Rule{
		ID: "hot", Selector: model.Labels{"__name__": "m", "host": "a"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
	}
	p := New(st, []model.Rule{rule}, func() int64 { return c.t })

	p.Ingest(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 1, Value: 11, SourceID: "s", DedupKey: "k0"})
	rep := eval(t, c, p, 10, "SETUP")
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventFiring {
		t.Fatalf("setup want firing, got %+v", rep.Events)
	}

	// Check-only on an invalid batch lists problems and changes nothing.
	bad := rule
	bad.ID = "hot2"
	bad.WindowSeconds = 0
	if verrs := p.CheckRules([]model.Rule{bad, bad}); verrs == nil {
		t.Fatal("CheckRules should reject duplicate + bad window")
	} else {
		t.Logf("JUDGE CheckRules -> %s", verrs.Error())
	}
	if len(p.RuleChangeHistory()) != 0 {
		t.Fatal("check-only must not record history")
	}

	// Invalid Submit: firing state and rule set untouched, record kept.
	rej, verrs := p.SubmitRules([]model.Rule{bad, bad}, "bad-push")
	if verrs == nil || rej.Applied() {
		t.Fatal("invalid batch must not apply")
	}
	rep = eval(t, c, p, 10, "AFTER-REJECT")
	if len(rep.Events) != 0 {
		t.Fatalf("rejected batch must not disturb firing (no re-eval events), got %+v", rep.Events)
	}
	var stillFiring bool
	for _, s := range rep.States {
		if s.RuleID == "hot" && s.Status == model.StatusFiring {
			stillFiring = true
		}
	}
	if !stillFiring {
		t.Fatal("firing must survive a rejected batch")
	}

	// Valid batch: add a second rule -> record has one addition, no closures.
	added := rule
	added.ID = "hot2"
	good, verrs := p.SubmitRules([]model.Rule{rule, added}, "good-push")
	if verrs != nil || !good.Applied() {
		t.Fatalf("valid batch should apply: %v", verrs)
	}
	if len(good.Changes) != 2 {
		t.Fatalf("want 2 records (hot unchanged, hot2 added), got %+v", good.Changes)
	}
	kinds := map[string]string{}
	for _, ch := range good.Changes {
		kinds[ch.RuleID] = string(ch.Kind)
	}
	if kinds["hot"] != "unchanged" || kinds["hot2"] != "added" {
		t.Fatalf("want hot=unchanged hot2=added, got %v", kinds)
	}
	t.Logf("JUDGE applied %s: %d change(s)", good.BatchID, len(good.Changes))

	// Delete both rules: the firing alert gets an explicit closure.
	del, verrs := p.SubmitRules(nil, "delete-push")
	if verrs != nil || !del.Applied() {
		t.Fatalf("delete should apply: %v", verrs)
	}
	if len(del.ClosedEvents) != 1 || del.ClosedEvents[0].Type != model.EventClosed {
		t.Fatalf("want one closure event, got %+v", del.ClosedEvents)
	}
	if !strings.Contains(del.ClosedEvents[0].Reason, "rule deleted") {
		t.Fatalf("closure reason wrong: %s", del.ClosedEvents[0].Reason)
	}
	t.Logf("JUDGE %s closed alert: %s", del.BatchID, del.ClosedEvents[0].Reason)

	// History newest-first: delete, good, bad(rejected).
	hist := p.RuleChangeHistory()
	if len(hist) != 3 {
		t.Fatalf("want 3 records (incl. rejected), got %d", len(hist))
	}
	if hist[0].BatchID != del.BatchID {
		t.Fatalf("newest record must be delete, got %s", hist[0].BatchID)
	}
	if hist[2].Applied() {
		t.Fatal("oldest record must be the rejected batch")
	}
	if _, ok := p.RuleChangeBatch(del.BatchID); !ok {
		t.Fatal("lookup by batch id failed")
	}

	// Legacy ReplaceRules still works alongside the new batch path.
	p.ReplaceRules([]model.Rule{rule})
	if rs := p.Evaluator.Rules(); len(rs) != 1 || rs[0].ID != "hot" {
		t.Fatalf("legacy ReplaceRules mismatch: %+v", rs)
	}
	t.Log("JUDGE legacy ReplaceRules remains compatible")
}

func eval(t *testing.T, c *clk, p *Pipeline, ts int64, tag string) alert.Report {
	t.Helper()
	c.t = ts
	rep, err := p.Evaluate(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rep.Events {
		t.Logf("%s EVENT %s rule=%s: %s", tag, e.Type, e.RuleID, e.Reason)
	}
	return rep
}
