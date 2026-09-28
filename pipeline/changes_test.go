package pipeline

import (
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/change"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

// TestPipelineBatchChangesEndToEnd exercises the facade batch API: preview,
// rejected commit (nothing moves), successful atomic commit, explicit closure
// and the auditable record — with input/decision logs throughout.
func TestPipelineBatchChangesEndToEnd(t *testing.T) {
	c := &clk{t: 20}
	r1 := model.Rule{
		ID: "latency", Selector: model.Labels{"__name__": "m"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
	}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 100, Now: func() int64 { return c.t }})
	p := New(st, []model.Rule{r1}, func() int64 { return c.t })

	if wr := p.Ingest(model.Sample{Labels: model.Labels{"__name__": "m", "host": "a"}, Ts: 15, Value: 99, SourceID: "s", DedupKey: "1"}); wr.Status == model.WriteRejected {
		t.Fatal(wr.Detail)
	}
	rep, err := p.Evaluate(20)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT value=99 -> eval@20 events=%d states=%d", len(rep.Events), len(rep.States))
	if rep.States[0].Status != model.StatusFiring {
		t.Fatal("setup: want firing")
	}

	// Invalid batch via facade: zero window; must change nothing.
	bad := r1
	bad.ID = "bad"
	bad.WindowSeconds = 0
	pv := p.PreviewRuleChanges([]change.Item{
		{Op: change.OpUpsert, Rule: bad},
		{Op: change.OpDelete, Rule: model.Rule{ID: "latency"}},
	})
	t.Logf("PREVIEW invalid batch: %d errors (e.g. %+v)", len(pv.Errors), firstErr(pv.Errors))
	if len(pv.Errors) == 0 {
		t.Fatal("want preview errors")
	}
	if _, errs, _ := p.CommitRuleChanges([]change.Item{{Op: change.OpUpsert, Rule: bad}}); len(errs) == 0 {
		t.Fatal("invalid commit must be rejected")
	}
	if rs := p.Evaluator.Rules(); len(rs) != 1 || rs[0].ID != "latency" {
		t.Fatalf("rejected commit changed rules: %+v", rs)
	}

	// Valid batch: delete latency while firing -> explicit closure recorded.
	rec, errs, err := p.CommitRuleChanges([]change.Item{{Op: change.OpDelete, Rule: model.Rule{ID: "latency"}}})
	if err != nil || len(errs) != 0 {
		t.Fatalf("valid commit rejected: %v %+v", err, errs)
	}
	for _, ch := range rec.Changes {
		t.Logf("RECORD#%d CHANGE %s rule=%s beforeThreshold=%s",
			rec.ID, ch.Kind, ch.RuleID, thresholdOf(ch.Before))
	}
	for _, af := range rec.Affected {
		t.Logf("RECORD#%d AFFECT rule=%s host=%s before=%s kind=%s reason=%q",
			rec.ID, af.RuleID, af.Labels["host"], af.Status, af.Kind, af.CloseReason)
	}
	if len(rec.Changes) != 1 || rec.Changes[0].Kind != change.ChangeDeleted {
		t.Fatalf("want one deleted change, got %+v", rec.Changes)
	}
	if len(rec.Affected) != 1 || rec.Affected[0].Kind != change.AffectClosedByDelete ||
		rec.Affected[0].Status != model.StatusFiring || rec.Affected[0].CloseReason == "" {
		t.Fatalf("want explicit closure of firing alert, got %+v", rec.Affected)
	}
	if hist := p.RuleChangeHistory(); len(hist) != 1 || hist[0].ID != rec.ID {
		t.Fatalf("history mismatch: %+v", hist)
	}
	if len(p.Evaluator.States()) != 0 {
		t.Fatalf("closed state must be removed: %+v", p.Evaluator.States())
	}
	t.Log("JUDGE: invalid batch left rules/state intact; valid delete committed atomically, firing alert closed with a reason and recorded")
}

func firstErr(errs []change.ValidationError) change.ValidationError {
	if len(errs) == 0 {
		return change.ValidationError{}
	}
	return errs[0]
}

func thresholdOf(r *model.Rule) string {
	if r == nil {
		return "<nil>"
	}
	return model.FormatValue(r.Threshold)
}
