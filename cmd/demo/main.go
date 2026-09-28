// Command demo runs the in-memory metrics aggregation and alerting pipeline
// against a deterministic script of out-of-order, duplicate and late samples,
// printing every input, write decision, aggregate, alert transition and the
// hot-reload/retention behavior. No external services are required:
//
//	go run ./cmd/demo
package main

import (
	"fmt"
	"strings"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/change"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/pipeline"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

type clock struct{ t int64 }

func labelsA() model.Labels {
	return model.Labels{"__name__": "http_requests", "host": "a"}
}

func main() {
	samples, rules, w := pipeline.ScriptedScenario()

	// Main pipeline: clock starts at 0 so the scripted 0..60s data is fresh;
	// a generous retention keeps it readable until the explicit eviction step.
	c := &clock{t: 0}
	st := storage.NewStore(storage.Config{
		// 20 windows: the 0..60s script is retained while the clock is low,
		// and evicted once the clock jumps far ahead in the final section.
		WindowSeconds: w, RetentionWindows: 20,
		Now: func() int64 { return c.t },
	})
	p := pipeline.New(st, rules, func() int64 { return c.t })

	fmt.Println("=== 1. Ingestion: out-of-order, duplicate and late samples ===")
	src := pipeline.NewMemorySource(samples)
	for {
		s, ok := src.Next()
		if !ok {
			break
		}
		r := p.Ingest(s)
		fmt.Printf("write host=%-2s ts=%3d value=%4s key=%-8s -> %-8s window=[%d,%d) %s\n",
			s.Labels["host"], s.Ts, model.FormatValue(s.Value), s.DedupKey,
			r.Status, r.WindowStart, r.WindowStart+w, r.Detail)
	}

	fmt.Println("\n=== 2. Retention: distinguishable rejection reasons (separate short store) ===")
	c2 := &clock{t: 100}
	short := storage.NewStore(storage.Config{
		WindowSeconds: 10, RetentionWindows: 5, MaxFutureSeconds: 5,
		Now: func() int64 { return c2.t },
	})
	short.Write(model.Sample{Labels: labelsA(), Ts: 95, Value: 1, SourceID: "x", DedupKey: "base"})
	rejects := []model.Sample{
		{Labels: labelsA(), Ts: 50, Value: 1, SourceID: "x", DedupKey: "too-old"},
		{Labels: labelsA(), Ts: 500, Value: 1, SourceID: "x", DedupKey: "future"},
		{Labels: labelsA(), Ts: -1, Value: 1, SourceID: "x", DedupKey: "bad"},
		{Labels: labelsA(), Ts: 60, Value: 1, SourceID: "", DedupKey: "no-source"},
	}
	for _, s := range rejects {
		r := short.Write(s)
		fmt.Printf("write ts=%4d key=%-9s -> %s reason=%s (%s)\n",
			s.Ts, s.DedupKey, r.Status, r.Reason, r.Detail)
	}

	fmt.Println("\n=== 3. Aggregation query (base and 2x granularity) ===")
	printQuery := func(gran int64) {
		res, err := p.Query(0, 40, gran, model.Labels{"__name__": "http_requests"})
		if err != nil {
			fmt.Printf("query error: %v\n", err)
			return
		}
		fmt.Printf("-- granularity %ds --\n", gran)
		for _, sp := range res.Series {
			for _, a := range sp.Points {
				fmt.Printf("host=%s [%3d,%3d) count=%d sum=%-3s min=%s max=%s latest=%s@%d\n",
					sp.Labels["host"], a.WindowStart, a.WindowEnd, a.Count,
					model.FormatValue(a.Sum), model.FormatValue(a.Min),
					model.FormatValue(a.Max), model.FormatValue(a.Latest), a.LatestTs)
			}
		}
	}
	printQuery(w)
	printQuery(2 * w)

	fmt.Println("\n=== 4. Alert evaluation: resolved -> pending -> resolved ===")
	// [0,10) count=3 (ok), [10,20) count=4 (violating), [20,30) count=1
	// (recovers during pending).
	for _, ts := range []int64{10, 20, 30} {
		c.t = ts
		rep, err := p.Evaluate(0)
		if err != nil {
			panic(err)
		}
		if len(rep.Events) == 0 {
			fmt.Printf("@%3d (no transition)\n", ts)
		}
		for _, e := range rep.Events {
			fmt.Printf("@%3d EVENT %-8s host=%s observed=%s: %s\n",
				ts, e.Type, e.Labels["host"], model.FormatValue(e.Observed), e.Reason)
		}
	}

	fmt.Println("\n=== 5. Hot rule reload preserves firing state (no duplicate firing) ===")
	// Two consecutive violating windows [40,50) and [50,60).
	for _, ts := range []int64{41, 42, 43, 44, 51, 52, 53, 54} {
		r := p.Ingest(model.Sample{Labels: labelsA(),
			Ts: ts, Value: 1, SourceID: "agent-a", DedupKey: fmt.Sprintf("burst-%d", ts)})
		if r.Status != model.WriteAccepted {
			panic(fmt.Sprintf("burst write ts=%d: %s/%s", ts, r.Status, r.Reason))
		}
	}
	for _, ts := range []int64{40, 50, 60} {
		c.t = ts
		rep, _ := p.Evaluate(0)
		for _, e := range rep.Events {
			fmt.Printf("@%3d EVENT %-8s host=%s: %s\n", ts, e.Type, e.Labels["host"], e.Reason)
		}
	}
	fmt.Println("-- re-pushing the SAME rules (hot reload), re-evaluating the same [50,60) window --")
	p.ReplaceRules(rules)
	c.t = 60
	rep, _ := p.Evaluate(60) // same violating window again: no duplicate event
	status := "<missing>"
	for _, s := range rep.States {
		if s.RuleID == "high_request_rate" {
			status = string(s.Status)
		}
	}
	fmt.Printf("post-reload repeat-eval events=%d state=%s (expect 0 events, firing kept)\n",
		len(rep.Events), status)

	fmt.Println("\n=== 6. Batched rule change management (validate -> atomic switch -> record) ===")
	// At this point high_request_rate is FIRING. Show:
	//  (a) check-only preview listing every problem of an invalid batch;
	//  (b) rejected commit leaves rules and the firing state untouched;
	//  (c) a valid batch switches atomically, explicitly closes the firing
	//      alert orphaned by the deletion, and leaves an auditable record.
	invalid := []change.Item{
		{Op: change.OpUpsert, Rule: model.Rule{ID: "bad-window", WindowSeconds: 0, AggField: "count", Op: model.CmpGT}},
		{Op: change.OpUpsert, Rule: model.Rule{ID: "bad-op", WindowSeconds: 10, AggField: "p99", Op: "~"}},
		{Op: change.OpDelete, Rule: model.Rule{ID: "ghost"}},
	}
	pv := p.PreviewRuleChanges(invalid)
	fmt.Printf("-- preview of invalid batch: %d problem(s), nothing applied --\n", len(pv.Errors))
	for _, ve := range pv.Errors {
		fmt.Printf("  INVALID rule=%s field=%s: %s\n", ve.RuleID, ve.Field, ve.Detail)
	}
	if _, errs, _ := p.CommitRuleChanges(invalid); len(errs) > 0 {
		fmt.Printf("-- commit rejected wholesale; firing state still present: states=%d --\n",
			len(p.Evaluator.States()))
	}

	valid := []change.Item{
		{Op: change.OpDelete, Rule: model.Rule{ID: "high_request_rate"}},
		{Op: change.OpUpsert, Rule: model.Rule{
			ID: "error_rate", Selector: model.Labels{"__name__": "http_requests"},
			WindowSeconds: w, AggField: "sum", Op: model.CmpGT, Threshold: 3,
			PendingWindows: 0,
		}},
	}
	rec, errs, err := p.CommitRuleChanges(valid)
	if err != nil || len(errs) != 0 {
		panic(fmt.Sprintf("valid batch: %v %+v", err, errs))
	}
	fmt.Printf("-- committed record #%d at ts=%d (ruleset v%d): %d rule change(s) --\n",
		rec.ID, rec.CommittedAt, rec.RulesetVersion, len(rec.Changes))
	for _, ch := range rec.Changes {
		fmt.Printf("  %-7s rule=%s\n", ch.Kind, ch.RuleID)
	}
	for _, af := range rec.Affected {
		fmt.Printf("  AFFECTED rule=%s host=%s was=%s -> %s\n", af.RuleID, af.Labels["host"], af.Status, af.Kind)
		fmt.Printf("           close reason: %s\n", af.CloseReason)
	}
	fmt.Printf("-- post-switch tracked states=%d (closed alert removed, will restart clean if re-added) --\n",
		len(p.Evaluator.States()))

	fmt.Println("\n=== 7. Eviction: queries over dropped windows fail loudly ===")
	c.t = 1100
	p.Tick()
	if _, err := p.Query(0, 40, w, nil); err != nil {
		fmt.Printf("query [0,40) -> ERROR (not silent partial data): %s\n", strings.Split(err.Error(), "\n")[0])
	}
	res, err := p.Query(1080, 1100, w, nil)
	fmt.Printf("query [1080,1100) post-horizon: err=%v series=%d (empty retained range is a valid result)\n",
		err, len(res.Series))
}
