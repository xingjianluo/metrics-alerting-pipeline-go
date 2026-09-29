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

	fmt.Println("\n=== 6. Batch rule change management (validate / atomic switch / audit) ===")
	demoBatchManagement(p, rules, c)

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

// demoBatchManagement walks one check-only failure, one rejected (atomic,
// nothing changes) submit, one valid atomic batch with an auditable diff, and
// a deletion that explicitly closes a tracked alert.
func demoBatchManagement(p *pipeline.Pipeline, rules []model.Rule, c *clock) {
	// At this point high_request_rate is firing (section 5). Audit a batch
	// BEFORE applying it: the bad rule's problems are listed in one shot.
	bad := rules[0]
	bad.ID = "broken"
	bad.WindowSeconds = 0 // illegal window
	bad.AggField = "p99"  // illegal aggregation
	invalid := []model.Rule{bad, bad}
	fmt.Println("-- CheckRules (check-only, no mutation) --")
	if verrs := p.CheckRules(invalid); verrs != nil {
		fmt.Printf("dry-run found %d problem(s); rules/state untouched:\n  %s\n",
			len(verrs.Errs), strings.ReplaceAll(verrs.Error(), "; ", "\n  "))
	}

	fmt.Println("-- SubmitRules invalid batch: all-or-nothing rejection --")
	rej, verrs := p.SubmitRules(invalid, "demo-invalid")
	if verrs != nil {
		fmt.Printf("batch %s applied=%v (%d problem(s)); existing firing alert preserved\n",
			rej.BatchID, rej.Applied(), len(verrs.Errs))
	}

	fmt.Println("-- SubmitRules valid batch: keep + add, atomic switch with record --")
	added := rules[0]
	added.ID = "high_request_rate_host_b"
	added.Selector = model.Labels{"__name__": "http_requests", "host": "b"}
	applied, verrs := p.SubmitRules([]model.Rule{rules[0], added}, "demo-valid")
	if verrs != nil {
		panic(verrs)
	}
	for _, ch := range applied.Changes {
		fmt.Printf("batch %s @%d: rule=%s %s fields=%v\n",
			applied.BatchID, applied.Ts, ch.RuleID, ch.Kind, ch.ChangedFields)
	}
	fmt.Printf("batch %s affected active alerts: continued=%d closed=%d\n",
		applied.BatchID, len(applied.ContinuedAlerts), len(applied.ClosedAlerts))

	fmt.Println("-- SubmitRules config-only batch: firing alert continues, selector unchanged --")
	updated := rules[0]
	updated.Threshold = 3
	configOnly, verrs := p.SubmitRules([]model.Rule{updated, added}, "demo-config-only")
	if verrs != nil {
		panic(verrs)
	}
	for _, ch := range configOnly.Changes {
		fmt.Printf("batch %s @%d: rule=%s %s fields=%v\n",
			configOnly.BatchID, configOnly.Ts, ch.RuleID, ch.Kind, ch.ChangedFields)
	}
	for _, a := range configOnly.ContinuedAlerts {
		fmt.Printf("batch %s CONTINUED rule=%s labels=%v previous=%s: %s\n",
			configOnly.BatchID, a.RuleID, a.Labels, a.PreviousStatus, a.Reason)
	}
	fmt.Printf("batch %s affected active alerts: continued=%d closed=%d\n",
		configOnly.BatchID, len(configOnly.ContinuedAlerts), len(configOnly.ClosedAlerts))
	// Re-evaluating the same violating window must not duplicate firing.
	rep, _ := p.Evaluate(60)
	fmt.Printf("post-switch repeat-eval events=%d (expect 0: surviving firing not re-triggered)\n",
		len(rep.Events))

	fmt.Println("-- SubmitRules delete batch: active alerts closed with a reason --")
	del, verrs := p.SubmitRules(nil, "demo-delete")
	if verrs != nil {
		panic(verrs)
	}
	if len(del.ClosedEvents) == 0 {
		fmt.Println("no active alerts needed closure")
	}
	for _, a := range del.ClosedAlerts {
		fmt.Printf("batch %s CLOSED rule=%s labels=%v previous=%s: %s\n",
			del.BatchID, a.RuleID, a.Labels, a.PreviousStatus, a.Reason)
	}

	fmt.Println("-- change history (newest first; rejected pushes are kept too) --")
	for _, rec := range p.RuleChangeHistory() {
		fmt.Printf("  %s applied=%v changes=%d continued=%d closed=%d problems=%d\n",
			rec.BatchID, rec.Applied(), len(rec.Changes), len(rec.ContinuedAlerts),
			len(rec.ClosedEvents), len(rec.ValidationErrors))
	}
}
