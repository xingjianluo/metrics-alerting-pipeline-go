package pipeline

import (
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

type clk struct{ t int64 }

// TestEndToEnd runs the scripted scenario and asserts stable aggregates plus
// the full pending->firing->resolved lifecycle, all with input/decision logs.
func TestEndToEnd(t *testing.T) {
	samples, rules, w := ScriptedScenario()
	c := &clk{t: 100}
	st := storage.NewStore(storage.Config{
		WindowSeconds: w, RetentionWindows: 100, MaxFutureSeconds: 0,
		Now: func() int64 { return c.t },
	})
	p := New(st, rules, func() int64 { return c.t })
	src := NewMemorySource(samples)

	var accepted, deduped int
	for {
		s, ok := src.Next()
		if !ok {
			break
		}
		r := p.Ingest(s)
		t.Logf("INGEST host=%s ts=%d value=%s key=%s -> %s window=%d %s",
			s.Labels["host"], s.Ts, model.FormatValue(s.Value), s.DedupKey,
			r.Status, r.WindowStart, r.Detail)
		switch r.Status {
		case model.WriteAccepted:
			accepted++
		case model.WriteDeduped:
			deduped++
		default:
			t.Fatalf("unexpected rejection: %s/%s %s", r.Status, r.Reason, r.Detail)
		}
	}
	if accepted != 11 || deduped != 2 {
		t.Fatalf("want 11 accepted / 2 deduped, got %d / %d", accepted, deduped)
	}

	// Aggregates at base granularity.
	res, err := p.Query(0, 40, w, model.Labels{"__name__": "http_requests"})
	if err != nil {
		t.Fatal(err)
	}
	type want struct {
		host  string
		start int64
		count int64
		sum   float64
	}
	wants := []want{
		{"a", 0, 3, 4},  // dup with value 99 ignored
		{"a", 10, 4, 4}, // out-of-order + late repeat deduped
		{"a", 20, 1, 1},
		{"a", 30, 1, 1},
		{"b", 10, 2, 2},
	}
	got := map[string]want{}
	for _, sp := range res.Series {
		for _, pt := range sp.Points {
			k := sp.Labels["host"]
			t.Logf("AGG host=%s window=[%d,%d) count=%d sum=%s min=%s max=%s",
				k, pt.WindowStart, pt.WindowEnd, pt.Count,
				model.FormatValue(pt.Sum), model.FormatValue(pt.Min), model.FormatValue(pt.Max))
			got[k+"@"+itoa(pt.WindowStart)] = want{k, pt.WindowStart, pt.Count, pt.Sum}
		}
	}
	for _, ww := range wants {
		g, ok := got[ww.host+"@"+itoa(ww.start)]
		if !ok {
			t.Fatalf("missing aggregate host=%s window=%d", ww.host, ww.start)
		}
		if g.count != ww.count || g.sum != ww.sum {
			t.Fatalf("host=%s window=%d got count=%d sum=%s want count=%d sum=%s",
				ww.host, ww.start, g.count, model.FormatValue(g.sum), ww.count, model.FormatValue(ww.sum))
		}
	}

	// Alert lifecycle driven window by window.
	// Rule fires only for host=a, count>=4, PendingWindows=1.
	eval := func(ts int64) alert.Report {
		c.t = ts
		rep, err := p.Evaluate(0)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range rep.Events {
			t.Logf("EVAL@%d EVENT %s host=%s observed=%s: %s",
				ts, e.Type, e.Labels["host"], model.FormatValue(e.Observed), e.Reason)
		}
		state := "<none>"
		for _, s2 := range rep.States {
			if s2.RuleID == "high_request_rate" {
				state = string(s2.Status)
				t.Logf("EVAL@%d STATE host=%s status=%s streak=%d observed=%s",
					ts, s2.Labels["host"], s2.Status, s2.Consecutive, model.FormatValue(s2.LastObserved))
			}
		}
		t.Logf("EVAL@%d JUDGE rule state=%s", ts, state)
		return rep
	}

	// [0,10): count 3 < 4 -> resolved, no events
	if rep := eval(10); len(rep.Events) != 0 {
		t.Fatalf("window0 must be quiet, got %+v", rep.Events)
	}
	// [10,20): count 4 -> pending (1st violating window)
	rep := eval(20)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventPending {
		t.Fatalf("window10 want pending, got %+v", rep.Events)
	}
	// [20,30): count 1 recovers during pending -> resolved
	rep = eval(30)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventResolved {
		t.Fatalf("window20 want resolve-from-pending, got %+v", rep.Events)
	}

	// Replay violating windows to reach firing: write count>=4 into [30,40)
	// and [40,50), then evaluate.
	for i, ts := range []int64{31, 32, 33, 34} {
		p.Ingest(model.Sample{Labels: model.Labels{"__name__": "http_requests", "host": "a"},
			Ts: ts, Value: 1, SourceID: "agent-a", DedupKey: "a-30-x" + itoa(int64(i))})
	}
	rep = eval(40)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventPending {
		t.Fatalf("window30 want fresh pending, got %+v", rep.Events)
	}
	for i, ts := range []int64{41, 42, 43, 44} {
		p.Ingest(model.Sample{Labels: model.Labels{"__name__": "http_requests", "host": "a"},
			Ts: ts, Value: 1, SourceID: "agent-a", DedupKey: "a-40-x" + itoa(int64(i))})
	}
	rep = eval(50)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventFiring {
		t.Fatalf("window40 want firing, got %+v", rep.Events)
	}

	// Hot reload mid-firing: identical rule set (e.g. description-equivalent
	// re-push) must not re-fire. Removing the rule drops its state;
	// re-adding starts cleanly.
	p.ReplaceRules(rules)
	rep = eval(50)
	if len(rep.Events) != 0 {
		t.Fatalf("rule re-push must not duplicate firing, got %+v", rep.Events)
	}
	var firing bool
	for _, s2 := range rep.States {
		if s2.RuleID == "high_request_rate" && s2.Status == model.StatusFiring {
			firing = true
		}
	}
	if !firing {
		t.Fatal("firing state must survive hot reload")
	}

	// Query across evicted data fails loudly instead of silently truncating.
	c.t = 2000
	st.Tick()
	if _, err := p.Query(0, 40, w, nil); err == nil {
		t.Fatal("query over evicted windows must return an error, got nil")
	} else {
		t.Logf("EVICTED-QUERY JUDGE: %v", err)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
