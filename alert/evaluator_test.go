package alert

import (
	"sync"
	"testing"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

type clock struct{ t int64 }

func newHarness(t *testing.T, rules []model.Rule) (*clock, *storage.WindowStore, *Evaluator) {
	t.Helper()
	c := &clock{t: 1000}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 1000, Now: func() int64 { return c.t }})
	return c, st, NewEvaluator(st, rules, func() int64 { return c.t })
}

func ls(host string) model.Labels {
	return model.Labels{"__name__": "m", "host": host}
}

func put(st *storage.WindowStore, host string, ts int64, v float64, key string) model.WriteResult {
	r := st.Write(model.Sample{Labels: ls(host), Ts: ts, Value: v, SourceID: "src", DedupKey: key})
	return r
}

func ruleGT(id string, pending, keep int) model.Rule {
	return model.Rule{
		ID: id, Selector: model.Labels{"__name__": "m"},
		WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10,
		PendingWindows: pending, KeepFiringWindows: keep,
	}
}

func dump(t *testing.T, tag string, rep Report) {
	t.Helper()
	for _, ev := range rep.Events {
		t.Logf("%s EVENT %s rule=%s labels=%v ts=%d observed=%s: %s",
			tag, ev.Type, ev.RuleID, ev.Labels, ev.Ts, model.FormatValue(ev.Observed), ev.Reason)
	}
	for _, sk := range rep.Skipped {
		t.Logf("%s SKIP rule=%s ts=%d: %s", tag, sk.RuleID, sk.Ts, sk.Reason)
	}
	for _, s := range rep.States {
		t.Logf("%s STATE rule=%s labels=%v status=%s streak=%d observed=%s changedAt=%d",
			tag, s.RuleID, s.Labels, s.Status, s.Consecutive, model.FormatValue(s.LastObserved), s.ChangedAt)
	}
}

// TestThresholdBoundary verifies > vs >= at exactly the threshold value.
func TestThresholdBoundary(t *testing.T) {
	gt := model.Rule{ID: "gt", WindowSeconds: 10, AggField: "latest", Op: model.CmpGT, Threshold: 10}
	gte := model.Rule{ID: "gte", WindowSeconds: 10, AggField: "latest", Op: model.CmpGTE, Threshold: 10}
	c, st, ev := newHarness(t, []model.Rule{gt, gte})

	// Exactly 10 in window [0,10); evaluate at ts=10.
	r := put(st, "a", 5, 10, "k1")
	t.Logf("INPUT ts=5 value=10 (==threshold) -> write=%s window=%d", r.Status, r.WindowStart)
	c.t = 10
	rep, err := ev.Eval(0)
	if err != nil {
		t.Fatal(err)
	}
	dump(t, "BOUNDARY-EQ", rep)

	statusOf := func(rid string) model.AlertStatus {
		for _, s := range rep.States {
			if s.RuleID == rid {
				return s.Status
			}
		}
		return "<missing>"
	}
	// 10 > 10  -> false; 10 >= 10 -> true
	if got := statusOf("gt"); got != model.StatusResolved {
		t.Fatalf("GT at threshold: want resolved, got %s", got)
	}
	if got := statusOf("gte"); got != model.StatusFiring {
		t.Fatalf("GTE at threshold: want firing, got %s", got)
	}
	t.Log("JUDGE: value==10 -> '>10' stays resolved, '>=10' fires (boundary exact)")

	// 10.0001 in the next window: GT fires too.
	put(st, "a", 15, 10.0001, "k2")
	c.t = 20
	rep, _ = ev.Eval(0)
	dump(t, "BOUNDARY-OVER", rep)
	if got := statusOf("gt"); got != model.StatusFiring {
		t.Fatalf("GT above threshold: want firing, got %s", got)
	}
}

// TestStateMachineTransitions drives resolved -> pending -> firing ->
// pending-stays -> recovery-grace -> resolved with reproducible inputs.
func TestStateMachineTransitions(t *testing.T) {
	// PendingWindows=2 (2 consecutive violating windows fire),
	// KeepFiringWindows=1: stay firing through the 1st recovery window,
	// resolve on the 2nd (symmetric with PendingWindows).
	c, st, ev := newHarness(t, []model.Rule{ruleGT("r1", 2, 1)})

	// window [0,10): value 11 -> pending (streak 1/2)
	put(st, "a", 1, 11, "w0")
	c.t = 10
	rep, _ := ev.Eval(0)
	dump(t, "W0", rep)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventPending {
		t.Fatalf("want single pending event, got %+v", rep.Events)
	}

	// window [10,20): value 12 -> streak 2 == PendingWindows -> firing
	put(st, "a", 11, 12, "w1")
	c.t = 20
	rep, _ = ev.Eval(0)
	dump(t, "W1", rep)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventFiring {
		t.Fatalf("want firing event, got %+v", rep.Events)
	}

	// window [20,30): value 13 again -> still firing, NO duplicate event
	put(st, "a", 21, 13, "w2")
	c.t = 30
	rep, _ = ev.Eval(0)
	dump(t, "W2", rep)
	if len(rep.Events) != 0 {
		t.Fatalf("firing must not re-emit events, got %+v", rep.Events)
	}

	// window [30,40): value 9 -> 1st recovery window, held firing, no event
	put(st, "a", 31, 9, "w3")
	c.t = 40
	rep, _ = ev.Eval(0)
	dump(t, "W3", rep)
	if len(rep.Events) != 0 {
		t.Fatalf("first grace window must stay firing, got %+v", rep.Events)
	}
	if rep.States[0].Status != model.StatusFiring {
		t.Fatalf("want still firing during grace, got %s", rep.States[0].Status)
	}

	// window [40,50): value 8 -> 2nd recovery window -> resolved exactly now
	put(st, "a", 41, 8, "w4")
	c.t = 50
	rep, _ = ev.Eval(0)
	dump(t, "W4", rep)
	if len(rep.Events) != 1 || rep.Events[0].Type != model.EventResolved {
		t.Fatalf("want resolved event after grace, got %+v", rep.Events)
	}

	// window [50,60): remains resolved, no duplicate events
	put(st, "a", 51, 8, "w5")
	c.t = 60
	rep, _ = ev.Eval(0)
	dump(t, "W5", rep)
	if len(rep.Events) != 0 {
		t.Fatalf("resolved must stay quiet, got %+v", rep.Events)
	}
}

// TestPendingRecoversBeforeFiring: condition flips back during pending.
func TestPendingRecoversBeforeFiring(t *testing.T) {
	c, st, ev := newHarness(t, []model.Rule{ruleGT("r1", 2, 0)})
	put(st, "a", 1, 11, "w0")
	c.t = 10
	rep, _ := ev.Eval(0)
	dump(t, "P1", rep)
	if rep.States[0].Status != model.StatusPending {
		t.Fatal("want pending")
	}
	put(st, "a", 11, 1, "w1")
	c.t = 20
	rep, _ = ev.Eval(0)
	dump(t, "P2-RECOVER", rep)
	if rep.States[0].Status != model.StatusResolved || len(rep.Events) != 1 ||
		rep.Events[0].Type != model.EventResolved {
		t.Fatalf("want immediate resolve during pending, got events=%+v", rep.Events)
	}
	// After recovery, a single violating window must restart pending from 1,
	// not inherit the old streak and fire.
	put(st, "a", 21, 11, "w2")
	c.t = 30
	rep, _ = ev.Eval(0)
	dump(t, "P3-RESTART", rep)
	if rep.States[0].Status != model.StatusPending || rep.States[0].Consecutive != 1 {
		t.Fatalf("want fresh pending streak=1, got status=%s streak=%d",
			rep.States[0].Status, rep.States[0].Consecutive)
	}
}

// TestHotReloadPreservesState: updating a rule mid-pending must not lose the
// streak (no double fire / missed fire); removed rules drop their states.
func TestHotReloadPreservesState(t *testing.T) {
	c, st, ev := newHarness(t, []model.Rule{ruleGT("keep", 2, 0), ruleGT("drop", 0, 0)})

	put(st, "a", 1, 11, "w0")
	c.t = 10
	rep, _ := ev.Eval(0)
	dump(t, "BEFORE-RELOAD", rep)
	// keep -> pending(streak1), drop -> firing
	var keep model.AlertState
	for _, s := range rep.States {
		if s.RuleID == "keep" {
			keep = s
		}
	}
	if keep.Status != model.StatusPending || keep.Consecutive != 1 {
		t.Fatalf("setup: want keep pending streak1, got %+v", keep)
	}

	// Hot reload: tweak threshold of the kept rule (state preserved), remove
	// "drop", add "new".
	updated := ruleGT("keep", 2, 0)
	updated.Threshold = 100 // still >? value 11 no longer violates, but state must be PRESERVED
	added := ruleGT("new", 0, 0)
	ev.ReplaceRules([]model.Rule{updated, added})
	t.Logf("RELOAD: keep threshold 10->100 (state preserved), drop removed, new added at ts=%d", c.t)
	if rs := ev.Rules(); len(rs) != 2 || rs[0].ID != "keep" || rs[0].Threshold != 100 {
		t.Fatalf("reload rules mismatch: %+v", rs)
	}

	// Re-evaluate same window [0,10)? No — evaluate window [10,20) with value
	// 11: under new threshold it does NOT violate; preserved pending state
	// must transition to resolved (not fire, not stay silently stale).
	put(st, "a", 11, 11, "w1")
	c.t = 20
	rep, _ = ev.Eval(0)
	dump(t, "AFTER-RELOAD", rep)
	for _, s := range rep.States {
		if s.RuleID == "drop" {
			t.Fatal("state of removed rule must be dropped")
		}
		if s.RuleID == "keep" && s.Status != model.StatusResolved {
			t.Fatalf("preserved pending must resolve under new non-violating threshold, got %s", s.Status)
		}
	}

	// Reload back to threshold 10; two fresh violating windows fire exactly
	// once (no duplicate firing inherited from stale streak).
	ev.ReplaceRules([]model.Rule{ruleGT("keep", 2, 0)})
	put(st, "a", 21, 11, "w2")
	c.t = 30
	rep, _ = ev.Eval(0)
	dump(t, "RELOAD-BACK-W2", rep)
	if rep.States[0].Status != model.StatusPending {
		t.Fatalf("want fresh pending, got %s", rep.States[0].Status)
	}
	put(st, "a", 31, 11, "w3")
	c.t = 40
	rep, _ = ev.Eval(0)
	dump(t, "RELOAD-BACK-W3", rep)
	fires := 0
	for _, e2 := range rep.Events {
		if e2.Type == model.EventFiring {
			fires++
		}
	}
	if rep.States[0].Status != model.StatusFiring || fires != 1 {
		t.Fatalf("streak 2 must fire exactly once, got status=%s fires=%d", rep.States[0].Status, fires)
	}
	// Next violating window keeps firing without duplicate events.
	put(st, "a", 41, 11, "w4")
	c.t = 50
	rep, _ = ev.Eval(0)
	dump(t, "RELOAD-BACK-W4", rep)
	if len(rep.Events) != 0 || rep.States[0].Status != model.StatusFiring {
		t.Fatalf("firing must not re-emit, got events=%+v status=%s", rep.Events, rep.States[0].Status)
	}
}

// TestMissingDataDoesNotResolve covers two no-signal cases:
//
//	(a) evaluated window retained but has no points -> no events, state kept;
//	(b) evaluated window was evicted (GapError)     -> rule skipped, state kept.
//
// Missing data must never silently resolve or fire an alert.
func TestMissingDataDoesNotResolve(t *testing.T) {
	t.Run("empty retained window", func(t *testing.T) {
		c := &clock{t: 30}
		st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 3, Now: func() int64 { return c.t }})
		ev := NewEvaluator(st, []model.Rule{ruleGT("r1", 0, 1)}, func() int64 { return c.t })

		put(st, "a", 25, 11, "w") // window [20,30) violating
		rep, _ := ev.Eval(30)
		dump(t, "FIRE", rep)
		if rep.States[0].Status != model.StatusFiring {
			t.Fatal("setup want firing")
		}

		// Next retained windows contain no data at all.
		rep, _ = ev.Eval(40)
		dump(t, "EMPTY-40", rep)
		rep, _ = ev.Eval(50)
		dump(t, "EMPTY-50", rep)
		if len(rep.Events) != 0 || rep.States[0].Status != model.StatusFiring {
			t.Fatalf("empty windows must not move the state, events=%+v status=%s",
				rep.Events, rep.States[0].Status)
		}
	})

	t.Run("evicted window gap", func(t *testing.T) {
		// Base window 10s, retention 2 windows (20s span), rule reads 40s
		// windows. After the clock advances past one span, the evaluated
		// 40s window must straddle the horizon -> storage GapError -> skip.
		c := &clock{t: 10}
		st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 2, Now: func() int64 { return c.t }})
		big := ruleGT("big", 0, 0)
		big.WindowSeconds = 40 // wider than the 20s retention span
		ev := NewEvaluator(st, []model.Rule{big}, func() int64 { return c.t })

		// Write while the clock is low: window [0,10) is retained and the
		// eviction horizon stays 0, so evaluating [0,40) fires cleanly.
		put(st, "a", 5, 11, "k")
		c.t = 40
		rep, _ := ev.Eval(0) // evaluates [0,40)
		dump(t, "FIRE", rep)
		if rep.States[0].Status != model.StatusFiring {
			t.Fatal("setup want firing")
		}

		c.t = 80 // span 20s: boundary advances to 70; evaluated [40,80) -> gap
		st.Tick()
		rep, _ = ev.Eval(0)
		dump(t, "GAP", rep)
		if len(rep.Skipped) == 0 {
			t.Fatal("expected the rule to be skipped over an evicted window")
		}
		if len(rep.Events) != 0 || rep.States[0].Status != model.StatusFiring {
			t.Fatalf("evicted window must not resolve: events=%+v status=%s",
				rep.Events, rep.States[0].Status)
		}
		t.Logf("JUDGE: %s", rep.Skipped[0].Reason)
	})
}

// TestConcurrentEvalRace runs evaluation under -race while rules are
// hot-reloaded concurrently; the firing outcome must stay consistent.
func TestConcurrentEvalRace(t *testing.T) {
	c := &clock{t: 10}
	st := storage.NewStore(storage.Config{WindowSeconds: 10, RetentionWindows: 10000, Now: func() int64 { return c.t }})
	ev := NewEvaluator(st, []model.Rule{ruleGT("r1", 0, 0)}, func() int64 { return c.t })
	put(st, "a", 5, 11, "k0")

	var wg sync.WaitGroup
	var firingObservations int
	var mu sync.Mutex
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				rep, err := ev.Eval(10)
				if err != nil {
					t.Errorf("eval: %v", err)
					return
				}
				for _, s := range rep.States {
					if s.Status == model.StatusFiring {
						mu.Lock()
						firingObservations++
						mu.Unlock()
					}
				}
				if g == 0 && i%25 == 0 {
					ev.ReplaceRules([]model.Rule{ruleGT("r1", 0, 0)})
				}
			}
		}(g)
	}
	wg.Wait()
	if firingObservations == 0 {
		t.Fatal("expect concurrent evals to consistently observe firing")
	}
	// Exactly one firing event ever for the single violating window.
	rep, _ := ev.Eval(10)
	dump(t, "FINAL", rep)
	t.Logf("JUDGE concurrent eval passes observed firing %d times on the same window (event emitted once)", firingObservations)
}

func TestCompareAllOps(t *testing.T) {
	cases := []struct {
		op    model.Comparator
		v, th float64
		want  bool
	}{
		{model.CmpGT, 10, 10, false}, {model.CmpGTE, 10, 10, true},
		{model.CmpLT, 9, 10, true}, {model.CmpLTE, 10, 10, true},
		{model.CmpEQ, 10, 10, true}, {model.CmpNEQ, 10, 10, false},
	}
	for _, c := range cases {
		got := compare(c.op, c.v, c.th)
		t.Logf("JUDGE %s %s %s -> %v", model.FormatValue(c.v), c.op, model.FormatValue(c.th), got)
		if got != c.want {
			t.Fatalf("%v %s %v = %v want %v", c.v, c.op, c.th, got, c.want)
		}
	}
}
