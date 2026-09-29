// Package alert implements rule evaluation and the per-(rule,series) alert
// state machine (resolved -> pending -> firing -> resolved). Rules can be
// hot-replaced without losing states or double-firing; evaluation is
// serialized and safe for concurrent callers.
package alert

import (
	"fmt"
	"sort"
	"sync"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

// AggReader is the storage capability the evaluator needs. Satisfied by
// *storage.WindowStore.
type AggReader interface {
	Query(start, end, granularitySeconds int64, selector model.Labels) (*model.QueryResult, error)
}

// Skip explains a rule that could not be evaluated (e.g. its window was
// evicted). Skipped rules never drive a state transition: missing data must
// not silently resolve an alert.
type Skip struct {
	RuleID string
	Ts     int64
	Reason string
}

// Report is the outcome of one evaluation pass.
type Report struct {
	Ts int64
	// Rules is the complete rule-set snapshot used by this evaluation pass. It is
	// captured while holding the evaluator lock, so concurrent tests can prove a
	// pass saw either the whole old batch or the whole new batch.
	Rules   []model.Rule
	Events  []model.AlertEvent // transitions produced in this pass
	States  []model.AlertState // snapshot of all tracked states
	Skipped []Skip
}

// Evaluator evaluates rules and owns alert states.
type Evaluator struct {
	mu     sync.Mutex
	rules  []model.Rule                 // ordered by ID for deterministic evaluation
	states map[string]*model.AlertState // key: ruleID + "\x00" + fingerprint
	reader AggReader
	now    func() int64
}

// NewEvaluator constructs an evaluator with an initial rule set.
func NewEvaluator(reader AggReader, rules []model.Rule, now func() int64) *Evaluator {
	e := &Evaluator{
		states: map[string]*model.AlertState{},
		reader: reader,
		now:    now,
	}
	cp := make([]model.Rule, len(rules))
	copy(cp, rules)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	e.rules = cp
	return e
}

func stateKey(ruleID, fp string) string { return ruleID + "\x00" + fp }

// compare performs the rule's threshold comparison. Boundary semantics are
// the literal operator: ">" at exactly the threshold is false, ">=" is true.
func compare(op model.Comparator, v, threshold float64) bool {
	switch op {
	case model.CmpGT:
		return v > threshold
	case model.CmpGTE:
		return v >= threshold
	case model.CmpLT:
		return v < threshold
	case model.CmpLTE:
		return v <= threshold
	case model.CmpEQ:
		return v == threshold
	case model.CmpNEQ:
		return v != threshold
	default:
		return false
	}
}

// aggregateValue extracts the configured field from an aggregate.
func aggregateValue(a model.Aggregate, field string) (float64, error) {
	switch field {
	case "count":
		return float64(a.Count), nil
	case "sum":
		return a.Sum, nil
	case "avg":
		if a.Count == 0 {
			return 0, nil
		}
		return a.Sum / float64(a.Count), nil
	case "min":
		return a.Min, nil
	case "max":
		return a.Max, nil
	case "latest":
		return a.Latest, nil
	default:
		return 0, fmt.Errorf("unknown AggField %q", field)
	}
}

// Eval runs one evaluation pass at evalTs (clock time when 0), returning the
// transitions and a deterministic state snapshot. The whole pass is
// serialized so concurrent evaluations cannot interleave state changes.
//
// Each rule is evaluated against the last fully-aligned window ending at or
// before evalTs: [alignFloor(evalTs)-window, alignFloor(evalTs)). A window
// with no data, or one that has been evicted, is skipped and leaves states
// untouched (missing data never resolves or fires an alert).
func (e *Evaluator) Eval(evalTs int64) (Report, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if evalTs == 0 {
		evalTs = e.now()
	}
	rep := Report{Ts: evalTs, Rules: e.snapshotRulesLocked()}

	for _, rule := range rep.Rules {
		if rule.WindowSeconds <= 0 {
			rep.Skipped = append(rep.Skipped, Skip{RuleID: rule.ID, Ts: evalTs, Reason: "WindowSeconds<=0"})
			continue
		}
		size := rule.WindowSeconds
		start := evalTs - (evalTs % size) - size
		end := start + size

		res, err := e.reader.Query(start, end, size, rule.Selector)
		if err != nil {
			// Evicted/partial window: do NOT touch states.
			reason := err.Error()
			if ge, ok := err.(*storage.GapError); ok {
				reason = fmt.Sprintf("window [%d,%d) spans evicted data: %v", start, end, ge)
			}
			rep.Skipped = append(rep.Skipped, Skip{RuleID: rule.ID, Ts: evalTs, Reason: reason})
			continue
		}

		sps := append([]model.SeriesPoints(nil), res.Series...)
		sort.Slice(sps, func(i, j int) bool {
			return sps[i].Labels.Fingerprint() < sps[j].Labels.Fingerprint()
		})

		for _, sp := range sps {
			if len(sp.Points) == 0 {
				continue // no data observed in this window: state untouched
			}
			agg := sp.Points[0]
			v, verr := aggregateValue(agg, rule.AggField)
			if verr != nil {
				rep.Skipped = append(rep.Skipped, Skip{RuleID: rule.ID, Ts: evalTs, Reason: verr.Error()})
				continue
			}
			ev := e.advance(rule, sp.Labels, v, evalTs, agg)
			rep.Events = append(rep.Events, ev...)
		}
	}

	rep.States = e.snapshotLocked()
	return rep, nil
}

// advance applies one observation to a tracked state and returns zero or one
// transition event. AlertState.Consecutive uses a sign encoding:
//
//	> 0: number of consecutive violating windows
//	< 0: number of consecutive recovered (non-violating) windows
//	== 0: no observation yet / resolved
//
// Caller holds e.mu.
func (e *Evaluator) advance(rule model.Rule, ls model.Labels, v float64, ts int64, agg model.Aggregate) []model.AlertEvent {
	key := stateKey(rule.ID, ls.Fingerprint())
	st, ok := e.states[key]
	if !ok {
		st = &model.AlertState{RuleID: rule.ID, Labels: ls, Status: model.StatusResolved}
		e.states[key] = st
	}

	violating := compare(rule.Op, v, rule.Threshold)
	basis := fmt.Sprintf("%s=%s %s %s over [%d,%d) (count=%d)",
		rule.AggField, model.FormatValue(v), rule.Op, model.FormatValue(rule.Threshold),
		agg.WindowStart, agg.WindowEnd, agg.Count)

	prev := st.Status
	st.LastObserved = v
	st.UpdatedAt = ts
	// Update the signed streak counter on a signal flip.
	if violating && st.Consecutive <= 0 {
		st.Consecutive = 1
	} else if !violating && st.Consecutive >= 0 {
		st.Consecutive = -1
	} else {
		if violating {
			st.Consecutive++
		} else {
			st.Consecutive--
		}
	}

	switch {
	case violating && prev == model.StatusResolved:
		if rule.PendingWindows <= 0 {
			st.Status = model.StatusFiring
			st.ChangedAt = ts
			return []model.AlertEvent{event(model.EventFiring, rule, ls, ts, v,
				"condition true with PendingWindows=0: "+basis)}
		}
		st.Status = model.StatusPending
		st.ChangedAt = ts
		return []model.AlertEvent{event(model.EventPending, rule, ls, ts, v,
			fmt.Sprintf("condition true, violating streak %d/%d: %s",
				st.Consecutive, rule.PendingWindows, basis))}

	case violating && prev == model.StatusPending:
		// PendingWindows is the number of consecutive violating windows
		// required: 1 means a second violating window fires immediately.
		if st.Consecutive >= rule.PendingWindows {
			st.Status = model.StatusFiring
			st.ChangedAt = ts
			return []model.AlertEvent{event(model.EventFiring, rule, ls, ts, v,
				fmt.Sprintf("violating streak %d reached PendingWindows=%d: %s",
					st.Consecutive, rule.PendingWindows, basis))}
		}
		return nil // still pending; no transition event

	case violating:
		// already firing: keep firing, never re-emit
		return nil

	case !violating && prev == model.StatusPending:
		st.Status = model.StatusResolved
		st.Consecutive = 0
		st.ChangedAt = ts
		return []model.AlertEvent{event(model.EventResolved, rule, ls, ts, v,
			"condition recovered before pending period elapsed: "+basis)}

	case !violating && prev == model.StatusFiring:
		if rule.KeepFiringWindows <= 0 {
			st.Status = model.StatusResolved
			st.Consecutive = 0
			st.ChangedAt = ts
			return []model.AlertEvent{event(model.EventResolved, rule, ls, ts, v,
				"condition false with KeepFiringWindows=0: "+basis)}
		}
		recovered := -st.Consecutive
		// KeepFiringWindows is the number of recovered windows the alert
		// stays FIRING through; resolution happens on the following window.
		// N=0 resolves immediately (handled above), N=1 needs a 2nd recovery.
		if recovered > rule.KeepFiringWindows {
			st.Status = model.StatusResolved
			st.Consecutive = 0
			st.ChangedAt = ts
			return []model.AlertEvent{event(model.EventResolved, rule, ls, ts, v,
				fmt.Sprintf("condition false for %d recovery window(s), past KeepFiringWindows=%d: %s",
					recovered, rule.KeepFiringWindows, basis))}
		}
		return nil // held firing through the grace window

	default:
		// resolved and not violating: remain resolved
		st.Consecutive = 0
		return nil
	}
}

func event(t model.EventType, rule model.Rule, ls model.Labels, ts int64, v float64, reason string) model.AlertEvent {
	return model.AlertEvent{
		Type: t, RuleID: rule.ID, Labels: ls, Ts: ts,
		Observed: v, Threshold: rule.Threshold, Op: rule.Op, Reason: reason,
	}
}

// snapshotRulesLocked returns a copy of the current rule slice. Selectors are
// treated as immutable inputs, so label maps are shared.
func (e *Evaluator) snapshotRulesLocked() []model.Rule {
	out := make([]model.Rule, len(e.rules))
	copy(out, e.rules)
	return out
}

// snapshotLocked returns all states ordered by (ruleID, fingerprint).
func (e *Evaluator) snapshotLocked() []model.AlertState {
	keys := make([]string, 0, len(e.states))
	for k := range e.states {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]model.AlertState, 0, len(keys))
	for _, k := range keys {
		out = append(out, *e.states[k])
	}
	return out
}

// ReplaceRules atomically hot-updates the rule set using the legacy
// unvalidated path: it performs the same all-at-once swap as ApplyRules but
// emits no closure events and keeps no batch record. Surviving rule IDs keep
// their states (pending progress and firing are preserved, so updates never
// re-fire); states of removed rules and of series no longer matched by a
// surviving selector are dropped.
//
// New code should prefer Evaluator.ApplyRules, which validates the whole batch
// first, closes orphaned active alerts explicitly and records the change.
func (e *Evaluator) ReplaceRules(rules []model.Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// ts=0 is irrelevant for the legacy path: closure records it may produce
	// are intentionally discarded below.
	events, _, _ := e.swapRulesLocked(e.rules, rules, e.now())
	_ = events
}

func indexSep(k string) int {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return i
		}
	}
	return -1
}

// Rules returns a snapshot copy of the current rules (sorted by ID).
func (e *Evaluator) Rules() []model.Rule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]model.Rule, len(e.rules))
	copy(out, e.rules)
	return out
}
