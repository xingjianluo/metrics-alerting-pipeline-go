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
	// Version is the ruleset version the pass evaluated against; it bumps on
	// every atomic ruleset switch, so a concurrent reader can prove the whole
	// pass saw one consistent ruleset (mixed old/new rules are impossible).
	Version int64
	Events  []model.AlertEvent // transitions produced in this pass
	States  []model.AlertState // snapshot of all tracked states
	Skipped []Skip
}

// Closure is an alert explicitly concluded while switching rulesets: the rule
// was deleted or its selector stopped covering the series. Pending/firing
// alerts are never dropped silently — each yields an EventClosed event.
type Closure struct {
	RuleID string
	Labels model.Labels
	// ReasonCode is "rule_deleted" or "selector_narrowed".
	ReasonCode string
	Event      model.AlertEvent
}

// ApplyResult is the outcome of one atomic ruleset switch.
type ApplyResult struct {
	Ts       int64
	Version  int64
	Closures []Closure
	States   []model.AlertState // snapshot right after the switch
}

// Evaluator evaluates rules and owns alert states.
type Evaluator struct {
	mu      sync.Mutex
	rules   []model.Rule                 // ordered by ID for deterministic evaluation
	states  map[string]*model.AlertState // key: ruleID + "\x00" + fingerprint
	reader  AggReader
	now     func() int64
	version int64 // ruleset version; bumped atomically on every ruleset switch
}

// Version returns the ruleset version the evaluator is currently on. It
// starts at 0 and bumps by one per atomic switch (ReplaceRules / ApplyRuleset).
func (e *Evaluator) Version() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.version
}

// States returns a snapshot copy of all tracked alert states.
func (e *Evaluator) States() []model.AlertState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotLocked()
}

// Reader exposes the aggregate reader the evaluator uses (mainly for building
// throwaway simulation evaluators in check-only previews).
func (e *Evaluator) Reader() AggReader { return e.reader }

// SeedState inserts a copy of an existing state keyed by (RuleID, labels).
// It is used to clone live state into a check-only simulation evaluator; it
// never touches the receiver's live state.
func (e *Evaluator) SeedState(st model.AlertState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := st
	cp.Labels = cloneLabels(st.Labels)
	e.states[stateKey(st.RuleID, st.Labels.Fingerprint())] = &cp
}

// Snapshot returns the current rules (sorted by ID), ruleset version and all
// tracked states in one atomic read under the evaluator lock, so callers
// never combine rules from one ruleset version with states from another.
func (e *Evaluator) Snapshot() (rules []model.Rule, version int64, states []model.AlertState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rules = make([]model.Rule, len(e.rules))
	copy(rules, e.rules)
	return rules, e.version, e.snapshotLocked()
}

// NewEvaluator constructs an evaluator with an initial rule set.
func NewEvaluator(reader AggReader, rules []model.Rule, now func() int64) *Evaluator {
	e := &Evaluator{
		states: map[string]*model.AlertState{},
		reader: reader,
		now:    now,
	}
	e.replaceRulesLocked(rules)
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
	rep := Report{Ts: evalTs, Version: e.version}

	for _, rule := range e.rules {
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

// ReplaceRules atomically hot-updates the rule set, preserving the original
// hot-reload semantics: states of surviving rule IDs are kept (no lost
// pending progress, no re-fire), states of removed rule IDs are dropped
// silently with no closure events.
//
// New code that deletes rules or narrows selectors should prefer
// Evaluator.ApplyRuleset (or change.Manager.Commit), which additionally
// validates, closes orphaned pending/firing alerts explicitly and records the
// batch. ReplaceRules stays for backward compatibility with callers that
// already accepted silent state cleanup.
func (e *Evaluator) ReplaceRules(rules []model.Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.replaceRulesLocked(rules)
}

func (e *Evaluator) replaceRulesLocked(rules []model.Rule) {
	cp := make([]model.Rule, len(rules))
	copy(cp, rules)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	keep := make(map[string]bool, len(cp))
	for _, r := range cp {
		keep[r.ID] = true
	}
	for k := range e.states {
		rid := k
		if i := indexSep(k); i >= 0 {
			rid = k[:i]
		}
		if !keep[rid] {
			delete(e.states, k)
		}
	}
	e.rules = cp
	e.version++
}

// ApplyRuleset atomically switches to the given complete ruleset and performs
// reconciliation while holding the same lock used by Eval, so a concurrent
// evaluation can never observe a half-switched set:
//
//   - states of surviving rules are preserved (no double fire, no lost track);
//   - pending/firing states whose rule was deleted, or whose rule's selector
//     no longer covers their series, are explicitly closed (EventClosed with
//     a reason) and removed;
//   - re-adding / re-widening a rule later starts from a clean state, because
//     the closed state was deleted rather than left dormant.
//
// Callers are expected to have validated the ruleset beforehand (see package
// change); ApplyRuleset itself does not validate rule contents.
//
// expectVersion enables optimistic concurrency: the switch proceeds only when
// the live ruleset version still equals expectVersion (the version the caller
// planned against). If another switch happened in between, ok is false with
// nothing applied, letting the caller re-plan against the fresh ruleset.
// Pass expectVersion < 0 to apply unconditionally.
func (e *Evaluator) ApplyRuleset(rules []model.Rule, applyTs, expectVersion int64) (res ApplyResult, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if expectVersion >= 0 && e.version != expectVersion {
		return ApplyResult{Ts: applyTs, Version: e.version}, false
	}

	if applyTs == 0 {
		applyTs = e.now()
	}

	// Index old and new rules by ID and normalize/copy the incoming set.
	oldByID := make(map[string]model.Rule, len(e.rules))
	for _, r := range e.rules {
		oldByID[r.ID] = r
	}
	next := make([]model.Rule, len(rules))
	copy(next, rules)
	sort.SliceStable(next, func(i, j int) bool { return next[i].ID < next[j].ID })
	newByID := make(map[string]model.Rule, len(next))
	for _, r := range next {
		newByID[r.ID] = r
	}

	// Reconcile tracked states against the incoming ruleset. Iteration order
	// over the state map is not deterministic, so collect and sort afterwards.
	var closures []Closure
	var removed []string
	for key, st := range e.states {
		rid := key
		if i := indexSep(key); i >= 0 {
			rid = key[:i]
		}
		old := oldByID[rid]
		nr, survives := newByID[rid]

		switch {
		case !survives:
			// Rule deleted from the ruleset.
			if st.Status == model.StatusPending || st.Status == model.StatusFiring {
				closures = append(closures, Closure{
					RuleID:     rid,
					Labels:     cloneLabels(st.Labels),
					ReasonCode: "rule_deleted",
					Event: model.AlertEvent{
						Type: model.EventClosed, RuleID: rid,
						Labels: cloneLabels(st.Labels), Ts: applyTs,
						Observed: st.LastObserved, Threshold: old.Threshold, Op: old.Op,
						Reason: fmt.Sprintf(
							"rule %q deleted by atomic ruleset switch at ts=%d: %s alert closed explicitly instead of vanishing (last observed %s)",
							rid, applyTs, st.Status, model.FormatValue(st.LastObserved)),
					},
				})
			}
			removed = append(removed, key)

		case !nr.Selector.Match(st.Labels):
			// Rule survives but no longer covers this series.
			if st.Status == model.StatusPending || st.Status == model.StatusFiring {
				closures = append(closures, Closure{
					RuleID:     rid,
					Labels:     cloneLabels(st.Labels),
					ReasonCode: "selector_narrowed",
					Event: model.AlertEvent{
						Type: model.EventClosed, RuleID: rid,
						Labels: cloneLabels(st.Labels), Ts: applyTs,
						Observed: st.LastObserved, Threshold: nr.Threshold, Op: nr.Op,
						Reason: fmt.Sprintf(
							"rule %q selector narrowed by update at ts=%d no longer matches series %s: %s alert closed explicitly (last observed %s)",
							rid, applyTs, st.Labels.Fingerprint(), st.Status, model.FormatValue(st.LastObserved)),
					},
				})
			}
			removed = append(removed, key)
		}
	}
	for _, k := range removed {
		delete(e.states, k)
	}

	sort.Slice(closures, func(i, j int) bool {
		if closures[i].RuleID != closures[j].RuleID {
			return closures[i].RuleID < closures[j].RuleID
		}
		return closures[i].Labels.Fingerprint() < closures[j].Labels.Fingerprint()
	})

	// Single commit point: rules and the reconciled state map become visible
	// together. Eval holds the same mutex, so no pass can observe a mix.
	e.rules = next
	e.version++

	return ApplyResult{
		Ts:       applyTs,
		Version:  e.version,
		Closures: closures,
		States:   e.snapshotLocked(),
	}, true
}

// cloneLabels returns a copy so records/events never alias mutable maps.
func cloneLabels(ls model.Labels) model.Labels {
	cp := make(model.Labels, len(ls))
	for k, v := range ls {
		cp[k] = v
	}
	return cp
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
