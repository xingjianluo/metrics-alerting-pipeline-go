// Batch rule change management on top of the evaluator: validate-then-apply
// semantics, an atomic all-at-once rule-set switch, explicit closure of alerts
// orphaned by the change, and an auditable per-batch record.
package alert

import (
	"fmt"
	"sort"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// ChangeKind classifies what happened to one rule inside a batch.
type ChangeKind string

const (
	ChangeAdded     ChangeKind = "added"
	ChangeModified  ChangeKind = "modified"
	ChangeDeleted   ChangeKind = "deleted"
	ChangeUnchanged ChangeKind = "unchanged"
)

// RuleChange is the before/after record of one rule within a batch.
type RuleChange struct {
	Kind   ChangeKind
	RuleID string
	Before model.Rule // zero value (ID=="") when Kind==added
	After  model.Rule // zero value (ID=="") when Kind==deleted
	// ChangedFields lists the critical config fields that differ (modified).
	ChangedFields []string
}

// AffectedOutcome classifies what happened to one tracked (pending/firing at
// submit time) alert touched by a batch change.
type AffectedOutcome string

const (
	// OutcomeTracked: the alert is still covered after the batch and keeps
	// being tracked (pending progress / firing preserved, never re-triggered).
	OutcomeTracked AffectedOutcome = "tracked"
	// OutcomeClosed: the alert reached an explicit administrative end in this
	// batch (its rule was deleted, or the updated selector dropped its series);
	// a matching EventClosed is emitted and the state removed.
	OutcomeClosed AffectedOutcome = "closed"
)

// Machine-stable conclusion keywords carried by AffectedAlert.Conclusion.
const (
	ConclusionStillTracked  = "still_tracked"
	ConclusionRuleDeleted   = "closed_rule_deleted"
	ConclusionNoLongerMatch = "closed_no_longer_matches"
)

// AffectedAlert records one tracked (pending/firing) alert touched by a batch
// change. Every entry says, at submit time, which rule/series it was, whether
// it was pending or firing (PreviousStatus/Streak/LastObserved), and which of
// the two disjoint outcomes it met:
//
//   - OutcomeTracked (Conclusion=still_tracked): still covered, tracking
//     continues; Reason explains why the config change did not end it;
//   - OutcomeClosed (Conclusion=closed_rule_deleted /
//     closed_no_longer_matches): administratively finished; Reason is the
//     explicit closure cause and a parallel EventClosed exists.
type AffectedAlert struct {
	RuleID         string
	Labels         model.Labels
	PreviousStatus model.AlertStatus
	// Streak is the signed consecutive counter captured at submit time
	// (>0 violating streak, <0 recovery streak), for forensics.
	Streak       int
	LastObserved float64
	Outcome      AffectedOutcome
	Conclusion   string
	// Reason is the human-readable cause; for OutcomeClosed it is exactly the
	// reason string used in the parallel EventClosed.
	Reason string
}

// Closure reasons emitted in EventClosed records (and AffectedAlert.Reason for
// OutcomeClosed entries).
const (
	CloseReasonRuleDeleted = "rule deleted by batch change"
	CloseReasonNoMatch     = "rule updated and no longer matches this series"
)

// StillTrackedReason explains an OutcomeTracked entry: the rule's critical
// configuration changed but its selector still covers the series, so tracking
// continues with the state preserved.
const StillTrackedReason = "rule updated but selector still covers this series; state preserved"

// BatchReport is the auditable outcome record of one submitted batch.
type BatchReport struct {
	BatchID string
	Ts      int64
	Changes []RuleChange
	// ClosedEvents are the administrative closure transitions (pending/firing
	// alerts that got an explicit ending instead of vanishing silently).
	ClosedEvents []model.AlertEvent
	// Kept lists the tracked (pending/firing) alerts that this batch touched
	// but which are still covered afterwards: tracking continues, their state
	// (pending progress / firing) is preserved and they are not re-triggered.
	Kept []AffectedAlert
	// Closed lists the tracked alerts that this batch gave an explicit
	// administrative ending (rule deleted, or selector narrowed past them);
	// each entry corresponds 1:1 to one ClosedEvents element.
	Closed []AffectedAlert
	// ValidationErrors is populated for a rejected batch (applied==false); it
	// lists every problem found so a failed push is itself auditable.
	ValidationErrors []ValidationError
	applied          bool
}

// AffectedAlerts returns Kept followed by Closed, i.e. every tracked
// (pending/firing) alert the batch touched, each carrying its explicit outcome.
// The two groups are never mixed: see Kept / Closed for the separated views.
func (b BatchReport) AffectedAlerts() []AffectedAlert {
	out := make([]AffectedAlert, 0, len(b.Kept)+len(b.Closed))
	out = append(out, b.Kept...)
	out = append(out, b.Closed...)
	return out
}

// Applied reports whether the batch actually took effect. It is false when
// validation rejected the batch; in that case no rule or alert state changed.
func (b BatchReport) Applied() bool { return b.applied }

// labelsEqual compares two selectors/label sets content-wise.
func labelsEqual(a, b model.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// ruleEqual reports whether the critical configuration of two rules matches.
func ruleEqual(a, b model.Rule) bool {
	return labelsEqual(a.Selector, b.Selector) &&
		a.WindowSeconds == b.WindowSeconds &&
		a.AggField == b.AggField &&
		a.Op == b.Op &&
		a.Threshold == b.Threshold &&
		a.PendingWindows == b.PendingWindows &&
		a.KeepFiringWindows == b.KeepFiringWindows
}

// changedCriticalFields returns the critical fields that differ, in a fixed
// order for reproducible audit records.
func changedCriticalFields(before, after model.Rule) []string {
	var out []string
	if !labelsEqual(before.Selector, after.Selector) {
		out = append(out, "Selector")
	}
	if before.WindowSeconds != after.WindowSeconds {
		out = append(out, "WindowSeconds")
	}
	if before.AggField != after.AggField {
		out = append(out, "AggField")
	}
	if before.Op != after.Op {
		out = append(out, "Op")
	}
	if before.Threshold != after.Threshold {
		out = append(out, "Threshold")
	}
	if before.PendingWindows != after.PendingWindows {
		out = append(out, "PendingWindows")
	}
	if before.KeepFiringWindows != after.KeepFiringWindows {
		out = append(out, "KeepFiringWindows")
	}
	return out
}

// diffRules computes the ordered per-rule change record between the old and
// new rule sets. Output is sorted by rule ID: deletions and modifications for
// surviving/dead IDs first, additions after, all in one deterministic slice.
func diffRules(old, next []model.Rule) []RuleChange {
	oldByID := make(map[string]model.Rule, len(old))
	for _, r := range old {
		oldByID[r.ID] = r
	}
	nextByID := make(map[string]model.Rule, len(next))
	for _, r := range next {
		nextByID[r.ID] = r
	}
	ids := make(map[string]struct{}, len(oldByID)+len(nextByID))
	for id := range oldByID {
		ids[id] = struct{}{}
	}
	for id := range nextByID {
		ids[id] = struct{}{}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)

	changes := make([]RuleChange, 0, len(ordered))
	for _, id := range ordered {
		o, inOld := oldByID[id]
		n, inNext := nextByID[id]
		switch {
		case inOld && !inNext:
			changes = append(changes, RuleChange{Kind: ChangeDeleted, RuleID: id, Before: o})
		case !inOld && inNext:
			changes = append(changes, RuleChange{Kind: ChangeAdded, RuleID: id, After: n})
		case ruleEqual(o, n):
			changes = append(changes, RuleChange{Kind: ChangeUnchanged, RuleID: id, Before: o, After: n})
		default:
			changes = append(changes, RuleChange{
				Kind:          ChangeModified,
				RuleID:        id,
				Before:        o,
				After:         n,
				ChangedFields: changedCriticalFields(o, n),
			})
		}
	}
	return changes
}

// closeEvent builds an administrative closure transition for one active alert.
func closeEvent(ruleID string, ls model.Labels, ts int64, st *model.AlertState, reason string) model.AlertEvent {
	return model.AlertEvent{
		Type:     model.EventClosed,
		RuleID:   ruleID,
		Labels:   ls,
		Ts:       ts,
		Observed: st.LastObserved,
		Reason:   fmt.Sprintf("%s (previous status=%s, streak=%d)", reason, st.Status, st.Consecutive),
	}
}

// makeAffected captures the submit-time tracked state of one pending/firing
// alert for the audit record.
func makeAffected(ruleID string, st *model.AlertState, outcome AffectedOutcome, conclusion, reason string) AffectedAlert {
	return AffectedAlert{
		RuleID:         ruleID,
		Labels:         cloneLabels(st.Labels),
		PreviousStatus: st.Status,
		Streak:         st.Consecutive,
		LastObserved:   st.LastObserved,
		Outcome:        outcome,
		Conclusion:     conclusion,
		Reason:         reason,
	}
}

// swapRulesLocked performs the atomic all-at-once rule-set replacement and
// reconciles alert states. changedRuleIDs is the set of rules the batch
// actually adds/modifies/deletes (unchanged rules are excluded), so the audit
// record attributes an affected alert to a rule only when that rule moved.
//
// Classification of each still-tracked (pending/firing) state:
//
//   - rule deleted: OutcomeClosed (closed_rule_deleted), event emitted, state
//     dropped; resolved (inert) states are dropped quietly;
//   - rule modified and new selector no longer matches the series:
//     OutcomeClosed (closed_no_longer_matches), event emitted, state dropped;
//   - rule modified and selector still matches: OutcomeTracked
//     (still_tracked), state kept verbatim (no duplicate firing);
//   - unchanged rule: state kept, not listed (the batch did not touch it);
//   - added rule: no prior state can exist; next evaluation starts clean.
//
// Caller holds e.mu.
func (e *Evaluator) swapRulesLocked(next []model.Rule, ts int64, changedRuleIDs map[string]struct{}) (events []model.AlertEvent, kept, closed []AffectedAlert) {
	cp := make([]model.Rule, len(next))
	copy(cp, next)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	nextByID := make(map[string]model.Rule, len(cp))
	for _, r := range cp {
		nextByID[r.ID] = r
	}

	for key, st := range e.states {
		rid := key
		if i := indexSep(key); i >= 0 {
			rid = key[:i]
		}
		_, changed := changedRuleIDs[rid]
		nr, survives := nextByID[rid]
		matches := survives && (nr.Selector == nil || st.Labels.Match(nr.Selector))

		if survives && matches {
			// State is preserved. Only record it when the rule itself was
			// touched by this batch (modified config, selector still covers),
			// so a config-only change is traceable to its tracked alerts.
			if changed && (st.Status == model.StatusPending || st.Status == model.StatusFiring) {
				kept = append(kept, makeAffected(rid, st, OutcomeTracked, ConclusionStillTracked, StillTrackedReason))
			}
			continue
		}

		// The rule either was deleted or its new selector dropped the series.
		reason := CloseReasonRuleDeleted
		conclusion := ConclusionRuleDeleted
		if survives {
			reason = CloseReasonNoMatch
			conclusion = ConclusionNoLongerMatch
		}
		active := st.Status == model.StatusPending || st.Status == model.StatusFiring
		if active {
			events = append(events, closeEvent(rid, st.Labels, ts, st, reason))
			closed = append(closed, makeAffected(rid, st, OutcomeClosed, conclusion, reason))
		}
		delete(e.states, key)
	}

	e.rules = cp
	return events, kept, closed
}

func cloneLabels(ls model.Labels) model.Labels {
	cp := make(model.Labels, len(ls))
	for k, v := range ls {
		cp[k] = v
	}
	return cp
}

// ApplyRules validates the whole batch and, only if every rule is valid,
// atomically swaps the rule set under the evaluator lock and closes alerts
// orphaned by the change. An invalid batch mutates nothing: the existing rule
// set and every tracked alert state stay exactly as before.
//
// batchID identifies the change in audit records; ts is the change timestamp
// (unix seconds) stamped on closure events (evaluator clock is used when 0).
func (e *Evaluator) ApplyRules(rules []model.Rule, batchID string, ts int64) (BatchReport, *ValidationErrors) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if verrs := validateBatch(rules); verrs != nil {
		// All-or-nothing: report without touching rules or states.
		return BatchReport{
			BatchID:          batchID,
			Ts:               ts,
			ValidationErrors: append([]ValidationError(nil), verrs.Errs...),
			applied:          false,
		}, verrs
	}

	if ts == 0 {
		ts = e.now()
	}
	old := e.rules
	changes := diffRules(old, rules)
	changedRuleIDs := make(map[string]struct{}, len(changes))
	for _, ch := range changes {
		if ch.Kind != ChangeUnchanged {
			changedRuleIDs[ch.RuleID] = struct{}{}
		}
	}
	events, kept, closed := e.swapRulesLocked(rules, ts, changedRuleIDs)

	// Deterministic ordering of the audit record (rule, then fingerprint).
	lessAffected := func(a, b AffectedAlert) bool {
		if a.RuleID != b.RuleID {
			return a.RuleID < b.RuleID
		}
		return a.Labels.Fingerprint() < b.Labels.Fingerprint()
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].RuleID != events[j].RuleID {
			return events[i].RuleID < events[j].RuleID
		}
		return events[i].Labels.Fingerprint() < events[j].Labels.Fingerprint()
	})
	sort.Slice(kept, func(i, j int) bool { return lessAffected(kept[i], kept[j]) })
	sort.Slice(closed, func(i, j int) bool { return lessAffected(closed[i], closed[j]) })

	return BatchReport{
		BatchID:      batchID,
		Ts:           ts,
		Changes:      changes,
		ClosedEvents: events,
		Kept:         kept,
		Closed:       closed,
		applied:      true,
	}, nil
}
