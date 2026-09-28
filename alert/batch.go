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

// AffectedAlert records one tracked alert whose lifecycle was ended by a
// batch change (rule deleted, or selector narrowed past its series).
type AffectedAlert struct {
	RuleID         string
	Labels         model.Labels
	PreviousStatus model.AlertStatus
	Reason         string
}

// Closure reasons emitted in EventClosed records.
const (
	CloseReasonRuleDeleted = "rule deleted by batch change"
	CloseReasonNoMatch     = "rule updated and no longer matches this series"
)

// BatchReport is the auditable outcome record of one submitted batch.
type BatchReport struct {
	BatchID string
	Ts      int64
	Changes []RuleChange
	// ClosedEvents are the administrative closure transitions (pending/firing
	// alerts that got an explicit ending instead of vanishing silently).
	ClosedEvents []model.AlertEvent
	// Affected is the structured parallel view of ClosedEvents (query-friendly).
	Affected []AffectedAlert
	// ValidationErrors is populated for a rejected batch (applied==false); it
	// lists every problem found so a failed push is itself auditable.
	ValidationErrors []ValidationError
	applied          bool
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

// swapRulesLocked performs the atomic all-at-once rule-set replacement and
// reconciles alert states:
//
//   - surviving rule ID whose new selector still matches the series: state kept
//     (pending progress / firing survive; no duplicate firing);
//   - surviving rule ID whose new selector no longer matches: the active
//     (pending/firing) alert is closed explicitly and its state dropped;
//   - deleted rule ID: all its active alerts are closed explicitly; resolved
//     (inert) states are simply dropped;
//   - added rule ID: no state, starts clean on next evaluation.
//
// Caller holds e.mu.
func (e *Evaluator) swapRulesLocked(next []model.Rule, ts int64) ([]model.AlertEvent, []AffectedAlert) {
	cp := make([]model.Rule, len(next))
	copy(cp, next)
	sort.SliceStable(cp, func(i, j int) bool { return cp[i].ID < cp[j].ID })
	nextByID := make(map[string]model.Rule, len(cp))
	for _, r := range cp {
		nextByID[r.ID] = r
	}

	var events []model.AlertEvent
	var affected []AffectedAlert
	for key, st := range e.states {
		rid := key
		if i := indexSep(key); i >= 0 {
			rid = key[:i]
		}
		nr, survives := nextByID[rid]
		labelsMatch := survives && (nr.Selector == nil || st.Labels.Match(nr.Selector))
		if survives && labelsMatch {
			continue // tracked alert stays tracked under the updated rule
		}

		reason := CloseReasonRuleDeleted
		if survives {
			reason = CloseReasonNoMatch
		}
		active := st.Status == model.StatusPending || st.Status == model.StatusFiring
		if active {
			events = append(events, closeEvent(rid, st.Labels, ts, st, reason))
			affected = append(affected, AffectedAlert{
				RuleID:         rid,
				Labels:         cloneLabels(st.Labels),
				PreviousStatus: st.Status,
				Reason:         reason,
			})
		}
		delete(e.states, key)
	}

	e.rules = cp
	return events, affected
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
	events, affected := e.swapRulesLocked(rules, ts)

	// Deterministic ordering of closures for the audit record.
	sort.Slice(events, func(i, j int) bool {
		if events[i].RuleID != events[j].RuleID {
			return events[i].RuleID < events[j].RuleID
		}
		return events[i].Labels.Fingerprint() < events[j].Labels.Fingerprint()
	})
	sort.Slice(affected, func(i, j int) bool {
		if affected[i].RuleID != affected[j].RuleID {
			return affected[i].RuleID < affected[j].RuleID
		}
		return affected[i].Labels.Fingerprint() < affected[j].Labels.Fingerprint()
	})

	return BatchReport{
		BatchID:      batchID,
		Ts:           ts,
		Changes:      diffRules(old, e.rules),
		ClosedEvents: events,
		Affected:     affected,
		applied:      true,
	}, nil
}
