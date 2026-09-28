// Package change adds batched, atomic and auditable rule change management on
// top of alert.Evaluator:
//
//   - a whole batch is validated up front (duplicate names, window, aggregate
//     field, comparator, threshold, pending/keep settings); any invalid entry
//     rejects the whole batch — nothing is applied;
//   - a valid batch is switched in atomically: concurrent evaluation never
//     sees a half-old/half-new ruleset, and one rule's failure cannot take the
//     whole batch or live alert states down;
//   - every accepted batch is recorded (what changed, before/after config,
//     which tracked alerts were affected and how they were concluded);
//   - alerts orphaned by deletions or narrowed selectors are explicitly
//     closed with a reason instead of vanishing silently.
package change

import (
	"fmt"
	"sort"
	"sync"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// ChangeKind is how one rule entry in a batch relates to the current ruleset.
type ChangeKind string

const (
	ChangeAdded   ChangeKind = "added"
	ChangeUpdated ChangeKind = "updated"
	ChangeDeleted ChangeKind = "deleted"
)

// Item is one entry of a submitted batch.
type Item struct {
	// Op is OpUpsert ("upsert", add-or-update) or OpDelete ("delete").
	Op   string
	Rule model.Rule // used by upsert; identified by Rule.ID
}

// ValidationError describes one invalid batch entry (or a batch-level issue).
type ValidationError struct {
	RuleID string
	Field  string
	Detail string
}

func (e ValidationError) Error() string {
	if e.RuleID == "" {
		return e.Field + ": " + e.Detail
	}
	return "rule " + e.RuleID + " " + e.Field + ": " + e.Detail
}

// RuleChange is one rule-level diff in an accepted batch record.
type RuleChange struct {
	Kind   ChangeKind
	RuleID string
	Before *model.Rule // nil for added
	After  *model.Rule // nil for deleted
}

// AffectKind enumerates what happened to a tracked alert in a batch.
type AffectKind string

const (
	// AffectClosedByDelete: rule removed while the alert was pending/firing.
	AffectClosedByDelete AffectKind = "closed_by_rule_delete"
	// AffectClosedBySelector: rule changed so its selector no longer matches.
	AffectClosedBySelector AffectKind = "closed_by_selector_narrowed"
	// AffectPreserved: rule was updated but survives; tracking continues with
	// its pending/firing state untouched (no duplicate fire, no missed track).
	AffectPreserved AffectKind = "preserved"
)

// AffectedAlert is one tracked (rule,series) alert impacted by a batch.
type AffectedAlert struct {
	RuleID      string
	Labels      model.Labels
	Status      model.AlertStatus // status right before the switch
	Kind        AffectKind
	CloseReason string // populated when the alert was closed
}

// Record is the auditable record of one successfully applied batch.
type Record struct {
	ID             int64
	CommittedAt    int64
	RulesetVersion int64
	Changes        []RuleChange
	Affected       []AffectedAlert
}

// Preview is the check-only result: nothing is applied.
type Preview struct {
	Errors   []ValidationError
	Changes  []RuleChange
	Affected []AffectedAlert
}

// Manager validates and applies batches and keeps the history of applied ones.
type Manager struct {
	ev  *alert.Evaluator
	now func() int64

	mu      sync.Mutex
	nextID  int64
	history []Record
}

// NewManager wraps an evaluator with batch change management.
func NewManager(ev *alert.Evaluator, now func() int64) *Manager {
	if now == nil {
		now = func() int64 { return 0 }
	}
	return &Manager{ev: ev, now: now, nextID: 1}
}

// plan is the derived, validated intent of a batch.
type plan struct {
	changes []RuleChange    // touched rules only, sorted by ID
	next    []model.Rule    // complete ruleset after applying the batch
	touched map[string]bool // IDs the batch touches (added/updated/deleted)
}

// buildPlan validates the batch against the given ruleset snapshot and
// derives its diff and resulting ruleset. It performs no mutation.
func (m *Manager) buildPlan(items []Item, current []model.Rule) (*plan, []ValidationError) {
	errs := validateBatch(items)

	curByID := make(map[string]model.Rule, len(current))
	for _, r := range current {
		curByID[r.ID] = r
	}

	// Deleting a rule that is not currently present is a stale/ambiguous batch
	// entry; surface it rather than silently accepting a no-op delete.
	for _, it := range items {
		if it.Op == OpDelete && it.Rule.ID != "" {
			if _, ok := curByID[it.Rule.ID]; !ok {
				errs = append(errs, ValidationError{
					RuleID: it.Rule.ID, Field: "Op",
					Detail: "cannot delete: rule does not exist in the current ruleset",
				})
			}
		}
	}
	if len(errs) > 0 {
		sort.SliceStable(errs, func(i, j int) bool {
			if errs[i].RuleID != errs[j].RuleID {
				return errs[i].RuleID < errs[j].RuleID
			}
			return errs[i].Field < errs[j].Field
		})
		return nil, errs
	}

	// Derive the complete post-batch ruleset.
	nextByID := make(map[string]model.Rule, len(current)+len(items))
	for _, r := range current {
		nextByID[r.ID] = r
	}
	var p plan
	p.touched = map[string]bool{}
	for _, it := range items {
		id := it.Rule.ID
		p.touched[id] = true
		before, existed := curByID[id]
		if it.Op == OpDelete {
			delete(nextByID, id)
			b := before
			p.changes = append(p.changes, RuleChange{
				Kind: ChangeDeleted, RuleID: id, Before: &b,
			})
			continue
		}
		r := it.Rule // copy
		if existed {
			b := before
			a := r
			p.changes = append(p.changes, RuleChange{
				Kind: ChangeUpdated, RuleID: id, Before: &b, After: &a,
			})
		} else {
			a := r
			p.changes = append(p.changes, RuleChange{
				Kind: ChangeAdded, RuleID: id, After: &a,
			})
		}
		nextByID[id] = r
	}
	for _, r := range nextByID {
		p.next = append(p.next, r)
	}
	sort.Slice(p.next, func(i, j int) bool { return p.next[i].ID < p.next[j].ID })
	sort.Slice(p.changes, func(i, j int) bool { return p.changes[i].RuleID < p.changes[j].RuleID })
	return &p, nil
}

// affectedByPlan derives which tracked alerts the batch impacts. The state
// snapshot must be taken right before the switch. closures are the closure
// events produced by the atomic switch (already rule+fingerprint sorted).
func affectedByPlan(states []model.AlertState, touched map[string]bool, closures []alert.Closure) []AffectedAlert {
	closeByKey := make(map[string]alert.Closure, len(closures))
	for _, c := range closures {
		closeByKey[c.RuleID+"\x00"+c.Labels.Fingerprint()] = c
	}
	var out []AffectedAlert
	for _, st := range states {
		if !touched[st.RuleID] {
			continue // untouched rules do not belong to this batch's record
		}
		key := st.RuleID + "\x00" + st.Labels.Fingerprint()
		if c, closed := closeByKey[key]; closed {
			kind := AffectClosedByDelete
			if c.ReasonCode == "selector_narrowed" {
				kind = AffectClosedBySelector
			}
			out = append(out, AffectedAlert{
				RuleID:      st.RuleID,
				Labels:      cloneLabels(st.Labels),
				Status:      st.Status,
				Kind:        kind,
				CloseReason: c.Event.Reason,
			})
			continue
		}
		// Surviving touched rule: only live alerts are recorded as preserved;
		// dormant resolved entries carry nothing worth auditing.
		if st.Status == model.StatusPending || st.Status == model.StatusFiring {
			out = append(out, AffectedAlert{
				RuleID: st.RuleID,
				Labels: cloneLabels(st.Labels),
				Status: st.Status,
				Kind:   AffectPreserved,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RuleID != out[j].RuleID {
			return out[i].RuleID < out[j].RuleID
		}
		return out[i].Labels.Fingerprint() < out[j].Labels.Fingerprint()
	})
	return out
}

func cloneLabels(ls model.Labels) model.Labels {
	cp := make(model.Labels, len(ls))
	for k, v := range ls {
		cp[k] = v
	}
	return cp
}

// Validate checks a batch without applying it. The returned preview lists
// every problem at once; Changes/Affected are populated only when valid.
func (m *Manager) Validate(items []Item) Preview {
	// A no-op batch is trivially valid (current ruleset, no diff).
	if len(items) == 0 {
		return Preview{}
	}
	// One atomic rules+states snapshot, so the preview never mixes rules from
	// one ruleset version with states from another.
	rules, _, states := m.ev.Snapshot()
	p, errs := m.buildPlan(items, rules)
	if errs != nil {
		return Preview{Errors: errs}
	}

	// Simulate the switch on a throwaway evaluator seeded from the snapshot to
	// preview closures without mutating live state.
	sim := alert.NewEvaluator(m.ev.Reader(), rules, m.now)
	for _, st := range states {
		sim.SeedState(st)
	}
	res, _ := sim.ApplyRuleset(p.next, m.now(), -1)
	return Preview{Changes: p.changes, Affected: affectedByPlan(states, p.touched, res.Closures)}
}

// Commit validates a batch and, only when fully valid, applies it atomically
// and appends an auditable record. Invalid batches change nothing: rules,
// alert states and history stay exactly as before the call.
func (m *Manager) Commit(items []Item) (Record, []ValidationError, error) {
	// Serialize commits so record IDs/history and ruleset versions never race.
	// Eval still takes the evaluator lock independently; the actual switch and
	// reconciliation is one atomic evaluator operation.
	m.mu.Lock()
	defer m.mu.Unlock()

	ts := m.now()
	if len(items) == 0 {
		// Nothing to do: record nothing, change nothing.
		return Record{}, nil, nil
	}

	// Optimistic plan/apply against one atomic rules+states snapshot. If a
	// concurrent switch via another path (e.g. the legacy ReplaceRules) bumps
	// the version between plan and apply, re-plan against the fresh snapshot.
	// Validation is read-only, so a retry never leaves partial effects.
	for attempt := 0; attempt < 16; attempt++ {
		rules, version, statesBefore := m.ev.Snapshot()
		p, errs := m.buildPlan(items, rules)
		if errs != nil {
			// Whole batch rejected: nothing was or will be mutated; return the
			// full problem list.
			return Record{}, errs, nil
		}

		res, applied := m.ev.ApplyRuleset(p.next, ts, version)
		if !applied {
			continue // ruleset changed concurrently; re-plan
		}
		rec := Record{
			ID:             m.nextID,
			CommittedAt:    ts,
			RulesetVersion: res.Version,
			Changes:        p.changes,
			Affected:       affectedByPlan(statesBefore, p.touched, res.Closures),
		}
		m.nextID++
		m.history = append(m.history, rec)
		return rec, nil, nil
	}
	return Record{}, nil, fmt.Errorf("rule change commit lost the ruleset version race after retries")
}

// History returns a deep-ish snapshot copy of all committed records.
func (m *Manager) History() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, len(m.history))
	copy(out, m.history)
	return out
}

// RecordByID returns one committed record.
func (m *Manager) RecordByID(id int64) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.history {
		if r.ID == id {
			return r, true
		}
	}
	return Record{}, false
}
