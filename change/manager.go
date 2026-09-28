// Package change adds a rule-change-management layer on top of the alert
// evaluator: whole-batch validation (one bad rule rejects the batch), an
// atomic all-at-once rule-set switch, explicit closure conclusions for alerts
// orphaned by a change, and a queryable, append-only per-batch audit record.
package change

import (
	"fmt"
	"sync"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// RuleApplier is the evaluator capability the manager needs. Satisfied by
// *alert.Evaluator.
type RuleApplier interface {
	ApplyRules(rules []model.Rule, batchID string, ts int64) (alert.BatchReport, *alert.ValidationErrors)
}

// Manager validates and applies batches and keeps their audit history. It is
// safe for concurrent use; submission history is append-only.
type Manager struct {
	mu      sync.RWMutex
	applier RuleApplier
	now     func() int64
	seq     int64
	history []alert.BatchReport
	// idempotency maps a submitter-provided key to the resulting batch ID, so
	// retrying a push cannot double-apply the same change.
	idempotency map[string]string
}

// NewManager constructs a change manager using the evaluator clock (or the
// injected one) for batch timestamps.
func NewManager(applier RuleApplier, now func() int64) *Manager {
	if now == nil {
		now = func() int64 { return 0 }
	}
	return &Manager{applier: applier, now: now, idempotency: map[string]string{}}
}

// Check validates a batch without changing anything: no rule or alert state is
// modified and no history entry is written. A nil result means the batch is
// valid and can be Submitted unchanged.
func (m *Manager) Check(rules []model.Rule) *alert.ValidationErrors {
	return alert.ValidateRules(rules)
}

// Submit runs the whole-batch check and, only when every rule is valid, applies
// the batch atomically (via the evaluator's serialized swap) and appends an
// audit record. On an invalid batch nothing changes and a rejected record
// (Applied()==false, carrying every validation error) is still kept.
//
// idempotencyKey may be empty. When non-empty, repeating the same key returns
// the previously recorded outcome instead of applying the batch a second
// time, so an operator retrying a push cannot double-switch the rule set.
func (m *Manager) Submit(rules []model.Rule, idempotencyKey string) (alert.BatchReport, *alert.ValidationErrors) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if idempotencyKey != "" {
		if bid, ok := m.idempotency[idempotencyKey]; ok {
			if rec, found := m.findByIDLocked(bid); found {
				// Same push retried: replay the recorded outcome, no re-apply.
				return rec, validationOrNil(rec)
			}
		}
	}

	m.seq++
	ts := m.now()
	batchID := fmt.Sprintf("batch-%d-%d", ts, m.seq)
	rep, verrs := m.applier.ApplyRules(rules, batchID, ts)
	rep.BatchID = batchID
	m.history = append(m.history, rep)
	if idempotencyKey != "" {
		m.idempotency[idempotencyKey] = batchID
	}
	return rep, verrs
}

func validationOrNil(rec alert.BatchReport) *alert.ValidationErrors {
	if rec.Applied() {
		return nil
	}
	return &alert.ValidationErrors{Errs: append([]alert.ValidationError(nil), rec.ValidationErrors...)}
}

// findByIDLocked returns a copy of the stored record for the batch ID.
func (m *Manager) findByIDLocked(batchID string) (alert.BatchReport, bool) {
	for i := range m.history {
		if m.history[i].BatchID == batchID {
			return m.history[i], true
		}
	}
	return alert.BatchReport{}, false
}

// History returns audit records newest-first, with applied and rejected
// batches included. The returned slice is a copy in reverse append order.
func (m *Manager) History() []alert.BatchReport {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := len(m.history)
	out := make([]alert.BatchReport, n)
	for i := 0; i < n; i++ {
		out[i] = m.history[n-1-i]
	}
	return out
}

// Batch returns one audit record by batch ID.
func (m *Manager) Batch(batchID string) (alert.BatchReport, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.findByIDLocked(batchID)
	return rec, ok
}
