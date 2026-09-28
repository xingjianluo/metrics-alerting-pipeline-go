// Package pipeline wires the in-memory data source, windowed store and alert
// evaluator into one locally-runnable pipeline.
package pipeline

import (
	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/change"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

// Pipeline is the facade combining storage and alerting.
type Pipeline struct {
	Store     *storage.WindowStore
	Evaluator *alert.Evaluator
	// Changes is the batch rule-change-management layer (audit history etc.).
	Changes *change.Manager
}

// New constructs a pipeline over the given store and initial rules. A change
// manager is attached so batch submissions are audited.
func New(store *storage.WindowStore, rules []model.Rule, now func() int64) *Pipeline {
	ev := alert.NewEvaluator(store, rules, now)
	return &Pipeline{
		Store:     store,
		Evaluator: ev,
		Changes:   change.NewManager(ev, now),
	}
}

// Ingest writes one sample and returns the write outcome.
func (p *Pipeline) Ingest(sample model.Sample) model.WriteResult {
	return p.Store.Write(sample)
}

// Query reads aggregates over [start,end) at the given granularity.
func (p *Pipeline) Query(start, end, granularitySeconds int64, selector model.Labels) (*model.QueryResult, error) {
	return p.Store.Query(start, end, granularitySeconds, selector)
}

// Evaluate runs one alert evaluation pass.
func (p *Pipeline) Evaluate(evalTs int64) (alert.Report, error) {
	return p.Evaluator.Eval(evalTs)
}

// ReplaceRules hot-updates rules without losing surviving states. This is the
// legacy single-shot (unvalidated) path, kept for compatibility; prefer
// SubmitRules which validates the whole batch, closes orphaned alerts with a
// recorded reason and keeps an auditable per-batch record.
func (p *Pipeline) ReplaceRules(rules []model.Rule) {
	p.Evaluator.ReplaceRules(rules)
}

// CheckRules validates a proposed batch without changing anything, returning
// every problem found at once (nil when the batch is applyable).
func (p *Pipeline) CheckRules(rules []model.Rule) *alert.ValidationErrors {
	return p.Changes.Check(rules)
}

// SubmitRules validates the whole batch and, only when fully valid, applies it
// atomically and records it. An invalid batch mutates nothing. A non-empty
// idempotencyKey makes a retried push return the original outcome instead of
// switching the rule set twice.
func (p *Pipeline) SubmitRules(rules []model.Rule, idempotencyKey string) (alert.BatchReport, *alert.ValidationErrors) {
	return p.Changes.Submit(rules, idempotencyKey)
}

// RuleChangeHistory returns batch change records newest-first.
func (p *Pipeline) RuleChangeHistory() []alert.BatchReport {
	return p.Changes.History()
}

// RuleChangeBatch returns one batch change record by ID.
func (p *Pipeline) RuleChangeBatch(batchID string) (alert.BatchReport, bool) {
	return p.Changes.Batch(batchID)
}

// Tick evicts expired windows using the store clock.
func (p *Pipeline) Tick() int64 { return p.Store.Tick() }
