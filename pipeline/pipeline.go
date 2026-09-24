// Package pipeline wires the in-memory data source, windowed store and alert
// evaluator into one locally-runnable pipeline.
package pipeline

import (
	"github.com/xingjianluo/metrics-alerting-pipeline-go/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/storage"
)

// Pipeline is the facade combining storage and alerting.
type Pipeline struct {
	Store     *storage.WindowStore
	Evaluator *alert.Evaluator
}

// New constructs a pipeline over the given store and initial rules.
func New(store *storage.WindowStore, rules []model.Rule, now func() int64) *Pipeline {
	return &Pipeline{
		Store:     store,
		Evaluator: alert.NewEvaluator(store, rules, now),
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

// ReplaceRules hot-updates rules without losing surviving states.
func (p *Pipeline) ReplaceRules(rules []model.Rule) {
	p.Evaluator.ReplaceRules(rules)
}

// Tick evicts expired windows using the store clock.
func (p *Pipeline) Tick() int64 { return p.Store.Tick() }
