// Package alert implements threshold alert rules and a three-state
// (resolved -> pending -> firing -> resolved) alert state machine with an
// explainable transition log and atomic hot rule replacement.
package alert

import (
	"fmt"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/model"
)

// Operator is the comparison applied to an aggregated value.
type Operator string

const (
	OpGT Operator = ">"
	OpGE Operator = ">="
	OpLT Operator = "<"
	OpLE Operator = "<="
	OpEQ Operator = "=="
)

// Rule describes one alerting rule.
type Rule struct {
	ID          string
	MetricName  string            // only series with this name are selected
	MatchLabels map[string]string // all must be present with equal values
	WindowMs    int64             // aggregation window ending at evaluation time
	Aggregation string            // sum|avg|min|max|count|last
	Operator    Operator
	Threshold   float64
	ForMs       int64 // condition must hold continuously for this long before firing
}

// Matches reports whether the rule selects the given named series.
func (r *Rule) Matches(name string, labels map[string]string) bool {
	if r.MetricName != name {
		return false
	}
	for k, v := range r.MatchLabels {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// Compare evaluates op(value, threshold).
func Compare(op Operator, value, threshold float64) bool {
	switch op {
	case OpGT:
		return value > threshold
	case OpGE:
		return value >= threshold
	case OpLT:
		return value < threshold
	case OpLE:
		return value <= threshold
	case OpEQ:
		return value == threshold
	default:
		return false
	}
}

// aggregateValue extracts the requested statistic from a window aggregate.
func aggregateValue(a model.WindowAggregate, kind string) (float64, error) {
	switch kind {
	case "sum":
		return a.Sum, nil
	case "avg":
		return a.Avg(), nil
	case "min":
		return a.Min, nil
	case "max":
		return a.Max, nil
	case "count":
		return float64(a.Count), nil
	case "last":
		return a.Last, nil
	default:
		return 0, fmt.Errorf("unknown aggregation %q", kind)
	}
}

// SeriesQuerier is the storage subset the engine depends on.
type SeriesQuerier interface {
	QueryRange(series model.SeriesID, startMs, endMs, stepMs int64) ([]model.WindowAggregate, error)
	OldestWindowStart(series model.SeriesID) (int64, bool)
	WindowSize() int64
}
