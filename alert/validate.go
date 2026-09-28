// Rule validation for batch change management. A batch of rules is applied
// only when every rule in it is individually valid AND the batch as a whole
// is consistent (no duplicate rule IDs): "one bad rule rejects the batch".
package alert

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// ValidationError explains one problem found in one proposed rule.
type ValidationError struct {
	RuleID string
	Field  string // empty for batch-level errors that are not rule-specific
	Detail string
}

// Error implements error.
func (e *ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("rule %q: %s", e.RuleID, e.Detail)
	}
	return fmt.Sprintf("rule %q field %s: %s", e.RuleID, e.Field, e.Detail)
}

// ValidationErrors aggregates every problem found in one validation pass, so
// a dry-run can list the whole batch's issues at once instead of one at a time.
type ValidationErrors struct {
	Errs []ValidationError
}

// Error implements error.
func (ve *ValidationErrors) Error() string {
	if ve == nil || len(ve.Errs) == 0 {
		return "validation passed"
	}
	parts := make([]string, 0, len(ve.Errs))
	for _, e := range ve.Errs {
		parts = append(parts, e.Error())
	}
	return fmt.Sprintf("batch invalid (%d problem(s)): %s", len(ve.Errs), strings.Join(parts, "; "))
}

// validAggFields is the set of aggregate fields evaluation can compare.
var validAggFields = map[string]bool{
	"count": true, "sum": true, "avg": true,
	"min": true, "max": true, "latest": true,
}

// validateRule checks the critical configuration of one rule: identity,
// window size, aggregation field, comparator and threshold. Several problems
// on the same rule are all reported at once.
func validateRule(r model.Rule) []ValidationError {
	var errs []ValidationError
	add := func(field, detail string) {
		errs = append(errs, ValidationError{RuleID: r.ID, Field: field, Detail: detail})
	}
	if strings.TrimSpace(r.ID) == "" {
		add("ID", "rule ID must not be empty")
	}
	if r.WindowSeconds <= 0 {
		add("WindowSeconds", fmt.Sprintf("must be > 0, got %d", r.WindowSeconds))
	}
	if !validAggFields[r.AggField] {
		add("AggField", fmt.Sprintf("must be one of count|sum|avg|min|max|latest, got %q", r.AggField))
	}
	switch r.Op {
	case model.CmpGT, model.CmpGTE, model.CmpLT, model.CmpLTE, model.CmpEQ, model.CmpNEQ:
	default:
		add("Op", fmt.Sprintf("unknown comparator %q", r.Op))
	}
	if math.IsNaN(r.Threshold) {
		add("Threshold", "must not be NaN")
	}
	if math.IsInf(r.Threshold, 0) {
		add("Threshold", "must not be +/-Inf")
	}
	if r.PendingWindows < 0 {
		add("PendingWindows", fmt.Sprintf("must be >= 0, got %d", r.PendingWindows))
	}
	if r.KeepFiringWindows < 0 {
		add("KeepFiringWindows", fmt.Sprintf("must be >= 0, got %d", r.KeepFiringWindows))
	}
	return errs
}

// validateBatch validates every rule and checks batch-level invariants
// (duplicate rule IDs). All problems are returned at once, ordered
// deterministically: batch-level (duplicate IDs, in input order) first, then
// per-rule problems sorted by rule ID.
func validateBatch(rules []model.Rule) *ValidationErrors {
	var batch []ValidationError
	positions := map[string][]int{}
	for i, r := range rules {
		if r.ID == "" {
			// Empty IDs are reported as a field error below; skip duplicate
			// bookkeeping for them to avoid a confusing second message.
			continue
		}
		positions[r.ID] = append(positions[r.ID], i)
	}
	dupIDs := make([]string, 0, len(positions))
	for id, ps := range positions {
		if len(ps) > 1 {
			dupIDs = append(dupIDs, id)
		}
	}
	sort.Strings(dupIDs)
	for _, id := range dupIDs {
		ps := positions[id]
		batch = append(batch, ValidationError{
			RuleID: id,
			Detail: fmt.Sprintf("duplicate rule name in batch at positions %v", ps),
		})
	}

	type perRule struct {
		id   string
		errs []ValidationError
	}
	var per []perRule
	for _, r := range rules {
		if es := validateRule(r); len(es) > 0 {
			per = append(per, perRule{r.ID, es})
		}
	}
	sort.SliceStable(per, func(i, j int) bool { return per[i].id < per[j].id })

	out := &ValidationErrors{}
	out.Errs = append(out.Errs, batch...)
	for _, p := range per {
		out.Errs = append(out.Errs, p.errs...)
	}
	if len(out.Errs) == 0 {
		return nil
	}
	return out
}

// ValidateRules is the check-only entry point: it never changes any rule or
// alert state. A nil result means the batch is valid and applyable.
func ValidateRules(rules []model.Rule) *ValidationErrors {
	return validateBatch(rules)
}
