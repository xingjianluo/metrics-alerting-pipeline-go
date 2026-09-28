package change

import (
	"math"
	"sort"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/model"
)

// validAggFields is the set of legal Rule.AggField values (mirrors the
// evaluator's aggregateValue switch).
var validAggFields = map[string]bool{
	"count": true, "sum": true, "avg": true,
	"min": true, "max": true, "latest": true,
}

// validComparators is the set of legal Rule.Op values.
var validComparators = map[model.Comparator]bool{
	model.CmpGT: true, model.CmpGTE: true, model.CmpLT: true,
	model.CmpLTE: true, model.CmpEQ: true, model.CmpNEQ: true,
}

// op constants for batch items.
const (
	OpUpsert = "upsert"
	OpDelete = "delete"
)

// validateRule validates the key configuration of one upserted rule. The id
// argument identifies the entry even when Rule.ID is empty.
func validateRule(id string, r model.Rule) []ValidationError {
	var errs []ValidationError
	add := func(field, detail string) {
		errs = append(errs, ValidationError{RuleID: id, Field: field, Detail: detail})
	}
	if r.ID == "" {
		add("ID", "rule ID is required")
	} else if r.ID != id {
		// Defensive: Item identity and Rule identity must agree.
		add("ID", "item key does not match Rule.ID")
	}
	if r.WindowSeconds <= 0 {
		add("WindowSeconds", "must be > 0")
	}
	if !validAggFields[r.AggField] {
		add("AggField", "must be one of count|sum|avg|min|max|latest")
	}
	if !validComparators[r.Op] {
		add("Op", "must be one of > >= < <= == !=")
	}
	if math.IsNaN(r.Threshold) || math.IsInf(r.Threshold, 0) {
		add("Threshold", "must be a finite number")
	}
	if r.PendingWindows < 0 {
		add("PendingWindows", "must be >= 0")
	}
	if r.KeepFiringWindows < 0 {
		add("KeepFiringWindows", "must be >= 0")
	}
	return errs
}

// validateBatch performs the batch-wide checks independent of the current
// ruleset: unknown op, duplicate rule names inside the batch and per-rule
// configuration validity. Errors are returned sorted (ruleID, field) so the
// full problem list is stable and reviewable in one pass.
func validateBatch(items []Item) []ValidationError {
	var errs []ValidationError
	seen := map[string]int{}

	for idx, it := range items {
		id := it.Rule.ID
		// Duplicate rule name anywhere in the batch is illegal, including a
		// delete + upsert mix: the intent of the batch must be unambiguous.
		if id != "" {
			if first, ok := seen[id]; ok {
				errs = append(errs, ValidationError{
					RuleID: id, Field: "ID",
					Detail: "duplicate rule name in batch (also appears at entry " + itoa(first) + ")",
				})
			} else {
				seen[id] = idx
			}
		}
		switch it.Op {
		case OpUpsert:
			errs = append(errs, validateRule(id, it.Rule)...)
		case OpDelete:
			if id == "" {
				errs = append(errs, ValidationError{RuleID: id, Field: "ID", Detail: "delete requires a rule ID"})
			}
		default:
			errs = append(errs, ValidationError{
				RuleID: id, Field: "Op",
				Detail: `op must be "upsert" or "delete"`,
			})
		}
	}

	sort.SliceStable(errs, func(i, j int) bool {
		if errs[i].RuleID != errs[j].RuleID {
			return errs[i].RuleID < errs[j].RuleID
		}
		return errs[i].Field < errs[j].Field
	})
	return errs
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
