// Package model defines the core data model of the metrics alerting pipeline:
// samples, aggregation windows, alert rules, alert states and events.
package model

// Labels is the immutable-ish tag set identifying a metric series, e.g.
// {"__name__":"http_requests","host":"a","path":"/v1/x"}.
type Labels map[string]string

// Fingerprint returns a stable string identity for the label set.
// Order-independent: labels are rendered as sorted key=value pairs.
func (ls Labels) Fingerprint() string { return fingerprint(ls) }

// Match reports whether the receiver carries every label in want.
func (ls Labels) Match(want Labels) bool {
	for k, v := range want {
		if ls[k] != v {
			return false
		}
	}
	return true
}

// Sample is one raw observation reported by a data source.
type Sample struct {
	Labels Labels // series identity
	Value  float64
	// Ts is the sample timestamp, unix seconds (int64 is deterministic in tests).
	Ts int64
	// SourceID + DedupKey uniquely identify an upstream report. The same
	// (SourceID, DedupKey) must never be counted twice, even on retries.
	SourceID string
	DedupKey string
}

// RejectReason explains why a write was rejected.
type RejectReason string

const (
	RejectNone       RejectReason = ""               // accepted (possibly de-duped)
	RejectTooOld     RejectReason = "sample_too_old" // older than retention boundary
	RejectInFuture   RejectReason = "sample_in_future"
	RejectBadWindow  RejectReason = "bad_window_alignment"
	RejectDuplicate  RejectReason = "duplicate" // informational: not counted twice
	RejectValidation RejectReason = "validation_error"
)

// WriteStatus distinguishes accept / dedup / rejection outcomes.
type WriteStatus string

const (
	WriteAccepted WriteStatus = "accepted"
	WriteDeduped  WriteStatus = "deduped"  // repeat report, not counted again
	WriteRejected WriteStatus = "rejected" // outside retention etc.
)

// WriteResult is the per-sample write outcome.
type WriteResult struct {
	Status WriteStatus
	Reason RejectReason // populated when Status == WriteRejected
	// WindowStart is the bucket the sample belongs to (even for deduped writes).
	WindowStart int64
	Detail      string
}

// Aggregate is the rolled-up value of one series over one window.
type Aggregate struct {
	Labels      Labels
	WindowStart int64 // inclusive, aligned to window size
	WindowEnd   int64 // exclusive
	Count       int64
	Sum         float64
	Min         float64
	Max         float64
	Latest      float64 // value of the sample with the largest Ts
	LatestTs    int64
}

// QueryResult is the answer to an aggregation query.
type QueryResult struct {
	Series []SeriesPoints
}

// SeriesPoints is one series' aggregates along the query time range.
type SeriesPoints struct {
	Labels Labels
	Points []Aggregate
}

// QueryGap marks a window whose data has been evicted (retention) inside an
// otherwise valid query range. Queries spanning evicted windows never silently
// return partial data: gaps are reported explicitly.
type QueryGap struct {
	WindowStart int64
	WindowEnd   int64
	Reason      RejectReason
}

// Comparator is the threshold operator of an alert rule.
type Comparator string

const (
	CmpGT  Comparator = ">"
	CmpGTE Comparator = ">="
	CmpLT  Comparator = "<"
	CmpLTE Comparator = "<="
	CmpEQ  Comparator = "=="
	CmpNEQ Comparator = "!="
)

// Rule is an alerting rule evaluated against window aggregates.
type Rule struct {
	ID string
	// Selector filters series by label subset (empty matches all).
	Selector Labels
	// WindowSeconds is the aggregation window granularity the rule reads.
	WindowSeconds int64
	// AggField selects which aggregate field is compared: count|sum|avg|min|max|latest.
	AggField  string
	Op        Comparator
	Threshold float64
	// PendingWindows: how many consecutive violating windows a series must
	// observe before the alert transitions to Firing. 0 means fire immediately.
	PendingWindows int
	// KeepFiringWindows, when > 0, auto-resolves an alert that has stopped
	// violating for that many consecutive windows.
	KeepFiringWindows int
}

// AlertStatus is the state-machine state.
type AlertStatus string

const (
	// StatusPending means the condition is violating but the pending period
	// has not elapsed.
	StatusPending AlertStatus = "pending"
	// StatusFiring means the alert is active.
	StatusFiring AlertStatus = "firing"
	// StatusResolved means the condition is not violating (initial state).
	StatusResolved AlertStatus = "resolved"
)

// AlertState is the current state of one (rule, series) pair.
type AlertState struct {
	RuleID string
	Labels Labels
	Status AlertStatus
	// Consecutive is the run length of the current violating/ok streak.
	Consecutive int
	ChangedAt   int64 // evaluation timestamp of the last transition
	UpdatedAt   int64
	// LastObserved records the value behind the latest decision (explainability).
	LastObserved float64
}

// EventType enumerates state-machine transitions.
type EventType string

const (
	EventPending  EventType = "pending"  // resolved -> pending
	EventFiring   EventType = "firing"   // pending -> firing
	EventResolved EventType = "resolved" // firing/pending -> resolved (condition recovered)
	// EventClosed is a terminal, administratively driven ending: the rule was
	// removed or its selector stopped covering the series. The tracked alert
	// is concluded explicitly with a reason, never dropped silently.
	EventClosed EventType = "closed"
)

// Event is an immutable transition record with the reason behind it.
type AlertEvent struct {
	Type      EventType
	RuleID    string
	Labels    Labels
	Ts        int64
	Observed  float64
	Threshold float64
	Op        Comparator
	Reason    string
}
