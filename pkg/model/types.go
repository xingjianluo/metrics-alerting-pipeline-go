// Package model defines the core data model of the metrics pipeline:
// raw samples, windowed aggregates and write rejection reasons.
package model

import (
	"fmt"
	"sort"
	"strings"
)

// Sample is a single metric data point reported by a collector.
type Sample struct {
	Name      string            // metric name, e.g. "http_requests"
	Labels    map[string]string // identifying labels, e.g. {"path":"/a"}
	Value     float64           // measured value
	Timestamp int64             // sample time, unix milliseconds
}

// SeriesID is the canonical, label-order-independent identity of one series.
type SeriesID string

// RejectReason explains why a write was rejected. Reasons are stable strings
// so callers can distinguish failure modes programmatically.
type RejectReason string

const (
	ReasonNone RejectReason = ""
	// ReasonTooOld: the sample belongs to a window already past the retention
	// horizon and can no longer be incorporated.
	ReasonTooOld RejectReason = "sample_too_old"
	// ReasonFuture: the sample timestamp lies in the future beyond clock skew.
	ReasonFuture RejectReason = "sample_from_future"
	// ReasonDuplicate: the exact same (series,timestamp,value) was already seen.
	ReasonDuplicate RejectReason = "duplicate_sample"
	// ReasonDuplicateMismatch: same (series,timestamp) but a different value.
	ReasonDuplicateMismatch RejectReason = "duplicate_timestamp_value_mismatch"
)

// WriteError carries a machine-distinguishable reject reason.
type WriteError struct {
	Reason RejectReason
	Msg    string
}

func (e *WriteError) Error() string { return string(e.Reason) + ": " + e.Msg }

// WindowAggregate is the aggregation of one series inside one aligned window
// [WindowStart, WindowEnd).
type WindowAggregate struct {
	Series      SeriesID
	WindowStart int64 // inclusive, unix milliseconds
	WindowEnd   int64 // exclusive, unix milliseconds
	Count       int64
	Sum         float64
	Min         float64
	Max         float64
	Last        float64
}

// Avg returns the arithmetic mean, or 0 for an empty window.
func (a *WindowAggregate) Avg() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Sum / float64(a.Count)
}

// SeriesIDOf builds the canonical id of a (name, labels) pair. Labels are
// sorted so that semantically identical label maps always map to the same id.
func SeriesIDOf(name string, labels map[string]string) SeriesID {
	if len(labels) == 0 {
		return SeriesID(name)
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	b.WriteByte('}')
	return SeriesID(b.String())
}

// AlignWindow returns the start of the fixed-size window containing ts.
// Windows are epoch-aligned: [k*size, (k+1)*size).
func AlignWindow(ts, size int64) int64 {
	if ts >= 0 {
		return ts - ts%size
	}
	// floor division for negative timestamps
	return ts - (ts%size+size)%size
}
