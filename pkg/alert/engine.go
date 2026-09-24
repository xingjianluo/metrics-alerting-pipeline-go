package alert

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/storage"
)

// State is the alert state-machine state.
type State string

const (
	// StateResolved: not alerting and not waiting (initial state).
	StateResolved State = "resolved"
	// StatePending: threshold condition is true but the For duration has not
	// elapsed yet.
	StatePending State = "pending"
	// StateFiring: condition held continuously long enough; alert is active.
	StateFiring State = "firing"
)

// Transition records one state-machine change with human-readable evidence so
// every alert decision can be explained after the fact.
type Transition struct {
	RuleID    string
	Series    model.SeriesID
	AtMs      int64 // evaluation tick (window end)
	From      State
	To        State
	Reason    string
	Condition bool
	Value     float64
}

// stateKey identifies one tracked alert instance.
type stateKey struct {
	ruleID string
	series model.SeriesID
}

// alertState is the mutable per-instance state-machine memory.
type alertState struct {
	state     State
	trueSince int64 // start of the first window in the current true run
	value     float64
	cond      bool
}

// Engine evaluates rules against a querier and tracks per (rule,series) state.
// All methods are safe for concurrent use; evaluation ticks are serialized so
// concurrent callers can never interleave state updates.
type Engine struct {
	mu      sync.Mutex
	querier SeriesQuerier
	now     func() int64
	rules   map[string]Rule
	states  map[stateKey]*alertState
	log     []Transition
}

// NewEngine builds an engine with the given clock.
func NewEngine(q SeriesQuerier, now func() int64) *Engine {
	if q == nil || now == nil {
		panic("alert: querier and clock are required")
	}
	return &Engine{
		querier: q,
		now:     now,
		rules:   make(map[string]Rule),
		states:  make(map[stateKey]*alertState),
	}
}

// SetRules atomically replaces the rule set. State is keyed by rule ID, so a
// rule that survives the replacement keeps its state (no duplicate firing).
// Alerts belonging to removed rules are explicitly resolved (never silently
// dropped). Invalid rules make the whole replacement fail atomically.
func (e *Engine) SetRules(rules []Rule) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	next := make(map[string]Rule, len(rules))
	for _, r := range rules {
		if r.ID == "" {
			return errors.New("alert: rule id is required")
		}
		if _, dup := next[r.ID]; dup {
			return fmt.Errorf("alert: duplicate rule id %q", r.ID)
		}
		if r.WindowMs <= 0 {
			return fmt.Errorf("alert: rule %q has non-positive window", r.ID)
		}
		if r.WindowMs%e.querier.WindowSize() != 0 {
			return fmt.Errorf("alert: rule %q window %d must be a multiple of storage window %d",
				r.ID, r.WindowMs, e.querier.WindowSize())
		}
		if _, err := aggregateValue(model.WindowAggregate{}, r.Aggregation); err != nil {
			return fmt.Errorf("alert: rule %q: %w", r.ID, err)
		}
		switch r.Operator {
		case OpGT, OpGE, OpLT, OpLE, OpEQ:
		default:
			return fmt.Errorf("alert: rule %q has unknown operator %q", r.ID, r.Operator)
		}
		next[r.ID] = r
	}

	// Close alerts whose rules disappeared, recording the reason explicitly.
	tick := e.now()
	for k, st := range e.states {
		if _, keep := next[k.ruleID]; keep {
			continue
		}
		if st.state != StateResolved {
			e.log = append(e.log, Transition{
				RuleID: k.ruleID, Series: k.series, AtMs: tick,
				From: st.state, To: StateResolved,
				Reason: "rule removed during hot reload; active alert explicitly resolved",
			})
		}
		delete(e.states, k)
	}
	e.rules = next
	return nil
}

// Rules returns a snapshot copy of the current rule set.
func (e *Engine) Rules() []Rule {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Rule, 0, len(e.rules))
	for _, r := range e.rules {
		out = append(out, r)
	}
	return out
}

// EvaluateOnce runs one evaluation tick for one named series against every
// currently matching rule. It returns the transitions produced by this tick
// (empty slice when no state changed).
func (e *Engine) EvaluateOnce(name string, labels map[string]string) ([]Transition, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	series := model.SeriesIDOf(name, labels)
	now := e.now()
	var produced []Transition

	for _, r := range e.rules {
		if !r.Matches(name, labels) {
			continue
		}

		// Evaluate the most recent complete rule window ending at this tick.
		base := e.querier.WindowSize()
		end := model.AlignWindow(now, base)
		start := end - r.WindowMs
		aggs, err := e.querier.QueryRange(series, start, end, r.WindowMs)
		if err != nil {
			if errors.Is(err, storage.ErrPartialData) {
				// Missing/evicted data is "unknown", never "recovered": keep
				// state untouched so alerts are neither lost nor duplicated.
				continue
			}
			return nil, fmt.Errorf("rule %s query: %w", r.ID, err)
		}
		if len(aggs) == 0 || aggs[0].Count == 0 {
			// No observations in the window: inconclusive as well.
			continue
		}
		agg := aggs[0]
		value, err := aggregateValue(agg, r.Aggregation)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", r.ID, err)
		}
		cond := Compare(r.Operator, value, r.Threshold)

		key := stateKey{ruleID: r.ID, series: series}
		st, ok := e.states[key]
		if !ok {
			st = &alertState{state: StateResolved}
			e.states[key] = st
		}
		st.value, st.cond = value, cond

		ev := fmt.Sprintf("rule=%s series=%s window=[%d,%d) %s=%v cond=%v (%v %s %v)",
			r.ID, series, agg.WindowStart, agg.WindowEnd, r.Aggregation, value, cond,
			value, r.Operator, r.Threshold)

		switch st.state {
		case StateResolved:
			if cond {
				if r.ForMs <= 0 {
					st.state = StateFiring
					st.trueSince = agg.WindowStart
					produced = append(produced, e.record(key, st, now, StateResolved, StateFiring,
						ev+"; firing immediately (no for-duration)"))
				} else {
					st.state = StatePending
					st.trueSince = agg.WindowStart
					produced = append(produced, e.record(key, st, now, StateResolved, StatePending,
						fmt.Sprintf("%s; waiting for condition to hold %dms", ev, r.ForMs)))
				}
			}
		case StatePending:
			if cond {
				heldFor := agg.WindowEnd - st.trueSince
				if heldFor >= r.ForMs {
					st.state = StateFiring
					produced = append(produced, e.record(key, st, now, StatePending, StateFiring,
						fmt.Sprintf("%s; held for %dms >= for=%dms", ev, heldFor, r.ForMs)))
				}
			} else {
				st.state = StateResolved
				st.trueSince = 0
				produced = append(produced, e.record(key, st, now, StatePending, StateResolved,
					ev+"; condition interrupted before for-duration elapsed"))
			}
		case StateFiring:
			if !cond {
				st.state = StateResolved
				st.trueSince = 0
				produced = append(produced, e.record(key, st, now, StateFiring, StateResolved,
					ev+"; condition cleared, alert resolved"))
			}
		}
	}
	return produced, nil
}

// record appends a transition to the durable log and returns it.
func (e *Engine) record(k stateKey, st *alertState, at int64, from, to State, reason string) Transition {
	t := Transition{
		RuleID: k.ruleID, Series: k.series, AtMs: at,
		From: from, To: to, Reason: reason,
		Condition: st.cond, Value: st.value,
	}
	e.log = append(e.log, t)
	return t
}

// StateOf returns the current state of one rule/series pair.
func (e *Engine) StateOf(ruleID string, series model.SeriesID) State {
	e.mu.Lock()
	defer e.mu.Unlock()
	if st, ok := e.states[stateKey{ruleID: ruleID, series: series}]; ok {
		return st.state
	}
	return StateResolved
}

// Snapshot returns a point-in-time copy of all non-resolved alert states,
// keyed by "ruleID|series".
func (e *Engine) Snapshot() map[string]State {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]State)
	for k, st := range e.states {
		if st.state != StateResolved {
			out[fmt.Sprintf("%s|%s", k.ruleID, k.series)] = st.state
		}
	}
	return out
}

// Transitions returns a copy of the accumulated, ordered transition log.
func (e *Engine) Transitions() []Transition {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Transition, len(e.log))
	copy(out, e.log)
	return out
}
