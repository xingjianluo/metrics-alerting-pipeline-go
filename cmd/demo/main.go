// Command demo runs a small in-process end-to-end scenario of the metrics
// aggregation and alerting pipeline against a controllable virtual clock:
//
//  1. out-of-order / duplicate writes with distinct reject reasons,
//  2. windowed reads at two granularities that stay self-consistent,
//  3. a threshold rule traversing pending -> firing -> resolved,
//  4. a hot rule reload that preserves alert state.
package main

import (
	"errors"
	"fmt"

	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/alert"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/model"
	"github.com/xingjianluo/metrics-alerting-pipeline-go/pkg/storage"
)

func main() {
	const win int64 = 1_000
	now := int64(2_000)
	store := storage.New(win, 60*win, func() int64 { return now })
	engine := alert.NewEngine(store, func() int64 { return now })

	labels := map[string]string{"host": "h1", "iface": "eth0"}
	series := model.SeriesIDOf("net_rx_errors", labels)

	rule := alert.Rule{
		ID: "rx-errors-high", MetricName: "net_rx_errors",
		WindowMs: win, Aggregation: "max",
		Operator: alert.OpGE, Threshold: 8, ForMs: 1_000,
	}
	if err := engine.SetRules([]alert.Rule{rule}); err != nil {
		panic(err)
	}

	write := func(ts int64, v float64) {
		res, err := store.Write(model.Sample{
			Name: "net_rx_errors", Labels: labels, Timestamp: ts, Value: v,
		})
		switch {
		case err == nil:
			fmt.Printf("  write ts=%-5d v=%-3.0f -> accepted into window %d\n", ts, v, res.Window)
		default:
			var we *model.WriteError
			if errors.As(err, &we) {
				fmt.Printf("  write ts=%-5d v=%-3.0f -> REJECTED reason=%s\n", ts, v, we.Reason)
			} else {
				fmt.Printf("  write ts=%-5d v=%-3.0f -> error: %v\n", ts, v, err)
			}
		}
	}

	eval := func() {
		trs, err := engine.EvaluateOnce("net_rx_errors", labels)
		if err != nil {
			panic(err)
		}
		for _, tr := range trs {
			fmt.Printf("  ALERT %s -> %s  [%s]\n", tr.From, tr.To, tr.Reason)
		}
	}

	query := func(step int64) {
		aggs, err := store.QueryRange(series, 1_000, 5_000, step)
		if err != nil {
			fmt.Printf("  query(step=%d) error: %v\n", step, err)
			return
		}
		for _, a := range aggs {
			fmt.Printf("  window [%d,%d) count=%d sum=%.0f min=%.0f max=%.0f last=%.0f avg=%.2f\n",
				a.WindowStart, a.WindowEnd, a.Count, a.Sum, a.Min, a.Max, a.Last, a.Avg())
		}
	}

	fmt.Println("== 1. out-of-order and duplicate writes ==")
	now = 2_000
	write(1_200, 3) // window 1000
	write(1_800, 9) // window 1000, arrives after 1200 (in-order here)
	write(1_100, 2) // LATE arrival into window 1000
	write(1_200, 3) // exact duplicate
	write(1_200, 4) // same timestamp, different value
	write(2_200, 5) // window 2000

	fmt.Println("== 2. reads at 1s and 2s granularity (same data) ==")
	fmt.Println("  step=1000:")
	query(1_000)
	fmt.Println("  step=2000:")
	query(2_000)

	fmt.Println("== 3. alert lifecycle (>=8 for 1000ms) ==")
	// tick 2000: window [1000,2000) max=9 -> pending
	fmt.Printf("tick %d:\n", now)
	eval()
	now = 3_000
	write(2_900, 9) // window 2000 still breaching
	fmt.Printf("tick %d:\n", now)
	eval() // -> firing (held >= for)
	now = 4_000
	write(3_900, 1) // recovered
	fmt.Printf("tick %d:\n", now)
	eval() // -> resolved

	fmt.Println("== 4. hot reload while firing, state preserved ==")
	now = 5_000
	write(4_900, 12)
	fmt.Printf("tick %d (threshold still 8):\n", now)
	eval() // pending again
	rule.Threshold = 10
	if err := engine.SetRules([]alert.Rule{rule}); err != nil {
		panic(err)
	}
	fmt.Printf("reloaded threshold=10; current state of %s at tick %d: %s\n",
		rule.ID, now, engine.StateOf(rule.ID, series))
	now = 6_000
	write(5_900, 12)
	fmt.Printf("tick %d under new threshold (12>=10, still holds):\n", now)
	eval() // pending -> firing, single transition despite reload

	fmt.Println("\nfull transition log:")
	for _, tr := range engine.Transitions() {
		fmt.Printf("  @%d %s: %s -> %s | %s\n", tr.AtMs, tr.RuleID, tr.From, tr.To, tr.Reason)
	}
}
