package pipeline

import "github.com/xingjianluo/metrics-alerting-pipeline-go/model"

// ScriptedScenario returns a deterministic sample script, its initial alert
// rules and the base window size. The script deliberately mixes:
//   - normal arrivals,
//   - out-of-order arrivals (older Ts delivered after newer ones),
//   - duplicate reports (same SourceID+DedupKey, possibly different values;
//     duplicates are ignored so the differing value never corrupts sums),
//   - a late arrival near the retention boundary,
//   - two hosts so selector behavior is observable.
func ScriptedScenario() (samples []model.Sample, rules []model.Rule, windowSeconds int64) {
	const W = int64(10)
	mk := func(host string, ts int64, v float64, key string) model.Sample {
		return model.Sample{
			Labels:   model.Labels{"__name__": "http_requests", "host": host},
			Ts:       ts,
			Value:    v,
			SourceID: "agent-" + host,
			DedupKey: key,
		}
	}

	samples = []model.Sample{
		// window [0,10) host=a
		mk("a", 1, 1, "a-0-1"),
		mk("a", 2, 2, "a-0-2"),
		// out of order: ts=0 arrives after ts=2, still window [0,10)
		mk("a", 0, 1, "a-0-0"),
		// duplicate of a-0-2 with a DIFFERENT value: must be ignored
		mk("a", 2, 99, "a-0-2"),

		// window [10,20) host=a — 4 samples -> count 4 (>= threshold)
		mk("a", 12, 1, "a-10-0"),
		mk("a", 15, 1, "a-10-1"),
		mk("a", 18, 1, "a-10-2"),
		mk("a", 11, 1, "a-10-3"), // out of order within the window

		// host=b stays calm
		mk("b", 11, 1, "b-10-0"),
		mk("b", 12, 1, "b-10-1"),

		// window [20,30) host=a drops back to 1 sample (recovery)
		mk("a", 25, 1, "a-20-0"),
		// repeat delivered much later (retry after network blip)
		mk("a", 15, 1, "a-10-1"),

		// window [30,40) host=a quiet
		mk("a", 31, 1, "a-30-0"),
	}

	rules = []model.Rule{{
		ID:                "high_request_rate",
		Selector:          model.Labels{"__name__": "http_requests", "host": "a"},
		WindowSeconds:     W,
		AggField:          "count",
		Op:                model.CmpGTE,
		Threshold:         4,
		PendingWindows:    1, // fire on the 2nd consecutive violating window
		KeepFiringWindows: 1, // resolve on the 2nd consecutive recovery window
	}}
	return samples, rules, W
}
