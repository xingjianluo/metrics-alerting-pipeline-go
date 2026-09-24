package model

import "testing"

func TestSeriesIDOfOrderIndependent(t *testing.T) {
	a := SeriesIDOf("m", map[string]string{"b": "2", "a": "1"})
	b := SeriesIDOf("m", map[string]string{"a": "1", "b": "2"})
	if a != b {
		t.Fatalf("label order changed identity: %q vs %q", a, b)
	}
	if c := SeriesIDOf("m", map[string]string{"a": "1"}); c == a {
		t.Fatal("different label sets must have different identities")
	}
	if SeriesIDOf("m", nil) != SeriesID("m") {
		t.Fatal("unlabelled series id should be the bare name")
	}
	t.Logf("[verdict] canonical series id=%q (label-order independent)", a)
}

func TestAlignWindow(t *testing.T) {
	cases := []struct{ ts, size, want int64 }{
		{0, 1000, 0}, {999, 1000, 0}, {1000, 1000, 1000}, {1500, 1000, 1000},
		{-1, 1000, -1000}, {-1000, 1000, -1000}, {-1001, 1000, -2000},
	}
	for _, c := range cases {
		if got := AlignWindow(c.ts, c.size); got != c.want {
			t.Fatalf("AlignWindow(%d,%d)=%d want %d", c.ts, c.size, got, c.want)
		}
		t.Logf("[verdict] AlignWindow(ts=%d,size=%d)=%d", c.ts, c.size, c.want)
	}
}

func TestAvgEmptyWindow(t *testing.T) {
	var a WindowAggregate
	if a.Avg() != 0 {
		t.Fatal("empty window avg must be 0")
	}
}
