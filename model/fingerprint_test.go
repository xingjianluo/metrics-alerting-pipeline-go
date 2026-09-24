package model

import "testing"

func TestFingerprintStableAndOrderIndependent(t *testing.T) {
	a := Labels{"host": "a", "__name__": "m", "path": "/x"}
	b := Labels{"path": "/x", "host": "a", "__name__": "m"}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("fingerprint must be order-independent: %q vs %q", a.Fingerprint(), b.Fingerprint())
	}
	c := Labels{"host": "b", "__name__": "m"}
	if a.Fingerprint() == c.Fingerprint() {
		t.Fatal("distinct label sets must have distinct fingerprints")
	}
	t.Logf("JUDGE fingerprint(m,a,/x)=%q fingerprint(m,b)=%q", a.Fingerprint(), c.Fingerprint())
}

func TestFingerprintEscaping(t *testing.T) {
	// Bytes | = \ are escaped so distinct maps cannot collide.
	x := Labels{"k": "a|b=c"}
	y := Labels{"k|a": "b=c"}
	if x.Fingerprint() == y.Fingerprint() {
		t.Fatalf("escaping must prevent collision: both %q", x.Fingerprint())
	}
	t.Logf("JUDGE escaped fingerprints differ: %q vs %q", x.Fingerprint(), y.Fingerprint())
}

func TestLabelMatch(t *testing.T) {
	ls := Labels{"__name__": "m", "host": "a", "zone": "z1"}
	cases := []struct {
		want Labels
		ok   bool
	}{
		{nil, true},
		{Labels{}, true},
		{Labels{"host": "a"}, true},
		{Labels{"host": "a", "zone": "z1"}, true},
		{Labels{"host": "b"}, false},
		{Labels{"missing": "x"}, false},
	}
	for _, c := range cases {
		got := ls.Match(c.want)
		t.Logf("JUDGE %v matches %v -> %v", ls, c.want, got)
		if got != c.ok {
			t.Fatalf("Match(%v)=%v want %v", c.want, got, c.ok)
		}
	}
}
