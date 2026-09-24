package model

import (
	"sort"
	"strconv"
	"strings"
)

// fingerprint builds an order-independent identity for a label set.
// Format: k1=v1|k2=v2 with keys sorted and bytes escaped.
func fingerprint(ls Labels) string {
	keys := make([]string, 0, len(ls))
	for k := range ls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(escape(k))
		b.WriteByte('=')
		b.WriteString(escape(ls[k]))
	}
	return b.String()
}

func escape(s string) string {
	if !strings.ContainsAny(s, "|=\\") {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '|', '=', '\\':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// FormatValue renders a float deterministically for logs/explanations.
func FormatValue(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
