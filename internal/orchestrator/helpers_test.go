package orchestrator

import (
	"strings"
	"time"
)

func timeSleep10() { time.Sleep(10 * time.Millisecond) }
func timeSleep50() { time.Sleep(50 * time.Millisecond) }

func firstNLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[:n], "\n") + "\n...(truncated)"
}
