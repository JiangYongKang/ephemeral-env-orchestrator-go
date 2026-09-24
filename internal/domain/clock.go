package domain

import "time"

// Clock abstracts time acquisition so expiry/retry logic is testable.
type Clock interface {
	Now() time.Time
}

// SystemClock uses wall-clock time.
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// FakeClock is a manually controlled clock for tests.
type FakeClock struct{ T time.Time }

// Now returns the fixed/advanced fake time.
func (f *FakeClock) Now() time.Time { return f.T }

// Advance moves the fake clock forward.
func (f *FakeClock) Advance(d time.Duration) { f.T = f.T.Add(d) }
