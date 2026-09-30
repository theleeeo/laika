package core

// RegisterOption tunes how RegisterChanges (and RegisterChange) submits the
// builds and deletes it schedules.
type RegisterOption func(*registerOptions)

type registerOptions struct {
	waitForSlot bool
}

// WaitForSlot makes RegisterChanges' own submissions — each accepted item's
// build or delete and its affected Parents' builds — wait while
// the build queue is at or above its high-water mark, instead of shedding
// them to the sweep. It is backpressure for in-process pull producers
// (pollers, CDC watchers) that would otherwise outrun the pool. Push
// producers behind an RPC never wait: a spike sheds to the sweep, which
// coalesces repeated changes into one build (ADR 0008).
//
// The stale marks are still written before any wait, so the registration is
// durable once RegisterChanges returns either way. A wait ended by ctx or by
// pool shutdown leaves the build to the sweep, as a shed one is, and
// RegisterChanges still returns its statuses and a nil error. Cascades submitted from inside running
// builds never wait.
func WaitForSlot() RegisterOption {
	return func(o *registerOptions) { o.waitForSlot = true }
}

func newRegisterOptions(opts []RegisterOption) registerOptions {
	var o registerOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
