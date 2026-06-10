package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// riverDrainer provides a Drain() helper for tests that waits until River
// has no pending/running/scheduled/retryable jobs.
type riverDrainer struct {
	client *river.Client[pgx.Tx]
	pool   *pgxpool.Pool
}

// Drain polls the river_job table until no non-terminal jobs remain.
// It returns when the queue is quiescent or the context is cancelled.
func (d *riverDrainer) Drain(ctx context.Context) {
	const q = `SELECT count(*) FROM river_job
	           WHERE state IN ('available','running','scheduled','retryable','pending')`

	for {
		if ctx.Err() != nil {
			return
		}
		var n int
		if err := d.pool.QueryRow(ctx, q).Scan(&n); err != nil {
			// The schema may not exist yet in some setups; treat as drained.
			panic(fmt.Errorf("river drain query: %w", err))
		}
		if n == 0 {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
}
