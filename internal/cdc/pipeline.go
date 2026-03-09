package cdc

import (
	"context"
	"data_cdc/internal/oltp"
	"data_cdc/internal/warehouse"
	"fmt"
	"time"
)

// Pipeline periodically pulls change events and loads them into the warehouse.
type Pipeline struct {
	Source        *oltp.MockOLTP
	Target        *warehouse.MockWarehouse
	FlushInterval time.Duration
	LastSeq       int64
}

func (p *Pipeline) Run(ctx context.Context) {
	var batch []oltp.Change
	const maxBatchSize = 100
	changeCh := p.Source.Subscribe(128)

	// Startup backfill: load any WAL entries written before the listener began processing.
	backfill := p.Source.ChangesSince(p.LastSeq)
	if len(backfill) > 0 {
		fmt.Printf("Startup Backfill: found %d changes since last seq=%d\n", len(backfill), p.LastSeq)
		p.flush(backfill)
	}

	ticker := time.NewTicker(p.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				p.flush(batch)
			}
			return

		case c := <-changeCh:
			if c.Seq <= p.LastSeq {
				continue
			}
			batch = append(batch, c)

			// Trigger 1: Buffer is full.
			if len(batch) >= maxBatchSize {
				p.flush(batch)
				batch = nil
			}

		case <-ticker.C:
			// Trigger 2: Time-based flush (even when below max batch size).
			if len(batch) > 0 {
				p.flush(batch)
				batch = nil
			}
		}
	}
}

// Helper method to keep the Run loop clean
func (p *Pipeline) flush(batch []oltp.Change) {
	loaded := p.Target.Apply(batch)
	// Update watermark to the last item in the successful batch.
	p.LastSeq = batch[len(batch)-1].Seq
	fmt.Printf("Batch Load: items=%d, watermark=%d\n", loaded, p.LastSeq)
}
