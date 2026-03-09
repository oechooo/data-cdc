package main

import (
	"context"
	"data_cdc/internal/cdc"
	"data_cdc/internal/oltp"
	"data_cdc/internal/warehouse"
	"fmt"
	"time"
)

func main() {
	source := oltp.NewMockOLTP()

	target := warehouse.NewMockWarehouse()
	pipeline := &cdc.Pipeline{
		Source:        source,
		Target:        target,
		FlushInterval: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pipeline.Run(ctx)

	backfilledChanges := source.SimulateUpserts(10) // likely to be backfilled
	time.Sleep(200 * time.Millisecond)
	streamedChanges := source.SimulateUpserts(5) // likely to be streamed in real-time
	time.Sleep(200 * time.Millisecond)
	cancel()

	fmt.Printf("Simulated %d upserts\n", len(backfilledChanges) + len(streamedChanges))

	fmt.Println("\nWarehouse snapshot:")
	for _, row := range target.Snapshot() {
		fmt.Printf("order=%s customer=%s status=%s amount=%.2f source_seq=%d ingested=%s\n",
			row.OrderID,
			row.CustomerID,
			row.Status,
			row.Amount,
			row.SourceSeq,
			row.IngestedAt.Format(time.RFC3339),
		)
	}

	fmt.Printf("\nFinal warehouse watermark: %d\n", target.LastLoadSeq())
}
