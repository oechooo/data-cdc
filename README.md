# Mock CDC in Go

Simple project to simulate Change Data Capture (CDC) from a mock OLTP system into a mock data warehouse.

## What this does

- Creates and updates mock OLTP `orders`, with changes attribute as the mock Write-Ahead Log
- Sets up CDC pipeline to stream WAL updates, loading changes into a mock data warehouse

Example Output:
```
Startup Backfill: found 10 changes since last seq=0
Batch Load: items=10, watermark=10
Batch Load: items=5, watermark=15
Simulated 15 upserts

Warehouse snapshot:
order=ORD-SIM-001 customer=CUST-04 status=SHIPPED amount=29.15 source_seq=11 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-002 customer=CUST-08 status=NEW amount=95.95 source_seq=12 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-003 customer=CUST-08 status=SHIPPED amount=306.10 source_seq=13 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-004 customer=CUST-02 status=NEW amount=266.78 source_seq=14 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-005 customer=CUST-04 status=PAID amount=107.52 source_seq=15 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-006 customer=CUST-07 status=NEW amount=150.31 source_seq=6 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-007 customer=CUST-08 status=PAID amount=241.61 source_seq=7 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-008 customer=CUST-06 status=NEW amount=190.11 source_seq=8 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-009 customer=CUST-01 status=NEW amount=134.46 source_seq=9 ingested=2026-03-09T13:08:28Z
order=ORD-SIM-010 customer=CUST-05 status=CANCELLED amount=181.41 source_seq=10 ingested=2026-03-09T13:08:28Z
```

## Project layout

- `cmd/mockcdc/main.go`: runnable demo
- `internal/oltp`: mock OLTP data + change-log generator
- `internal/cdc`: polling CDC pipeline
- `internal/warehouse`: mock warehouse loader

## Run

```bash
go run ./cmd/mockcdc
```
