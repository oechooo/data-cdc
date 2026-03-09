package warehouse

import (
	"data_cdc/internal/oltp"
	"sort"
	"sync"
	"time"
)

// FactOrder represents a "denormalized" record in the warehouse.
type FactOrder struct {
	OrderID     string
	CustomerID  string
	Amount      float64
	Status      string
	SourceSeq   int64
	SourceTime  time.Time
	IngestedAt  time.Time
}

// MockWarehouse stores loaded records and tracks ingestion watermark.
type MockWarehouse struct {
	mu          sync.Mutex
	orders      map[string]FactOrder
	lastLoadSeq int64
}

func NewMockWarehouse() *MockWarehouse {
	return &MockWarehouse{
		orders: make(map[string]FactOrder),
	}
}

func (w *MockWarehouse) Apply(changes []oltp.Change) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, c := range changes {
		w.orders[c.Order.ID] = FactOrder{
			OrderID:    c.Order.ID,
			CustomerID: c.Order.CustomerID,
			Amount:     c.Order.Amount,
			Status:     c.Order.Status,
			SourceSeq:  c.Seq,
			SourceTime: c.ChangedAt,
			IngestedAt: time.Now().UTC(),
		}
		if c.Seq > w.lastLoadSeq {
			w.lastLoadSeq = c.Seq
		}
	}
	return len(changes)
}

func (w *MockWarehouse) Snapshot() []FactOrder {
	w.mu.Lock()
	defer w.mu.Unlock()

	rows := make([]FactOrder, 0, len(w.orders))
	for _, row := range w.orders {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].OrderID < rows[j].OrderID
	})
	return rows
}

func (w *MockWarehouse) LastLoadSeq() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastLoadSeq
}
