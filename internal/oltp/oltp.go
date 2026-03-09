package oltp

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Order represents a transactional row in the mock OLTP system.
type Order struct {
	ID         string
	CustomerID string
	Amount     float64
	Status     string
	UpdatedAt  time.Time
}

// Change is an entry in the write-ahead log emitted by the OLTP.
type Change struct {
	Seq       int64
	Operation string
	Order     Order
	ChangedAt time.Time
}

// MockOLTP keeps current rows and a simple append-only change log.
type MockOLTP struct {
	mu          sync.Mutex
	seq         int64
	orders      map[string]Order
	changes     []Change
	subscribers []chan Change
	rng         *rand.Rand
}

// Initialise mock OLTP system; * and & used as pointers to stateful OLTP
func NewMockOLTP() *MockOLTP {
	return &MockOLTP{
		orders: make(map[string]Order),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (db *MockOLTP) Seed(n int) {
	for i := 1; i <= n; i++ {
		order := Order{
			ID:         fmt.Sprintf("ORD-%03d", i),
			CustomerID: fmt.Sprintf("CUST-%02d", db.rng.Intn(9)+1), // format string for CUST- with 2 digits (valued 1-9, padded with 1 zero)
			Amount:     float64(db.rng.Intn(30000)+1000) / 100,
			Status:     "NEW",
		}
		db.UpsertOrder(order)
	}
}

func (db *MockOLTP) Subscribe(buffer int) <-chan Change { // receive-only channel
	db.mu.Lock()
	defer db.mu.Unlock()

	ch := make(chan Change, buffer) // buffered channel so that OLTP can continue emitting changes, should be replaced with Kafka
	db.subscribers = append(db.subscribers, ch)
	return ch
}

func (db *MockOLTP) broadcast(change Change) {
	// no mutex lock so that no one subscriber holds up everyone else
	for _, ch := range db.subscribers {
		select {
		case ch <- change:
		default:
			// skip if buffer is full
		}
	}
}

func (db *MockOLTP) UpsertOrder(order Order) Change {
	db.mu.Lock()
	defer db.mu.Unlock()

	now := time.Now().UTC()
	order.UpdatedAt = now
	db.seq++
	change := Change{
		Seq:       db.seq,
		Operation: "UPSERT",
		Order:     order,
		ChangedAt: now,
	}

	db.changes = append(db.changes, change) // updating the WAL before the OLTP db
	db.orders[order.ID] = order
	db.broadcast(change)

	return change
}

// SimulateUpserts creates n mock orders and upserts them into OLTP.
// It returns the n emitted change events in order.
func (db *MockOLTP) SimulateUpserts(n int) []Change {
	changes := make([]Change, 0, n)
	statuses := []string{"NEW", "PAID", "SHIPPED", "CANCELLED"}

	for i := 1; i <= n; i++ {
		order := Order{
			ID:         fmt.Sprintf("ORD-SIM-%03d", i),
			CustomerID: fmt.Sprintf("CUST-%02d", db.rng.Intn(9)+1),
			Amount:     float64(db.rng.Intn(30000)+1000) / 100,
			Status:     statuses[db.rng.Intn(len(statuses))],
		}
		changes = append(changes, db.UpsertOrder(order))
	}

	return changes
}

// ChangesSince returns WAL entries strictly newer than lastSeq.
func (db *MockOLTP) ChangesSince(lastSeq int64) []Change {
	db.mu.Lock()
	defer db.mu.Unlock()

	out := make([]Change, 0)
	for _, c := range db.changes {
		if c.Seq > lastSeq {
			out = append(out, c)
		}
	}
	return out
}
