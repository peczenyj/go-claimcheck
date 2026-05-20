package extpubsub

import (
	"context"
	"sync"

	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub/driver"
)

type memDriver struct {
	mu     sync.Mutex
	msgs   []*driver.Message
	closed bool
}

func (m *memDriver) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = append(m.msgs, msgs...)
	return nil
}

func (m *memDriver) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.msgs) == 0 {
		return nil, nil
	}
	n := maxMessages
	if len(m.msgs) < n {
		n = len(m.msgs)
	}
	res := m.msgs[:n]
	m.msgs = m.msgs[n:]
	return res, nil
}

func (m *memDriver) Close() error {
	m.closed = true
	return nil
}

func (m *memDriver) IsEmpty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.msgs) == 0
}

// Subscription methods
func (m *memDriver) CanNack() bool { return false }
func (m *memDriver) SendAcks(ctx context.Context, ids []driver.AckID) error { return nil }
func (m *memDriver) SendNacks(ctx context.Context, ids []driver.AckID) error { return nil }
func (m *memDriver) IsRetryable(err error) bool { return false }
func (m *memDriver) As(i interface{}) bool { return false }
func (m *memDriver) ErrorAs(err error, i interface{}) bool { return false }
func (m *memDriver) ErrorCode(err error) gcerrors.ErrorCode { return gcerrors.Unknown }
