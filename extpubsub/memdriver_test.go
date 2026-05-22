package extpubsub

import (
	"context"
	"sync"

	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub/driver"
)

type MemDriver struct {
	mu     sync.Mutex
	msgs   []*driver.Message
	closed bool
	acks   []driver.AckID
	nacks  []driver.AckID
}

func (m *MemDriver) SendBatch(_ context.Context, msgs []*driver.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = append(m.msgs, msgs...)
	return nil
}

func (m *MemDriver) ReceiveBatch(_ context.Context, maxMessages int) ([]*driver.Message, error) {
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

func (m *MemDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MemDriver) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MemDriver) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = nil
	m.closed = false
	m.acks = nil
	m.nacks = nil
}

// Subscription methods
func (m *MemDriver) CanNack() bool { return true }

func (m *MemDriver) SendAcks(_ context.Context, ids []driver.AckID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks = append(m.acks, ids...)
	return nil
}

func (m *MemDriver) SendNacks(_ context.Context, ids []driver.AckID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacks = append(m.nacks, ids...)
	return nil
}

func (m *MemDriver) IsRetryable(_ error) bool             { return false }
func (m *MemDriver) As(_ interface{}) bool                { return false }
func (m *MemDriver) ErrorAs(_ error, _ interface{}) bool  { return false }
func (m *MemDriver) ErrorCode(_ error) gcerrors.ErrorCode { return gcerrors.Unknown }

// Helper methods for tests

func (m *MemDriver) Messages() []*driver.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.msgs
}

func (m *MemDriver) AddMessages(msgs ...*driver.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = append(m.msgs, msgs...)
}

func (m *MemDriver) Acks() []driver.AckID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acks
}

func (m *MemDriver) Nacks() []driver.AckID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nacks
}
