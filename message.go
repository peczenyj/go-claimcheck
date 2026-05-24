package claimcheck

// Message is a serializable representation of a pubsub message.
type Message struct {
	Body     []byte            `json:"body"`
	Metadata map[string]string `json:"metadata,omitempty"`
}
