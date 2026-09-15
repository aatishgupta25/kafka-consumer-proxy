package model

// Record is the proxy's transport-neutral representation of a Kafka record.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}
