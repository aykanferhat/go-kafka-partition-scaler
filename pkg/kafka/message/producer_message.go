package message

type ProducerMessage struct {
	Body     any
	Topic    string
	Key      string
	Headers  []Header
	ByteBody bool
}
