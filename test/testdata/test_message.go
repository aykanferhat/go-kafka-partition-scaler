package testdata

type TestConsumedMessage struct {
	Id               int32  `json:"id"`
	Name             string `json:"name"`
	Reason           string `json:"reason"`
	Version          int    `json:"version"`
	Topic            string
	Partition        int32
	VirtualPartition int
	Offset           int64
	Headers          map[string]string
}
