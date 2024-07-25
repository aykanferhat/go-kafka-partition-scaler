package config

type RequiredAcks string

const (
	NoResponse   RequiredAcks = "NoResponse"
	WaitForLocal RequiredAcks = "WaitForLocal"
	WaitForAll   RequiredAcks = "WaitForAll"
)

type OffsetInitial string

const (
	OffsetNewest OffsetInitial = "newest"
	OffsetOldest OffsetInitial = "oldest"
)

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionGZIP   Compression = "gzip"
	CompressionSnappy Compression = "snappy"
	CompressionLZ4    Compression = "lz4"
	CompressionZSTD   Compression = "zstd"
)
