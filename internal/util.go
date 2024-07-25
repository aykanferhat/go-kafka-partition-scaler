package internal

import (
	"crypto/rand"
	"math"
	"math/big"
)

func calculateVirtualPartition(key string, totalPartitionCount int) int {
	if len(key) == 0 {
		nBig, _ := rand.Int(rand.Reader, big.NewInt(int64(totalPartitionCount)))
		return int(nBig.Int64())
	}
	var sum int64
	for i := 0; i < len(key); i++ {
		sum += int64(key[i])
	}
	return int(math.Mod(float64(sum), float64(totalPartitionCount)))
}
